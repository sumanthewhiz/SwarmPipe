"""`swarmpipe` command-line interface. Every operational lever of the system is reachable here."""
from __future__ import annotations

import json
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.tree import Tree

from swarmpipe.core.util import dumps, loads

console = Console()
app = typer.Typer(no_args_is_help=True, add_completion=False, help="SwarmPipe - a hands-on multi-agent data pipeline.")
_SVC = None


def svc(quiet: bool = True):
    global _SVC
    if _SVC is None:
        from swarmpipe.app import build_services

        _SVC = build_services(console_logs=not quiet, log_level="WARNING" if quiet else "INFO")
    return _SVC


def _table(rows: list[dict], cols: list[str], title: str = "") -> None:
    t = Table(title=title, show_lines=False, header_style="bold cyan")
    for c in cols:
        t.add_column(c, overflow="fold")
    for r in rows:
        t.add_row(*[("" if r.get(c) is None else str(r.get(c)))[:120] for c in cols])
    console.print(t)


def _j(obj) -> None:
    console.print_json(dumps(obj))


# ================================================================== lifecycle
@app.command()
def init():
    """Create data folders, the state DB, import contracts, seed knowledge, register agents, lock prompts."""
    s = svc()
    console.print(f"[green]SwarmPipe initialised[/green] - state {s.settings.state_db}")
    console.print(f"Watched folder (drop files here): [bold]{s.settings.inbox}[/bold]")
    _j(getattr(s, "bootstrap_result", None) or s.bootstrap())


@app.command()
def run(port: int = typer.Option(None, help="dashboard port (default from config)"), no_web: bool = typer.Option(False, help="no dashboard"),
        workers: int = typer.Option(None, help="worker threads"), profile: str = typer.Option(None, help="LLM profile: offline|ollama|azure|openai")):
    """Start the watcher, workers, event dispatcher, scheduler and the web dashboard."""
    from swarmpipe.app import Runtime, build_services

    global _SVC
    _SVC = build_services(console_logs=True, log_level="INFO")
    s = _SVC
    if profile:
        s.flags.set("llm.active_profile", profile, by="cli")
    rt = Runtime(s, workers=workers).start()
    console.print(f"[bold green]SwarmPipe running[/bold green]  inbox: {s.settings.inbox}  LLM profile: {s.llm.active_profile()}")
    if no_web:
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            rt.stop()
            s.close()
        return
    import uvicorn

    from swarmpipe.web.api import create_app

    host = s.settings.web.get("host", "127.0.0.1")
    port = port or int(s.settings.web.get("port", 8765))
    console.print(f"Dashboard: [link]http://{host}:{port}[/link]   API docs: http://{host}:{port}/docs")
    try:
        uvicorn.run(create_app(s, rt), host=host, port=port, log_level="warning")
    finally:
        rt.stop()
        s.close()


def _admit_all(s) -> int:
    """Poll until every stable file in the inbox has been admitted (stability needs N consecutive polls)."""
    total = 0
    for _ in range(s.settings.watcher.stability_polls + 6):
        total += s.watcher.poll_once()
        pending = [p for p in list(s.settings.inbox.glob("*")) + list(s.settings.inbox.glob("*/*"))
                   if p.is_file() and not s.watcher._ignored(p)]
        if not pending:
            break
        time.sleep(0.15)
    return total


@app.command()
def tick(timeout: float = 180, scheduler: bool = typer.Option(True, help="also run monitors (freshness, OOB, ...)")):
    """Process everything synchronously (no server): watcher poll -> runs -> events -> monitors, until quiet."""
    s = svc()
    total = _admit_all(s)
    t0 = time.time()
    s.engine.run_until_quiescent(timeout_s=timeout, scheduler=s.scheduler if scheduler else None)
    console.print(f"admitted {total} file(s); processed in {time.time() - t0:.1f}s")
    status()


@app.command()
def status():
    """Overview: runs, incidents, approvals, datasets, cost, kill switches."""
    s = svc()
    runs = s.db.query("SELECT workflow, status, COUNT(*) n FROM runs GROUP BY workflow, status ORDER BY workflow")
    _table(runs, ["workflow", "status", "n"], "Runs")
    inc = s.db.query("SELECT id, status, severity, dataset, substr(title,1,70) title FROM incidents ORDER BY created_at DESC LIMIT 10")
    _table(inc, ["id", "status", "severity", "dataset", "title"], "Recent incidents")
    ap = s.approvals.list("pending")
    _table(ap, ["id", "kind", "subject", "risk", "requires_confirmation"], f"Pending approvals ({len(ap)})")
    ds = s.db.query("SELECT tenant, dataset, published_version_id, hold, last_success_at FROM dataset_state ORDER BY tenant, dataset")
    _table(ds, ["tenant", "dataset", "published_version_id", "hold", "last_success_at"], "Datasets")
    c = s.db.query_one("SELECT COUNT(*) calls, ROUND(COALESCE(SUM(cost_usd),0),5) usd, COALESCE(SUM(input_tokens+output_tokens),0) tokens FROM llm_calls")
    console.print(f"LLM: {c['calls']} calls, ${c['usd']}, {c['tokens']} tokens | profile {s.llm.active_profile()} | "
                  f"kill switches: {[k['scope'] for k in s.killswitch.active()] or 'none'} | inbox: {s.settings.inbox}")


@app.command()
def reset(yes: bool = typer.Option(False, "--yes", help="confirm")):
    """Move the data folder to data/../.trash-<timestamp> (recoverable) and start fresh."""
    from swarmpipe.config import load_settings

    st = load_settings()
    if not yes:
        console.print("[yellow]This moves the whole data folder to a .trash folder. Re-run with --yes.[/yellow]")
        raise typer.Exit(1)
    d = st.data_dir
    if d.exists():
        dest = d.parent / f".trash-data-{datetime.now():%Y%m%d-%H%M%S}"
        shutil.move(str(d), str(dest))
        console.print(f"moved {d} -> {dest}")
    console.print("run `swarmpipe init` to start fresh")


# ================================================================== scenarios
scen = typer.Typer(no_args_is_help=True, help="Drop synthetic files / chaos into the pipeline.")
app.add_typer(scen, name="scenarios")


@scen.command("list")
def scen_list():
    from swarmpipe.scenarios import SCENARIOS

    _table([{"name": s.name, "description": s.description, "teaches": s.teaches} for s in SCENARIOS.values()], ["name", "description", "teaches"])


@scen.command("drop")
def scen_drop(name: str, tenant: str = typer.Option(None, help="tenant sub-folder"), process: bool = typer.Option(False, help="process now (no server needed)")):
    from swarmpipe import scenarios

    s = svc()
    if name == "baseline" and process:
        for part in ("reference", "history"):
            paths = scenarios.drop(part, s.settings.inbox, tenant, svc=s)
            console.print(f"dropped {[p.name for p in paths]}")
            tick(timeout=180, scheduler=False)
        return
    paths = scenarios.drop(name, s.settings.inbox, tenant, svc=s)
    console.print(f"dropped {[p.name for p in paths] or '(chaos/runtime change only)'} into {s.settings.inbox}")
    if process:
        tick(timeout=180, scheduler=True)


# ================================================================== runs / traces
runs_app = typer.Typer(no_args_is_help=True, help="Durable workflow runs.")
app.add_typer(runs_app, name="runs")


@runs_app.command("list")
def runs_list(status: str = None, workflow: str = None, limit: int = 30):
    s = svc()
    conds, params = [], []
    if status:
        conds.append("status=?")
        params.append(status)
    if workflow:
        conds.append("workflow=?")
        params.append(workflow)
    where = ("WHERE " + " AND ".join(conds)) if conds else ""
    rows = s.db.query(f"SELECT id, workflow, status, tenant, dataset, current_step, attempt, recovered_count, waiting_on, created_at FROM runs {where} "
                      f"ORDER BY created_at DESC LIMIT ?", [*params, limit])
    _table(rows, ["id", "workflow", "status", "tenant", "dataset", "current_step", "attempt", "recovered_count", "waiting_on", "created_at"])


@runs_app.command("show")
def runs_show(run_id: str):
    s = svc()
    r = s.db.query_one("SELECT * FROM runs WHERE id=?", (run_id,))
    if not r:
        raise typer.BadParameter("run not found")
    _j({k: (loads(v, v) if k in ("input", "context", "output") else v) for k, v in r.items()})
    _table(s.db.query("SELECT name, status, attempt, kind, duration_ms, substr(error,1,80) error FROM steps WHERE run_id=? ORDER BY started_at", (run_id,)),
           ["name", "status", "attempt", "kind", "duration_ms", "error"], "Steps (checkpoints)")
    _table(s.db.query("SELECT step, attempt, status, duration_ms, substr(error,1,80) error FROM step_attempts WHERE run_id=? ORDER BY id", (run_id,)),
           ["step", "attempt", "status", "duration_ms", "error"], "Attempts")


@runs_app.command("redrive")
def runs_redrive(run_id: str):
    console.print("requeued" if svc().engine.redrive(run_id, "user:cli") else "[red]not failed/dead-lettered[/red]")


@runs_app.command("cancel")
def runs_cancel(run_id: str):
    console.print("cancelled" if svc().engine.cancel(run_id, "user:cli") else "[red]cannot cancel[/red]")


@app.command()
def trace(ident: str):
    """Show a trace waterfall for a run id, incident id or trace id."""
    s = svc()
    tid = ident
    if ident.startswith("run_"):
        tid = (s.db.query_one("SELECT trace_id FROM runs WHERE id=?", (ident,)) or {}).get("trace_id")
    elif ident.startswith("inc_"):
        tid = (s.db.query_one("SELECT trace_id FROM incidents WHERE id=?", (ident,)) or {}).get("trace_id")
    roots = s.tracer.trace_tree(tid) if tid else []
    if not roots:
        console.print("[yellow]no spans (sampled out, not yet flushed, or unknown id)[/yellow]")
        return
    t0 = min(r["start_ts"] for r in s.tracer.get_trace(tid))

    def add(node, span):
        a = span["attributes"]
        extra = " ".join(f"{k.split('.')[-1]}={a[k]}" for k in ("gen_ai.request.model", "gen_ai.usage.input_tokens", "gen_ai.usage.output_tokens",
                                                                 "swarmpipe.cost_usd", "gen_ai.tool.name", "swarmpipe.evidence_id") if k in a)
        color = "red" if span["status"] == "error" else "white"
        label = f"[{color}]{span['name']}[/{color}] +{(span['start_ts'] - t0) * 1000:.0f}ms {span['duration_ms'] or 0:.0f}ms {extra}"
        child = node.add(label)
        for c in sorted(span["children"], key=lambda x: x["start_ts"]):
            add(child, c)

    tree = Tree(f"trace {tid}")
    for r in sorted(roots, key=lambda x: x["start_ts"]):
        add(tree, r)
    console.print(tree)


# ================================================================== incidents / approvals / actions
inc_app = typer.Typer(no_args_is_help=True, help="Incidents raised by the triage swarm.")
app.add_typer(inc_app, name="incidents")


@inc_app.command("list")
def inc_list(status: str = None, limit: int = 30):
    s = svc()
    rows = s.incidents.list(status=status, limit=limit)
    for r in rows:
        r["root_cause"] = (r.get("diagnosis") or {}).get("root_cause_category")
        r["title"] = r["title"][:60]
    _table(rows, ["id", "status", "severity", "tenant", "dataset", "signal_count", "root_cause", "cost_usd", "title"])


@inc_app.command("show")
def inc_show(incident_id: str):
    s = svc()
    inc = s.incidents.get(incident_id)
    if not inc:
        raise typer.BadParameter("not found")
    _j({k: inc[k] for k in ("id", "status", "severity", "tenant", "dataset", "title", "diagnosis", "created_at", "resolved_at", "cost_usd")})
    _table(s.signals.for_incident(incident_id), ["id", "type", "severity", "summary"], "Signals")
    _table(s.db.query("SELECT version, author, kind, substr(content,1,110) content FROM blackboard WHERE incident_id=? ORDER BY version", (incident_id,)),
           ["version", "author", "kind", "content"], "Blackboard (shared case file)")
    _table(s.db.query("SELECT id, rank, action, status, policy_effect, autonomy_level, executed_by, on_behalf_of FROM proposals WHERE incident_id=? ORDER BY rank",
                      (incident_id,)), ["id", "rank", "action", "status", "policy_effect", "autonomy_level", "executed_by", "on_behalf_of"], "Proposals")


@inc_app.command("resolve")
def inc_resolve(incident_id: str, note: str = "resolved by operator", as_user: str = typer.Option("oncall", "--as")):
    s = svc()
    s.incidents.update(incident_id, status="resolved", resolved_at=s.clock.now_iso())
    s.audit.record(f"user:{as_user}", "incident.resolve", incident_id, "resolved", {"note": note})
    console.print("resolved")


@inc_app.command("feedback")
def inc_feedback(incident_id: str, rating: int = typer.Option(..., help="1-5"), category: str = typer.Option(None, help="correct root cause if wrong"),
                 comment: str = "", as_user: str = typer.Option("oncall", "--as")):
    """Human feedback on a diagnosis (online evaluation + future eval cases)."""
    s = svc()
    from swarmpipe.core.util import iso

    s.db.insert("feedback", {"incident_id": incident_id, "user": f"user:{as_user}", "rating": rating, "correct_category": category,
                             "comment": comment, "created_at": iso()})
    console.print("feedback recorded")


ap_app = typer.Typer(no_args_is_help=True, help="Human-in-the-loop approvals (server-side elicitation).")
app.add_typer(ap_app, name="approvals")


@ap_app.command("list")
def ap_list(status: str = "pending"):
    rows = svc().approvals.list(status if status != "all" else None)
    _table(rows, ["id", "kind", "status", "tenant", "subject", "risk", "requires_confirmation", "summary"])


def _decide(approval_id: str, decision: str, as_user: str, comment: str, confirm: str | None):
    s = svc()
    user = s.identity.user(as_user)
    res = s.approvals.decide(approval_id, decision, user, comment=comment, confirm_text=confirm)
    console.print(f"[green]{decision}[/green] {res['subject']} by {res['decided_by']}")
    s.dispatcher.poll_once()
    s.engine.run_until_quiescent(timeout_s=60)


@ap_app.command("approve")
def ap_approve(approval_id: str, as_user: str = typer.Option("oncall", "--as"), comment: str = "approved via CLI",
               confirm: str = typer.Option(None, help="typed confirmation for high-risk actions")):
    _decide(approval_id, "approved", as_user, comment, confirm)


@ap_app.command("reject")
def ap_reject(approval_id: str, as_user: str = typer.Option("oncall", "--as"), comment: str = "rejected via CLI"):
    _decide(approval_id, "rejected", as_user, comment, None)


act_app = typer.Typer(no_args_is_help=True, help="Human execution of recommendations and one-click rollback.")
app.add_typer(act_app, name="actions")


@act_app.command("execute")
def act_execute(proposal_id: str, as_user: str = typer.Option("oncall", "--as")):
    """Execute an L1 recommendation yourself (attributed to you; counts as agreement evidence)."""
    from swarmpipe.runtime.workflows import execute_recommendation

    s = svc()
    _j(execute_recommendation(s, proposal_id, s.identity.user(as_user)))


@act_app.command("rollback")
def act_rollback(proposal_id: str, as_user: str = typer.Option("oncall", "--as")):
    """Compensate an executed action (the L3 'act and notify' one-click rollback)."""
    from swarmpipe.runtime.workflows import rollback_proposal

    s = svc()
    _j(rollback_proposal(s, proposal_id, s.identity.user(as_user)))


@app.command()
def ask(question: str, as_user: str = typer.Option("analyst", "--as"), tenant: str = "default"):
    """Ask the Analyst agent a natural-language question over published data."""
    s = svc()
    res = s.agents.analyst.ask_question(question, s.identity.user(as_user), tenant)
    if res.get("refused"):
        console.print(f"[yellow]refused:[/yellow] {res.get('reason')}")
        return
    console.print(f"[bold]{res['answer']}[/bold]\nSQL: {res['sql']}\nacting as: {res['acting_as']} | tables: {res['tables']} | "
                  f"PII detokenized: {res['pii_detokenized']} | evidence: {res['evidence_id']}")
    _table([dict(zip(res["columns"], r)) for r in res["rows"][:20]], res["columns"])


# ================================================================== governance
@app.command()
def killswitch(state: str = typer.Argument(..., help="on|off"), scope: str = "global", reason: str = "operator action",
               as_user: str = typer.Option("admin", "--as")):
    """Engage/release a kill switch. Scopes: global, agent:<id>, tenant:<t>, action:<class>, llm."""
    s = svc()
    s.killswitch.set(scope, state == "on", reason, by=f"user:{as_user}")
    console.print(f"kill switch {scope} -> {state}")


auto_app = typer.Typer(no_args_is_help=True, help="Autonomy ladder per action class.")
app.add_typer(auto_app, name="autonomy")


@auto_app.command("list")
def auto_list(tenant: str = None):
    _table(svc().autonomy.stats(tenant), ["tenant", "action", "level", "max_level", "risk", "proposals", "approved", "rejected", "executed",
                                          "verified_ok", "verified_fail", "rolled_back", "human_agreed", "human_disagreed"])


@auto_app.command("set")
def auto_set(action: str, level: str, tenant: str = "default", reason: str = "operator decision", as_user: str = typer.Option("admin", "--as")):
    _j(svc().autonomy.set_level(tenant, action, level, f"user:{as_user}", reason))


@auto_app.command("review")
def auto_review():
    _j(svc().autonomy.review_all())


@app.command()
def policy(action: str, dataset: str = "sales_daily", tenant: str = "default", confidence: float = 0.9, blast: int = 1,
           injection: bool = False, regulated: bool = False):
    """Simulate the policy engine for a hypothetical action (what-if)."""
    from swarmpipe.governance.policy import ActionContext

    s = svc()
    _j(s.policy.evaluate(ActionContext(tenant=tenant, action=action, dataset=dataset, target=dataset, blast_radius=blast,
                                       diagnosis_confidence=confidence, injection_suspected=injection, regulated_consumer=regulated)).to_dict())


audit_app = typer.Typer(no_args_is_help=True, help="Tamper-evident audit log.")
app.add_typer(audit_app, name="audit")


@audit_app.command("verify")
def audit_verify():
    res = svc().audit.verify()
    console.print(("[green]OK[/green] " if res["ok"] else "[red]TAMPERED[/red] ") + dumps(res))


@audit_app.command("tail")
def audit_tail(n: int = 20):
    _table(svc().audit.query(limit=n), ["seq", "ts", "actor", "on_behalf_of", "action", "resource", "decision"])


@audit_app.command("export")
def audit_export(path: str = "data/exports/audit.jsonl"):
    s = svc()
    p = s.settings.resolve(path)
    console.print(f"exported {s.audit.export_jsonl(p)} records to {p}")


@audit_app.command("tamper")
def audit_tamper(seq: int = 3):
    """LAB ONLY: silently edit one audit record so you can watch `audit verify` catch it."""
    s = svc()
    s.db.execute("UPDATE audit SET decision='approved-by-nobody' WHERE seq=?", (seq,))
    console.print(f"[red]edited audit record {seq}[/red] - now run `swarmpipe audit verify`")


@app.command()
def evidence(incident_id: str, html: bool = True):
    """Export the evidence pack of an incident (JSON + HTML)."""
    s = svc()
    p = s.evidence.export(incident_id, "both" if html else "json")
    console.print(f"evidence pack: {p}")


mem_app = typer.Typer(no_args_is_help=True, help="Episodic memory (human-curated lessons).")
app.add_typer(mem_app, name="memory")


@mem_app.command("list")
def mem_list(status: str = None):
    _table(svc().memory.list(status), ["id", "kind", "status", "trust", "title", "content"])


@mem_app.command("promote")
def mem_promote(memory_id: str, as_user: str = typer.Option("oncall", "--as")):
    _j(svc().memory.decide(memory_id, True, f"user:{as_user}"))


@mem_app.command("reject")
def mem_reject(memory_id: str, as_user: str = typer.Option("oncall", "--as")):
    _j(svc().memory.decide(memory_id, False, f"user:{as_user}"))


kb_app = typer.Typer(no_args_is_help=True, help="Knowledge base (RAG).")
app.add_typer(kb_app, name="knowledge")


@kb_app.command("list")
def kb_list():
    _table(svc().knowledge.documents(), ["id", "tenant", "title", "doc_type", "trust", "source"])


@kb_app.command("search")
def kb_search(query: str, tenant: str = "default", k: int = 4):
    _table(svc().knowledge.search(query, tenant, k=k), ["score", "trust", "title", "source", "text"])


dlq_app = typer.Typer(no_args_is_help=True, help="Dead-letter queue.")
app.add_typer(dlq_app, name="dlq")


@dlq_app.command("list")
def dlq_list():
    _table(svc().db.query("SELECT * FROM dlq ORDER BY created_at DESC"), ["id", "tenant", "reason", "error", "path", "run_id", "redriven_at"])


@dlq_app.command("redrive")
def dlq_redrive(dlq_id: str):
    """Move a dead-lettered file back into the inbox (after fixing the cause)."""
    s = svc()
    d = s.db.query_one("SELECT * FROM dlq WHERE id=?", (dlq_id,))
    if not d or not d["path"] or not Path(d["path"]).exists():
        raise typer.BadParameter("DLQ entry or file not found")
    src = Path(d["path"])
    name = src.name.split("__", 1)[-1] if src.name.startswith("file_") else src.name
    tenant_dir = s.settings.inbox if d["tenant"] == s.settings.default_tenant else s.settings.inbox / d["tenant"]
    tenant_dir.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(tenant_dir / name))
    from swarmpipe.core.util import iso

    s.db.update("dlq", {"id": dlq_id}, {"redriven_at": iso()})
    s.audit.record("user:cli", "dlq.redrive", dlq_id, "requeued", {"file": name})
    console.print(f"moved back to {tenant_dir / name}")


# ================================================================== chaos / llm / prompts
chaos_app = typer.Typer(no_args_is_help=True, help="Chaos engineering: inject failures at runtime (affects a running server too).")
app.add_typer(chaos_app, name="chaos")


@chaos_app.command("show")
def chaos_show():
    from swarmpipe.llm.mock import DEFAULTS

    s = svc()
    flags = s.flags.all()
    rows = [{"key": f"chaos.{k}", "value": flags.get(f"chaos.{k}", v), "default": v} for k, v in DEFAULTS.items()]
    rows += [{"key": k, "value": v, "default": ""} for k, v in flags.items() if not k.startswith("chaos.") or k[6:] not in DEFAULTS]
    _table(rows, ["key", "value", "default"])


@chaos_app.command("set")
def chaos_set(key: str, value: str):
    """e.g. `chaos set llm_timeout_rate 0.3`, `chaos set guardrails.spotlighting false`, `chaos set feature.critic_review false`."""
    import yaml

    s = svc()
    k = key if "." in key else f"chaos.{key}"
    s.flags.set(k, yaml.safe_load(value), by="user:cli")
    console.print(f"{k} = {value}")


@chaos_app.command("clear")
def chaos_clear():
    s = svc()
    n = s.flags.clear("chaos.") + s.flags.clear("guardrails.") + s.flags.clear("feature.")
    s.flags.set("clock_offset_min", 0, by="user:cli")
    console.print(f"cleared {n} flags and reset the clock offset")


@chaos_app.command("crash-after")
def chaos_crash(step: str = "transform"):
    """The worker process exits right after checkpointing STEP of the next run (restart `swarmpipe run` to see recovery)."""
    svc().flags.set("chaos.crash_after_step", step, by="user:cli")
    console.print(f"next run will crash after step '{step}'")


@chaos_app.command("advance-clock")
def chaos_clock(minutes: float):
    """Fast-forward the business clock (freshness SLAs)."""
    s = svc()
    cur = float(s.flags.get("clock_offset_min", 0) or 0)
    s.flags.set("clock_offset_min", cur + minutes, by="user:cli")
    console.print(f"business clock offset: {cur + minutes} min")


@chaos_app.command("tamper-warehouse")
def chaos_tamper(dataset: str = "sales_daily", tenant: str = "default"):
    """Modify a published table directly (the out-of-band detector should catch it)."""
    from swarmpipe import scenarios

    s = svc()
    scenarios.drop("oob_tamper", s.settings.inbox, tenant, svc=s)
    console.print(f"tampered with {dataset}; the out-of-band detector runs every 30s (or `swarmpipe tick`)")


llm_app = typer.Typer(no_args_is_help=True, help="Model gateway: routes, breakers, providers.")
app.add_typer(llm_app, name="llm")


@llm_app.command("status")
def llm_status():
    _j(svc().llm.status())


@llm_app.command("use")
def llm_use(profile: str):
    """Switch the active LLM profile at runtime (offline | ollama | azure | openai)."""
    s = svc()
    if profile not in s.settings.llm.profiles:
        raise typer.BadParameter(f"unknown profile; choose from {list(s.settings.llm.profiles)}")
    s.flags.set("llm.active_profile", profile, by="user:cli")
    console.print(f"active LLM profile -> {profile}")


@llm_app.command("test")
def llm_test(role: str = "router", profile: str = typer.Option(None)):
    """Send one real request through the gateway for ROLE and show which model answered."""
    from swarmpipe.llm.prompts import DataBlock, build_messages
    from swarmpipe.llm.types import LLMRequest, RouterOut

    s = svc()
    if profile:
        s.flags.set("llm.active_profile", profile, by="user:cli")
    tpl = s.prompts.get("router")
    msgs = build_messages(tpl, "Classify this arriving file.", [DataBlock("context", {"file_name": "sales.csv", "extension": ".csv",
                          "sniff": {"kind_hint": "tabular", "delimiter": ",", "reason": "consistent comma delimiter"}}, "trusted"),
                          DataBlock("first_lines", "order_id,amount\nORD-1,10\nORD-2,20", "untrusted")], s.settings, RouterOut)
    t0 = time.time()
    r = s.llm.chat(LLMRequest(role=role, prompt_id=tpl.id, prompt_version=tpl.version, prompt_hash=tpl.hash, messages=msgs, schema=RouterOut,
                              agent="agent:cli", tenant="default", purpose="llm_test", cacheable=False))
    console.print(f"served by [bold]{r.model}[/bold] ({r.provider}) in {time.time() - t0:.2f}s, tokens {r.input_tokens}/{r.output_tokens}, "
                  f"repairs {r.repairs}, chain {r.fallback_chain}")
    _j(r.parsed.model_dump())


pr_app = typer.Typer(no_args_is_help=True, help="Versioned, hash-locked prompts.")
app.add_typer(pr_app, name="prompts")


@pr_app.command("list")
def pr_list():
    _table([{"key": t.key, "role": t.role, "approved": t.approved, "hash": t.hash[:12], "description": t.description} for t in svc().prompts.list()],
           ["key", "role", "approved", "hash", "description"])


# ================================================================== observability
@app.command()
def metrics(prom: bool = False):
    """Metric summaries (or Prometheus exposition with --prom)."""
    s = svc()
    if prom:
        sys.stdout.write(s.metrics.prometheus())
        return
    rows = []
    for n in s.metrics.names():
        sm = s.metrics.summary(n)
        rows.append({"metric": n, **{k: sm.get(k) for k in ("count", "sum", "p50", "p95", "max")}})
    _table(rows, ["metric", "count", "sum", "p50", "p95", "max"])


@app.command()
def slo():
    """SLOs of the pipeline itself: SLI, error-budget burn."""
    _table(svc().slos.evaluate(), ["id", "description", "objective", "total", "bad", "sli", "burn_rate", "error_budget_remaining", "status"])


@app.command()
def maintenance(task: str = typer.Argument("retention", help="retention | backup")):
    s = svc()
    if task == "retention":
        _j(s.scheduler.retention())
    elif task == "backup":
        dest = s.settings.data_path("exports", f"backup-{datetime.now():%Y%m%d-%H%M%S}")
        dest.mkdir(parents=True, exist_ok=True)
        import sqlite3
        from contextlib import closing

        for src in (s.settings.state_db, s.settings.warehouse_db):
            # `with sqlite3.connect(...)` only commits/rolls back - it does NOT close; closing() does
            with closing(sqlite3.connect(src)) as a, closing(sqlite3.connect(dest / Path(src).name)) as b:
                a.backup(b)
        console.print(f"online backup written to {dest}")


# ================================================================== evals / mcp
ev_app = typer.Typer(no_args_is_help=True, help="Offline evaluation, CI gate, model certification, judge calibration.")
app.add_typer(ev_app, name="evals")


@ev_app.command("run")
def ev_run(suite: str = typer.Option("all", help="all | triage | redteam | router | analyst | judge"), k: int = typer.Option(1, help="trials per case"),
           noise: float = typer.Option(0.0, help="simulated model non-determinism (wrong-answer rate)"),
           profile: str = typer.Option(None, help="LLM profile to evaluate"), cases: str = typer.Option(None, help="comma-separated case ids")):
    from swarmpipe.evals.harness import run_suites

    report = run_suites(suite=suite, k=k, noise=noise, profile=profile, only=cases.split(",") if cases else None, console=console)
    console.print(f"report: {report['report_path']}")


@ev_app.command("gate")
def ev_gate(update_lock: bool = typer.Option(False, help="if the gate passes, approve current prompt hashes"), k: int = 2):
    """CI gate: run the suites and fail (exit 1) if any threshold is missed."""
    from swarmpipe.evals.harness import gate

    ok = gate(k=k, update_lock=update_lock, console=console)
    raise typer.Exit(0 if ok else 1)


@ev_app.command("certify")
def ev_certify(model: str = typer.Option(..., help="model id from config, e.g. llama3.2"), roles: str = "router,diagnoser,planner,analyst"):
    """Run the eval suites with MODEL for each role and record whether it is certified for that role."""
    from swarmpipe.evals.harness import certify

    _j(certify(model, roles.split(","), console=console))


@ev_app.command("calibrate-judge")
def ev_calibrate(version: str = typer.Option(None, help="judge prompt version (v1 or v2)")):
    """Measure LLM-as-judge agreement with human labels (kappa) plus position and verbosity bias."""
    from swarmpipe.evals.judge import calibrate

    _j(calibrate(version=version, console=console))


@ev_app.command("harvest")
def ev_harvest():
    """Turn reviewed production incidents into candidate regression cases (feedback -> eval flywheel)."""
    from swarmpipe.evals.harness import harvest

    _j(harvest(svc()))


@app.command()
def mcp(as_user: str = typer.Option(None, "--as", help="identity the MCP server acts as (default from config)")):
    """Run the MCP server over stdio (for GitHub Copilot CLI, Claude Desktop, VS Code, ...)."""
    from swarmpipe.mcp_server import serve

    serve(as_user=as_user)


if __name__ == "__main__":
    app()
