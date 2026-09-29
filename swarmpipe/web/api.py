"""FastAPI app: the operator dashboard's API, Prometheus metrics, health checks and A2A endpoints.

Identity: the dashboard sends `X-User: <name>` (a local demo stand-in for real auth such as OIDC).
Everything a user does here goes through the same governed services as agents: approvals check
roles/scopes/tenants, the Analyst acts with the intersection of its and the user's permissions."""
from __future__ import annotations

from pathlib import Path

from fastapi import Body, FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from swarmpipe.core.errors import AuthorizationError, SwarmError
from swarmpipe.core.util import iso, loads, new_id

STATIC = Path(__file__).parent / "static"


def create_app(svc, runtime=None) -> FastAPI:
    app = FastAPI(title="SwarmPipe", version="0.1.0", description="Multi-agent data pipeline - operator API")
    app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")

    def user(x_user: str | None):
        try:
            return svc.identity.user(x_user or "oncall")
        except AuthorizationError as exc:
            raise HTTPException(401, str(exc))

    @app.exception_handler(AuthorizationError)
    async def _authz(_: Request, exc: AuthorizationError):
        return JSONResponse({"error": exc.to_dict()}, status_code=403)

    @app.exception_handler(SwarmError)
    async def _swarm(_: Request, exc: SwarmError):
        return JSONResponse({"error": exc.to_dict()}, status_code=400)

    @app.exception_handler(PermissionError)
    async def _perm(_: Request, exc: PermissionError):
        return JSONResponse({"error": {"code": "FORBIDDEN", "message": str(exc)}}, status_code=403)

    @app.exception_handler(ValueError)
    async def _val(_: Request, exc: ValueError):
        return JSONResponse({"error": {"code": "INVALID", "message": str(exc)}}, status_code=400)

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(STATIC / "index.html")

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    @app.get("/readyz")
    def readyz():
        alive = [t.name for t in (runtime.threads if runtime else []) if t.is_alive()]
        return {"ready": bool(alive) or runtime is None, "threads": alive, "schema_version": svc.db.schema_version()}

    # ---- overview -------------------------------------------------------------------------------
    @app.get("/api/overview")
    def overview():
        db = svc.db
        runs = db.query("SELECT workflow, status, COUNT(*) n FROM runs GROUP BY workflow, status")
        cost = db.query_one("SELECT COUNT(*) calls, COALESCE(SUM(cost_usd),0) usd, COALESCE(SUM(input_tokens+output_tokens),0) tokens, "
                            "COALESCE(SUM(cached),0) cached FROM llm_calls")
        return {
            "inbox": str(svc.settings.inbox), "profile": svc.llm.active_profile(), "queue_depth": svc.engine.queue_depth(),
            "runs": runs, "open_incidents": db.scalar("SELECT COUNT(*) FROM incidents WHERE status NOT IN ('resolved','closed')", default=0),
            "incidents_by_status": db.query("SELECT status, COUNT(*) n FROM incidents GROUP BY status"),
            "pending_approvals": db.scalar("SELECT COUNT(*) FROM approvals WHERE status='pending'", default=0),
            "files": db.query("SELECT status, COUNT(*) n FROM files GROUP BY status"),
            "datasets": db.scalar("SELECT COUNT(*) FROM dataset_state WHERE published_version_id IS NOT NULL", default=0),
            "llm": cost, "kill_switches": svc.killswitch.active(), "slos": svc.slos.evaluate(),
            "breakers": svc.llm.breaker.snapshot(), "clock_offset_min": svc.flags.get("clock_offset_min", 0),
            "recent_signals": db.query("SELECT id, type, severity, dataset, summary, created_at, incident_id FROM signals ORDER BY created_at DESC LIMIT 12"),
        }

    # ---- runs & traces ---------------------------------------------------------------------------
    @app.get("/api/runs")
    def runs(status: str | None = None, workflow: str | None = None, limit: int = 100):
        conds, params = [], []
        if status:
            conds.append("status=?")
            params.append(status)
        if workflow:
            conds.append("workflow=?")
            params.append(workflow)
        where = ("WHERE " + " AND ".join(conds)) if conds else ""
        return svc.db.query(f"SELECT id, workflow, status, tenant, dataset, current_step, attempt, recovered_count, waiting_on, parent_run_id, "
                            f"incident_id, trace_id, created_at, finished_at, substr(error,1,300) error FROM runs {where} ORDER BY created_at DESC LIMIT ?",
                            [*params, limit])

    @app.get("/api/runs/{run_id}")
    def run_detail(run_id: str):
        r = svc.db.query_one("SELECT * FROM runs WHERE id=?", (run_id,))
        if not r:
            raise HTTPException(404, "run not found")
        for k in ("input", "context", "output"):
            r[k] = loads(r[k], r[k])
        steps = svc.db.query("SELECT name, status, attempt, kind, started_at, finished_at, duration_ms, error, output FROM steps WHERE run_id=? "
                             "ORDER BY started_at", (run_id,))
        for s in steps:
            s["output"] = loads(s["output"], None)
        return {"run": r, "steps": steps,
                "attempts": svc.db.query("SELECT * FROM step_attempts WHERE run_id=? ORDER BY id", (run_id,)),
                "children": svc.db.query("SELECT id, workflow, status, dataset FROM runs WHERE parent_run_id=?", (run_id,)),
                "checks": svc.db.query("SELECT check_name, check_type, status, severity, observed, expected FROM check_results WHERE run_id=?", (run_id,)),
                "llm_calls": svc.db.query("SELECT agent, model, prompt_id, prompt_version, status, input_tokens, output_tokens, cost_usd, latency_ms, cached, "
                                          "purpose FROM llm_calls WHERE run_id=? ORDER BY ts", (run_id,)),
                "lineage": svc.lineage.events(run_id)}

    @app.post("/api/runs/{run_id}/redrive")
    def redrive(run_id: str, x_user: str | None = Header(None)):
        return {"requeued": svc.engine.redrive(run_id, user(x_user).principal)}

    @app.get("/api/traces/{trace_id}")
    def trace(trace_id: str):
        return {"trace_id": trace_id, "spans": svc.tracer.get_trace(trace_id)}

    # ---- incidents / approvals / actions ------------------------------------------------------------
    @app.get("/api/incidents")
    def incidents(status: str | None = None, limit: int = 100):
        rows = svc.incidents.list(status=status, limit=limit)
        for r in rows:
            r["root_cause"] = (r.get("diagnosis") or {}).get("root_cause_category")
            r["confidence"] = (r.get("diagnosis") or {}).get("confidence")
            r.pop("impact", None)
            r.pop("postmortem", None)
        return rows

    @app.get("/api/incidents/{incident_id}")
    def incident(incident_id: str):
        inc = svc.incidents.get(incident_id)
        if not inc:
            raise HTTPException(404, "incident not found")
        props = svc.db.query("SELECT * FROM proposals WHERE incident_id=? ORDER BY rank", (incident_id,))
        for p in props:
            for k in ("params", "citations", "policy_details", "result", "verification"):
                p[k] = loads(p[k], p[k])
        bb = svc.db.query("SELECT version, author, kind, content, evidence_ids, created_at FROM blackboard WHERE incident_id=? ORDER BY version", (incident_id,))
        for b in bb:
            b["content"] = loads(b["content"], b["content"])
            b["evidence_ids"] = loads(b["evidence_ids"], [])
        return {"incident": inc, "signals": svc.signals.for_incident(incident_id), "blackboard": bb, "proposals": props,
                "approvals": svc.db.query("SELECT * FROM approvals WHERE incident_id=? ORDER BY requested_at", (incident_id,)),
                "evidence": svc.db.query("SELECT id, tool, trust, created_by, created_at, substr(content,1,600) content FROM evidence WHERE incident_id=? "
                                         "ORDER BY created_at", (incident_id,)),
                "llm": svc.db.query_one("SELECT COUNT(*) calls, COALESCE(SUM(cost_usd),0) usd, COALESCE(SUM(input_tokens+output_tokens),0) tokens "
                                        "FROM llm_calls WHERE incident_id=?", (incident_id,)),
                "feedback": svc.db.query("SELECT * FROM feedback WHERE incident_id=?", (incident_id,)),
                "shadow": svc.db.query("SELECT agent, agreed, created_at FROM shadow_comparisons WHERE incident_id=?", (incident_id,))}

    @app.post("/api/incidents/{incident_id}/feedback")
    def feedback(incident_id: str, body: dict = Body(...), x_user: str | None = Header(None)):
        u = user(x_user)
        svc.db.insert("feedback", {"incident_id": incident_id, "user": u.principal, "rating": int(body.get("rating", 3)),
                                   "correct_category": body.get("correct_category"), "comment": body.get("comment", ""), "created_at": iso()})
        return {"ok": True}

    @app.post("/api/incidents/{incident_id}/resolve")
    def resolve(incident_id: str, body: dict = Body(default={}), x_user: str | None = Header(None)):
        u = user(x_user)
        svc.incidents.update(incident_id, status="resolved", resolved_at=svc.clock.now_iso())
        svc.audit.record(u.principal, "incident.resolve", incident_id, "resolved", {"note": body.get("note", "")})
        return {"ok": True}

    @app.get("/api/evidence/{incident_id}")
    def evidence(incident_id: str):
        return svc.evidence.build(incident_id)

    @app.get("/api/approvals")
    def approvals(status: str | None = "pending"):
        return svc.approvals.list(None if status == "all" else status)

    @app.post("/api/approvals/{approval_id}/decide")
    def decide(approval_id: str, body: dict = Body(...), x_user: str | None = Header(None)):
        res = svc.approvals.decide(approval_id, body.get("decision"), user(x_user), comment=body.get("comment", ""),
                                   confirm_text=body.get("confirm_text"))
        return res

    @app.post("/api/proposals/{proposal_id}/execute")
    def execute(proposal_id: str, x_user: str | None = Header(None)):
        from swarmpipe.runtime.workflows import execute_recommendation

        return execute_recommendation(svc, proposal_id, user(x_user))

    @app.post("/api/proposals/{proposal_id}/rollback")
    def rollback(proposal_id: str, x_user: str | None = Header(None)):
        from swarmpipe.runtime.workflows import rollback_proposal

        return rollback_proposal(svc, proposal_id, user(x_user))

    # ---- data -----------------------------------------------------------------------------------------
    @app.get("/api/datasets")
    def datasets(tenant: str | None = None):
        rows = svc.db.query("SELECT * FROM dataset_state" + (" WHERE tenant=?" if tenant else "") + " ORDER BY tenant, dataset", (tenant,) if tenant else ())
        for r in rows:
            v = svc.db.query_one("SELECT version, row_count, batch_rows, status, published_at FROM dataset_versions WHERE id=?", (r["published_version_id"],)) \
                if r["published_version_id"] else None
            r["current"] = v
            r["versions"] = svc.db.scalar("SELECT COUNT(*) FROM dataset_versions WHERE tenant=? AND dataset=?", (r["tenant"], r["dataset"]), default=0)
            c = svc.contracts.active(r["dataset"])
            r["contract_version"] = c.get("version") if c else None
            r["classification"] = c.get("classification") if c else None
            r["freshness"] = svc.context.freshness(r["tenant"], r["dataset"])
        return rows

    @app.get("/api/datasets/{dataset}")
    def dataset(dataset: str, tenant: str = "default", x_user: str | None = Header(None)):
        u = user(x_user)
        versions = svc.db.query("SELECT id, version, status, batch_rows, row_count, contract_version, checksum, created_at, published_at, note, run_id "
                                "FROM dataset_versions WHERE tenant=? AND dataset=? ORDER BY version DESC LIMIT 20", (tenant, dataset))
        sample = None
        if u.can("data:read:published") and u.can_access_tenant(tenant):
            df = svc.wh.read(svc.wh.view_name(tenant, dataset), limit=15)
            sample = {"columns": list(df.columns), "rows": df.astype(object).where(df.notna(), None).values.tolist()} if df is not None else None
        last_run = versions[0]["run_id"] if versions else None
        return {"dataset": dataset, "tenant": tenant, "contract": svc.contracts.active(dataset), "contract_versions": svc.contracts.versions(dataset),
                "versions": versions, "context": svc.context.node(tenant, dataset), "sample": sample,
                "last_checks": svc.db.query("SELECT check_name, check_type, status, severity, observed, expected FROM check_results WHERE run_id=?",
                                            (last_run,)) if last_run else []}

    @app.get("/api/lineage")
    def lineage(tenant: str = "default"):
        return svc.lineage.graph(tenant)

    @app.get("/api/lineage/events")
    def lineage_events(limit: int = 50):
        return svc.lineage.events(limit=limit)

    @app.post("/api/ask")
    def ask(body: dict = Body(...), x_user: str | None = Header(None)):
        return svc.agents.analyst.ask_question(body.get("question", ""), user(x_user), body.get("tenant", "default"))

    @app.get("/api/knowledge")
    def knowledge(q: str | None = None, tenant: str = "default"):
        if q:
            return svc.knowledge.search(q, tenant, k=6, trust_levels=("trusted", "unverified", "untrusted"))
        return svc.knowledge.documents()

    @app.get("/api/memory")
    def memory(status: str | None = None):
        return svc.memory.list(status)

    @app.post("/api/memory/{memory_id}/decide")
    def memory_decide(memory_id: str, body: dict = Body(...), x_user: str | None = Header(None)):
        u = user(x_user)
        if not u.has_role("approver"):
            raise HTTPException(403, "approver role required")
        return svc.memory.decide(memory_id, bool(body.get("approve")), u.principal)

    @app.get("/api/dlq")
    def dlq():
        return svc.db.query("SELECT * FROM dlq ORDER BY created_at DESC")

    # ---- governance ---------------------------------------------------------------------------------
    @app.get("/api/agents")
    def agents():
        cards = svc.agents.cards()
        for c in cards:
            aid = c["x-swarmpipe"]["agent_id"]
            c["x-swarmpipe"]["stats"] = svc.db.query_one("SELECT COUNT(*) calls, COALESCE(SUM(cost_usd),0) usd FROM llm_calls WHERE agent=?", (f"agent:{aid}",))
            c["x-swarmpipe"]["tool_calls"] = svc.db.scalar("SELECT COUNT(*) FROM tool_calls WHERE agent=?", (aid,), default=0)
        return cards

    @app.get("/api/tools")
    def tools():
        return svc.tools.describe()

    @app.get("/api/autonomy")
    def autonomy(tenant: str | None = None):
        return {"stats": svc.autonomy.stats(tenant), "history": svc.db.query("SELECT * FROM autonomy_history ORDER BY id DESC LIMIT 50")}

    @app.post("/api/autonomy")
    def autonomy_set(body: dict = Body(...), x_user: str | None = Header(None)):
        u = user(x_user)
        if not u.has_role("admin"):
            raise HTTPException(403, "admin role required to change autonomy levels")
        return svc.autonomy.set_level(body.get("tenant", "default"), body["action"], body["level"], u.principal, body.get("reason", "set via dashboard"))

    @app.post("/api/policy/simulate")
    def simulate(body: dict = Body(...)):
        from swarmpipe.governance.policy import ActionContext

        return svc.policy.evaluate(ActionContext(tenant=body.get("tenant", "default"), action=body["action"], dataset=body.get("dataset"),
                                                 target=body.get("dataset"), blast_radius=int(body.get("blast_radius", 0)),
                                                 regulated_consumer=bool(body.get("regulated", False)),
                                                 diagnosis_confidence=float(body.get("confidence", 0.9)),
                                                 injection_suspected=bool(body.get("injection", False)))).to_dict()

    @app.get("/api/policies")
    def policies():
        return svc.settings.policies

    @app.get("/api/killswitch")
    def ks():
        return svc.killswitch.active()

    @app.post("/api/killswitch")
    def ks_set(body: dict = Body(...), x_user: str | None = Header(None)):
        u = user(x_user)
        if not (u.has_role("admin") or u.has_role("operator")):
            raise HTTPException(403, "operator role required")
        svc.killswitch.set(body.get("scope", "global"), bool(body.get("enabled")), body.get("reason", ""), by=u.principal)
        return svc.killswitch.active()

    @app.get("/api/audit")
    def audit(limit: int = 200, action: str | None = None):
        return svc.audit.query(limit=limit, action=action)

    @app.get("/api/audit/verify")
    def audit_verify():
        return svc.audit.verify()

    # ---- cost / metrics / evals / llm ----------------------------------------------------------------
    @app.get("/api/cost")
    def cost():
        q = svc.db.query
        return {
            "by_agent": q("SELECT agent, COUNT(*) calls, SUM(input_tokens) tin, SUM(output_tokens) tout, ROUND(SUM(cost_usd),6) usd, "
                          "ROUND(AVG(latency_ms),1) avg_ms, SUM(cached) cached FROM llm_calls GROUP BY agent ORDER BY usd DESC"),
            "by_model": q("SELECT model, status, COUNT(*) calls, ROUND(SUM(cost_usd),6) usd, ROUND(AVG(latency_ms),1) avg_ms FROM llm_calls "
                          "GROUP BY model, status ORDER BY calls DESC"),
            "by_tenant_day": q("SELECT tenant, day, llm_requests, ROUND(cost_usd,6) usd FROM quotas ORDER BY day DESC, tenant"),
            "per_incident": q("SELECT i.id, i.dataset, i.status, COUNT(c.id) calls, ROUND(COALESCE(SUM(c.cost_usd),0),6) usd FROM incidents i "
                              "LEFT JOIN llm_calls c ON c.incident_id=i.id GROUP BY i.id ORDER BY i.created_at DESC LIMIT 30"),
            "cost_per_resolved_incident": svc.db.scalar(
                "SELECT ROUND(AVG(usd),6) FROM (SELECT i.id, COALESCE(SUM(c.cost_usd),0) usd FROM incidents i LEFT JOIN llm_calls c ON c.incident_id=i.id "
                "WHERE i.status IN ('resolved','mitigated') GROUP BY i.id)"),
            "cluster_ratio": svc.db.query_one("SELECT COUNT(*) signals, COUNT(DISTINCT incident_id) incidents FROM signals WHERE incident_id IS NOT NULL"),
        }

    @app.get("/api/metrics")
    def metrics_api():
        return [{"metric": n, **svc.metrics.summary(n)} for n in svc.metrics.names()]

    @app.get("/metrics", response_class=PlainTextResponse)
    def prometheus():
        return svc.metrics.prometheus()

    @app.get("/api/slo")
    def slo():
        return svc.slos.evaluate()

    @app.get("/api/evals")
    def evals():
        rows = svc.db.query("SELECT * FROM eval_runs ORDER BY started_at DESC LIMIT 20")
        for r in rows:
            r["summary"] = loads(r["summary"], {})
            r["config"] = loads(r["config"], {})
        latest = Path(svc.settings.resolve(svc.settings.paths.evals_dir)) / "reports" / "latest.json"
        return {"runs": rows, "latest": loads(latest.read_text(encoding="utf-8"), None) if latest.exists() else None,
                "candidates": svc.db.query("SELECT * FROM eval_candidates ORDER BY created_at DESC LIMIT 30"),
                "certifications": svc.db.query("SELECT * FROM model_certifications"),
                "shadow": svc.db.query("SELECT agent, COUNT(*) n, SUM(agreed) agreed FROM shadow_comparisons GROUP BY agent"),
                "feedback": svc.db.query("SELECT * FROM feedback ORDER BY created_at DESC LIMIT 30")}

    @app.get("/api/llm")
    def llm():
        st = svc.llm.status()
        st["prompts"] = [{"key": t.key, "approved": t.approved, "hash": t.hash[:12], "description": t.description} for t in svc.prompts.list()]
        return st

    @app.post("/api/llm/profile")
    def llm_profile(body: dict = Body(...), x_user: str | None = Header(None)):
        u = user(x_user)
        if body.get("profile") not in svc.settings.llm.profiles:
            raise HTTPException(400, "unknown profile")
        svc.flags.set("llm.active_profile", body["profile"], by=u.principal)
        svc.audit.record(u.principal, "llm.set_profile", body["profile"], "ok")
        return {"active_profile": body["profile"]}

    # ---- scenarios / chaos ------------------------------------------------------------------------------
    @app.get("/api/scenarios")
    def scenarios_list():
        from swarmpipe.scenarios import SCENARIOS

        return [{"name": s.name, "description": s.description, "teaches": s.teaches} for s in SCENARIOS.values()]

    @app.post("/api/scenarios/{name}")
    def scenario_drop(name: str, body: dict = Body(default={})):
        from swarmpipe import scenarios

        paths = scenarios.drop(name, svc.settings.inbox, body.get("tenant"), svc=svc)
        return {"dropped": [p.name for p in paths]}

    @app.get("/api/chaos")
    def chaos():
        from swarmpipe.llm.mock import DEFAULTS

        f = svc.flags.all()
        return {"defaults": DEFAULTS, "flags": f}

    @app.post("/api/chaos")
    def chaos_set(body: dict = Body(...), x_user: str | None = Header(None)):
        u = user(x_user)
        key = body["key"] if "." in body["key"] else f"chaos.{body['key']}"
        svc.flags.set(key, body.get("value"), by=u.principal)
        svc.audit.record(u.principal, "chaos.set", key, "ok", {"value": body.get("value")})
        return svc.flags.all()

    @app.post("/api/chaos/clear")
    def chaos_clear(x_user: str | None = Header(None)):
        u = user(x_user)
        for p in ("chaos.", "guardrails.", "feature."):
            svc.flags.clear(p)
        svc.flags.set("clock_offset_min", 0, by=u.principal)
        return svc.flags.all()

    # ---- A2A (agent-to-agent) --------------------------------------------------------------------------
    @app.get("/.well-known/agent-card.json")
    def agent_card():
        return svc.agents.analyst.card()

    @app.get("/a2a/agents")
    def a2a_agents():
        return svc.agents.cards()

    @app.get("/a2a/agents/{agent_id}")
    def a2a_agent(agent_id: str):
        a = svc.agents.get(agent_id)
        if not a:
            raise HTTPException(404, "unknown agent")
        return a.card()

    @app.post("/a2a/agents/analyst/tasks")
    def a2a_task(body: dict = Body(...), x_user: str | None = Header(None)):
        """A2A-style task: another agent delegates a data question to our Analyst."""
        u = user(x_user or "analyst")
        parts = ((body.get("message") or {}).get("parts") or [])
        text = " ".join(p.get("text", "") for p in parts if isinstance(p, dict)) or body.get("question", "")
        task_id = body.get("id") or new_id("task")
        res = svc.agents.analyst.ask_question(text, u, body.get("tenant", "default"))
        state = "completed" if not res.get("refused") else "rejected"
        return {"id": task_id, "kind": "task", "status": {"state": state, "timestamp": iso()},
                "artifacts": [{"artifactId": new_id("art"), "name": "answer", "parts": [{"kind": "text", "text": res.get("answer") or res.get("reason", "")},
                                                                                        {"kind": "data", "data": {k: res.get(k) for k in ("sql", "columns", "rows", "tables")}}]}],
                "metadata": {"acting_as": res.get("acting_as"), "evidence_id": res.get("evidence_id")}}

    return app
