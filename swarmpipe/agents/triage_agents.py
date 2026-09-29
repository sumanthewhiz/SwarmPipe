"""Incident-triage swarm: data-aware triage and governed remediation, built from multi-agent patterns.

Correlator (deterministic clustering: "cluster before you reason")
  -> Supervisor (orchestrator-workers + hierarchical): plans specialists per signal type
       -> fan-out to read-only Investigator specialists (ReAct tool loops, signed messages)
       -> fan-in on the Blackboard; the Supervisor is the single owner of the Diagnosis
          (groundedness gate: every citation must be a real evidence id of this incident)
  -> ImpactAnalyzer (deterministic graph traversal: blast radius, owners, regulated consumers)
  -> Planner (LLM, catalog-constrained) + Critic (evaluator-optimizer)
  -> PolicyEngine (deterministic) -> Approvals (humans) -> Executor (only agent with write tools)
  -> Verifier (ground truth; compensation on failure) -> Learner (postmortem, lessons, eval cases)
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from pydantic import ValidationError

from swarmpipe.agents.base import DEGRADE_ERRORS, Agent
from swarmpipe.agents.messaging import Blackboard
from swarmpipe.core.util import dumps, iso, loads, new_id
from swarmpipe.llm.types import CATEGORIES, Diagnosis, PlanOut, PostmortemOut
from swarmpipe.signals import SEVERITIES
from swarmpipe.tools.actions import ACTIONS, catalog_for_prompt

SPECIALISTS_BY_SIGNAL = {
    "schema_drift": ["schema"], "volume_anomaly": ["volume", "quality"], "quality_failure": ["quality"],
    "referential_break": ["quality", "lineage"], "stale_data": ["quality"], "distribution_shift": ["quality"],
    "reconciliation_break": ["quality"], "pii_undeclared": ["privacy"], "injection_attempt": ["security"],
    "freshness_overdue": ["freshness"], "out_of_band_change": ["integrity"], "pipeline_failure": ["intake"],
    "egress_blocked": ["security"], "rogue_agent": ["security"],
}
SPECIALIST_TOOLS = {
    "intake": ("get_run_failure", "search_knowledge"),
    "schema": ("get_schema_diff", "get_contract", "recall_similar_incidents", "search_knowledge"),
    "volume": ("get_volume_history", "get_check_results", "search_knowledge", "recall_similar_incidents"),
    "quality": ("get_check_results", "get_profile_comparison", "get_sample_rows", "search_knowledge", "recall_similar_incidents"),
    "freshness": ("get_freshness_status", "get_dataset_versions", "search_knowledge"),
    "privacy": ("get_check_results", "get_contract", "search_knowledge"),
    "security": ("get_signal_details", "search_knowledge"),
    "lineage": ("get_lineage", "get_open_incidents"),
    "integrity": ("get_version_integrity", "get_dataset_versions"),
}
FALLBACK_CATEGORY = {
    "schema_drift": "schema_change_upstream", "volume_anomaly": "truncated_extract", "quality_failure": "data_quality_regression",
    "referential_break": "referential_integrity_break", "stale_data": "stale_data_resent", "distribution_shift": "unit_or_scale_change",
    "reconciliation_break": "pipeline_bug", "pii_undeclared": "pii_exposure", "injection_attempt": "malicious_content",
    "freshness_overdue": "late_or_missing_delivery", "out_of_band_change": "out_of_band_modification", "pipeline_failure": "malformed_input",
}


class CorrelatorAgent(Agent):
    id, name, kind = "correlator", "Correlator", "deterministic"
    description = "Clusters related signals into one incident per probable cause (time window + lineage), before any model reasons."
    scopes = frozenset({"incident:write", "data:read"})

    def on_signal(self, signal: dict) -> tuple[str | None, bool]:
        """Return (incident_id, is_new)."""
        svc = self.svc
        min_sev = svc.settings.incidents.min_severity
        if SEVERITIES.index(signal["severity"]) < SEVERITIES.index(min_sev):
            return None, False
        details = signal.get("details") or {}
        if details.get("incident_id") and svc.incidents.get(details["incident_id"]):
            svc.incidents.attach(details["incident_id"], signal)
            return details["incident_id"], False
        tenant, ds = signal["tenant"], signal.get("dataset") or "_platform"
        root = ds
        if signal.get("dataset") and signal["type"] in ("referential_break", "reconciliation_break", "derive_blocked"):
            for up in svc.lineage.upstream(f"dataset:{ds}", tenant):
                if up["node"].startswith("dataset:"):
                    name = up["node"].split(":", 1)[1]
                    active = [i for i in svc.incidents.open_for_dataset(tenant, name)
                              if i["status"] in ("open", "triaging", "diagnosed", "awaiting_approval")]
                    if active:
                        root = name
                        break
        key = f"{tenant}:{root}"
        with self.invoke("correlate", signal=signal["id"]):
            existing = svc.incidents.open_for_key(tenant, key, svc.settings.incidents.correlation_window_min)
            if existing:
                same_problem = svc.db.scalar("SELECT COUNT(*) FROM signals WHERE incident_id=? AND type=?", (existing["id"], signal["type"]), default=0)
                if existing["status"] in ("open", "triaging") or (same_problem and existing["status"] in ("diagnosed", "awaiting_approval", "mitigated")):
                    svc.incidents.attach(existing["id"], signal)
                    svc.metrics.inc("signals_correlated_total", tenant=tenant)
                    return existing["id"], False
            iid = svc.incidents.open(tenant, signal.get("dataset") or (root if root != "_platform" else None), signal["severity"],
                                     signal["summary"], key, signal["created_at"])
            svc.incidents.attach(iid, signal)
            return iid, True


class InvestigatorAgent(Agent):
    role, kind = "investigator", "llm"
    scopes = frozenset({"data:read", "incident:read", "knowledge:read"})

    def __init__(self, svc, specialist: str):
        super().__init__(svc)
        self.specialist = specialist
        self.id = f"investigator_{specialist}"
        self.name = f"{specialist.title()} Investigator"
        self.description = f"Read-only {specialist} specialist: gathers evidence with tools and reports a cited finding."
        self.tools = SPECIALIST_TOOLS[specialist]

    def investigate(self, incident: dict, signals: list[dict], run_id: str | None, triage_run_id: str) -> dict:
        with self.invoke("investigate", incident=incident["id"]) as span:
            ctx = {"specialist": self.specialist, "run_id": run_id,
                   "incident": {"id": incident["id"], "tenant": incident["tenant"], "dataset": incident["dataset"],
                                "signals": [{"type": s["type"], "severity": s["severity"], "summary": s["summary"]} for s in signals]}}
            try:
                out = self.react("investigator", task=f"Investigate this incident from the {self.specialist} angle.", context=ctx,
                                 tenant=incident["tenant"], run_id=triage_run_id, incident_id=incident["id"], dataset=incident["dataset"])
                finding = out["finding"]
                if finding is None:
                    finding = {"category": "unknown", "summary": f"{self.specialist}: stopped ({out['stop_reason']}) without a finding",
                               "confidence": 0.2, "evidence_ids": out["evidence_ids"]}
                finding["evidence_ids"] = [e for e in finding.get("evidence_ids", []) if e in out["evidence_ids"]] or out["evidence_ids"]
                res = {"specialist": self.specialist, "finding": finding, "steps": out["steps"], "tools_used": out["tools_used"],
                       "stop_reason": out["stop_reason"], "degraded": False}
            except DEGRADE_ERRORS as exc:
                sig = signals[0]["type"] if signals else "pipeline_failure"
                res = {"specialist": self.specialist, "degraded": True, "steps": 0, "tools_used": [], "stop_reason": f"degraded: {type(exc).__name__}",
                       "finding": {"category": FALLBACK_CATEGORY.get(sig, "unknown"), "confidence": 0.45, "evidence_ids": [],
                                   "summary": f"(deterministic fallback, model unavailable) signal {sig} usually means {FALLBACK_CATEGORY.get(sig, 'unknown')}"}}
            span.set_attrs({"swarmpipe.category": res["finding"]["category"], "swarmpipe.steps": res["steps"]})
            return res


class TriageSupervisorAgent(Agent):
    id, name, role, kind = "supervisor", "Triage Supervisor", "diagnoser", "llm"
    description = "Plans the investigation, fans out to specialists, merges findings and owns the final grounded diagnosis."
    scopes = frozenset({"data:read", "incident:read", "incident:write"})

    def plan(self, signals: list[dict]) -> list[str]:
        specs: list[str] = []
        for s in signals:
            for sp in SPECIALISTS_BY_SIGNAL.get(s["type"], ["quality"]):
                if sp not in specs:
                    specs.append(sp)
        return specs[:5]

    def investigate(self, incident: dict, signals: list[dict], triage_run_id: str) -> list[dict]:
        svc = self.svc
        bb = Blackboard(svc, incident["id"])
        specs = self.plan(signals)
        run_id = next((s["run_id"] for s in reversed(signals) if s.get("run_id")), None)
        bb.post("agent:supervisor", "plan", {"specialists": specs, "signals": [s["type"] for s in signals], "run_id": run_id})
        with self.invoke("fan_out", specialists=",".join(specs)):
            tasks = []
            for sp in specs:
                inv = svc.agents.investigators[sp]
                msg = svc.bus.send("supervisor", inv.id, "task", {"incident_id": incident["id"], "specialist": sp}, incident["id"])
                tasks.append((inv, msg))

            def work(inv, msg):
                payload = svc.bus.receive(msg, inv.id)
                res = inv.investigate(incident, signals, run_id, triage_run_id)
                return svc.bus.send(inv.id, "supervisor", "finding", {**res, "task": payload}, incident["id"])

            results: list[dict] = []
            with ThreadPoolExecutor(max_workers=min(4, len(tasks) or 1)) as pool:
                futs = [pool.submit(svc.tracer.run_in_context(work, inv, msg)) for inv, msg in tasks]
                for f in futs:
                    reply = f.result()
                    res = svc.bus.receive(reply, "supervisor")
                    bb.post(f"agent:investigator_{res['specialist']}", "finding", res["finding"], res["finding"].get("evidence_ids"))
                    results.append(res)
        return results

    def _valid_evidence(self, incident_id: str) -> set[str]:
        return {r["id"] for r in self.svc.db.query("SELECT id FROM evidence WHERE incident_id=?", (incident_id,))}

    def diagnose(self, incident: dict, results: list[dict], triage_run_id: str, version: str | None = None, shadow: bool = False) -> dict:
        svc = self.svc
        findings = [{"specialist": r["specialist"], **r["finding"]} for r in results]
        valid = self._valid_evidence(incident["id"])
        with self.invoke("diagnose", incident=incident["id"], shadow=shadow) as span:
            try:
                resp = self.ask("diagnoser", task="Merge the specialists' findings into one grounded root-cause diagnosis.",
                                context={"incident": {"id": incident["id"], "dataset": incident["dataset"], "title": incident["title"]},
                                         "findings": findings, "valid_evidence_ids": sorted(valid)},
                                schema=Diagnosis, tenant=incident["tenant"], run_id=triage_run_id, incident_id=incident["id"],
                                version=version, purpose="shadow_diagnosis" if shadow else "diagnosis")
                d = resp.parsed.model_dump()
                d["model"] = resp.model
                d["degraded"] = False
            except DEGRADE_ERRORS as exc:
                votes: dict[str, float] = {}
                for f in findings:
                    if f["category"] != "unknown":
                        votes[f["category"]] = votes.get(f["category"], 0) + f["confidence"]
                cat = max(votes, key=votes.get) if votes else "unknown"
                cites = [e for f in findings if f["category"] == cat for e in f.get("evidence_ids", [])]
                d = {"root_cause_category": cat, "summary": f"(deterministic fallback: {type(exc).__name__}) majority of specialist findings",
                     "confidence": min(0.65, max((f["confidence"] for f in findings if f["category"] == cat), default=0.3)),
                     "citations": cites[:5], "alternatives": [], "abstain": cat == "unknown", "next_checks": [], "model": None, "degraded": True}
            bad = [c for c in d.get("citations", []) if c not in valid]
            if bad:
                d["citations"] = [c for c in d["citations"] if c in valid]
                d["confidence"] = round(max(0.0, d["confidence"] - 0.2), 3)
                d["ungrounded_citations"] = bad
                svc.metrics.inc("ungrounded_citations_total", agent=self.id)
                span.event("groundedness_gate", removed=bad)
            if not d.get("citations") and d["root_cause_category"] != "unknown":
                d["confidence"] = round(min(d["confidence"], 0.55), 3)
            if d["confidence"] < svc.settings.incidents.diagnosis_min_confidence:
                d["abstain"] = True
            d["grounded"] = not bad and bool(d.get("citations"))
            span.set_attrs({"swarmpipe.root_cause": d["root_cause_category"], "swarmpipe.confidence": d["confidence"],
                            "swarmpipe.abstain": d["abstain"]})
            if not shadow:
                Blackboard(svc, incident["id"]).post("agent:supervisor", "diagnosis", d, d.get("citations"))
            return d


class ImpactAnalyzerAgent(Agent):
    id, name, kind = "impact", "Impact Analyzer", "deterministic"
    description = "Walks lineage downstream to consumers, owners, SLAs and regulated outputs (blast radius)."
    scopes = frozenset({"data:read"})

    def run(self, tenant: str, dataset: str | None) -> dict:
        with self.invoke("impact", dataset=dataset):
            if not dataset:
                return {"dataset": None, "downstream_datasets": [], "consumers": [], "owners": [], "regulated": False,
                        "max_criticality": "low", "blast_radius": 0}
            return self.svc.context.impact(tenant, dataset)


class RemediationPlannerAgent(Agent):
    id, name, role, kind = "planner", "Remediation Planner", "planner", "llm"
    description = "Proposes remediation strictly from the allowlisted action catalog; plans are critiqued before policy evaluation."
    scopes = frozenset({"data:read", "incident:read"})

    def _validate(self, proposals: list[dict]) -> tuple[list[dict], list[dict]]:
        ok, bad = [], []
        for p in proposals:
            spec = ACTIONS.get(p.get("action"))
            if spec is None:
                bad.append({**p, "invalid_reason": "not in the action catalog"})
                continue
            try:
                p["params"] = spec.params.model_validate(p.get("params") or {}).model_dump()
                ok.append(p)
            except ValidationError as exc:
                bad.append({**p, "invalid_reason": f"invalid params: {str(exc)[:200]}"})
        return ok, bad

    def plan(self, incident: dict, diagnosis: dict, impact: dict, facts: dict, evidence_snippets: str, triage_run_id: str) -> dict:
        svc = self.svc
        catalog = catalog_for_prompt(svc.settings.policies)
        hints = svc.knowledge.search(f"{diagnosis.get('root_cause_category', '').replace('_', ' ')} remediation steps", incident["tenant"], k=2)
        hint_text = "\n---\n".join(f"[{h['trust']}] {h['title']}: {h['text'][:600]}" for h in hints)
        hints_trusted = bool(hints) and all(h["trust"] == "trusted" for h in hints)
        with self.invoke("plan", incident=incident["id"]) as span:
            feedback: list[str] = []
            rounds = []
            ok: list[dict] = []
            bad: list[dict] = []
            critique = None
            for r in range(2):
                ctx = {"incident": {"id": incident["id"], "dataset": incident["dataset"], "title": incident["title"]},
                       "diagnosis": {k: diagnosis.get(k) for k in ("root_cause_category", "summary", "confidence", "citations")},
                       "impact": {k: impact.get(k) for k in ("blast_radius", "regulated", "max_criticality", "owners")},
                       "catalog": catalog, "facts": facts, "critic_feedback": feedback or None,
                       "runbook_hints": hint_text if hints_trusted else None}
                untrusted = {"evidence_snippets": evidence_snippets} if evidence_snippets else {}
                if hint_text and not hints_trusted:
                    untrusted["runbook_hints"] = hint_text
                try:
                    resp = self.ask("planner", task="Propose remediation actions for this incident.", context=ctx,
                                    untrusted=untrusted or None,
                                    schema=PlanOut, tenant=incident["tenant"], run_id=triage_run_id, incident_id=incident["id"],
                                    purpose=f"plan_round_{r + 1}")
                    props = [p.model_dump() for p in resp.parsed.proposals]
                except DEGRADE_ERRORS as exc:
                    props = [{"action": "notify_owner", "params": {"recipient": "owner",
                                                                   "subject": f"[{incident['dataset']}] incident needs attention",
                                                                   "message": diagnosis.get("summary", "")[:400]},
                              "rationale": f"deterministic fallback ({type(exc).__name__}): inform the owner", "citations": diagnosis.get("citations", [])[:3]}]
                ok, bad = self._validate(props)
                critique = svc.agents.critic.review("remediation_plan", {"proposals": props},
                                                    {"catalog": list(ACTIONS), "failed_checks": facts.get("failed_checks", []),
                                                     "diagnosis": diagnosis.get("root_cause_category")},
                                                    incident["tenant"], run_id=triage_run_id, incident_id=incident["id"])
                rounds.append({"round": r + 1, "proposals": props, "invalid": bad, "critique": critique})
                if critique.get("verdict") != "revise":
                    break
                feedback = critique.get("issues", [])
            if facts.get("injection_suspected") and not any(p["action"] == "notify_owner" and "security" in str(p["params"].get("recipient")) for p in ok):
                ok.append({"action": "notify_owner", "params": {"recipient": "security", "subject": f"[{incident['dataset']}] prompt-injection attempt",
                                                                "message": "Data in this incident contained instructions aimed at the agents. Review the evidence pack."},
                           "rationale": "workflow rule: security is always informed of injection attempts", "citations": [], "expected_outcome": ""})
            span.set_attrs({"swarmpipe.proposals": len(ok), "swarmpipe.invalid": len(bad), "swarmpipe.rounds": len(rounds)})
            return {"proposals": ok, "invalid": bad, "rounds": rounds, "critique": critique}


class ExecutorAgent(Agent):
    id, name, kind = "executor", "Executor", "deterministic"
    description = "The only agent holding write tools; executes approved/allowed catalog actions idempotently and records compensations."
    scopes = frozenset({"action"})
    tools = tuple(f"act_{a}" for a in ACTIONS)

    def execute(self, proposal: dict, tenant: str, incident_id: str, triage_run_id: str | None, on_behalf_of=None) -> dict:
        svc = self.svc
        key = f"exec:{proposal['id']}"
        prior = svc.db.query_one("SELECT result FROM idempotency WHERE key=?", (key,))
        if prior:
            return loads(prior["result"], {})
        ctx = self.tool_ctx(tenant, incident_id=incident_id, run_id=triage_run_id, dataset=proposal.get("dataset"), on_behalf_of=on_behalf_of)
        with self.invoke("execute", action=proposal["action"], proposal=proposal["id"]):
            res = self.call_tool(f"act_{proposal['action']}", proposal["params"], ctx)
            out = {"ok": res.ok, "data": res.data if res.ok else None, "error": res.error, "evidence_id": res.evidence_id,
                   "executed_by": ctx.identity.describe(), "executed_at": iso()}
            status = "executed" if res.ok else "failed"
            svc.db.update("proposals", {"id": proposal["id"]}, {"status": status, "result": dumps(out), "executed_at": out["executed_at"],
                                                              "executed_by": self.identity.principal,
                                                              "on_behalf_of": on_behalf_of.principal if on_behalf_of else None,
                                                              "idempotency_key": key})
            svc.db.execute("INSERT OR IGNORE INTO idempotency(key, scope, result, created_at) VALUES(?,?,?,?)", (key, "action", dumps(out), iso()))
            if res.ok:
                svc.autonomy.record(tenant, proposal["action"], "executed")
            svc.metrics.inc("actions_executed_total", action=proposal["action"], ok=str(res.ok).lower())
            return out


class VerifierAgent(Agent):
    id, name, kind = "verifier", "Verifier", "deterministic"
    description = "Verifies every executed action against ground truth; runs the saga compensation when verification fails."
    scopes = frozenset({"data:read", "action"})

    def verify(self, proposal: dict, tenant: str, incident_id: str, triage_run_id: str | None) -> dict:
        svc = self.svc
        spec = ACTIONS[proposal["action"]]
        result = loads(proposal["result"], {}) if isinstance(proposal.get("result"), str) else (proposal.get("result") or {})
        params = spec.params.model_validate(loads(proposal["params"], {}) if isinstance(proposal["params"], str) else proposal["params"])
        ctx = svc.agents.executor.tool_ctx(tenant, incident_id=incident_id, run_id=triage_run_id, dataset=proposal.get("dataset"))
        with self.invoke("verify", action=proposal["action"]):
            if not result.get("ok"):
                return {"ok": False, "detail": "action failed at execution", "compensated": False}
            v = spec.verify(ctx, params, result.get("data") or {})
            if v.get("pending"):
                return v
            if v["ok"]:
                svc.db.update("proposals", {"id": proposal["id"]}, {"status": "verified", "verification": dumps(v), "verified_at": iso()})
                svc.autonomy.record(tenant, proposal["action"], "verified_ok")
                return v
            comp = None
            if spec.compensate:
                try:
                    comp = spec.compensate(ctx, params, result.get("data") or {})
                    svc.audit.record("agent:verifier", "action.compensate", proposal["id"], "compensated", {"action": proposal["action"], "result": comp})
                except Exception as exc:  # noqa: BLE001
                    comp = {"error": str(exc)}
            status = "rolled_back" if spec.compensate else "verification_failed"
            svc.db.update("proposals", {"id": proposal["id"]}, {"status": status, "verification": dumps({**v, "compensation": comp}),
                                                              "verified_at": iso()})
            svc.autonomy.record(tenant, proposal["action"], "verified_fail")
            if spec.compensate:
                svc.autonomy.record(tenant, proposal["action"], "rolled_back")
            return {**v, "compensated": bool(spec.compensate), "compensation": comp}


class LearnerAgent(Agent):
    id, name, role, kind = "learner", "Learner", "learner", "llm"
    description = "Writes the blameless postmortem and proposes a lesson (episodic memory) and a regression eval case - humans curate both."
    scopes = frozenset({"incident:read", "memory:propose"})

    def learn(self, incident: dict, diagnosis: dict, proposals: list[dict], signals: list[dict], timings: dict, triage_run_id: str) -> dict:
        svc = self.svc
        with self.invoke("learn", incident=incident["id"]):
            ctx = {"incident": {"id": incident["id"], "dataset": incident["dataset"], "title": incident["title"]},
                   "diagnosis": {k: diagnosis.get(k) for k in ("root_cause_category", "summary", "confidence")},
                   "actions": [{"action": p["action"], "status": p["status"], "policy_effect": p.get("policy_effect")} for p in proposals],
                   "signals": [{"type": s["type"], "severity": s["severity"]} for s in signals], "timings": timings}
            try:
                resp = self.ask("learner", task="Write the postmortem, one lesson and one eval case.", context=ctx, schema=PostmortemOut,
                                tenant=incident["tenant"], run_id=triage_run_id, incident_id=incident["id"])
                pm = resp.parsed.model_dump()
            except DEGRADE_ERRORS:
                pm = {"summary": diagnosis.get("summary", ""), "root_cause": diagnosis.get("root_cause_category"), "what_went_well": [],
                      "what_to_improve": [], "lesson": "", "eval_case": {}, "degraded": True}
            mem_id = None
            if pm.get("lesson"):
                mem_id = svc.memory.propose(tenant=incident["tenant"], kind="episodic", title=f"{incident['dataset']}: {pm.get('root_cause')}",
                                            content=pm["lesson"], provenance={"incident_id": incident["id"], "author": "agent:learner"})
            cand_id = None
            if pm.get("eval_case"):
                cand_id = new_id("evc")
                svc.db.insert("eval_candidates", {"id": cand_id, "incident_id": incident["id"], "case_json": dumps(pm["eval_case"]),
                                                  "status": "candidate", "created_at": iso(), "reviewed_by": None})
            return {"postmortem": pm, "memory_id": mem_id, "eval_candidate_id": cand_id}


def valid_category(c: str) -> str:
    return c if c in CATEGORIES else "unknown"
