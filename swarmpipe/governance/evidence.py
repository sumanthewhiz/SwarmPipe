"""Evidence packs: everything an auditor needs for one incident, assembled from records
that are written continuously during normal operation (never reconstructed at audit time).

Contents: triggering signals + data snapshot refs (dataset versions, content hashes), context and
sources used (tool evidence, runbooks, memories, contract version), diagnosis, proposed actions and
alternatives, policy evaluations, approvals, executed actions + run ids, verification, the model /
prompt / tool / agent versions involved, cost, and the audit-chain head hash as an anchor."""
from __future__ import annotations

import html
from pathlib import Path

from swarmpipe.core.util import dumps, iso, loads


class EvidenceService:
    def __init__(self, svc):
        self.svc = svc

    def build(self, incident_id: str) -> dict:
        db = self.svc.db
        inc = db.query_one("SELECT * FROM incidents WHERE id=?", (incident_id,))
        if not inc:
            raise KeyError(incident_id)
        for k in ("diagnosis", "impact", "postmortem"):
            inc[k] = loads(inc[k], None)
        signals = db.query("SELECT * FROM signals WHERE incident_id=? ORDER BY created_at", (incident_id,))
        for s in signals:
            s["details"] = loads(s["details"], {})
        run_ids = sorted({s["run_id"] for s in signals if s.get("run_id")})
        versions = []
        for rid in run_ids:
            versions += db.query("SELECT id, tenant, dataset, version, status, content_hash, batch_rows, row_count, "
                                 "contract_version, created_at FROM dataset_versions WHERE run_id=?", (rid,))
        evidence = db.query("SELECT id, tool, tool_version, args, trust, created_by, created_at, substr(content,1,1500) AS content "
                            "FROM evidence WHERE incident_id=? ORDER BY created_at", (incident_id,))
        blackboard = db.query("SELECT version, author, kind, content, evidence_ids, created_at FROM blackboard "
                              "WHERE incident_id=? ORDER BY version", (incident_id,))
        for b in blackboard:
            b["content"] = loads(b["content"], b["content"])
            b["evidence_ids"] = loads(b["evidence_ids"], [])
        proposals = db.query("SELECT * FROM proposals WHERE incident_id=? ORDER BY rank", (incident_id,))
        for p in proposals:
            for k in ("params", "citations", "policy_details", "result", "verification"):
                p[k] = loads(p[k], None)
        approvals = db.query("SELECT id, kind, subject, risk, status, requested_at, decided_at, decided_by, comment "
                             "FROM approvals WHERE incident_id=? ORDER BY requested_at", (incident_id,))
        llm = db.query("SELECT agent, role, model, provider, prompt_id, prompt_version, prompt_hash, status, cached, "
                       "input_tokens, output_tokens, cost_usd, latency_ms FROM llm_calls WHERE incident_id=? ORDER BY ts",
                       (incident_id,))
        tools = db.query("SELECT agent, tool, tool_version, ok, error_code, evidence_id, ts FROM tool_calls "
                         "WHERE incident_id=? ORDER BY ts", (incident_id,))
        agents_used = sorted({c["agent"] for c in llm} | {t["agent"] for t in tools} | {b["author"] for b in blackboard})
        registry = {r["agent_id"]: {"version": r["version"], "card_hash": r["card_hash"], "status": r["status"]}
                    for r in db.query("SELECT * FROM agent_registry")}
        audit = self.svc.audit.query(limit=500, resource_like=incident_id)
        models = sorted({f"{c['model']} ({c['provider']})" for c in llm})
        prompts = sorted({f"{c['prompt_id']}.{c['prompt_version']}#{(c['prompt_hash'] or '')[:12]}" for c in llm if c["prompt_id"]})
        contract_versions = sorted({(v["dataset"], v["contract_version"]) for v in versions if v.get("contract_version")})
        return {
            "evidence_pack_version": 1,
            "generated_at": iso(),
            "incident": inc,
            "triggering_signals": signals,
            "data_snapshots": versions,
            "contract_versions": [{"dataset": d, "version": v} for d, v in contract_versions],
            "context_and_sources": {"tool_evidence": evidence, "case_file": blackboard},
            "diagnosis": inc.get("diagnosis"),
            "impact": inc.get("impact"),
            "proposed_actions": proposals,
            "approvals": approvals,
            "executed_actions": [p for p in proposals if p["status"] in ("executed", "verified", "verification_failed", "rolled_back")],
            "verification": [{"proposal": p["id"], "action": p["action"], "verification": p["verification"]} for p in proposals if p.get("verification")],
            "versions": {"models": models, "prompts": prompts,
                         "tools": sorted({f"{t['tool']}@{t['tool_version']}" for t in tools}),
                         "agents": {a: registry.get(a.replace("agent:", ""), {}) for a in agents_used}},
            "cost": {"llm_calls": len(llm), "input_tokens": sum(c["input_tokens"] or 0 for c in llm),
                     "output_tokens": sum(c["output_tokens"] or 0 for c in llm),
                     "usd": round(sum(c["cost_usd"] or 0 for c in llm), 6)},
            "llm_calls": llm,
            "tool_calls": tools,
            "audit_records": audit,
            "audit_anchor": self.svc.audit.head(),
        }

    def export(self, incident_id: str, fmt: str = "json") -> Path:
        pack = self.build(incident_id)
        out_dir = self.svc.settings.data_path("exports", "evidence")
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{incident_id}.json"
        path.write_text(dumps(pack, indent=2), encoding="utf-8")
        if fmt in ("html", "both"):
            hpath = out_dir / f"{incident_id}.html"
            hpath.write_text(self._html(pack), encoding="utf-8")
            return hpath
        return path

    @staticmethod
    def _html(pack: dict) -> str:
        inc = pack["incident"]
        rows = "".join(
            f"<tr><td>{html.escape(p['action'])}</td><td>{html.escape(str(p['status']))}</td><td>{html.escape(str(p['policy_effect']))}</td>"
            f"<td>{html.escape(str(p.get('executed_by') or ''))}</td><td>{html.escape(str(p.get('rationale') or ''))[:300]}</td></tr>"
            for p in pack["proposed_actions"])
        diag = pack.get("diagnosis") or {}
        return f"""<!doctype html><html><head><meta charset="utf-8"><title>Evidence pack {inc['id']}</title>
<style>body{{font-family:Segoe UI,Arial;margin:24px;max-width:1100px}} td,th{{border:1px solid #ccc;padding:4px 8px;font-size:13px}} table{{border-collapse:collapse}} pre{{background:#f6f6f6;padding:8px;white-space:pre-wrap}}</style>
</head><body><h1>Evidence pack - {html.escape(inc['title'] or inc['id'])}</h1>
<p><b>Incident</b> {inc['id']} | tenant {inc['tenant']} | dataset {inc['dataset']} | status {inc['status']} | generated {pack['generated_at']}</p>
<h2>Diagnosis</h2><p><b>{html.escape(str(diag.get('root_cause_category')))}</b> (confidence {diag.get('confidence')})<br>{html.escape(str(diag.get('summary')))}</p>
<h2>Actions</h2><table><tr><th>action</th><th>status</th><th>policy</th><th>executed by</th><th>rationale</th></tr>{rows}</table>
<h2>Versions</h2><pre>{html.escape(dumps(pack['versions'], indent=2))}</pre>
<h2>Cost</h2><pre>{html.escape(dumps(pack['cost'], indent=2))}</pre>
<h2>Audit anchor</h2><pre>{html.escape(dumps(pack['audit_anchor'], indent=2))}</pre>
<p>Full machine-readable pack: {inc['id']}.json</p></body></html>"""
