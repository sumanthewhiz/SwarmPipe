"""MCP server over stdio (Model Context Protocol, revision 2025-06-18).

Exposes SwarmPipe to any MCP host (GitHub Copilot CLI, VS Code, Claude Desktop...): read tools
(status, incidents, datasets, lineage impact, knowledge, NL data questions) and two governed write
tools (decide an approval, drop a scenario). Notes that matter in production:
  * Annotations (readOnlyHint, destructiveHint, ...) are HINTS for the host UI, not security.
  * The server enforces authorization itself: it acts as ONE configured identity (mcp.act_as_user)
    and never forwards client tokens downstream (MCP forbids token passthrough).
  * High-risk approvals still require typed confirmation - the server does not rely on the client
    supporting elicitation.
  * stdout is reserved for protocol messages; all logs go to stderr.
"""
from __future__ import annotations

import json
import sys
import traceback

PROTOCOL = "2025-06-18"

TOOLS = [
    {"name": "pipeline_status", "title": "Pipeline status", "description": "Queue depth, runs by status, open incidents, pending approvals, LLM cost.",
     "inputSchema": {"type": "object", "properties": {}}, "annotations": {"readOnlyHint": True, "openWorldHint": False}},
    {"name": "list_incidents", "title": "List incidents", "description": "Recent incidents with root cause and status.",
     "inputSchema": {"type": "object", "properties": {"status": {"type": "string"}, "limit": {"type": "integer", "default": 20}}},
     "annotations": {"readOnlyHint": True, "openWorldHint": False}},
    {"name": "get_incident", "title": "Get incident", "description": "Diagnosis, signals, proposals and policy decisions for one incident.",
     "inputSchema": {"type": "object", "properties": {"incident_id": {"type": "string"}}, "required": ["incident_id"]},
     "annotations": {"readOnlyHint": True, "openWorldHint": False}},
    {"name": "list_datasets", "title": "List datasets", "description": "Published datasets, versions, holds and freshness.",
     "inputSchema": {"type": "object", "properties": {"tenant": {"type": "string", "default": "default"}}}, "annotations": {"readOnlyHint": True}},
    {"name": "get_lineage_impact", "title": "Lineage impact", "description": "Downstream datasets, consumers, owners and blast radius of a dataset.",
     "inputSchema": {"type": "object", "properties": {"dataset": {"type": "string"}, "tenant": {"type": "string", "default": "default"}}, "required": ["dataset"]},
     "annotations": {"readOnlyHint": True}},
    {"name": "search_knowledge", "title": "Search runbooks", "description": "BM25 search over runbooks and ingested documents (with trust labels).",
     "inputSchema": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}, "annotations": {"readOnlyHint": True}},
    {"name": "ask_data_question", "title": "Ask the data", "description": "Natural-language question answered by the Analyst agent with read-only SQL over published data.",
     "inputSchema": {"type": "object", "properties": {"question": {"type": "string"}, "tenant": {"type": "string", "default": "default"}}, "required": ["question"]},
     "annotations": {"readOnlyHint": True, "openWorldHint": False}},
    {"name": "list_pending_approvals", "title": "Pending approvals", "description": "Agent actions waiting for a human decision.",
     "inputSchema": {"type": "object", "properties": {}}, "annotations": {"readOnlyHint": True}},
    {"name": "decide_approval", "title": "Decide an approval", "description": "Approve or reject a pending agent action. High-risk actions need confirm_text = the dataset name and a comment.",
     "inputSchema": {"type": "object", "properties": {"approval_id": {"type": "string"}, "decision": {"type": "string", "enum": ["approved", "rejected"]},
                                                     "comment": {"type": "string"}, "confirm_text": {"type": "string"}}, "required": ["approval_id", "decision"]},
     "annotations": {"readOnlyHint": False, "destructiveHint": True, "idempotentHint": False, "openWorldHint": False}},
    {"name": "drop_scenario", "title": "Drop a scenario", "description": "Write synthetic files for a named scenario into the watched folder (e.g. volume_drop, schema_drift).",
     "inputSchema": {"type": "object", "properties": {"name": {"type": "string"}, "tenant": {"type": "string"}}, "required": ["name"]},
     "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": False}},
    {"name": "get_evidence_pack", "title": "Evidence pack", "description": "The audit-grade evidence pack of an incident.",
     "inputSchema": {"type": "object", "properties": {"incident_id": {"type": "string"}}, "required": ["incident_id"]}, "annotations": {"readOnlyHint": True}},
]


class McpServer:
    def __init__(self, svc, as_user: str):
        self.svc = svc
        self.user = svc.identity.user(as_user)

    def _call(self, name: str, args: dict):
        s = self.svc
        if name == "pipeline_status":
            return {"queue_depth": s.engine.queue_depth(), "runs": s.db.query("SELECT workflow, status, COUNT(*) n FROM runs GROUP BY workflow, status"),
                    "open_incidents": s.db.scalar("SELECT COUNT(*) FROM incidents WHERE status NOT IN ('resolved','closed')", default=0),
                    "pending_approvals": s.db.scalar("SELECT COUNT(*) FROM approvals WHERE status='pending'", default=0),
                    "llm": s.db.query_one("SELECT COUNT(*) calls, ROUND(COALESCE(SUM(cost_usd),0),6) usd FROM llm_calls"), "inbox": str(s.settings.inbox)}
        if name == "list_incidents":
            rows = s.incidents.list(status=args.get("status"), limit=int(args.get("limit", 20)))
            return [{"id": r["id"], "status": r["status"], "severity": r["severity"], "dataset": r["dataset"], "title": r["title"],
                     "root_cause": (r.get("diagnosis") or {}).get("root_cause_category")} for r in rows]
        if name == "get_incident":
            inc = s.incidents.get(args["incident_id"])
            if not inc:
                raise KeyError("incident not found")
            props = s.db.query("SELECT id, action, status, policy_effect, autonomy_level, approval_id FROM proposals WHERE incident_id=? ORDER BY rank", (inc["id"],))
            return {"incident": {k: inc[k] for k in ("id", "status", "severity", "dataset", "title", "diagnosis", "impact")},
                    "signals": [{"type": x["type"], "severity": x["severity"], "summary": x["summary"]} for x in s.signals.for_incident(inc["id"])],
                    "proposals": props}
        if name == "list_datasets":
            t = args.get("tenant", "default")
            return [{**r, "freshness": s.context.freshness(t, r["dataset"])} for r in
                    s.db.query("SELECT dataset, published_version_id, hold, last_success_at FROM dataset_state WHERE tenant=?", (t,))]
        if name == "get_lineage_impact":
            return s.context.impact(args.get("tenant", "default"), args["dataset"])
        if name == "search_knowledge":
            return [{"title": h["title"], "trust": h["trust"], "source": h["source"], "text": h["text"][:800]} for h in s.knowledge.search(args["query"], "default", k=4)]
        if name == "ask_data_question":
            return s.agents.analyst.ask_question(args["question"], self.user, args.get("tenant", "default"))
        if name == "list_pending_approvals":
            return [{k: a[k] for k in ("id", "kind", "subject", "risk", "summary", "requires_confirmation")} for a in s.approvals.list("pending")]
        if name == "decide_approval":
            res = s.approvals.decide(args["approval_id"], args["decision"], self.user, comment=args.get("comment", "decided via MCP"),
                                     confirm_text=args.get("confirm_text"))
            s.audit.record(self.user.principal, "mcp.decide_approval", args["approval_id"], args["decision"], {"channel": "mcp"})
            return {"approval": {k: res[k] for k in ("id", "status", "decided_by", "subject")},
                    "note": "the running pipeline resumes the waiting workflow (or run `swarmpipe tick`)"}
        if name == "drop_scenario":
            from swarmpipe import scenarios

            paths = scenarios.drop(args["name"], s.settings.inbox, args.get("tenant"), svc=s)
            s.audit.record(self.user.principal, "mcp.drop_scenario", args["name"], "ok", {"channel": "mcp"})
            return {"dropped": [p.name for p in paths], "inbox": str(s.settings.inbox)}
        if name == "get_evidence_pack":
            return s.evidence.build(args["incident_id"])
        raise KeyError(f"unknown tool {name}")

    def _resources(self) -> list[dict]:
        out = [{"uri": f"swarmpipe://contracts/{c['dataset']}", "name": f"contract {c['dataset']} v{c['version']}", "mimeType": "application/x-yaml",
                "description": c.get("description", "")} for c in self.svc.contracts.all_active()]
        out += [{"uri": f"swarmpipe://knowledge/{d['id']}", "name": d["title"], "mimeType": "text/markdown", "description": f"trust={d['trust']}"}
                for d in self.svc.knowledge.documents() if d["status"] == "active"]
        return out

    def _read(self, uri: str) -> dict:
        if uri.startswith("swarmpipe://contracts/"):
            ds = uri.rsplit("/", 1)[1]
            return {"uri": uri, "mimeType": "application/x-yaml", "text": self.svc.contracts.export_yaml(ds)}
        if uri.startswith("swarmpipe://knowledge/"):
            did = uri.rsplit("/", 1)[1]
            rows = self.svc.db.query("SELECT text FROM chunks WHERE doc_id=? ORDER BY seq", (did,))
            return {"uri": uri, "mimeType": "text/markdown", "text": "\n\n".join(r["text"] for r in rows)}
        raise KeyError(uri)

    def handle(self, msg: dict) -> dict | None:
        mid, method, params = msg.get("id"), msg.get("method"), msg.get("params") or {}
        if mid is None:
            return None
        try:
            if method == "initialize":
                want = params.get("protocolVersion") or PROTOCOL
                result = {"protocolVersion": want if want in ("2025-06-18", "2025-03-26", "2024-11-05") else PROTOCOL,
                          "capabilities": {"tools": {"listChanged": False}, "resources": {"listChanged": False}},
                          "serverInfo": {"name": "swarmpipe", "title": "SwarmPipe agentic data pipeline", "version": "0.1.0"},
                          "instructions": f"SwarmPipe control surface. Acting as {self.user.principal}. Write tools are governed: approvals check "
                                          "roles, tenants and typed confirmations server-side."}
            elif method == "ping":
                result = {}
            elif method == "tools/list":
                result = {"tools": TOOLS}
            elif method == "tools/call":
                name = params.get("name")
                try:
                    data = self._call(name, params.get("arguments") or {})
                    text = json.dumps(data, default=str, ensure_ascii=False)
                    result = {"content": [{"type": "text", "text": text[:60000]}], "isError": False}
                    if isinstance(data, dict):
                        result["structuredContent"] = json.loads(json.dumps(data, default=str))
                except Exception as exc:  # tool errors are results, not protocol errors
                    result = {"content": [{"type": "text", "text": f"{type(exc).__name__}: {exc}"}], "isError": True}
            elif method == "resources/list":
                result = {"resources": self._resources()}
            elif method == "resources/read":
                result = {"contents": [self._read(params["uri"])]}
            else:
                return {"jsonrpc": "2.0", "id": mid, "error": {"code": -32601, "message": f"method not found: {method}"}}
            return {"jsonrpc": "2.0", "id": mid, "result": result}
        except Exception as exc:  # noqa: BLE001
            return {"jsonrpc": "2.0", "id": mid, "error": {"code": -32603, "message": f"{type(exc).__name__}: {exc}"}}


def serve(as_user: str | None = None) -> None:
    from swarmpipe.app import build_services

    svc = build_services(console_logs=False, log_level="WARNING")
    server = McpServer(svc, as_user or svc.settings.mcp.get("act_as_user", "oncall"))
    out = sys.stdout
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except ValueError:
            out.write(json.dumps({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "parse error"}}) + "\n")
            out.flush()
            continue
        batch = msg if isinstance(msg, list) else [msg]
        for m in batch:
            try:
                resp = server.handle(m)
            except Exception:  # noqa: BLE001
                sys.stderr.write(traceback.format_exc())
                resp = {"jsonrpc": "2.0", "id": m.get("id"), "error": {"code": -32603, "message": "internal error"}}
            if resp is not None:
                out.write(json.dumps(resp, default=str, ensure_ascii=False) + "\n")
                out.flush()
