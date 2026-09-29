"""End-to-end: file drop -> swarm -> incident -> policy -> action -> verification, plus interfaces."""
import json
import shutil
import tempfile
from pathlib import Path

from tests.conftest import approve_all, drive, make_svc


def _primary(svc):
    incs = svc.incidents.list()
    return max(incs, key=lambda i: i["signal_count"]) if incs else None


def test_clean_day_publishes_and_rebuilds_derived_without_incidents(base_svc):
    drive(base_svc, "clean_day")
    assert base_svc.incidents.list() == []
    assert base_svc.db.scalar("SELECT COUNT(*) FROM runs WHERE workflow='derive' AND status='succeeded'") >= 1


def test_volume_drop_is_diagnosed_grounded_and_auto_mitigated(base_svc):
    drive(base_svc, "volume_drop")
    inc = _primary(base_svc)
    d = inc["diagnosis"]
    assert d["root_cause_category"] == "truncated_extract" and d["grounded"]
    done = {p["action"]: p["status"] for p in base_svc.db.query("SELECT action, status FROM proposals WHERE incident_id=?", (inc["id"],))}
    assert done.get("request_resend") == "verified" and done.get("hold_downstream") == "verified"
    assert base_svc.db.query_one("SELECT hold FROM dataset_state WHERE dataset='sales_enriched'")["hold"] == 1
    pack = base_svc.evidence.build(inc["id"])
    assert pack["audit_anchor"]["hash"] and pack["versions"]["prompts"]


def test_schema_drift_requires_approval_then_reprocesses(base_svc):
    drive(base_svc, "schema_drift")
    inc = _primary(base_svc)
    assert inc["status"] == "awaiting_approval"
    approve_all(base_svc)
    inc = base_svc.incidents.get(inc["id"])
    assert inc["status"] == "resolved"
    p = base_svc.db.query_one("SELECT * FROM proposals WHERE incident_id=? AND action='reprocess_with_mapping'", (inc["id"],))
    assert p["status"] == "verified" and p["on_behalf_of"] == "user:admin"


def test_injection_without_defenses_is_still_contained(base_svc):
    base_svc.flags.set("guardrails.spotlighting", False)
    base_svc.flags.set("feature.critic_review", False)
    drive(base_svc, "injection")
    approve_all(base_svc)
    executed = {r["action"] for r in base_svc.db.query("SELECT action FROM proposals WHERE status IN ('executed','verified')")}
    assert "force_publish" not in executed
    assert not base_svc.db.query("SELECT 1 FROM notifications WHERE channel='webhook' AND status='sent'")
    assert base_svc.db.scalar("SELECT COUNT(*) FROM proposals WHERE status='invalid'") >= 1


def test_mass_failure_clusters_into_one_incident(base_svc):
    drive(base_svc, "mass_failure")
    incs = base_svc.incidents.list()
    assert len(incs) == 1 and incs[0]["signal_count"] >= 6


def test_duplicate_delivery_is_skipped(base_svc):
    drive(base_svc, "clean_day", "duplicate")
    assert base_svc.db.scalar("SELECT COUNT(*) FROM runs WHERE status='skipped'") == 1


def test_analyst_masks_pii_by_identity(base_svc):
    q = "emails of customers"
    as_analyst = base_svc.agents.analyst.ask_question(q, base_svc.identity.user("analyst"), "default")
    as_admin = base_svc.agents.analyst.ask_question(q, base_svc.identity.user("admin"), "default")
    assert not any("@" in str(v) for r in as_analyst["rows"] for v in r)
    assert any("@" in str(v) for r in as_admin["rows"] for v in r)
    refused = base_svc.agents.analyst.ask_question("delete all sales rows", base_svc.identity.user("analyst"), "default")
    assert refused["refused"]


def test_web_api_smoke(base_svc):
    from fastapi.testclient import TestClient

    from swarmpipe.web.api import create_app

    client = TestClient(create_app(base_svc))
    assert client.get("/healthz").json()["ok"]
    assert client.get("/api/overview").status_code == 200
    assert "swarmpipe_" in client.get("/metrics").text
    assert client.get("/a2a/agents/analyst").json()["name"] == "Analyst"
    r = client.post("/api/ask", json={"question": "total revenue by region"}, headers={"X-User": "analyst"})
    assert r.status_code == 200 and not r.json()["refused"]
    assert client.post("/api/autonomy", json={"action": "notify_owner", "level": "L4"}, headers={"X-User": "analyst"}).status_code == 403


def test_mcp_server_protocol(base_svc):
    from swarmpipe.mcp_server import McpServer

    m = McpServer(base_svc, "oncall")
    init = m.handle({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18"}})
    assert init["result"]["serverInfo"]["name"] == "swarmpipe"
    tools = m.handle({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})["result"]["tools"]
    assert any(t["name"] == "decide_approval" and t["annotations"]["destructiveHint"] for t in tools)
    res = m.handle({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "ask_data_question", "arguments": {"question": "average order value"}}})
    assert not res["result"]["isError"] and "avg_order_value" in json.loads(res["result"]["content"][0]["text"])["answer"]
    assert m.handle({"jsonrpc": "2.0", "method": "notifications/initialized"}) is None
    assert m.handle({"jsonrpc": "2.0", "id": 4, "method": "nope"})["error"]["code"] == -32601


def test_close_releases_every_file_handle(baseline_dir):
    """Regression: connections opened on agent-step threads (kept alive by reference cycles) and the per-workspace
    log file used to survive shutdown, so on Windows eval/test workspaces could never be deleted."""
    d = Path(tempfile.mkdtemp(prefix="swarmpipe_test_close_"))
    shutil.copytree(baseline_dir / "data", d / "data")
    s = make_svc(d)
    drive(s, "volume_drop")
    assert s.db.scalar("SELECT COUNT(*) FROM incidents") >= 1
    s.close()
    shutil.rmtree(d)  # no ignore_errors, no gc.collect(): raises PermissionError on Windows if anything is still open
    assert not d.exists()
