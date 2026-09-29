"""Governance: policy engine, autonomy ladder, approvals, tool gateway controls."""
import pytest

from swarmpipe.core.errors import AuthorizationError, SwarmError
from swarmpipe.governance.policy import ActionContext
from swarmpipe.tools.gateway import ToolContext


def ctx(action, **kw):
    return ActionContext(tenant="default", action=action, dataset="sales_daily", target="sales_daily", **kw)


def test_policy_effects_follow_the_ladder(svc):
    assert svc.policy.evaluate(ctx("notify_owner", diagnosis_confidence=0.9)).effect == "auto_execute_notify"
    assert svc.policy.evaluate(ctx("rollback_dataset", diagnosis_confidence=0.9)).effect == "require_approval"
    assert svc.policy.evaluate(ctx("force_publish", diagnosis_confidence=0.9)).effect == "recommend"
    assert svc.policy.evaluate(ctx("drop_table")).effect == "deny"


def test_escalations_only_tighten(svc):
    d = svc.policy.evaluate(ctx("notify_owner", diagnosis_confidence=0.9, injection_suspected=True))
    assert d.effect == "require_approval" and "injection-suspected" in d.matched_rules
    d = svc.policy.evaluate(ctx("notify_owner", diagnosis_confidence=0.5))
    assert d.effect == "require_approval"


def test_kill_switch_denies_everything(svc):
    svc.killswitch.set("global", True, "test")
    svc.killswitch._loaded = 0
    assert svc.policy.evaluate(ctx("notify_owner", diagnosis_confidence=0.9)).effect == "deny"
    svc.killswitch.set("global", False, "test")


def test_autonomy_is_capped_and_demoted_automatically(svc):
    assert svc.autonomy.set_level("default", "force_publish", "L4", "user:admin", "try")["to"] == "L2"
    svc.autonomy.set_level("default", "rollback_dataset", "L3", "user:admin", "evidence")
    svc.autonomy.record("default", "rollback_dataset", "verified_fail")
    assert svc.autonomy.level("default", "rollback_dataset") == "L2"


def test_approvals_enforce_roles_and_typed_confirmation(svc):
    aid = svc.approvals.request("action", tenant="default", subject="force_publish on sales_daily", risk="critical", summary="x",
                                payload={"annotation_required": True}, requires_confirmation="sales_daily")
    with pytest.raises(AuthorizationError):
        svc.approvals.decide(aid, "approved", svc.identity.user("analyst"))
    with pytest.raises(SwarmError, match="typed confirmation"):
        svc.approvals.decide(aid, "approved", svc.identity.user("oncall"), comment="long enough comment", confirm_text="wrong")
    with pytest.raises(SwarmError, match="justification"):
        svc.approvals.decide(aid, "approved", svc.identity.user("oncall"), comment="short", confirm_text="sales_daily")
    assert svc.approvals.decide(aid, "approved", svc.identity.user("oncall"), comment="verified with the owner", confirm_text="sales_daily")["status"] == "approved"


def test_tool_gateway_least_privilege_and_rogue_containment(svc):
    inv = svc.agents.investigators["volume"]
    tctx = ToolContext(svc, inv.identity, "default", inv.id)
    res = svc.tools.call("act_force_publish", {"version_id": "x", "justification": "please let me"}, tctx, allowed=set(inv.tools))
    assert not res.ok and res.error["code"] == "TOOL_FORBIDDEN"
    for _ in range(3):
        svc.tools.call("act_rollback_dataset", {"dataset": "sales_daily"}, tctx, allowed=set(inv.tools))
    svc.killswitch._loaded = 0
    assert svc.killswitch.engaged(agent=inv.id) == f"agent:{inv.id}"
    assert svc.db.scalar("SELECT COUNT(*) FROM signals WHERE type='rogue_agent'") >= 1


def test_tool_gateway_validates_arguments_and_records_evidence(svc):
    inv = svc.agents.investigators["freshness"]
    tctx = ToolContext(svc, inv.identity, "default", inv.id)
    bad = svc.tools.call("get_dataset_versions", {"dataset": "sales_daily", "limit": 999}, tctx, allowed=set(inv.tools))
    assert not bad.ok and bad.error["code"] == "INVALID_ARGUMENTS"
    ok = svc.tools.call("get_freshness_status", {"dataset": "sales_daily"}, tctx, allowed=set(inv.tools))
    assert ok.ok and ok.evidence_id and svc.db.query_one("SELECT id FROM evidence WHERE id=?", (ok.evidence_id,))
