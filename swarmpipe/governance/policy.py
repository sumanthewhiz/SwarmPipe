"""Policy engine: deterministic authorization for every agent action, outside the model
(a policy engine, not a prompt: runtime policy + risk tiering + the autonomy ladder).

evaluate() returns one of (loosest -> strictest):
    auto_execute < auto_execute_notify < require_approval < recommend < inform_only < deny
Autonomy level sets the base effect; escalations can only make it stricter; denials always win.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import timedelta

from swarmpipe.core.util import iso

EFFECTS = ["auto_execute", "auto_execute_notify", "require_approval", "recommend", "inform_only", "deny"]
AUTO_EFFECTS = {"auto_execute", "auto_execute_notify"}


def stricter(a: str, b: str) -> str:
    return a if EFFECTS.index(a) >= EFFECTS.index(b) else b


@dataclass
class ActionContext:
    tenant: str
    action: str
    params: dict = field(default_factory=dict)
    dataset: str | None = None
    target: str | None = None
    blast_radius: int = 0
    regulated_consumer: bool = False
    classification: str | None = None
    diagnosis_confidence: float | None = None
    injection_suspected: bool = False
    incident_id: str | None = None
    actor: str = "agent:planner"


@dataclass
class PolicyDecision:
    effect: str
    action: str
    level: str
    risk: str
    reasons: list[str]
    matched_rules: list[str]
    policy_version: int
    typed_confirmation: bool = False
    annotation_required: bool = False
    evaluated_at: str = field(default_factory=iso)

    @property
    def auto(self) -> bool:
        return self.effect in AUTO_EFFECTS

    def to_dict(self) -> dict:
        return asdict(self)


class PolicyEngine:
    def __init__(self, svc):
        self.svc = svc

    @property
    def policy(self) -> dict:
        return self.svc.settings.policies

    def spec(self, action: str) -> dict | None:
        return self.policy.get("actions", {}).get(action)

    def in_freeze_window(self) -> bool:
        today = self.svc.clock.now()
        for w in self.policy.get("freeze_windows", []):
            if w.get("enabled") and today.day in set(w.get("days_of_month", [])):
                return True
        return bool(self.svc.flags.get("policy.freeze_window", False))

    def _auto_actions_last_hour(self) -> int:
        since = iso(self.svc.clock.real_now() - timedelta(hours=1))
        return self.svc.db.scalar(
            "SELECT COUNT(*) FROM proposals WHERE executed_at>=? AND executed_by LIKE 'agent:%' AND on_behalf_of IS NULL", (since,), default=0)

    def _recent_same(self, ctx: ActionContext, minutes: float) -> bool:
        since = iso(self.svc.clock.real_now() - timedelta(minutes=minutes))
        target = ctx.target or ctx.dataset or ""
        n = self.svc.db.scalar(
            "SELECT COUNT(*) FROM proposals WHERE tenant=? AND action=? AND dataset=? AND executed_at>=? "
            "AND status IN ('executed','verified') AND (incident_id IS NULL OR incident_id != ?)",
            (ctx.tenant, ctx.action, target, since, ctx.incident_id or ""), default=0)
        return n > 0

    def _match(self, when: dict, ctx: ActionContext, spec: dict) -> bool:
        for key, val in when.items():
            if key == "diagnosis_confidence_lt":
                if ctx.diagnosis_confidence is None or ctx.diagnosis_confidence >= float(val):
                    return False
            elif key == "injection_suspected":
                if bool(ctx.injection_suspected) != bool(val):
                    return False
            elif key == "blast_radius_gt":
                if ctx.blast_radius <= int(val):
                    return False
            elif key == "regulated_consumer":
                if bool(ctx.regulated_consumer) != bool(val):
                    return False
            elif key == "risk_in":
                if spec.get("risk") not in val:
                    return False
            elif key == "in_freeze_window":
                if self.in_freeze_window() != bool(val):
                    return False
            elif key == "classification_in":
                if ctx.classification not in val:
                    return False
            elif key == "action_in":
                if ctx.action not in val:
                    return False
            elif key == "auto_actions_last_hour_gt":
                if self._auto_actions_last_hour() <= int(val):
                    return False
            elif key == "same_action_same_target_within_min":
                if not self._recent_same(ctx, float(val)):
                    return False
            else:
                return False
        return True

    def evaluate(self, ctx: ActionContext) -> PolicyDecision:
        pol = self.policy
        version = int(pol.get("version", 1))
        spec = self.spec(ctx.action)
        if spec is None:
            return PolicyDecision("deny", ctx.action, "-", "unknown", [f"action '{ctx.action}' is not in the action catalog"],
                                  ["catalog"], version)
        base = dict(action=ctx.action, risk=spec.get("risk", "high"), policy_version=version,
                    typed_confirmation=bool(spec.get("typed_confirmation")),
                    annotation_required=bool(spec.get("annotation_required")))
        scope = self.svc.killswitch.engaged(tenant=ctx.tenant, action=ctx.action)
        if scope:
            return PolicyDecision("deny", level="L0", reasons=[f"kill switch engaged ({scope})"], matched_rules=["kill-switch"], **base)
        for rule in pol.get("denials", []):
            if self._match(rule.get("when", {}), ctx, spec):
                return PolicyDecision("deny", level="-", reasons=[rule.get("reason", rule["id"])], matched_rules=[rule["id"]], **base)
        level = self.svc.autonomy.level(ctx.tenant, ctx.action)
        effect = pol.get("level_effects", {}).get(level, "require_approval")
        reasons = [f"autonomy level {level} for '{ctx.action}' (tenant {ctx.tenant}) -> {effect}"]
        matched: list[str] = [f"level:{level}"]
        if not self.svc.settings.features.get("auto_remediation", True) and effect in AUTO_EFFECTS:
            effect = "require_approval"
            reasons.append("feature flag auto_remediation=false -> approval required")
            matched.append("feature:auto_remediation")
        for rule in pol.get("escalations", []):
            if self._match(rule.get("when", {}), ctx, spec):
                new = stricter(effect, rule.get("min_effect", "require_approval"))
                if new != effect:
                    reasons.append(rule.get("reason", rule["id"]))
                    matched.append(rule["id"])
                effect = new
        return PolicyDecision(effect, level=level, reasons=reasons, matched_rules=matched, **base)
