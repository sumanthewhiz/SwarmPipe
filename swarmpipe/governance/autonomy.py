"""Autonomy ladder per (tenant, action class), earned with evidence and withdrawn automatically
(automatic demotions are triggered by quality drops).

Counters are collected from real outcomes: approvals/rejections (L2), human agreement with
recommendations (L1), post-action verification and rollbacks (L3/L4). Promotion needs a human
decision by default (an `autonomy_promotion` approval); demotion is immediate and automatic."""
from __future__ import annotations

from swarmpipe.core.util import iso

LEVELS = ["L0", "L1", "L2", "L3", "L4"]
_COUNTERS = {"proposed": "proposals", "approved": "approved", "rejected": "rejected", "executed": "executed",
             "verified_ok": "verified_ok", "verified_fail": "verified_fail", "rolled_back": "rolled_back",
             "human_agreed": "human_agreed", "human_disagreed": "human_disagreed"}


class AutonomyManager:
    def __init__(self, svc):
        self.svc = svc

    def _spec(self, action: str) -> dict:
        return self.svc.settings.policies.get("actions", {}).get(action, {"default_level": "L1", "max_level": "L1"})

    def _cap(self, action: str, level: str) -> str:
        mx = self._spec(action).get("max_level", "L2")
        return level if LEVELS.index(level) <= LEVELS.index(mx) else mx

    def level(self, tenant: str, action: str) -> str:
        row = self.svc.db.query_one("SELECT level FROM autonomy WHERE tenant=? AND action_class=?", (tenant, action))
        lvl = row["level"] if row and row["level"] else self._spec(action).get("default_level", "L1")
        return self._cap(action, lvl)

    def _ensure(self, tenant: str, action: str) -> None:
        self.svc.db.execute(
            "INSERT OR IGNORE INTO autonomy(tenant, action_class, level, updated_at, reason) VALUES(?,?,?,?,?)",
            (tenant, action, self.level(tenant, action), iso(), "initial (policy default)"))

    def set_level(self, tenant: str, action: str, level: str, by: str, reason: str) -> dict:
        if level not in LEVELS:
            raise ValueError(f"unknown level {level}")
        capped = self._cap(action, level)
        self._ensure(tenant, action)
        old = self.level(tenant, action)
        with self.svc.db.tx():
            self.svc.db.execute(
                "UPDATE autonomy SET level=?, proposals=0, approved=0, rejected=0, executed=0, verified_ok=0, verified_fail=0, "
                "rolled_back=0, human_agreed=0, human_disagreed=0, updated_at=?, reason=? WHERE tenant=? AND action_class=?",
                (capped, iso(), reason, tenant, action))
            self.svc.db.insert("autonomy_history", {"tenant": tenant, "action_class": action, "from_level": old,
                                                    "to_level": capped, "reason": reason, "changed_by": by, "ts": iso()})
        self.svc.audit.record(by, "autonomy.set_level", f"{tenant}/{action}", capped,
                              {"from": old, "to": capped, "requested": level, "reason": reason})
        return {"tenant": tenant, "action": action, "from": old, "to": capped}

    def record(self, tenant: str, action: str, event: str) -> None:
        col = _COUNTERS.get(event)
        if not col:
            return
        self._ensure(tenant, action)
        self.svc.db.execute(f"UPDATE autonomy SET {col}={col}+1, updated_at=? WHERE tenant=? AND action_class=?",
                            (iso(), tenant, action))
        if event in ("verified_fail", "rolled_back"):
            self._maybe_demote(tenant, action)

    def _maybe_demote(self, tenant: str, action: str) -> None:
        rules = self.svc.settings.policies.get("autonomy_rules", {}).get("demote", {})
        row = self.svc.db.query_one("SELECT * FROM autonomy WHERE tenant=? AND action_class=?", (tenant, action))
        if not row:
            return
        if row["verified_fail"] >= int(rules.get("max_verification_failures", 1)) or \
                row["rolled_back"] >= int(rules.get("max_rollbacks", 1)):
            cur = self.level(tenant, action)
            idx = LEVELS.index(cur)
            if idx > 1:
                self.set_level(tenant, action, LEVELS[idx - 1], "system:autonomy",
                               f"automatic demotion: verified_fail={row['verified_fail']}, rolled_back={row['rolled_back']}")

    def stats(self, tenant: str | None = None) -> list[dict]:
        actions = self.svc.settings.policies.get("actions", {})
        tenants = [tenant] if tenant else sorted(set(self.svc.settings.tenants) | {
            r["tenant"] for r in self.svc.db.query("SELECT DISTINCT tenant FROM autonomy")})
        out = []
        for t in tenants:
            for a, spec in actions.items():
                row = self.svc.db.query_one("SELECT * FROM autonomy WHERE tenant=? AND action_class=?", (t, a)) or {}
                decided = row.get("approved", 0) + row.get("rejected", 0)
                agreed = row.get("human_agreed", 0) + row.get("human_disagreed", 0)
                verified = row.get("verified_ok", 0) + row.get("verified_fail", 0)
                out.append({
                    "tenant": t, "action": a, "level": self.level(t, a), "risk": spec.get("risk"),
                    "max_level": spec.get("max_level"), "proposals": row.get("proposals", 0),
                    "approved": row.get("approved", 0), "rejected": row.get("rejected", 0),
                    "executed": row.get("executed", 0), "verified_ok": row.get("verified_ok", 0),
                    "verified_fail": row.get("verified_fail", 0), "rolled_back": row.get("rolled_back", 0),
                    "human_agreed": row.get("human_agreed", 0), "human_disagreed": row.get("human_disagreed", 0),
                    "approval_rate": (row.get("approved", 0) / decided) if decided else None,
                    "agreement_rate": (row.get("human_agreed", 0) / agreed) if agreed else None,
                    "verification_rate": (row.get("verified_ok", 0) / verified) if verified else None,
                    "reason": row.get("reason"),
                })
        return out

    def review(self, tenant: str, action: str) -> dict:
        rules = self.svc.settings.policies.get("autonomy_rules", {}).get("promote", {})
        st = next((s for s in self.stats(tenant) if s["action"] == action), None)
        if not st:
            return {"eligible": False, "reason": "no stats"}
        level = st["level"]
        if LEVELS.index(level) >= LEVELS.index(st["max_level"] or "L2"):
            return {"eligible": False, "reason": f"already at max level {st['max_level']}"}
        if level == "L1":
            samples, rate = st["human_agreed"] + st["human_disagreed"], st["agreement_rate"]
        else:
            samples, rate = st["approved"] + st["rejected"], st["approval_rate"]
        vr = st["verification_rate"] if st["verification_rate"] is not None else 1.0
        need = int(rules.get("min_samples", 5))
        if samples < need:
            return {"eligible": False, "reason": f"{samples}/{need} samples"}
        if (rate or 0) < float(rules.get("min_approval_rate", 0.9)):
            return {"eligible": False, "reason": f"approval/agreement rate {rate:.2f} below bar"}
        if vr < float(rules.get("min_verification_rate", 1.0)):
            return {"eligible": False, "reason": f"verification rate {vr:.2f} below bar"}
        if st["rolled_back"] > int(rules.get("max_rollbacks", 0)):
            return {"eligible": False, "reason": "rollbacks present"}
        nxt = LEVELS[LEVELS.index(level) + 1]
        return {"eligible": True, "from": level, "to": nxt,
                "reason": f"{samples} samples, rate {rate:.2f}, verification {vr:.2f}"}

    def review_all(self) -> list[dict]:
        rules = self.svc.settings.policies.get("autonomy_rules", {}).get("promote", {})
        results = []
        for st in self.stats():
            if st["proposals"] == 0 and st["human_agreed"] == 0:
                continue
            rec = self.review(st["tenant"], st["action"])
            if not rec.get("eligible"):
                continue
            results.append({**rec, "tenant": st["tenant"], "action": st["action"]})
            if rules.get("auto_promote"):
                self.set_level(st["tenant"], st["action"], rec["to"], "system:autonomy", "auto-promotion: " + rec["reason"])
            else:
                subject = f"{st['tenant']}/{st['action']}"
                exists = self.svc.db.scalar(
                    "SELECT COUNT(*) FROM approvals WHERE kind='autonomy_promotion' AND subject=? AND status='pending'",
                    (subject,), default=0)
                if not exists:
                    self.svc.approvals.request(
                        "autonomy_promotion", tenant=st["tenant"], subject=subject, risk="medium",
                        summary=f"Promote '{st['action']}' from {rec['from']} to {rec['to']} ({rec['reason']})",
                        payload={"tenant": st["tenant"], "action": st["action"], "to": rec["to"], "evidence": rec})
        return results
