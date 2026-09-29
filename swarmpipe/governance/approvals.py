"""Server-side approval queue for human oversight (for clients without MCP elicitation, the server
itself must enforce approval policy).

Approvals are durable: the workflow that asked is paused (WaitingFor approval:<id>) and resumes when
a human decides through the dashboard, the CLI or MCP. High-risk actions require typed confirmation
and a written annotation, which also counters rubber-stamping (ASI09 human-agent trust exploitation).
"""
from __future__ import annotations

from datetime import timedelta

from swarmpipe.core.errors import AuthorizationError, SwarmError
from swarmpipe.core.util import dumps, iso, loads, new_id


class ApprovalService:
    def __init__(self, svc):
        self.svc = svc

    def request(self, kind: str, *, tenant: str, subject: str, risk: str, summary: str, payload: dict | None = None,
                proposal_id: str | None = None, incident_id: str | None = None, run_id: str | None = None,
                requires_confirmation: str | None = None, ttl_min: float = 240) -> str:
        aid = new_id("apr")
        now = self.svc.clock.real_now()
        self.svc.db.insert("approvals", {
            "id": aid, "kind": kind, "proposal_id": proposal_id, "incident_id": incident_id, "run_id": run_id,
            "tenant": tenant, "subject": subject, "risk": risk, "summary": summary, "payload": dumps(payload or {}),
            "status": "pending", "requires_confirmation": requires_confirmation, "requested_at": iso(now),
            "expires_at": iso(now + timedelta(minutes=ttl_min)), "decided_at": None, "decided_by": None, "comment": None})
        self.svc.audit.record("system:approvals", "approval.requested", aid, "pending",
                              {"kind": kind, "subject": subject, "risk": risk, "incident_id": incident_id, "proposal_id": proposal_id})
        self.svc.events.publish("approval.requested", {"approval_id": aid, "kind": kind, "incident_id": incident_id}, tenant)
        self.svc.metrics.inc("approvals_requested_total", kind=kind, risk=risk)
        return aid

    def get(self, approval_id: str) -> dict | None:
        row = self.svc.db.query_one("SELECT * FROM approvals WHERE id=?", (approval_id,))
        if row:
            row["payload"] = loads(row["payload"], {})
        return row

    def list(self, status: str | None = "pending", tenant: str | None = None, limit: int = 100) -> list[dict]:
        conds, params = [], []
        if status:
            conds.append("status=?")
            params.append(status)
        if tenant:
            conds.append("tenant=?")
            params.append(tenant)
        where = ("WHERE " + " AND ".join(conds)) if conds else ""
        rows = self.svc.db.query(f"SELECT * FROM approvals {where} ORDER BY requested_at DESC LIMIT ?", [*params, limit])
        for r in rows:
            r["payload"] = loads(r["payload"], {})
        return rows

    def decide(self, approval_id: str, decision: str, user, comment: str = "", confirm_text: str | None = None) -> dict:
        if decision not in ("approved", "rejected"):
            raise SwarmError("decision must be 'approved' or 'rejected'", code="INVALID_ARGUMENTS")
        row = self.get(approval_id)
        if row is None:
            raise SwarmError(f"approval {approval_id} not found", code="NOT_FOUND")
        if row["status"] != "pending":
            raise SwarmError(f"approval {approval_id} is already {row['status']}", code="CONFLICT")
        if not (user.has_role("approver") and user.can("approval:decide")):
            self.svc.audit.record(user.principal, "approval.decide", approval_id, "forbidden", {"decision": decision})
            raise AuthorizationError(f"{user.principal} is not allowed to decide approvals")
        if not user.can_access_tenant(row["tenant"]):
            raise AuthorizationError(f"{user.principal} has no access to tenant {row['tenant']}")
        if decision == "approved":
            need = row.get("requires_confirmation")
            if need and (confirm_text or "").strip() != need:
                raise SwarmError(f"typed confirmation required: type '{need}' to approve this high-risk action",
                                 code="CONFIRMATION_REQUIRED", details={"expected": need})
            if row["payload"].get("annotation_required") and len((comment or "").strip()) < 10:
                raise SwarmError("a written justification (>= 10 characters) is required for this action",
                                 code="ANNOTATION_REQUIRED")
        self.svc.db.update("approvals", {"id": approval_id},
                           {"status": decision, "decided_at": iso(), "decided_by": user.principal, "comment": comment})
        self.svc.audit.record(user.principal, "approval.decide", approval_id, decision,
                              {"kind": row["kind"], "subject": row["subject"], "comment": comment,
                               "incident_id": row["incident_id"], "proposal_id": row["proposal_id"]})
        if row["kind"] == "autonomy_promotion" and decision == "approved":
            p = row["payload"]
            self.svc.autonomy.set_level(p["tenant"], p["action"], p["to"], user.principal, f"promotion approved: {comment}")
        self.svc.events.publish("approval.decided", {"approval_id": approval_id, "decision": decision,
                                                     "run_id": row["run_id"], "incident_id": row["incident_id"]}, row["tenant"])
        self.svc.metrics.inc("approvals_decided_total", decision=decision, kind=row["kind"])
        return self.get(approval_id)

    def expire_due(self) -> int:
        now = iso(self.svc.clock.real_now())
        rows = self.svc.db.query("SELECT * FROM approvals WHERE status='pending' AND expires_at < ?", (now,))
        for r in rows:
            self.svc.db.update("approvals", {"id": r["id"]}, {"status": "expired", "decided_at": iso(), "decided_by": "system:expiry"})
            self.svc.audit.record("system:approvals", "approval.expired", r["id"], "expired", {"subject": r["subject"]})
            self.svc.events.publish("approval.decided", {"approval_id": r["id"], "decision": "expired", "run_id": r["run_id"],
                                                         "incident_id": r["incident_id"]}, r["tenant"])
        return len(rows)
