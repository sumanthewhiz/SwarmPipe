"""Signals, incidents and notifications.

A *signal* is one detector firing (a failed check, an overdue dataset, an out-of-band change...).
An *incident* groups correlated signals (one incident per probable cause: group by root cause, not by
symptom). Notifications go to an outbox folder (simulated email/chat); URL recipients are webhooks
and must pass the egress allowlist (which cuts the exfiltration leg of the "lethal trifecta")."""
from __future__ import annotations

from datetime import timedelta

from swarmpipe.core.errors import SwarmError
from swarmpipe.core.util import dumps, iso, loads, new_id
from swarmpipe.governance.guardrails import check_egress

SEVERITIES = ["info", "warning", "high", "critical"]


def max_sev(a: str | None, b: str | None) -> str:
    a, b = a or "info", b or "info"
    return a if SEVERITIES.index(a) >= SEVERITIES.index(b) else b


class SignalService:
    def __init__(self, svc):
        self.svc = svc

    def raise_signal(self, tenant: str, type_: str, dataset: str | None, severity: str, summary: str,
                     details: dict | None = None, run_id: str | None = None) -> str:
        sid = new_id("sig")
        self.svc.db.insert("signals", {"id": sid, "tenant": tenant, "type": type_, "dataset": dataset, "severity": severity,
                                       "summary": summary[:500], "details": dumps(details or {}), "run_id": run_id,
                                       "incident_id": None, "correlation_key": None, "created_at": self.svc.clock.now_iso()})
        span = self.svc.tracer.current()
        if span:
            span.event("signal", type=type_, severity=severity, dataset=dataset)
        self.svc.metrics.inc("signals_total", type=type_, severity=severity, tenant=tenant)
        self.svc.events.publish("signal.raised", {"signal_id": sid, "type": type_, "severity": severity, "dataset": dataset}, tenant,
                                span.trace_id if span else None)
        return sid

    def get(self, sid: str) -> dict | None:
        r = self.svc.db.query_one("SELECT * FROM signals WHERE id=?", (sid,))
        if r:
            r["details"] = loads(r["details"], {})
        return r

    def for_incident(self, incident_id: str) -> list[dict]:
        rows = self.svc.db.query("SELECT * FROM signals WHERE incident_id=? ORDER BY created_at", (incident_id,))
        for r in rows:
            r["details"] = loads(r["details"], {})
        return rows


class IncidentService:
    def __init__(self, svc):
        self.svc = svc

    def open(self, tenant: str, dataset: str | None, severity: str, title: str, correlation_key: str, first_signal_at: str) -> str:
        iid = new_id("inc")
        now = self.svc.clock.now_iso()
        trace_id = self.svc.tracer.new_trace_id()
        self.svc.db.insert("incidents", {"id": iid, "tenant": tenant, "dataset": dataset, "status": "open", "severity": severity,
                                         "title": title[:200], "correlation_key": correlation_key, "trace_id": trace_id,
                                         "triage_run_id": None, "diagnosis": None, "impact": None, "postmortem": None,
                                         "created_at": now, "updated_at": now, "first_signal_at": first_signal_at,
                                         "diagnosed_at": None, "resolved_at": None, "cost_usd": 0, "signal_count": 0})
        self.svc.audit.record("agent:correlator", "incident.open", iid, "open", {"dataset": dataset, "severity": severity, "title": title},
                              trace_id=trace_id)
        self.svc.events.publish("incident.opened", {"incident_id": iid}, tenant, trace_id)
        self.svc.metrics.inc("incidents_opened_total", tenant=tenant, severity=severity)
        return iid

    def attach(self, incident_id: str, signal: dict) -> None:
        inc = self.get(incident_id)
        with self.svc.db.tx():
            self.svc.db.update("signals", {"id": signal["id"]}, {"incident_id": incident_id, "correlation_key": inc["correlation_key"]})
            self.svc.db.update("incidents", {"id": incident_id}, {"signal_count": inc["signal_count"] + 1,
                                                                 "severity": max_sev(inc["severity"], signal["severity"]),
                                                                 "updated_at": self.svc.clock.now_iso()})

    def open_for_key(self, tenant: str, key: str, window_min: float) -> dict | None:
        since = iso(self.svc.clock.now() - timedelta(minutes=window_min))
        return self.svc.db.query_one(
            "SELECT * FROM incidents WHERE tenant=? AND correlation_key=? AND status NOT IN ('resolved','closed','escalated') "
            "AND updated_at >= ? ORDER BY created_at DESC LIMIT 1", (tenant, key, since))

    def open_for_dataset(self, tenant: str, dataset: str) -> list[dict]:
        rows = self.svc.db.query("SELECT * FROM incidents WHERE tenant=? AND dataset=? AND status NOT IN ('resolved','closed')",
                                 (tenant, dataset))
        for r in rows:
            for k in ("diagnosis", "impact", "postmortem"):
                r[k] = loads(r[k], None)
        return rows

    def update(self, incident_id: str, **fields) -> None:
        for k in ("diagnosis", "impact", "postmortem"):
            if k in fields and not isinstance(fields[k], (str, type(None))):
                fields[k] = dumps(fields[k])
        fields["updated_at"] = self.svc.clock.now_iso()
        self.svc.db.update("incidents", {"id": incident_id}, fields)

    def get(self, incident_id: str) -> dict | None:
        r = self.svc.db.query_one("SELECT * FROM incidents WHERE id=?", (incident_id,))
        if r:
            for k in ("diagnosis", "impact", "postmortem"):
                r[k] = loads(r[k], None)
        return r

    def list(self, status: str | None = None, tenant: str | None = None, limit: int = 100) -> list[dict]:
        conds, params = [], []
        if status:
            conds.append("status=?")
            params.append(status)
        if tenant:
            conds.append("tenant=?")
            params.append(tenant)
        where = ("WHERE " + " AND ".join(conds)) if conds else ""
        rows = self.svc.db.query(f"SELECT * FROM incidents {where} ORDER BY created_at DESC LIMIT ?", [*params, limit])
        for r in rows:
            for k in ("diagnosis", "impact", "postmortem"):
                r[k] = loads(r[k], None)
        return rows


class Notifier:
    def __init__(self, svc):
        self.svc = svc

    def send(self, tenant: str, recipient: str, subject: str, body: str, incident_id: str | None = None,
             sender: str = "system:notifier") -> dict:
        nid = new_id("ntf")
        is_url = recipient.lower().startswith(("http://", "https://"))
        channel = "webhook" if is_url else ("email" if "@" in recipient else "chat")
        row = {"id": nid, "tenant": tenant, "channel": channel, "recipient": recipient, "subject": subject[:200], "body": body[:4000],
               "status": "sent", "incident_id": incident_id, "path": None, "created_at": iso()}
        if is_url:
            ok, host = check_egress(recipient, self.svc.settings.guardrails.egress_allowlist)
            if not ok:
                row["status"] = "blocked_egress"
                self.svc.db.insert("notifications", row)
                self.svc.audit.record(sender, "egress.blocked", recipient, "denied", {"host": host, "incident_id": incident_id})
                self.svc.metrics.inc("egress_blocked_total", host=host)
                self.svc.signals.raise_signal(tenant, "egress_blocked", None, "high",
                                              f"Blocked outbound notification to non-allowlisted host {host}",
                                              {"recipient": recipient, "incident_id": incident_id, "sender": sender})
                raise SwarmError(f"egress to {host} is not allowlisted", code="EGRESS_BLOCKED")
        out = self.svc.settings.data_path("outbox", channel)
        out.mkdir(parents=True, exist_ok=True)
        path = out / f"{nid}.json"
        path.write_text(dumps({"to": recipient, "subject": subject, "body": body, "incident_id": incident_id,
                               "tenant": tenant, "sent_at": row["created_at"]}, indent=2), encoding="utf-8")
        row["path"] = str(path)
        self.svc.db.insert("notifications", row)
        self.svc.metrics.inc("notifications_total", channel=channel)
        return row
