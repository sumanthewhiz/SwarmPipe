"""Inter-agent communication and shared state (OWASP ASI07 insecure inter-agent comms).

- AgentMessage: typed envelope (A2A-like task/finding), HMAC-signed with a key derived from the
  local master key. Receivers verify the signature, the recipient and that the route
  (sender role -> recipient role, message type) is allowed. Tampered or unexpected messages are
  rejected, audited and raised as a security signal.
- Blackboard: the incident "case file" - an append-only, versioned shared state where every
  entry records its author and the evidence ids it relies on (provenance), so there are no
  write conflicts and the single decision owner (the supervisor) can explain its decision."""
from __future__ import annotations

import hashlib
import hmac
from dataclasses import asdict, dataclass, field

from swarmpipe.core.errors import SwarmError
from swarmpipe.core.util import canonical_json, dumps, iso, loads, new_id

ROUTES = {
    ("supervisor", "investigator"): {"task"},
    ("investigator", "supervisor"): {"finding"},
    ("supervisor", "planner"): {"task"},
    ("planner", "supervisor"): {"plan"},
    ("external", "analyst"): {"task"},
    ("analyst", "external"): {"result"},
}


def _role_of(agent_id: str) -> str:
    return agent_id.split("_", 1)[0] if agent_id.startswith("investigator") else agent_id


@dataclass
class AgentMessage:
    id: str
    type: str
    sender: str
    recipient: str
    task_id: str
    payload: dict
    created_at: str = field(default_factory=iso)
    signature: str = ""

    def body(self) -> str:
        d = asdict(self)
        d.pop("signature")
        return canonical_json(d)


class MessageBus:
    def __init__(self, svc):
        self.svc = svc
        self._key = None

    @property
    def key(self) -> bytes:
        if self._key is None:
            self._key = self.svc.secrets.derive_key("inter-agent-messages")
        return self._key

    def send(self, sender: str, recipient: str, type_: str, payload: dict, task_id: str) -> AgentMessage:
        msg = AgentMessage(new_id("msg"), type_, sender, recipient, task_id, payload)
        msg.signature = hmac.new(self.key, msg.body().encode(), hashlib.sha256).hexdigest()
        self.svc.metrics.inc("agent_messages_total", type=type_, sender=_role_of(sender))
        return msg

    def receive(self, msg: AgentMessage, recipient: str) -> dict:
        expected = hmac.new(self.key, msg.body().encode(), hashlib.sha256).hexdigest()
        problem = None
        if not hmac.compare_digest(expected, msg.signature or ""):
            problem = "signature mismatch (message tampered or forged)"
        elif msg.recipient != recipient:
            problem = f"message addressed to {msg.recipient}, received by {recipient}"
        elif msg.type not in ROUTES.get((_role_of(msg.sender), _role_of(recipient)), set()):
            problem = f"route {msg.sender} -> {recipient} ({msg.type}) is not allowed"
        if problem:
            self.svc.audit.record(f"agent:{recipient}", "message.rejected", msg.id, "rejected", {"reason": problem, "sender": msg.sender})
            self.svc.metrics.inc("agent_messages_rejected_total", reason=problem.split(" ")[0])
            raise SwarmError(problem, code="MESSAGE_REJECTED")
        return msg.payload


class Blackboard:
    def __init__(self, svc, incident_id: str):
        self.svc = svc
        self.incident_id = incident_id

    def post(self, author: str, kind: str, content, evidence_ids: list[str] | None = None) -> int:
        with self.svc.db.tx():
            v = self.svc.db.scalar("SELECT MAX(version) FROM blackboard WHERE incident_id=?", (self.incident_id,), default=0) + 1
            self.svc.db.insert("blackboard", {"incident_id": self.incident_id, "version": v, "author": author, "kind": kind,
                                              "content": dumps(content), "evidence_ids": dumps(evidence_ids or []), "created_at": iso()})
        return v

    def entries(self, kind: str | None = None) -> list[dict]:
        if kind:
            rows = self.svc.db.query("SELECT * FROM blackboard WHERE incident_id=? AND kind=? ORDER BY version", (self.incident_id, kind))
        else:
            rows = self.svc.db.query("SELECT * FROM blackboard WHERE incident_id=? ORDER BY version", (self.incident_id,))
        for r in rows:
            r["content"] = loads(r["content"], r["content"])
            r["evidence_ids"] = loads(r["evidence_ids"], [])
        return rows
