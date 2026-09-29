"""Agent memory: short-term (the incident blackboard / run context), long-term
episodic (past incidents and their resolutions) and procedural (runbooks, in the knowledge base).

Memory poisoning defenses (OWASP ASI06): every memory carries provenance and an expiry; agents
only *propose* lessons (status=candidate); a human promotes them (status=approved) and only
approved memories are recalled by default. Candidate content is injection-scanned so the approver
sees a warning before promoting something that looks like an instruction."""
from __future__ import annotations

from datetime import timedelta

from swarmpipe.core.util import dumps, iso, loads, new_id
from swarmpipe.data.knowledge import bm25_rank, tokenize
from swarmpipe.governance.guardrails import scan_text


class MemoryStore:
    def __init__(self, svc):
        self.svc = svc

    def propose(self, *, tenant: str, kind: str, title: str, content: str, provenance: dict, ttl_days: int = 90) -> str:
        mid = new_id("mem")
        rep = scan_text(content)
        self.svc.db.insert("memory", {
            "id": mid, "tenant": tenant, "kind": kind, "title": title, "content": content,
            "terms": dumps(tokenize(f"{title} {content}")), "provenance": dumps(provenance),
            "trust": "untrusted" if rep.suspicious else "unverified", "status": "candidate", "created_at": iso(),
            "expires_at": iso(self.svc.clock.real_now() + timedelta(days=ttl_days)), "approved_by": None, "uses": 0,
            "flags": dumps({"injection": rep.to_dict()} if rep.findings else {})})
        self.svc.audit.record(provenance.get("author", "system:memory"), "memory.propose", mid, "candidate",
                              {"kind": kind, "title": title, "suspicious": rep.suspicious})
        return mid

    def decide(self, memory_id: str, approve: bool, by: str) -> dict:
        status = "approved" if approve else "rejected"
        trust = "trusted" if approve else "untrusted"
        self.svc.db.update("memory", {"id": memory_id}, {"status": status, "approved_by": by, "trust": trust})
        self.svc.audit.record(by, "memory.decide", memory_id, status)
        return self.get(memory_id)

    def get(self, memory_id: str) -> dict | None:
        r = self.svc.db.query_one("SELECT * FROM memory WHERE id=?", (memory_id,))
        if r:
            r["provenance"] = loads(r["provenance"], {})
            r["flags"] = loads(r["flags"], {})
            r.pop("terms", None)
        return r

    def list(self, status: str | None = None, tenant: str | None = None, limit: int = 100) -> list[dict]:
        conds, params = [], []
        if status:
            conds.append("status=?")
            params.append(status)
        if tenant:
            conds.append("tenant IN (?, '*')")
            params.append(tenant)
        where = ("WHERE " + " AND ".join(conds)) if conds else ""
        rows = self.svc.db.query(f"SELECT * FROM memory {where} ORDER BY created_at DESC LIMIT ?", [*params, limit])
        for r in rows:
            r["provenance"] = loads(r["provenance"], {})
            r["flags"] = loads(r["flags"], {})
            r.pop("terms", None)
        return rows

    def recall(self, query: str, tenant: str, k: int = 3, include_unapproved: bool = False) -> list[dict]:
        statuses = ("approved", "candidate") if include_unapproved else ("approved",)
        qs = ",".join("?" for _ in statuses)
        rows = self.svc.db.query(
            f"SELECT * FROM memory WHERE kind='episodic' AND tenant IN (?, '*') AND status IN ({qs}) AND expires_at > ?",
            [tenant, *statuses, iso(self.svc.clock.real_now())])
        by_id = {r["id"]: r for r in rows}
        ranked = bm25_rank(query, [(r["id"], loads(r["terms"], [])) for r in rows], k=k)
        out = []
        for mid, score in ranked:
            r = by_id[mid]
            self.svc.db.execute("UPDATE memory SET uses=uses+1 WHERE id=?", (mid,))
            out.append({"memory_id": mid, "title": r["title"], "content": r["content"], "trust": r["trust"],
                        "status": r["status"], "provenance": loads(r["provenance"], {}), "score": round(score, 3)})
        return out

    def expire(self) -> int:
        cur = self.svc.db.execute("UPDATE memory SET status='expired' WHERE status IN ('approved','candidate') AND expires_at < ?",
                                  (iso(self.svc.clock.real_now()),))
        return cur.rowcount
