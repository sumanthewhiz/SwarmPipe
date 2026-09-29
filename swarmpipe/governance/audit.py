"""Tamper-evident audit log (unlike telemetry, audit is complete, tamper-evident and long-retained).

Every record stores hash = sha256(prev_hash + canonical(record)). `verify()` recomputes the chain
and pinpoints the first record that was modified, deleted or reordered. Anchor `head()` somewhere
outside this database (a ticket, an evidence pack, a WORM bucket) to also detect truncation."""
from __future__ import annotations

import hashlib
import threading
from pathlib import Path

from swarmpipe.core.util import canonical_json, dumps, iso, loads

GENESIS = "0" * 64
_FIELDS = ("seq", "ts", "actor", "actor_type", "on_behalf_of", "action", "resource", "decision", "details", "trace_id")


def _actor_type(actor: str) -> str:
    return actor.split(":", 1)[0] if ":" in actor else "system"


def _hash(prev: str, row: dict) -> str:
    body = canonical_json({k: row.get(k) for k in _FIELDS})
    return hashlib.sha256((prev + body).encode("utf-8")).hexdigest()


class AuditLog:
    def __init__(self, db):
        self.db = db
        self._lock = threading.Lock()

    def record(self, actor: str, action: str, resource: str = "", decision: str = "ok", details: dict | None = None,
               on_behalf_of: str | None = None, trace_id: str | None = None) -> int:
        if trace_id is None:
            from swarmpipe.observability.tracing import Tracer

            span = Tracer.current()
            trace_id = span.trace_id if span else None
        with self._lock, self.db.tx():
            last = self.db.query_one("SELECT seq, hash FROM audit ORDER BY seq DESC LIMIT 1")
            seq = (last["seq"] + 1) if last else 1
            prev = last["hash"] if last else GENESIS
            row = {"seq": seq, "ts": iso(), "actor": actor, "actor_type": _actor_type(actor), "on_behalf_of": on_behalf_of,
                   "action": action, "resource": resource, "decision": decision, "details": dumps(details or {}),
                   "trace_id": trace_id}
            row["prev_hash"] = prev
            row["hash"] = _hash(prev, row)
            self.db.insert("audit", row)
        return seq

    def head(self) -> dict:
        return self.db.query_one("SELECT seq, hash, ts FROM audit ORDER BY seq DESC LIMIT 1") or {"seq": 0, "hash": GENESIS}

    def verify(self) -> dict:
        prev = GENESIS
        expected_seq = 1
        checked = 0
        for row in self.db.query("SELECT * FROM audit ORDER BY seq"):
            if row["seq"] != expected_seq:
                return {"ok": False, "checked": checked, "first_bad_seq": expected_seq,
                        "reason": f"gap in sequence: expected {expected_seq}, found {row['seq']} (record deleted?)"}
            if row["prev_hash"] != prev:
                return {"ok": False, "checked": checked, "first_bad_seq": row["seq"], "reason": "prev_hash does not link to previous record"}
            if _hash(prev, row) != row["hash"]:
                return {"ok": False, "checked": checked, "first_bad_seq": row["seq"], "reason": "record content does not match its hash (modified)"}
            prev = row["hash"]
            expected_seq += 1
            checked += 1
        return {"ok": True, "checked": checked, "head": prev}

    def query(self, limit: int = 100, actor: str | None = None, action: str | None = None,
              resource_like: str | None = None, trace_id: str | None = None) -> list[dict]:
        conds, params = [], []
        if actor:
            conds.append("actor LIKE ?")
            params.append(actor + "%")
        if action:
            conds.append("action LIKE ?")
            params.append(action + "%")
        if resource_like:
            conds.append("resource LIKE ?")
            params.append(f"%{resource_like}%")
        if trace_id:
            conds.append("trace_id = ?")
            params.append(trace_id)
        where = ("WHERE " + " AND ".join(conds)) if conds else ""
        rows = self.db.query(f"SELECT * FROM audit {where} ORDER BY seq DESC LIMIT ?", [*params, limit])
        for r in rows:
            r["details"] = loads(r["details"], {})
        return rows

    def export_jsonl(self, path: Path) -> int:
        rows = self.db.query("SELECT * FROM audit ORDER BY seq")
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(dumps(r) + "\n")
        return len(rows)
