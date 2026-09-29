"""Event bus on an outbox table + a polling dispatcher with per-consumer offsets.

Delivery is at-least-once: a consumer's offset only advances after its handler succeeds, so every
handler must be idempotent (at-least-once delivery + idempotent handlers = effectively once). A poison
event is skipped after 3 failed deliveries so it cannot block the stream forever."""
from __future__ import annotations

import logging
import threading
from typing import Callable

from swarmpipe.core.util import dumps, iso, loads

log = logging.getLogger("swarmpipe.events")


class EventBus:
    def __init__(self, db):
        self.db = db

    def publish(self, type_: str, payload: dict, tenant: str | None = None, trace_id: str | None = None) -> int:
        cur = self.db.execute(
            "INSERT INTO events(type, payload, tenant, trace_id, created_at) VALUES(?,?,?,?,?)",
            (type_, dumps(payload), tenant, trace_id, iso()),
        )
        return cur.lastrowid

    def offset(self, consumer: str) -> int:
        return self.db.scalar("SELECT last_id FROM event_offsets WHERE consumer=?", (consumer,), default=0)

    def read(self, consumer: str, types: list[str] | None = None, limit: int = 100) -> list[dict]:
        last = self.offset(consumer)
        if types:
            qs = ",".join("?" for _ in types)
            rows = self.db.query(
                f"SELECT * FROM events WHERE id > ? AND type IN ({qs}) ORDER BY id LIMIT ?", [last, *types, limit]
            )
        else:
            rows = self.db.query("SELECT * FROM events WHERE id > ? ORDER BY id LIMIT ?", (last, limit))
        for r in rows:
            r["payload"] = loads(r["payload"], {})
        return rows

    def ack(self, consumer: str, event_id: int) -> None:
        self.db.execute(
            "INSERT INTO event_offsets(consumer, last_id) VALUES(?,?) "
            "ON CONFLICT(consumer) DO UPDATE SET last_id=MAX(last_id, excluded.last_id)",
            (consumer, event_id),
        )

    def skip_to_latest(self, consumer: str) -> None:
        latest = self.db.scalar("SELECT MAX(id) FROM events", default=0)
        self.ack(consumer, latest)


class Dispatcher:
    def __init__(self, bus: EventBus):
        self.bus = bus
        self._subs: list[tuple[str, list[str], Callable[[dict], None]]] = []
        self._failures: dict[tuple[str, int], int] = {}
        self._lock = threading.Lock()

    def subscribe(self, consumer: str, types: list[str], handler: Callable[[dict], None]) -> None:
        self._subs.append((consumer, types, handler))

    def poll_once(self, limit: int = 100) -> int:
        handled = 0
        with self._lock:
            for consumer, types, handler in self._subs:
                for ev in self.bus.read(consumer, types, limit):
                    key = (consumer, ev["id"])
                    try:
                        handler(ev)
                    except Exception as exc:  # noqa: BLE001 - consumer isolation
                        n = self._failures.get(key, 0) + 1
                        self._failures[key] = n
                        log.exception("event handler %s failed on event %s (%s/3)", consumer, ev["id"], n)
                        if n < 3:
                            break
                        log.error("skipping poison event %s for consumer %s: %s", ev["id"], consumer, exc)
                    self.bus.ack(consumer, ev["id"])
                    self._failures.pop(key, None)
                    handled += 1
        return handled
