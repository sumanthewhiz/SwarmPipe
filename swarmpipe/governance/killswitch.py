"""Kill switch (incident response). Scopes: `global`, `agent:<id>`, `tenant:<t>`,
`action:<class>`, `llm`. Checked by the tool gateway, the model gateway and the policy engine, so a
single command stops all agent side effects immediately while deterministic ingestion continues."""
from __future__ import annotations

import time

from swarmpipe.core.util import iso


class KillSwitch:
    def __init__(self, db, audit, ttl_s: float = 1.0):
        self.db = db
        self.audit = audit
        self.ttl_s = ttl_s
        self._cache: dict[str, dict] = {}
        self._loaded = 0.0

    def _refresh(self) -> None:
        if time.monotonic() - self._loaded < self.ttl_s:
            return
        self._cache = {r["scope"]: r for r in self.db.query("SELECT * FROM kill_switches WHERE enabled=1")}
        self._loaded = time.monotonic()

    def set(self, scope: str, enabled: bool, reason: str = "", by: str = "user:admin") -> None:
        self.db.execute(
            "INSERT INTO kill_switches(scope, enabled, reason, set_by, set_at) VALUES(?,?,?,?,?) "
            "ON CONFLICT(scope) DO UPDATE SET enabled=excluded.enabled, reason=excluded.reason, set_by=excluded.set_by, set_at=excluded.set_at",
            (scope, 1 if enabled else 0, reason, by, iso()))
        self._loaded = 0.0
        self.audit.record(by, "killswitch.set", scope, "engaged" if enabled else "released", {"reason": reason})

    def engaged(self, *, agent: str | None = None, tenant: str | None = None, action: str | None = None,
                llm: bool = False) -> str | None:
        """Return the engaged scope that blocks this operation, or None."""
        self._refresh()
        candidates = ["global"]
        if agent:
            candidates.append(f"agent:{agent}")
        if tenant:
            candidates.append(f"tenant:{tenant}")
        if action:
            candidates.append(f"action:{action}")
        if llm:
            candidates.append("llm")
        for c in candidates:
            if c in self._cache:
                return c
        return None

    def active(self) -> list[dict]:
        self._refresh()
        return list(self._cache.values())
