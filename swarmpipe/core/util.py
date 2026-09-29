"""Shared utilities: ids, time, hashing, JSON, runtime flags and the business clock."""
from __future__ import annotations

import dataclasses
import datetime as _dt
import hashlib
import json
import math
import secrets
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

_B36 = "0123456789abcdefghijklmnopqrstuvwxyz"


def _b36(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n, 36)
        s = _B36[r] + s
    return s or "0"


def new_id(prefix: str) -> str:
    """Time-ordered, collision-resistant id, e.g. run_mfz3k2a1b2c3."""
    return f"{prefix}_{_b36(int(time.time() * 1000))}{secrets.token_hex(3)}"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime | None = None) -> str:
    dt = dt or utcnow()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def seconds_between(a: str | None, b: str | None) -> float | None:
    da, db = parse_iso(a), parse_iso(b)
    if not da or not db:
        return None
    return (db - da).total_seconds()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _json_default(o: Any):
    if isinstance(o, (datetime, _dt.date)):
        return o.isoformat()
    if isinstance(o, (set, frozenset)):
        return sorted(o)
    if isinstance(o, Path):
        return str(o)
    if dataclasses.is_dataclass(o) and not isinstance(o, type):
        return dataclasses.asdict(o)
    if hasattr(o, "model_dump"):
        return o.model_dump()
    try:
        import numpy as np

        if isinstance(o, np.integer):
            return int(o)
        if isinstance(o, np.floating):
            v = float(o)
            return None if math.isnan(v) else v
        if isinstance(o, np.bool_):
            return bool(o)
        if isinstance(o, np.ndarray):
            return o.tolist()
    except ImportError:  # pragma: no cover
        pass
    try:
        import pandas as pd

        if isinstance(o, pd.Timestamp):
            return o.isoformat()
        if o is pd.NaT:
            return None
    except ImportError:  # pragma: no cover
        pass
    return str(o)


def _clean_floats(o: Any):
    if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
        return None
    if isinstance(o, dict):
        return {k: _clean_floats(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_clean_floats(v) for v in o]
    return o


def dumps(obj: Any, indent: int | None = None) -> str:
    return json.dumps(_clean_floats(obj), default=_json_default, ensure_ascii=False, indent=indent)


def loads(s: str | None, default: Any = None) -> Any:
    if s is None or s == "":
        return default
    try:
        return json.loads(s)
    except (TypeError, ValueError):
        return default


def canonical_json(obj: Any) -> str:
    return json.dumps(_clean_floats(obj), default=_json_default, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def truncate(text: str, limit: int, marker: str = " …[truncated {n} chars]") -> tuple[str, bool]:
    if text is None:
        return "", False
    if len(text) <= limit:
        return text, False
    cut = len(text) - limit
    return text[:limit] + marker.format(n=cut), True


def backoff_delay(attempt: int, base: float, cap: float) -> float:
    """Exponential backoff with full jitter (AWS architecture blog style)."""
    exp = min(cap, base * (2 ** max(0, attempt - 1)))
    return secrets.SystemRandom().uniform(exp / 2, exp)


def remove_tree(path: Path, attempts: int = 5, delay_s: float = 0.1) -> bool:
    """Delete a directory tree, retrying through Windows "file in use" races (antivirus/indexer scans,
    handles released a moment later). Returns True when the tree is gone."""
    import gc
    import shutil

    p = Path(path)
    for i in range(attempts):
        if not p.exists():
            return True
        shutil.rmtree(p, ignore_errors=True)
        if not p.exists():
            return True
        gc.collect()
        time.sleep(delay_s * (i + 1))
    return not p.exists()


class RuntimeFlags:
    """Runtime-tunable flags stored in the state DB so a CLI in another process can change the
    behaviour of a running server (chaos toggles, clock offset, feature switches)."""

    def __init__(self, db, ttl_s: float = 1.0):
        self.db = db
        self.ttl_s = ttl_s
        self._cache: dict[str, Any] = {}
        self._loaded_at = 0.0
        self._lock = threading.Lock()

    def _refresh(self) -> None:
        now = time.monotonic()
        if now - self._loaded_at < self.ttl_s:
            return
        rows = self.db.query("SELECT key, value FROM runtime_flags")
        with self._lock:
            self._cache = {r["key"]: loads(r["value"]) for r in rows}
            self._loaded_at = now

    def get(self, key: str, default: Any = None) -> Any:
        self._refresh()
        return self._cache.get(key, default)

    def all(self) -> dict[str, Any]:
        self._refresh()
        return dict(self._cache)

    def set(self, key: str, value: Any, by: str = "system") -> None:
        self.db.execute(
            "INSERT INTO runtime_flags(key, value, updated_at, updated_by) VALUES(?,?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at, updated_by=excluded.updated_by",
            (key, dumps(value), iso(), by),
        )
        self._loaded_at = 0.0

    def clear(self, prefix: str = "") -> int:
        cur = self.db.execute("DELETE FROM runtime_flags WHERE key LIKE ?", (prefix + "%",))
        self._loaded_at = 0.0
        return cur.rowcount


class Clock:
    """Business clock. `now()` honours the `clock_offset_min` runtime flag so you can fast-forward
    time to trigger freshness SLAs; engine mechanics (leases, retries) use `real_now()`."""

    def __init__(self, flags: RuntimeFlags | None = None):
        self.flags = flags

    def offset(self) -> timedelta:
        minutes = float(self.flags.get("clock_offset_min", 0) or 0) if self.flags else 0.0
        return timedelta(minutes=minutes)

    def now(self) -> datetime:
        return utcnow() + self.offset()

    def now_iso(self) -> str:
        return iso(self.now())

    @staticmethod
    def real_now() -> datetime:
        return utcnow()

    @staticmethod
    def real_iso(delta_s: float = 0.0) -> str:
        return iso(utcnow() + timedelta(seconds=delta_s))

    def today(self) -> _dt.date:
        return self.now().date()
