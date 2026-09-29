"""Versioned dataset storage in a separate SQLite "warehouse".

- Every publish writes an immutable version table `t__<tenant>__<dataset>__v<N>`.
- The consumer-facing name is a view `<tenant>__<dataset>`; publishing = atomically re-pointing the
  view (blue/green). Rollback is just re-pointing to an older version (the saga compensation).
- Quarantined batches are kept as `q__...` tables for forensics or a governed force-publish.
- `readonly_query` is the Analyst agent's only way in: read-only connection + SQLite authorizer +
  per-tenant temp views + row limit + VM-step budget (prevents unbounded queries)."""
from __future__ import annotations

import hashlib
import re
import sqlite3
import threading
from pathlib import Path

import pandas as pd

from swarmpipe.core.db import ConnectionTracker, TrackedConnection

_IDENT = re.compile(r"[^a-z0-9_]")
_SAFE_FUNCS = {"count", "sum", "avg", "min", "max", "round", "abs", "lower", "upper", "length", "substr", "coalesce",
               "ifnull", "date", "strftime", "julianday", "total", "group_concat", "cast", "trim", "replace", "instr",
               "nullif", "printf", "datetime", "iif"}


def ident(s: str) -> str:
    return _IDENT.sub("_", s.lower())


class Warehouse:
    def __init__(self, path: str | Path):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.snap_path = str(Path(self.path).with_name("snapshots.db"))
        self._local = threading.local()
        self._lock = threading.RLock()
        self._tracker = ConnectionTracker()

    def conn(self) -> sqlite3.Connection:
        c = getattr(self._local, "conn", None)
        if c is None or getattr(self._local, "gen", None) != self._tracker.generation:
            c = sqlite3.connect(self.path, timeout=30, isolation_level=None, check_same_thread=False, factory=TrackedConnection)
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA busy_timeout=30000")
            c.execute("ATTACH DATABASE ? AS snap", (self.snap_path,))
            self._local.conn, self._local.gen = c, self._tracker.generation
            self._tracker.add(c)
        return c

    def close_all(self) -> int:
        """Close every warehouse connection opened on any thread (releases warehouse.db and snapshots.db)."""
        self._local.conn = None
        return self._tracker.close_all()

    # ---- immutable snapshots ("WORM" copies used to restore tampered tables) ------------------
    def snapshot(self, table: str) -> None:
        c = self.conn()
        with self._lock:
            c.execute(f'DROP TABLE IF EXISTS snap."{table}"')
            c.execute(f'CREATE TABLE snap."{table}" AS SELECT * FROM main."{table}" ORDER BY rowid')

    def has_snapshot(self, table: str) -> bool:
        return self.conn().execute("SELECT 1 FROM snap.sqlite_master WHERE name=?", (table,)).fetchone() is not None

    def restore(self, table: str) -> bool:
        if not self.has_snapshot(table):
            return False
        c = self.conn()
        with self._lock:
            c.execute("BEGIN IMMEDIATE")
            try:
                c.execute(f'DROP TABLE IF EXISTS main."{table}"')
                c.execute(f'CREATE TABLE main."{table}" AS SELECT * FROM snap."{table}" ORDER BY rowid')
                c.execute("COMMIT")
            except BaseException:
                c.execute("ROLLBACK")
                raise
        return True

    def copy_table(self, src: str, dst: str) -> None:
        c = self.conn()
        with self._lock:
            c.execute(f'DROP TABLE IF EXISTS main."{dst}"')
            c.execute(f'CREATE TABLE main."{dst}" AS SELECT * FROM main."{src}" ORDER BY rowid')

    @staticmethod
    def table_name(tenant: str, dataset: str, version: int, quarantined: bool = False) -> str:
        return f"{'q' if quarantined else 't'}__{ident(tenant)}__{ident(dataset)}__v{int(version)}"

    @staticmethod
    def view_name(tenant: str, dataset: str) -> str:
        return f"{ident(tenant)}__{ident(dataset)}"

    def exists(self, name: str) -> bool:
        r = self.conn().execute("SELECT 1 FROM main.sqlite_master WHERE name=?", (name,)).fetchone()
        return r is not None

    def write_table(self, table: str, df: pd.DataFrame) -> int:
        with self._lock:
            df.to_sql(table, self.conn(), index=False, if_exists="replace", chunksize=2000)
        return len(df)

    def drop_table(self, table: str) -> None:
        with self._lock:
            self.conn().execute(f'DROP TABLE IF EXISTS main."{table}"')

    def swap_view(self, tenant: str, dataset: str, table: str) -> None:
        view = self.view_name(tenant, dataset)
        c = self.conn()
        with self._lock:
            c.execute("BEGIN IMMEDIATE")
            try:
                c.execute(f'DROP VIEW IF EXISTS main."{view}"')
                c.execute(f'CREATE VIEW main."{view}" AS SELECT * FROM "{table}"')
                c.execute("COMMIT")
            except BaseException:
                c.execute("ROLLBACK")
                raise

    def drop_view(self, tenant: str, dataset: str) -> None:
        with self._lock:
            self.conn().execute(f'DROP VIEW IF EXISTS main."{self.view_name(tenant, dataset)}"')

    def view_target(self, tenant: str, dataset: str) -> str | None:
        r = self.conn().execute("SELECT sql FROM main.sqlite_master WHERE type='view' AND name=?",
                                (self.view_name(tenant, dataset),)).fetchone()
        if not r:
            return None
        m = re.search(r'FROM\s+"([^"]+)"', r[0])
        return m.group(1) if m else None

    def read(self, name: str, columns: list[str] | None = None, limit: int | None = None) -> pd.DataFrame | None:
        if not self.exists(name):
            return None
        cols = ", ".join(f'"{c}"' for c in columns) if columns else "*"
        sql = f'SELECT {cols} FROM "{name}"' + (f" LIMIT {int(limit)}" if limit else "")
        return pd.read_sql_query(sql, self.conn())

    def row_count(self, name: str) -> int | None:
        if not self.exists(name):
            return None
        return self.conn().execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]

    def column_values(self, name: str, column: str) -> set[str] | None:
        if not self.exists(name):
            return None
        try:
            rows = self.conn().execute(f'SELECT DISTINCT "{column}" FROM "{name}"').fetchall()
        except sqlite3.OperationalError:
            return None
        return {str(r[0]) for r in rows if r[0] is not None}

    def checksum(self, name: str) -> str | None:
        """Content fingerprint used by the out-of-band change detector."""
        if not self.exists(name):
            return None
        h = hashlib.sha256()
        n = 0
        for row in self.conn().execute(f'SELECT * FROM "{name}" ORDER BY rowid'):
            h.update(repr(row).encode("utf-8"))
            n += 1
        return f"{n}:{h.hexdigest()[:32]}"

    def tables(self) -> list[dict]:
        return [{"name": r[0], "type": r[1]} for r in
                self.conn().execute("SELECT name, type FROM main.sqlite_master WHERE type IN ('table','view') ORDER BY name")]

    # ---- the analyst's governed read path ---------------------------------------------------
    def readonly_query(self, sql: str, tenant: str, aliases: dict[str, str], max_rows: int = 200,
                       max_vm_steps: int = 2_000_000) -> dict:
        uri = f"file:{Path(self.path).as_posix()}?mode=ro"
        con = sqlite3.connect(uri, uri=True, timeout=10, check_same_thread=False)
        try:
            for alias, view in aliases.items():
                con.execute(f'CREATE TEMP VIEW "{ident(alias)}" AS SELECT * FROM main."{view}"')
            prefix_t, prefix_v = f"t__{ident(tenant)}__", f"{ident(tenant)}__"
            allowed = {ident(a) for a in aliases} | set(aliases.values())
            denied: list[str] = []

            def authorizer(action, arg1, arg2, dbname, source):
                if action in (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_RECURSIVE):
                    return sqlite3.SQLITE_OK
                if action == sqlite3.SQLITE_READ:
                    t = arg1 or ""
                    if t in allowed or t.startswith(prefix_t) or t.startswith(prefix_v):
                        return sqlite3.SQLITE_OK
                    denied.append(f"read {t}")
                    return sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_FUNCTION:
                    if (arg2 or "").lower() in _SAFE_FUNCS:
                        return sqlite3.SQLITE_OK
                    denied.append(f"function {arg2}")
                    return sqlite3.SQLITE_DENY
                denied.append(f"action {action}")
                return sqlite3.SQLITE_DENY

            con.set_authorizer(authorizer)
            steps = {"n": 0}

            def progress():
                steps["n"] += 1
                return 1 if steps["n"] * 1000 > max_vm_steps else 0

            con.set_progress_handler(progress, 1000)
            try:
                cur = con.execute(sql)
            except sqlite3.DatabaseError as exc:
                return {"ok": False, "error": str(exc), "denied": denied}
            cols = [d[0] for d in cur.description or []]
            try:
                rows = cur.fetchmany(max_rows + 1)
            except sqlite3.OperationalError as exc:
                return {"ok": False, "error": f"query aborted: {exc}", "denied": denied}
            truncated = len(rows) > max_rows
            return {"ok": True, "columns": cols, "rows": [list(r) for r in rows[:max_rows]], "truncated": truncated,
                    "vm_steps": steps["n"] * 1000}
        finally:
            con.close()
