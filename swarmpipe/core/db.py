"""SQLite state store with thread-local connections, explicit transactions and versioned migrations.

One file holds the control-plane state: runs/steps (durable execution), events (outbox), incidents,
approvals, audit, telemetry, memory, evals. The data plane (published datasets) lives in a separate
warehouse DB so large writes never block control-plane writes."""
from __future__ import annotations

import sqlite3
import threading
import weakref
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable

from swarmpipe.core.util import iso


class TrackedConnection(sqlite3.Connection):
    """Plain sqlite3.Connection objects cannot be weak-referenced; this subclass can, so a pool can track
    every connection it hands out without keeping dead threads' connections alive."""


class ConnectionTracker:
    """Remembers every connection a thread-local pool opened, on any thread, so shutdown can close them all.

    Thread-local storage alone is not enough: a connection opened on a worker/step thread can outlive that
    thread when something keeps it in a reference cycle (e.g. a stored exception traceback), and it is only
    closed when the cyclic GC happens to run. On Windows that open handle blocks deleting/moving the file."""

    def __init__(self):
        self._conns: weakref.WeakSet = weakref.WeakSet()
        self._lock = threading.Lock()
        self.generation = 0

    def add(self, c: sqlite3.Connection) -> None:
        with self._lock:
            self._conns.add(c)

    def discard(self, c: sqlite3.Connection) -> None:
        with self._lock:
            self._conns.discard(c)

    def close_all(self) -> int:
        with self._lock:
            conns, self._conns = list(self._conns), weakref.WeakSet()
            self.generation += 1  # threads still holding an old connection transparently reopen
        closed = 0
        for c in conns:
            try:
                c.close()
                closed += 1
            except sqlite3.Error:
                pass
        return closed

MIGRATIONS: list[tuple[int, str]] = [
    (1, """
CREATE TABLE IF NOT EXISTS runtime_flags (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT, updated_by TEXT);

CREATE TABLE IF NOT EXISTS files (
  id TEXT PRIMARY KEY, tenant TEXT, original_name TEXT, original_path TEXT, staged_path TEXT,
  size INTEGER, sha256 TEXT, detected_at TEXT, status TEXT, run_id TEXT, duplicate_of TEXT, final_path TEXT);
CREATE INDEX IF NOT EXISTS idx_files_sha ON files(tenant, sha256);

CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY, workflow TEXT, tenant TEXT, status TEXT, priority INTEGER DEFAULT 5,
  input TEXT, context TEXT, current_step TEXT, attempt INTEGER DEFAULT 0,
  next_attempt_at TEXT, lease_owner TEXT, lease_expires_at TEXT, trace_id TEXT, parent_run_id TEXT,
  dataset TEXT, file_id TEXT, incident_id TEXT, error TEXT, recovered_count INTEGER DEFAULT 0,
  waiting_on TEXT, created_at TEXT, updated_at TEXT, started_at TEXT, finished_at TEXT, output TEXT);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_runs_parent ON runs(parent_run_id);

CREATE TABLE IF NOT EXISTS steps (
  run_id TEXT, name TEXT, status TEXT, attempt INTEGER, kind TEXT, started_at TEXT, finished_at TEXT,
  duration_ms REAL, output TEXT, error TEXT, PRIMARY KEY (run_id, name));
CREATE TABLE IF NOT EXISTS step_attempts (
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, step TEXT, attempt INTEGER, status TEXT, error TEXT,
  started_at TEXT, finished_at TEXT, duration_ms REAL);
CREATE INDEX IF NOT EXISTS idx_step_attempts_run ON step_attempts(run_id);

CREATE TABLE IF NOT EXISTS idempotency (key TEXT PRIMARY KEY, scope TEXT, result TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS compensations (
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, step TEXT, action TEXT, params TEXT, status TEXT,
  created_at TEXT, executed_at TEXT, error TEXT);
CREATE TABLE IF NOT EXISTS locks (name TEXT PRIMARY KEY, owner TEXT, expires_at TEXT);
CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT, payload TEXT, tenant TEXT, trace_id TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS event_offsets (consumer TEXT PRIMARY KEY, last_id INTEGER);
CREATE TABLE IF NOT EXISTS dlq (id TEXT PRIMARY KEY, run_id TEXT, file_id TEXT, tenant TEXT, reason TEXT, error TEXT, path TEXT, created_at TEXT, redriven_at TEXT);

CREATE TABLE IF NOT EXISTS contract_versions (
  dataset TEXT, version INTEGER, contract TEXT, status TEXT, created_at TEXT, created_by TEXT, approved_by TEXT, change_note TEXT,
  PRIMARY KEY (dataset, version));
CREATE TABLE IF NOT EXISTS dataset_versions (
  id TEXT PRIMARY KEY, tenant TEXT, dataset TEXT, version INTEGER, run_id TEXT, file_id TEXT, content_hash TEXT,
  batch_rows INTEGER, row_count INTEGER, table_name TEXT, schema TEXT, profile TEXT, status TEXT, contract_version INTEGER,
  checksum TEXT, warnings INTEGER DEFAULT 0, created_at TEXT, published_at TEXT, note TEXT);
CREATE INDEX IF NOT EXISTS idx_dsv ON dataset_versions(tenant, dataset, version);
CREATE TABLE IF NOT EXISTS dataset_state (
  tenant TEXT, dataset TEXT, published_version_id TEXT, last_success_at TEXT, last_arrival_at TEXT,
  hold INTEGER DEFAULT 0, hold_reason TEXT, hold_by TEXT, freshness_alerted_at TEXT, PRIMARY KEY (tenant, dataset));
CREATE TABLE IF NOT EXISTS check_results (
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, tenant TEXT, dataset TEXT, version_id TEXT, check_name TEXT, check_type TEXT,
  status TEXT, severity TEXT, observed TEXT, expected TEXT, details TEXT, created_at TEXT);
CREATE INDEX IF NOT EXISTS idx_checks_run ON check_results(run_id);
CREATE TABLE IF NOT EXISTS quarantine_rows (
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, tenant TEXT, dataset TEXT, row_number INTEGER, reason TEXT, data TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS pii_vault (token TEXT PRIMARY KEY, tenant TEXT, pii_type TEXT, value TEXT, created_at TEXT);

CREATE TABLE IF NOT EXISTS lineage_events (id INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT, event_time TEXT, run_id TEXT, job_name TEXT, payload TEXT);
CREATE TABLE IF NOT EXISTS lineage_edges (src TEXT, dst TEXT, kind TEXT, tenant TEXT, first_seen TEXT, last_seen TEXT, PRIMARY KEY (src, dst, kind, tenant));

CREATE TABLE IF NOT EXISTS documents (
  id TEXT PRIMARY KEY, tenant TEXT, file_id TEXT, title TEXT, doc_type TEXT, summary TEXT, trust TEXT, status TEXT,
  source TEXT, flags TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS chunks (id TEXT PRIMARY KEY, doc_id TEXT, tenant TEXT, seq INTEGER, text TEXT, terms TEXT, n_terms INTEGER);
CREATE INDEX IF NOT EXISTS idx_chunks_doc ON chunks(doc_id);

CREATE TABLE IF NOT EXISTS signals (
  id TEXT PRIMARY KEY, tenant TEXT, type TEXT, dataset TEXT, severity TEXT, summary TEXT, details TEXT, run_id TEXT,
  incident_id TEXT, correlation_key TEXT, created_at TEXT);
CREATE INDEX IF NOT EXISTS idx_signals_incident ON signals(incident_id);
CREATE TABLE IF NOT EXISTS incidents (
  id TEXT PRIMARY KEY, tenant TEXT, dataset TEXT, status TEXT, severity TEXT, title TEXT, correlation_key TEXT,
  trace_id TEXT, triage_run_id TEXT, diagnosis TEXT, impact TEXT, postmortem TEXT, created_at TEXT, updated_at TEXT,
  first_signal_at TEXT, diagnosed_at TEXT, resolved_at TEXT, cost_usd REAL DEFAULT 0, signal_count INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS blackboard (
  id INTEGER PRIMARY KEY AUTOINCREMENT, incident_id TEXT, version INTEGER, author TEXT, kind TEXT, content TEXT,
  evidence_ids TEXT, created_at TEXT);
CREATE INDEX IF NOT EXISTS idx_blackboard_incident ON blackboard(incident_id);
CREATE TABLE IF NOT EXISTS evidence (
  id TEXT PRIMARY KEY, incident_id TEXT, run_id TEXT, tool TEXT, tool_version TEXT, args TEXT, content TEXT, trust TEXT,
  created_by TEXT, created_at TEXT);
CREATE INDEX IF NOT EXISTS idx_evidence_incident ON evidence(incident_id);
CREATE TABLE IF NOT EXISTS proposals (
  id TEXT PRIMARY KEY, incident_id TEXT, tenant TEXT, dataset TEXT, action TEXT, params TEXT, rank INTEGER, rationale TEXT,
  citations TEXT, risk TEXT, blast_radius INTEGER, autonomy_level TEXT, policy_effect TEXT, policy_details TEXT, status TEXT,
  proposed_by TEXT, approval_id TEXT, executed_by TEXT, on_behalf_of TEXT, idempotency_key TEXT, result TEXT,
  verification TEXT, created_at TEXT, decided_at TEXT, executed_at TEXT, verified_at TEXT);
CREATE INDEX IF NOT EXISTS idx_proposals_incident ON proposals(incident_id);
CREATE TABLE IF NOT EXISTS approvals (
  id TEXT PRIMARY KEY, kind TEXT, proposal_id TEXT, incident_id TEXT, run_id TEXT, tenant TEXT, subject TEXT, risk TEXT,
  summary TEXT, payload TEXT, status TEXT, requires_confirmation TEXT, requested_at TEXT, expires_at TEXT,
  decided_at TEXT, decided_by TEXT, comment TEXT);
CREATE TABLE IF NOT EXISTS notifications (
  id TEXT PRIMARY KEY, tenant TEXT, channel TEXT, recipient TEXT, subject TEXT, body TEXT, status TEXT, incident_id TEXT,
  path TEXT, created_at TEXT);

CREATE TABLE IF NOT EXISTS audit (
  seq INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, actor TEXT, actor_type TEXT, on_behalf_of TEXT, action TEXT,
  resource TEXT, decision TEXT, details TEXT, trace_id TEXT, prev_hash TEXT, hash TEXT);
CREATE TABLE IF NOT EXISTS autonomy (
  tenant TEXT, action_class TEXT, level TEXT, proposals INTEGER DEFAULT 0, approved INTEGER DEFAULT 0,
  rejected INTEGER DEFAULT 0, executed INTEGER DEFAULT 0, verified_ok INTEGER DEFAULT 0, verified_fail INTEGER DEFAULT 0,
  rolled_back INTEGER DEFAULT 0, human_agreed INTEGER DEFAULT 0, human_disagreed INTEGER DEFAULT 0, updated_at TEXT,
  reason TEXT, PRIMARY KEY (tenant, action_class));
CREATE TABLE IF NOT EXISTS autonomy_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT, tenant TEXT, action_class TEXT, from_level TEXT, to_level TEXT, reason TEXT,
  changed_by TEXT, ts TEXT);
CREATE TABLE IF NOT EXISTS kill_switches (scope TEXT PRIMARY KEY, enabled INTEGER, reason TEXT, set_by TEXT, set_at TEXT);
CREATE TABLE IF NOT EXISTS agent_registry (
  agent_id TEXT, version TEXT, status TEXT, owner TEXT, card TEXT, card_hash TEXT, registered_at TEXT, suspended_reason TEXT,
  PRIMARY KEY (agent_id, version));
CREATE TABLE IF NOT EXISTS model_certifications (
  model TEXT, role TEXT, status TEXT, scores TEXT, eval_run_id TEXT, certified_at TEXT, PRIMARY KEY (model, role));

CREATE TABLE IF NOT EXISTS memory (
  id TEXT PRIMARY KEY, tenant TEXT, kind TEXT, title TEXT, content TEXT, terms TEXT, provenance TEXT, trust TEXT,
  status TEXT, created_at TEXT, expires_at TEXT, approved_by TEXT, uses INTEGER DEFAULT 0, flags TEXT);

CREATE TABLE IF NOT EXISTS spans (
  span_id TEXT PRIMARY KEY, trace_id TEXT, parent_span_id TEXT, name TEXT, kind TEXT, start_ts REAL, end_ts REAL,
  duration_ms REAL, status TEXT, attributes TEXT, events TEXT);
CREATE INDEX IF NOT EXISTS idx_spans_trace ON spans(trace_id);
CREATE TABLE IF NOT EXISTS llm_calls (
  id TEXT PRIMARY KEY, ts TEXT, trace_id TEXT, span_id TEXT, run_id TEXT, incident_id TEXT, tenant TEXT, agent TEXT,
  role TEXT, model TEXT, provider TEXT, prompt_id TEXT, prompt_version TEXT, prompt_hash TEXT, input_tokens INTEGER,
  output_tokens INTEGER, cost_usd REAL, latency_ms REAL, cached INTEGER, status TEXT, error TEXT, attempt INTEGER,
  purpose TEXT);
CREATE INDEX IF NOT EXISTS idx_llm_calls_run ON llm_calls(run_id);
CREATE INDEX IF NOT EXISTS idx_llm_calls_incident ON llm_calls(incident_id);
CREATE TABLE IF NOT EXISTS llm_cache (key TEXT PRIMARY KEY, model TEXT, response TEXT, created_at REAL, hits INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS tool_calls (
  id TEXT PRIMARY KEY, ts TEXT, trace_id TEXT, agent TEXT, tool TEXT, tool_version TEXT, args TEXT, ok INTEGER,
  error_code TEXT, latency_ms REAL, output_chars INTEGER, truncated INTEGER, evidence_id TEXT, incident_id TEXT, run_id TEXT);
CREATE TABLE IF NOT EXISTS metric_points (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, labels TEXT, value REAL, ts REAL);
CREATE INDEX IF NOT EXISTS idx_metric_name ON metric_points(name, ts);
CREATE TABLE IF NOT EXISTS quotas (tenant TEXT, day TEXT, llm_requests INTEGER DEFAULT 0, cost_usd REAL DEFAULT 0, PRIMARY KEY (tenant, day));

CREATE TABLE IF NOT EXISTS eval_runs (id TEXT PRIMARY KEY, suite TEXT, started_at TEXT, finished_at TEXT, config TEXT, summary TEXT, passed INTEGER, report_path TEXT);
CREATE TABLE IF NOT EXISTS eval_results (id INTEGER PRIMARY KEY AUTOINCREMENT, eval_run_id TEXT, case_id TEXT, trial INTEGER, passed INTEGER, scores TEXT, details TEXT);
CREATE TABLE IF NOT EXISTS eval_candidates (id TEXT PRIMARY KEY, incident_id TEXT, case_json TEXT, status TEXT, created_at TEXT, reviewed_by TEXT);
CREATE TABLE IF NOT EXISTS shadow_comparisons (id INTEGER PRIMARY KEY AUTOINCREMENT, incident_id TEXT, agent TEXT, production TEXT, candidate TEXT, agreed INTEGER, created_at TEXT);
CREATE TABLE IF NOT EXISTS feedback (id INTEGER PRIMARY KEY AUTOINCREMENT, incident_id TEXT, user TEXT, rating INTEGER, correct_category TEXT, comment TEXT, created_at TEXT);
"""),
]


class Database:
    def __init__(self, path: str | Path):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._write_lock = threading.RLock()
        self._tracker = ConnectionTracker()

    # ---- connections --------------------------------------------------------------------
    def conn(self) -> sqlite3.Connection:
        c = getattr(self._local, "conn", None)
        if c is None or getattr(self._local, "gen", None) != self._tracker.generation:
            c = sqlite3.connect(self.path, timeout=30, isolation_level=None, check_same_thread=False, factory=TrackedConnection)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=NORMAL")
            c.execute("PRAGMA busy_timeout=30000")
            c.execute("PRAGMA foreign_keys=ON")
            self._local.conn, self._local.gen = c, self._tracker.generation
            self._tracker.add(c)
        return c

    def close(self) -> None:
        """Close the calling thread's connection."""
        c = getattr(self._local, "conn", None)
        if c is not None:
            self._tracker.discard(c)
            c.close()
            self._local.conn = None

    def close_all(self) -> int:
        """Close every connection this pool opened on any thread (shutdown, or before moving/deleting the file)."""
        self._local.conn = None
        return self._tracker.close_all()

    # ---- statements ---------------------------------------------------------------------
    def execute(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Cursor:
        return self.conn().execute(sql, tuple(params))

    def executemany(self, sql: str, rows: Iterable[Iterable[Any]]) -> sqlite3.Cursor:
        return self.conn().executemany(sql, [tuple(r) for r in rows])

    def query(self, sql: str, params: Iterable[Any] = ()) -> list[dict]:
        return [dict(r) for r in self.conn().execute(sql, tuple(params)).fetchall()]

    def query_one(self, sql: str, params: Iterable[Any] = ()) -> dict | None:
        r = self.conn().execute(sql, tuple(params)).fetchone()
        return dict(r) if r else None

    def scalar(self, sql: str, params: Iterable[Any] = (), default: Any = None) -> Any:
        r = self.conn().execute(sql, tuple(params)).fetchone()
        return r[0] if r and r[0] is not None else default

    @contextmanager
    def tx(self):
        """BEGIN IMMEDIATE transaction: takes the write lock up-front (no upgrade deadlocks)."""
        c = self.conn()
        if c.in_transaction:
            yield c
            return
        c.execute("BEGIN IMMEDIATE")
        try:
            yield c
            c.execute("COMMIT")
        except BaseException:
            c.execute("ROLLBACK")
            raise

    def insert(self, table: str, row: dict) -> None:
        cols = ",".join(row.keys())
        qs = ",".join("?" for _ in row)
        self.execute(f"INSERT INTO {table} ({cols}) VALUES ({qs})", list(row.values()))

    def update(self, table: str, where: dict, values: dict) -> int:
        sets = ",".join(f"{k}=?" for k in values)
        conds = " AND ".join(f"{k}=?" for k in where)
        cur = self.execute(f"UPDATE {table} SET {sets} WHERE {conds}", list(values.values()) + list(where.values()))
        return cur.rowcount

    # ---- migrations ---------------------------------------------------------------------
    def migrate(self) -> int:
        c = self.conn()
        c.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY, applied_at TEXT)")
        current = self.scalar("SELECT MAX(version) FROM schema_version", default=0)
        applied = 0
        for version, script in MIGRATIONS:
            if version <= current:
                continue
            c.executescript(script)
            c.execute("INSERT INTO schema_version(version, applied_at) VALUES(?,?)", (version, iso()))
            applied += 1
        return applied

    def schema_version(self) -> int:
        return self.scalar("SELECT MAX(version) FROM schema_version", default=0)
