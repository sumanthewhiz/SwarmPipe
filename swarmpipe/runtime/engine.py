"""Durable workflow engine: durable execution, state and reliability.

- Runs and steps are persisted in SQLite. Every completed step is a checkpoint with its recorded
  output; on resume, completed steps are skipped and their recorded outputs reused - an LLM step is
  never blindly replayed (it could answer differently).
- Work is claimed with a lease (owner + expiry) and kept alive by a heartbeat. If a worker dies,
  the lease expires and the reaper hands the run to another worker (crash recovery). A worker that
  loses its lease stops before the next step (fencing), so two workers never both run a run.
- Retries: RetryableError -> exponential backoff with jitter up to max_attempts; Deferred ->
  reschedule without consuming an attempt (e.g. a dataset lock is busy).
- Timeouts: agent steps run in a watchdog thread; on timeout the step is retried. The abandoned
  thread may still finish its work, which is exactly why side effects use idempotency keys.
- Human interrupts and fan-in: WaitingFor("approval:<id>" | "children" | "run:<id>") parks the run
  durably until the event arrives (even across restarts).
- Saga: steps register compensations; on permanent failure they run in reverse order.
- Permanent failure -> workflow.on_failure hook (e.g. dead-letter the file).
"""
from __future__ import annotations

import contextvars
import os
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Callable

from swarmpipe.core.errors import Deferred, PermanentError, RetryableError, StepTimeout, StopWorkflow, WaitingFor
from swarmpipe.core.util import Clock, backoff_delay, dumps, iso, loads, new_id
from swarmpipe.observability.logging import get_logger, log_context

log = get_logger("engine")
TERMINAL = {"succeeded", "failed", "dead_lettered", "cancelled", "skipped", "quarantined", "handed_off", "partial", "rejected", "held", "blocked"}


@dataclass
class RetryPolicy:
    max_attempts: int = 3
    base_s: float = 1.0
    max_s: float = 20.0


@dataclass
class Step:
    name: str
    fn: Callable
    kind: str = "deterministic"
    retry: RetryPolicy | None = None
    timeout_s: float | None = None
    when: Callable | None = None


@dataclass
class Workflow:
    name: str
    steps: list[Step]
    description: str = ""
    on_failure: Callable | None = None
    on_finish: Callable | None = None


class LeaseLost(Exception):
    pass


class _Heartbeat:
    def __init__(self, engine: "Engine", run_id: str):
        self.engine, self.run_id = engine, run_id
        self.lost = False
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._loop, daemon=True, name=f"hb-{run_id}")

    def _loop(self):
        s = self.engine.svc.settings.engine
        while not self._stop.wait(s.heartbeat_s):
            n = self.engine.svc.db.execute("UPDATE runs SET lease_expires_at=? WHERE id=? AND lease_owner=? AND status='running'",
                                           (Clock.real_iso(s.lease_s), self.run_id, self.engine.worker_id)).rowcount
            if n == 0:
                self.lost = True
                return

    def start(self):
        self._t.start()
        return self

    def stop(self):
        self._stop.set()


@dataclass
class StepContext:
    engine: "Engine"
    run: dict
    workflow: Workflow
    outputs: dict = field(default_factory=dict)
    context: dict = field(default_factory=dict)
    cache: dict = field(default_factory=dict)
    heartbeat: _Heartbeat | None = None
    result_status: str = "succeeded"

    @property
    def svc(self):
        return self.engine.svc

    @property
    def run_id(self) -> str:
        return self.run["id"]

    @property
    def tenant(self) -> str:
        return self.run["tenant"]

    @property
    def input(self) -> dict:
        return self.run["_input"]

    def set(self, key: str, value) -> None:
        self.context[key] = value
        self.svc.db.execute("UPDATE runs SET context=? WHERE id=?", (dumps(self.context), self.run_id))

    def set_result(self, status: str) -> None:
        self.result_status = status
        self.set("result_status", status)

    def idempotent(self, key: str, fn: Callable[[], dict]) -> dict:
        row = self.svc.db.query_one("SELECT result FROM idempotency WHERE key=?", (key,))
        if row:
            self.svc.metrics.inc("idempotent_replays_total", key_prefix=key.split(":", 1)[0])
            return loads(row["result"], {})
        result = fn()
        self.svc.db.execute("INSERT OR REPLACE INTO idempotency(key, scope, result, created_at) VALUES(?,?,?,?)",
                            (key, self.workflow.name, dumps(result), iso()))
        return result

    def add_compensation(self, action: str, params: dict, step: str = "") -> None:
        self.svc.db.insert("compensations", {"run_id": self.run_id, "step": step, "action": action, "params": dumps(params),
                                             "status": "pending", "created_at": iso(), "executed_at": None, "error": None})

    def child(self, workflow: str, input_: dict, **kw) -> str:
        return self.engine.submit(workflow, input_, tenant=self.tenant, parent_run_id=self.run_id, trace_id=self.run["trace_id"], **kw)

    def wait(self, waiting_on: str, reason: str = "") -> None:
        raise WaitingFor(waiting_on, reason)

    def stop(self, status: str, reason: str = "", output: dict | None = None) -> None:
        raise StopWorkflow(status, reason, output)


def run_with_timeout(fn: Callable, ctx: StepContext, timeout_s: float | None):
    if not timeout_s:
        return fn(ctx)
    box: dict = {}

    def target():
        try:
            box["v"] = fn(ctx)
        except BaseException as exc:  # noqa: BLE001
            box["e"] = exc

    runner = contextvars.copy_context()
    t = threading.Thread(target=runner.run, args=(target,), daemon=True, name=f"step-{ctx.run_id}")
    t.start()
    t.join(timeout_s)
    if t.is_alive():
        raise StepTimeout(f"step exceeded {timeout_s}s (the abandoned attempt may still finish; idempotency keys protect side effects)")
    if "e" in box:
        raise box["e"]
    return box.get("v")


class Engine:
    def __init__(self, svc):
        self.svc = svc
        self.workflows: dict[str, Workflow] = {}
        self.compensators: dict[str, Callable] = {}
        self.worker_id = f"{socket.gethostname()}-{os.getpid()}"
        self.stop_event = threading.Event()

    def register(self, wf: Workflow) -> None:
        self.workflows[wf.name] = wf

    def register_compensator(self, action: str, fn: Callable) -> None:
        self.compensators[action] = fn

    # ---- submission & claiming ------------------------------------------------------------
    def submit(self, workflow: str, input_: dict, tenant: str, *, dataset: str | None = None, file_id: str | None = None,
               incident_id: str | None = None, parent_run_id: str | None = None, priority: int = 5,
               trace_id: str | None = None, delay_s: float = 0.0) -> str:
        if workflow not in self.workflows:
            raise KeyError(f"unknown workflow {workflow}")
        rid = new_id("run")
        now = iso()
        self.svc.db.insert("runs", {
            "id": rid, "workflow": workflow, "tenant": tenant, "status": "pending", "priority": priority, "input": dumps(input_),
            "context": "{}", "current_step": None, "attempt": 0, "next_attempt_at": Clock.real_iso(delay_s) if delay_s else None,
            "lease_owner": None, "lease_expires_at": None, "trace_id": trace_id or self.svc.tracer.new_trace_id(),
            "parent_run_id": parent_run_id, "dataset": dataset, "file_id": file_id, "incident_id": incident_id, "error": None,
            "recovered_count": 0, "waiting_on": None, "created_at": now, "updated_at": now, "started_at": None,
            "finished_at": None, "output": None})
        self.svc.metrics.inc("runs_submitted_total", workflow=workflow, tenant=tenant)
        return rid

    def claim(self) -> dict | None:
        now = iso()
        with self.svc.db.tx():
            rows = self.svc.db.query(
                "UPDATE runs SET status='running', lease_owner=?, lease_expires_at=?, started_at=COALESCE(started_at, ?), updated_at=? "
                "WHERE id = (SELECT id FROM runs WHERE status IN ('pending','retry_wait') AND (next_attempt_at IS NULL OR next_attempt_at <= ?) "
                "ORDER BY priority ASC, created_at ASC LIMIT 1) RETURNING *",
                (self.worker_id, Clock.real_iso(self.svc.settings.engine.lease_s), now, now, now))
        return rows[0] if rows else None

    def queue_depth(self) -> int:
        return self.svc.db.scalar("SELECT COUNT(*) FROM runs WHERE status IN ('pending','retry_wait','running')", default=0)

    # ---- execution ---------------------------------------------------------------------------
    def execute(self, run: dict) -> str:
        wf = self.workflows[run["workflow"]]
        run["_input"] = loads(run["input"], {})
        ctx = StepContext(self, run, wf, context=loads(run["context"], {}) or {})
        ctx.result_status = ctx.context.get("result_status", "succeeded")
        for s in self.svc.db.query("SELECT name, status, output FROM steps WHERE run_id=? AND status IN ('succeeded','skipped')", (run["id"],)):
            ctx.outputs[s["name"]] = loads(s["output"], {}) or {}
        ctx.heartbeat = _Heartbeat(self, run["id"]).start()
        parent_span = ctx.context.get("root_span_id")
        attrs = {"swarmpipe.run_id": run["id"], "swarmpipe.workflow": wf.name, "swarmpipe.tenant": run["tenant"],
                 "swarmpipe.dataset": run.get("dataset"), "swarmpipe.incident_id": run.get("incident_id"),
                 "swarmpipe.resumed": bool(ctx.outputs), "swarmpipe.recovered_count": run.get("recovered_count")}
        step: Step | None = None
        with log_context(run_id=run["id"], tenant=run["tenant"], workflow=wf.name, incident_id=run.get("incident_id")):
            with self.svc.tracer.span(f"workflow {wf.name}" + (" (resumed)" if ctx.outputs else ""), "internal", attrs,
                                      trace_id=run["trace_id"], parent=parent_span) as span:
                if not parent_span:
                    ctx.set("root_span_id", span.span_id)
                try:
                    for step in wf.steps:
                        if step.name in ctx.outputs:
                            continue
                        if ctx.heartbeat.lost:
                            raise LeaseLost(f"lease on {run['id']} lost; another worker owns it now")
                        if step.when and not step.when(ctx):
                            self._record_step(ctx, step, "skipped", {}, None, 0.0, attempt=0)
                            ctx.outputs[step.name] = {}
                            continue
                        self._run_step(ctx, step)
                    return self._finish(ctx, ctx.result_status, {k: v for k, v in ctx.outputs.items() if k in ("summary", "result")})
                except WaitingFor as w:
                    return self._wait(ctx, w)
                except StopWorkflow as s:
                    return self._finish(ctx, s.status, {**s.output, "reason": s.reason}, s.reason)
                except Deferred as d:
                    return self._defer(ctx, d)
                except LeaseLost as exc:
                    log.warning("%s", exc)
                    span.event("lease_lost")
                    return "lease_lost"
                except Exception as exc:  # noqa: BLE001
                    return self._fail_or_retry(ctx, step, exc)
                finally:
                    ctx.heartbeat.stop()

    def _record_step(self, ctx: StepContext, step: Step, status: str, output, error, dur_ms: float, attempt: int) -> None:
        now = iso()
        self.svc.db.execute(
            "INSERT INTO steps(run_id, name, status, attempt, kind, started_at, finished_at, duration_ms, output, error) VALUES(?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(run_id, name) DO UPDATE SET status=excluded.status, attempt=excluded.attempt, finished_at=excluded.finished_at, "
            "duration_ms=excluded.duration_ms, output=excluded.output, error=excluded.error",
            (ctx.run_id, step.name, status, attempt, step.kind, now, now, round(dur_ms, 2), dumps(output) if output is not None else None, error))

    def _run_step(self, ctx: StepContext, step: Step) -> None:
        prev = self.svc.db.query_one("SELECT attempt FROM steps WHERE run_id=? AND name=?", (ctx.run_id, step.name))
        attempt = (prev["attempt"] if prev else 0) + 1
        self.svc.db.execute("UPDATE runs SET current_step=?, updated_at=? WHERE id=?", (step.name, iso(), ctx.run_id))
        started = iso()
        t0 = time.perf_counter()
        timeout = step.timeout_s or (self.svc.settings.engine.step_timeout_s if step.kind == "agent" else None)
        with self.svc.tracer.span(f"step {step.name}", "internal", {"swarmpipe.step": step.name, "swarmpipe.step.kind": step.kind,
                                                                    "swarmpipe.attempt": attempt}):
            try:
                out = run_with_timeout(step.fn, ctx, timeout)
            except (WaitingFor, StopWorkflow, Deferred):
                self.svc.db.execute("INSERT OR IGNORE INTO steps(run_id, name, status, attempt, kind, started_at) VALUES(?,?,?,?,?,?)",
                                    (ctx.run_id, step.name, "waiting", attempt - 1, step.kind, started))
                raise
            except Exception as exc:
                dur = (time.perf_counter() - t0) * 1000
                self._record_step(ctx, step, "failed", None, f"{type(exc).__name__}: {exc}"[:2000], dur, attempt)
                self.svc.db.insert("step_attempts", {"run_id": ctx.run_id, "step": step.name, "attempt": attempt, "status": "failed",
                                                     "error": f"{type(exc).__name__}: {exc}"[:1000], "started_at": started,
                                                     "finished_at": iso(), "duration_ms": round(dur, 2)})
                raise
        dur = (time.perf_counter() - t0) * 1000
        if ctx.heartbeat and ctx.heartbeat.lost:
            raise LeaseLost(f"lease lost while running {step.name}; result discarded")
        self._record_step(ctx, step, "succeeded", out or {}, None, dur, attempt)
        self.svc.db.insert("step_attempts", {"run_id": ctx.run_id, "step": step.name, "attempt": attempt, "status": "succeeded",
                                             "error": None, "started_at": started, "finished_at": iso(), "duration_ms": round(dur, 2)})
        ctx.outputs[step.name] = out or {}
        self.svc.metrics.observe("step_duration_ms", dur, workflow=ctx.workflow.name, step=step.name)
        crash_at = self.svc.flags.get("chaos.crash_after_step")
        if crash_at and crash_at == step.name and self.svc.flags.get("chaos.crash_enabled", True) is not False:
            self.svc.flags.set("chaos.crash_after_step", None, by="system:chaos")
            log.error("CHAOS: simulating a worker crash right after checkpointing step '%s' of %s", step.name, ctx.run_id)
            self.svc.metrics.flush()
            os._exit(137)

    def _release(self, ctx: StepContext, **fields) -> None:
        fields.update({"lease_owner": None, "lease_expires_at": None, "updated_at": iso()})
        self.svc.db.update("runs", {"id": ctx.run_id}, fields)

    def _wait(self, ctx: StepContext, w: WaitingFor) -> str:
        self._release(ctx, status="waiting", waiting_on=w.waiting_on)
        self.svc.metrics.inc("runs_waiting_total", workflow=ctx.workflow.name, on=w.waiting_on.split(":")[0])
        if w.waiting_on == "children":
            self.maybe_resume_parent(ctx.run_id)
        elif w.waiting_on.startswith("run:"):
            other = self.svc.db.query_one("SELECT status FROM runs WHERE id=?", (w.waiting_on[4:],))
            if other and other["status"] in TERMINAL:
                self.resume(ctx.run_id, "awaited run already finished")
        elif w.waiting_on.startswith("approval:"):
            ap = self.svc.db.query_one("SELECT status FROM approvals WHERE id=?", (w.waiting_on.split(":", 1)[1],))
            if ap and ap["status"] != "pending":
                self.resume(ctx.run_id, "approval already decided")
        return "waiting"

    def _defer(self, ctx: StepContext, d: Deferred) -> str:
        self._release(ctx, status="retry_wait", next_attempt_at=Clock.real_iso(d.delay_s), error=f"deferred: {d}")
        return "deferred"

    def _fail_or_retry(self, ctx: StepContext, step: Step | None, exc: Exception) -> str:
        retryable = isinstance(exc, RetryableError) or bool(getattr(exc, "retryable", False))
        if isinstance(exc, PermanentError):
            retryable = False
        policy = (step.retry if step and step.retry else None) or RetryPolicy(self.svc.settings.engine.default_max_attempts,
                                                                              self.svc.settings.engine.backoff_base_s,
                                                                              self.svc.settings.engine.backoff_max_s)
        attempt = self.svc.db.scalar("SELECT attempt FROM steps WHERE run_id=? AND name=?", (ctx.run_id, step.name if step else ""), default=1)
        err = f"{type(exc).__name__}: {exc}"
        if retryable and attempt < policy.max_attempts:
            delay = backoff_delay(attempt, policy.base_s, policy.max_s)
            self._release(ctx, status="retry_wait", next_attempt_at=Clock.real_iso(delay), attempt=ctx.run["attempt"] + 1, error=err[:2000])
            self.svc.metrics.inc("step_retries_total", workflow=ctx.workflow.name, step=step.name if step else "-")
            log.warning("step %s failed (attempt %s/%s), retrying in %.1fs: %s", step.name if step else "?", attempt, policy.max_attempts, delay, err)
            return "retry_wait"
        log.error("run %s failed permanently at step %s: %s", ctx.run_id, step.name if step else "?", err)
        self._compensate(ctx)
        status = "failed"
        if ctx.workflow.on_failure:
            try:
                status = ctx.workflow.on_failure(ctx, exc) or "failed"
            except Exception:  # noqa: BLE001
                log.exception("on_failure hook failed for %s", ctx.run_id)
        return self._finish(ctx, status, {"failed_step": step.name if step else None}, err)

    def _compensate(self, ctx: StepContext) -> None:
        rows = self.svc.db.query("SELECT * FROM compensations WHERE run_id=? AND status='pending' ORDER BY id DESC", (ctx.run_id,))
        for r in rows:
            fn = self.compensators.get(r["action"])
            try:
                if fn:
                    fn(self.svc, loads(r["params"], {}))
                self.svc.db.update("compensations", {"id": r["id"]}, {"status": "executed", "executed_at": iso()})
                self.svc.audit.record("system:engine", "saga.compensate", ctx.run_id, "executed", {"action": r["action"], "params": loads(r["params"], {})})
            except Exception as exc:  # noqa: BLE001
                self.svc.db.update("compensations", {"id": r["id"]}, {"status": "failed", "error": str(exc)[:500]})

    def _finish(self, ctx: StepContext, status: str, output: dict | None = None, error: str | None = None) -> str:
        self._release(ctx, status=status, finished_at=iso(), output=dumps(output or {}), error=(error or None) and error[:2000],
                      waiting_on=None)
        run = ctx.run
        self.svc.metrics.inc("runs_total", workflow=ctx.workflow.name, status=status, tenant=ctx.tenant)
        started = run.get("created_at")
        if started:
            from swarmpipe.core.util import seconds_between

            secs = seconds_between(started, iso())
            if secs is not None:
                self.svc.metrics.observe("run_duration_seconds", secs, workflow=ctx.workflow.name)
        if ctx.workflow.on_finish:
            try:
                ctx.workflow.on_finish(ctx, status)
            except Exception:  # noqa: BLE001
                log.exception("on_finish hook failed for %s", ctx.run_id)
        self.svc.events.publish("run.finished", {"run_id": ctx.run_id, "workflow": ctx.workflow.name, "status": status,
                                                 "parent_run_id": run.get("parent_run_id")}, ctx.tenant, run.get("trace_id"))
        if run.get("parent_run_id"):
            self.maybe_resume_parent(run["parent_run_id"])
        for waiter in self.svc.db.query("SELECT id FROM runs WHERE status='waiting' AND waiting_on=?", (f"run:{ctx.run_id}",)):
            self.resume(waiter["id"], f"run {ctx.run_id} finished")
        return status

    # ---- resumption ----------------------------------------------------------------------------
    def resume(self, run_id: str, reason: str = "") -> bool:
        n = self.svc.db.execute("UPDATE runs SET status='pending', waiting_on=NULL, next_attempt_at=NULL, updated_at=? "
                                "WHERE id=? AND status='waiting'", (iso(), run_id)).rowcount
        if n:
            self.svc.metrics.inc("runs_resumed_total")
        return bool(n)

    def maybe_resume_parent(self, parent_id: str) -> None:
        parent = self.svc.db.query_one("SELECT status, waiting_on FROM runs WHERE id=?", (parent_id,))
        if not parent or parent["status"] != "waiting" or parent["waiting_on"] != "children":
            return
        open_children = self.svc.db.scalar(
            f"SELECT COUNT(*) FROM runs WHERE parent_run_id=? AND status NOT IN ({','.join('?' * len(TERMINAL))})",
            (parent_id, *TERMINAL), default=0)
        if open_children == 0:
            self.resume(parent_id, "all children finished")

    def reap_expired_leases(self) -> int:
        rows = self.svc.db.query("UPDATE runs SET status='pending', lease_owner=NULL, lease_expires_at=NULL, "
                                 "recovered_count=recovered_count+1, updated_at=? WHERE status='running' AND lease_expires_at < ? "
                                 "RETURNING id, current_step", (iso(), iso()))
        for r in rows:
            self.svc.audit.record("system:reaper", "run.recovered", r["id"], "requeued", {"step": r["current_step"]})
            self.svc.metrics.inc("runs_recovered_total")
            log.warning("recovered run %s from an expired lease (was at step %s)", r["id"], r["current_step"])
        return len(rows)

    def redrive(self, run_id: str, by: str) -> bool:
        n = self.svc.db.execute("UPDATE runs SET status='pending', attempt=0, error=NULL, next_attempt_at=NULL, finished_at=NULL, updated_at=? "
                                "WHERE id=? AND status IN ('failed','dead_lettered')", (iso(), run_id)).rowcount
        if n:
            self.svc.db.execute("DELETE FROM steps WHERE run_id=? AND status='failed'", (run_id,))
            self.svc.audit.record(by, "run.redrive", run_id, "requeued")
        return bool(n)

    def cancel(self, run_id: str, by: str) -> bool:
        n = self.svc.db.execute("UPDATE runs SET status='cancelled', finished_at=?, updated_at=? WHERE id=? AND status IN ('pending','retry_wait','waiting')",
                                (iso(), iso(), run_id)).rowcount
        if n:
            self.svc.audit.record(by, "run.cancel", run_id, "cancelled")
        return bool(n)

    # ---- loops -----------------------------------------------------------------------------------
    def worker_loop(self, name: str) -> None:
        idle = self.svc.settings.engine.idle_sleep_s
        while not self.stop_event.is_set():
            try:
                run = self.claim()
                if run:
                    self.execute(run)
                else:
                    self.stop_event.wait(idle)
            except Exception:  # noqa: BLE001
                log.exception("worker %s loop error", name)
                self.stop_event.wait(1.0)

    def run_until_quiescent(self, timeout_s: float = 120.0, scheduler=None, max_idle_wait_s: float = 8.0) -> bool:
        """Synchronous driver for tests, evals and `swarmpipe tick`: run events, work and (optionally)
        monitors until nothing is runnable. Runs waiting for humans count as quiescent."""
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            progressed = self.svc.dispatcher.poll_once() > 0
            self.reap_expired_leases()
            run = self.claim()
            if run:
                self.execute(run)
                continue
            if scheduler is not None:
                progressed = scheduler.tick(force=False) or progressed
            if progressed:
                continue
            nxt = self.svc.db.scalar("SELECT MIN(COALESCE(next_attempt_at, '')) FROM runs WHERE status IN ('pending','retry_wait')")
            if nxt is None:
                self.svc.metrics.flush()
                return True
            from swarmpipe.core.util import parse_iso, utcnow

            wait = 0.05 if not nxt else max(0.0, (parse_iso(nxt) - utcnow()).total_seconds())
            if wait > max_idle_wait_s:
                self.svc.metrics.flush()
                return True
            time.sleep(min(max(wait, 0.02), 0.5))
        self.svc.metrics.flush()
        return False
