"""Distributed-tracing style spans with OpenTelemetry GenAI semantic-convention attribute names.
One trace spans every agent involved in a run or incident, so a multi-agent failure can be followed
end to end.

Sampling: head sampling by trace id (tracing.sample_rate) plus tail rule "always keep traces with
errors" - spans are buffered per execution segment and the keep/drop decision is taken when the
segment root ends. Telemetry is sampled and short-lived; the audit log (governance/audit.py) is not.
"""
from __future__ import annotations

import collections
import contextvars
import secrets
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from swarmpipe.core.errors import Deferred, StopWorkflow, WaitingFor
from swarmpipe.core.util import dumps, loads

# OpenTelemetry GenAI semantic conventions (still evolving upstream; names as of 2025/26)
GEN_AI_OPERATION = "gen_ai.operation.name"          # chat | execute_tool | invoke_agent
GEN_AI_PROVIDER = "gen_ai.provider.name"
GEN_AI_REQUEST_MODEL = "gen_ai.request.model"
GEN_AI_RESPONSE_MODEL = "gen_ai.response.model"
GEN_AI_TEMPERATURE = "gen_ai.request.temperature"
GEN_AI_MAX_TOKENS = "gen_ai.request.max_tokens"
GEN_AI_INPUT_TOKENS = "gen_ai.usage.input_tokens"
GEN_AI_OUTPUT_TOKENS = "gen_ai.usage.output_tokens"
GEN_AI_AGENT_NAME = "gen_ai.agent.name"
GEN_AI_AGENT_ID = "gen_ai.agent.id"
GEN_AI_TOOL_NAME = "gen_ai.tool.name"
GEN_AI_TOOL_CALL_ID = "gen_ai.tool.call.id"

_CURRENT: contextvars.ContextVar["Span | None"] = contextvars.ContextVar("swarmpipe_span", default=None)
_CONTROL_FLOW = (WaitingFor, StopWorkflow, Deferred)


@dataclass
class Span:
    trace_id: str
    span_id: str
    parent_span_id: str | None
    name: str
    kind: str
    segment_id: str
    sampled: bool
    start_ts: float = field(default_factory=time.time)
    end_ts: float | None = None
    attributes: dict = field(default_factory=dict)
    events: list = field(default_factory=list)
    status: str = "ok"

    def set(self, key: str, value: Any) -> "Span":
        self.attributes[key] = value
        return self

    def set_attrs(self, attrs: dict) -> "Span":
        self.attributes.update({k: v for k, v in (attrs or {}).items() if v is not None})
        return self

    def event(self, name: str, **attrs: Any) -> None:
        self.events.append({"name": name, "ts": time.time(), "attributes": attrs})

    def error(self, exc: BaseException | str) -> None:
        self.status = "error"
        self.event("exception", type=type(exc).__name__ if not isinstance(exc, str) else "Error", message=str(exc)[:500])

    @property
    def duration_ms(self) -> float | None:
        return None if self.end_ts is None else (self.end_ts - self.start_ts) * 1000.0


class Tracer:
    def __init__(self, db, settings=None):
        self.db = db
        self.settings = settings
        self._buffers: dict[str, list[Span]] = collections.defaultdict(list)
        self._closed: collections.OrderedDict[str, bool] = collections.OrderedDict()
        self._lock = threading.Lock()

    @staticmethod
    def new_trace_id() -> str:
        return secrets.token_hex(16)

    @staticmethod
    def current() -> Span | None:
        return _CURRENT.get()

    def _head_sampled(self, trace_id: str) -> bool:
        rate = self.settings.tracing.sample_rate if self.settings else 1.0
        return (int(trace_id[:8], 16) / 0xFFFFFFFF) < rate

    @contextmanager
    def span(self, name: str, kind: str = "internal", attrs: dict | None = None, *,
             trace_id: str | None = None, parent: "Span | str | None" = None):
        parent_obj = parent if isinstance(parent, Span) else (None if isinstance(parent, str) else _CURRENT.get())
        if trace_id and parent_obj is not None and parent_obj.trace_id != trace_id:
            parent_obj = None
        parent_id = parent if isinstance(parent, str) else (parent_obj.span_id if parent_obj else None)
        tid = trace_id or (parent_obj.trace_id if parent_obj else self.new_trace_id())
        span_id = secrets.token_hex(8)
        is_root = parent_obj is None
        s = Span(trace_id=tid, span_id=span_id, parent_span_id=parent_id, name=name, kind=kind,
                 segment_id=span_id if is_root else parent_obj.segment_id,
                 sampled=self._head_sampled(tid) if is_root else parent_obj.sampled)
        s.set_attrs(attrs or {})
        token = _CURRENT.set(s)
        try:
            yield s
        except _CONTROL_FLOW as cf:
            s.event("control_flow", type=type(cf).__name__, detail=str(cf)[:300])
            raise
        except BaseException as exc:
            s.error(exc)
            raise
        finally:
            s.end_ts = time.time()
            _CURRENT.reset(token)
            self._finish(s, is_root)

    def run_in_context(self, fn, *args, **kwargs):
        """Capture the current span context for use in another thread (fan-out)."""
        ctx = contextvars.copy_context()
        return lambda: ctx.run(fn, *args, **kwargs)

    # ---- buffering / sampling / export ---------------------------------------------------
    def _finish(self, span: Span, is_root: bool) -> None:
        with self._lock:
            if span.segment_id in self._closed:
                late = [span]
            else:
                self._buffers[span.segment_id].append(span)
                late = None
                if is_root:
                    batch = self._buffers.pop(span.segment_id, [])
                    self._closed[span.segment_id] = True
                    while len(self._closed) > 5000:
                        self._closed.popitem(last=False)
        if late:
            self._write(late, force=True)
        elif is_root:
            self._write(batch)

    def _write(self, spans: list[Span], force: bool = False) -> None:
        if not spans:
            return
        keep_errors = self.settings.tracing.always_keep_errors if self.settings else True
        has_error = any(s.status == "error" for s in spans)
        if not force and not (spans[-1].sampled or (keep_errors and has_error)):
            return
        rows = [(s.span_id, s.trace_id, s.parent_span_id, s.name, s.kind, s.start_ts, s.end_ts, s.duration_ms,
                 s.status, dumps(s.attributes), dumps(s.events)) for s in spans]
        try:
            self.db.executemany(
                "INSERT OR REPLACE INTO spans(span_id, trace_id, parent_span_id, name, kind, start_ts, end_ts, duration_ms, "
                "status, attributes, events) VALUES(?,?,?,?,?,?,?,?,?,?,?)", rows)
        except Exception:  # noqa: BLE001 - telemetry must never break the pipeline
            return
        if self.settings and self.settings.tracing.export_jsonl:
            self._export_jsonl(spans)

    def _export_jsonl(self, spans: list[Span]) -> None:
        day = datetime.now(timezone.utc).strftime("%Y%m%d")
        path = self.settings.data_path("traces", f"spans-{day}.jsonl")
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = []
        for s in spans:
            lines.append(dumps({
                "traceId": s.trace_id, "spanId": s.span_id, "parentSpanId": s.parent_span_id or "",
                "name": s.name, "kind": s.kind, "startTimeUnixNano": int(s.start_ts * 1e9),
                "endTimeUnixNano": int((s.end_ts or s.start_ts) * 1e9),
                "status": {"code": "ERROR" if s.status == "error" else "OK"},
                "attributes": s.attributes, "events": s.events,
            }))
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
        except OSError:
            pass

    # ---- queries ---------------------------------------------------------------------------
    def get_trace(self, trace_id: str) -> list[dict]:
        rows = self.db.query("SELECT * FROM spans WHERE trace_id=? ORDER BY start_ts", (trace_id,))
        for r in rows:
            r["attributes"] = loads(r["attributes"], {})
            r["events"] = loads(r["events"], [])
        return rows

    def trace_tree(self, trace_id: str) -> list[dict]:
        rows = self.get_trace(trace_id)
        by_id = {r["span_id"]: {**r, "children": []} for r in rows}
        roots = []
        for r in by_id.values():
            p = by_id.get(r["parent_span_id"])
            (p["children"] if p else roots).append(r)
        return roots
