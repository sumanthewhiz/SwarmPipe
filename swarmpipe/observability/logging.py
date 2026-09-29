"""Structured JSON logs with correlation ids (trace_id, run_id, incident_id, tenant, agent) injected
automatically from context, plus a readable console handler."""
from __future__ import annotations

import contextvars
import json
import logging
import os
import sys
import threading
from contextlib import contextmanager
from pathlib import Path

_LOG_CTX: contextvars.ContextVar[dict] = contextvars.ContextVar("swarmpipe_log_ctx", default={})


@contextmanager
def log_context(**fields):
    token = _LOG_CTX.set({**_LOG_CTX.get(), **{k: v for k, v in fields.items() if v is not None}})
    try:
        yield
    finally:
        _LOG_CTX.reset(token)


class _ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        ctx = dict(_LOG_CTX.get())
        try:
            from swarmpipe.observability.tracing import Tracer

            span = Tracer.current()
            if span is not None:
                ctx.setdefault("trace_id", span.trace_id)
                ctx.setdefault("span_id", span.span_id)
        except Exception:  # noqa: BLE001
            pass
        record.ctx = ctx
        return True


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {"ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"), "level": record.levelname,
                   "logger": record.name, "msg": record.getMessage(), **getattr(record, "ctx", {})}
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


class _ConsoleFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ctx = getattr(record, "ctx", {})
        tags = " ".join(f"{k}={ctx[k]}" for k in ("tenant", "run_id", "incident_id", "agent") if k in ctx)
        base = f"{self.formatTime(record, '%H:%M:%S')} {record.levelname:<7} {record.name.replace('swarmpipe.', '')}: {record.getMessage()}"
        return f"{base}  [{tags}]" if tags else base


_configured = False
_lock = threading.Lock()
_filter = _ContextFilter()
_file_handler: logging.FileHandler | None = None
_file_targets: list[str] = []  # stack: the JSON file handler always writes to the newest live workspace's log


def _point_file_handler(root: logging.Logger, target: str | None) -> None:
    global _file_handler
    if _file_handler is not None and _file_handler.baseFilename == target:
        return
    if _file_handler is not None:
        root.removeHandler(_file_handler)
        _file_handler.close()
        _file_handler = None
    if target:
        Path(target).parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(target, encoding="utf-8")
        fh.setFormatter(_JsonFormatter())
        fh.addFilter(_filter)
        root.addHandler(fh)
        _file_handler = fh


def setup_logging(log_dir: Path | None = None, level: str = "INFO", console: bool = True) -> None:
    """Level and console handler are process-wide (the first caller wins); the JSON log file follows the
    most recently created Services so isolated workspaces (tests, eval trials) each get their own log."""
    global _configured
    root = logging.getLogger("swarmpipe")
    with _lock:
        if not _configured:
            root.setLevel(level)
            root.propagate = False
            if console:
                h = logging.StreamHandler(sys.stderr)
                h.setFormatter(_ConsoleFormatter())
                h.addFilter(_filter)
                root.addHandler(h)
            _configured = True
        if log_dir is not None:
            target = os.path.abspath(Path(log_dir) / "swarmpipe.jsonl")
            _file_targets.append(target)
            _point_file_handler(root, target)


def release_log_dir(log_dir: Path) -> None:
    """Stop writing to `log_dir` (closing the file so its folder can be moved or deleted) and fall back to
    the previous still-existing log target, if any."""
    target = os.path.abspath(Path(log_dir) / "swarmpipe.jsonl")
    root = logging.getLogger("swarmpipe")
    with _lock:
        for i in range(len(_file_targets) - 1, -1, -1):
            if _file_targets[i] == target:
                del _file_targets[i]
                break
        while _file_targets and not os.path.isdir(os.path.dirname(_file_targets[-1])):
            _file_targets.pop()
        _point_file_handler(root, _file_targets[-1] if _file_targets else None)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"swarmpipe.{name}")
