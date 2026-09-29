"""Tool gateway: the only way an agent touches the world.

MCP-style tool contracts - name, version, description, JSON-schema args, annotations
(readOnlyHint / destructiveHint / idempotentHint / openWorldHint - HINTS for UIs, never security) -
plus the controls MCP leaves to the implementer, enforced deterministically on every call:

  kill switch -> tool exists & fingerprint approved (supply chain) -> tool allowlisted for this agent
  (least privilege, read/write separation) -> identity scope + tenant (delegation = intersection) ->
  per-agent rate limit -> typed argument validation -> execution with stable error codes ->
  bounded output -> evidence id (citable) -> telemetry + audit (for side effects).

Repeated forbidden calls by one agent auto-suspend it (rogue-agent containment, OWASP ASI10)."""
from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Callable

from pydantic import BaseModel, ValidationError

from swarmpipe.core.errors import SwarmError
from swarmpipe.core.util import dumps, iso, new_id, sha256_text, truncate
from swarmpipe.observability import tracing as T


@dataclass
class ToolSpec:
    name: str
    version: str
    description: str
    args_model: type[BaseModel]
    handler: Callable
    scope: str
    read_only: bool = True
    destructive: bool = False
    idempotent: bool = True
    open_world: bool = False
    trust: str = "trusted"
    max_output_chars: int = 4000
    rate_limit_per_min: int = 120

    @property
    def annotations(self) -> dict:
        return {"title": self.name.replace("_", " "), "readOnlyHint": self.read_only, "destructiveHint": self.destructive,
                "idempotentHint": self.idempotent, "openWorldHint": self.open_world}

    def input_schema(self) -> dict:
        return self.args_model.model_json_schema()

    def fingerprint(self) -> str:
        return sha256_text(f"{self.name}|{self.version}|{self.description}|{dumps(self.input_schema())}|{self.scope}")[:16]

    def describe(self) -> dict:
        return {"name": self.name, "version": self.version, "description": self.description,
                "input_schema": self.input_schema(), "annotations": self.annotations, "scope": self.scope}


@dataclass
class ToolContext:
    svc: Any
    identity: Any
    tenant: str
    agent_id: str
    incident_id: str | None = None
    run_id: str | None = None
    dataset: str | None = None


@dataclass
class ToolResult:
    tool: str
    version: str
    ok: bool
    data: Any = None
    error: dict | None = None
    evidence_id: str | None = None
    truncated: bool = False
    trust: str = "trusted"
    latency_ms: float = 0.0

    def content(self) -> Any:
        return self.data if self.ok else {"error": self.error}


class _Bucket:
    def __init__(self, per_min: int):
        self.capacity = float(per_min)
        self.tokens = float(per_min)
        self.rate = per_min / 60.0
        self.ts = time.monotonic()

    def take(self) -> bool:
        now = time.monotonic()
        self.tokens = min(self.capacity, self.tokens + (now - self.ts) * self.rate)
        self.ts = now
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False


class ToolGateway:
    ROGUE_THRESHOLD = 3

    def __init__(self, svc):
        self.svc = svc
        self.tools: dict[str, ToolSpec] = {}
        self.approved: dict[str, str] = {}
        self._buckets: dict[tuple[str, str], _Bucket] = {}
        self._lock = threading.Lock()

    def register(self, spec: ToolSpec, approve: bool = True) -> None:
        self.tools[spec.name] = spec
        if approve:
            self.approved[spec.name] = spec.fingerprint()

    def describe(self, names: list[str] | None = None) -> list[dict]:
        return [t.describe() for n, t in sorted(self.tools.items()) if names is None or n in names]

    def _err(self, spec: ToolSpec | None, name: str, code: str, msg: str, ctx: ToolContext, t0: float, span) -> ToolResult:
        span.set_attrs({"swarmpipe.tool.ok": False, "swarmpipe.tool.error": code})
        self._log_call(ctx, name, spec.version if spec else "-", {}, False, code, t0, 0, False, None)
        return ToolResult(name, spec.version if spec else "-", False, error={"code": code, "message": msg,
                                                                           "retryable": code in ("RATE_LIMITED", "TOOL_ERROR_TRANSIENT")})

    def call(self, name: str, args: dict, ctx: ToolContext, allowed: set[str] | None) -> ToolResult:
        t0 = time.perf_counter()
        spec = self.tools.get(name)
        with self.svc.tracer.span(f"execute_tool {name}", "internal", {T.GEN_AI_OPERATION: "execute_tool", T.GEN_AI_TOOL_NAME: name,
                                                                     T.GEN_AI_AGENT_NAME: ctx.agent_id, "swarmpipe.incident_id": ctx.incident_id}) as span:
            ks = self.svc.killswitch.engaged(agent=ctx.agent_id, tenant=ctx.tenant)
            if ks:
                return self._err(spec, name, "KILL_SWITCH", f"kill switch engaged ({ks})", ctx, t0, span)
            if spec is None:
                self._suspicion(ctx, name, "TOOL_NOT_FOUND")
                return self._err(None, name, "TOOL_NOT_FOUND", f"no tool named '{name}'", ctx, t0, span)
            if self.approved.get(name) != spec.fingerprint():
                return self._err(spec, name, "TOOL_NOT_APPROVED", "tool definition changed since approval (supply-chain check)", ctx, t0, span)
            if allowed is not None and name not in allowed:
                self._suspicion(ctx, name, "TOOL_FORBIDDEN")
                return self._err(spec, name, "TOOL_FORBIDDEN", f"agent {ctx.agent_id} is not allowed to use {name}", ctx, t0, span)
            if not ctx.identity.can(spec.scope):
                return self._err(spec, name, "FORBIDDEN", f"{ctx.identity.describe()} lacks scope {spec.scope}", ctx, t0, span)
            if not ctx.identity.can_access_tenant(ctx.tenant):
                return self._err(spec, name, "FORBIDDEN", f"no access to tenant {ctx.tenant}", ctx, t0, span)
            with self._lock:
                b = self._buckets.setdefault((ctx.agent_id, name), _Bucket(spec.rate_limit_per_min))
                ok_rate = b.take()
            if not ok_rate:
                return self._err(spec, name, "RATE_LIMITED", f"rate limit {spec.rate_limit_per_min}/min exceeded", ctx, t0, span)
            try:
                parsed = spec.args_model.model_validate(args or {})
            except ValidationError as exc:
                return self._err(spec, name, "INVALID_ARGUMENTS", str(exc)[:600], ctx, t0, span)
            try:
                if random.random() < float(self.svc.flags.get("chaos.tool_error_rate", 0) or 0):
                    raise SwarmError("simulated tool backend failure", code="TOOL_ERROR_TRANSIENT")
                data = spec.handler(ctx, parsed)
            except SwarmError as exc:
                return self._err(spec, name, exc.code, str(exc), ctx, t0, span)
            except KeyError as exc:
                return self._err(spec, name, "NOT_FOUND", f"not found: {exc}", ctx, t0, span)
            except Exception as exc:  # noqa: BLE001
                return self._err(spec, name, "TOOL_ERROR", f"{type(exc).__name__}: {exc}", ctx, t0, span)
            text, truncated = truncate(dumps(data), spec.max_output_chars)
            trust = spec.trust
            if isinstance(data, dict) and data.get("_trust"):
                trust = data["_trust"]
            ev_id = new_id("ev")
            self.svc.db.insert("evidence", {"id": ev_id, "incident_id": ctx.incident_id, "run_id": ctx.run_id, "tool": name,
                                            "tool_version": spec.version, "args": dumps(args), "content": text, "trust": trust,
                                            "created_by": f"agent:{ctx.agent_id}", "created_at": iso()})
            self._log_call(ctx, name, spec.version, args, True, None, t0, len(text), truncated, ev_id)
            span.set_attrs({"swarmpipe.tool.ok": True, "swarmpipe.evidence_id": ev_id, "swarmpipe.tool.truncated": truncated})
            if not spec.read_only:
                self.svc.audit.record(ctx.identity.principal, f"tool.{name}", ctx.incident_id or ctx.run_id or "", "executed",
                                      {"args": args, "evidence_id": ev_id},
                                      on_behalf_of=ctx.identity.on_behalf_of.principal if ctx.identity.on_behalf_of else None)
            return ToolResult(name, spec.version, True, data=data if not truncated else text, evidence_id=ev_id,
                              truncated=truncated, trust=trust, latency_ms=(time.perf_counter() - t0) * 1000)

    def _log_call(self, ctx: ToolContext, name, version, args, ok, code, t0, chars, truncated, ev_id) -> None:
        span = self.svc.tracer.current()
        self.svc.db.insert("tool_calls", {"id": new_id("tc"), "ts": iso(), "trace_id": span.trace_id if span else None,
                                          "agent": ctx.agent_id, "tool": name, "tool_version": version, "args": dumps(args)[:2000],
                                          "ok": 1 if ok else 0, "error_code": code, "latency_ms": round((time.perf_counter() - t0) * 1000, 2),
                                          "output_chars": chars, "truncated": 1 if truncated else 0, "evidence_id": ev_id,
                                          "incident_id": ctx.incident_id, "run_id": ctx.run_id})
        self.svc.metrics.inc("tool_calls_total", tool=name, ok=str(ok).lower(), agent=ctx.agent_id)

    def _suspicion(self, ctx: ToolContext, name: str, code: str) -> None:
        self.svc.audit.record(f"agent:{ctx.agent_id}", "tool.denied", name, code, {"incident_id": ctx.incident_id})
        since = iso(self.svc.clock.real_now() - timedelta(minutes=10))
        n = self.svc.db.scalar("SELECT COUNT(*) FROM tool_calls WHERE agent=? AND error_code IN ('TOOL_FORBIDDEN','TOOL_NOT_FOUND') AND ts>=?",
                               (ctx.agent_id, since), default=0) + 1
        if n >= self.ROGUE_THRESHOLD and not self.svc.killswitch.engaged(agent=ctx.agent_id):
            self.svc.killswitch.set(f"agent:{ctx.agent_id}", True, f"auto-suspended: {n} forbidden/unknown tool calls in 10 min",
                                    by="system:tool-gateway")
            self.svc.signals.raise_signal(ctx.tenant, "rogue_agent", None, "high",
                                          f"Agent {ctx.agent_id} auto-suspended after {n} forbidden tool calls",
                                          {"agent": ctx.agent_id, "last_tool": name}, run_id=ctx.run_id)
