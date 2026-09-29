"""Agent base class: identity, A2A-style agent card, governed model access (`ask`), governed tool
access (`call_tool`), and a ReAct tool loop with step budgets and loop/ping-pong detection
(planning controls and multi-agent controls).

Design rule used throughout the swarm: a deterministic skeleton with probabilistic
steps. Every LLM-backed method has a deterministic fallback, so a model outage, a quota or a budget
degrades quality ("advice unavailable") instead of breaking the pipeline."""
from __future__ import annotations

import time
from contextlib import contextmanager

from pydantic import BaseModel, ValidationError

from swarmpipe.core.errors import BudgetExceeded, GuardrailViolation, KillSwitchEngaged, LLMUnavailable, QuotaExceeded
from swarmpipe.core.util import canonical_json, sha256_text
from swarmpipe.governance.identity import Identity, IdentityService
from swarmpipe.llm.prompts import DataBlock, build_messages, tool_result_message
from swarmpipe.llm.types import Finding, LLMRequest, LLMResponse, ReactStep
from swarmpipe.observability import tracing as T
from swarmpipe.tools.gateway import ToolContext, ToolResult

DEGRADE_ERRORS = (LLMUnavailable, BudgetExceeded, QuotaExceeded, KillSwitchEngaged, GuardrailViolation)


class Agent:
    id = "base"
    name = "Base agent"
    version = "1.0.0"
    description = ""
    role: str | None = None
    kind = "deterministic"
    scopes: frozenset[str] = frozenset()
    tools: tuple[str, ...] = ()
    owner = "platform-team@contoso.example"
    skills: list[dict] = []
    pii_access = False

    def __init__(self, svc):
        self.svc = svc

    @property
    def identity(self) -> Identity:
        ident = IdentityService.agent(self.id, self.scopes)
        if self.pii_access:
            ident = Identity(ident.principal, ident.scopes, ident.tenants, ident.roles, True)
        return ident

    def card(self) -> dict:
        port = self.svc.settings.web.get("port", 8765)
        host = self.svc.settings.web.get("host", "127.0.0.1")
        return {
            "name": self.name, "description": self.description, "version": self.version,
            "url": f"http://{host}:{port}/a2a/agents/{self.id}",
            "provider": {"organization": "SwarmPipe (local)"},
            "capabilities": {"streaming": False, "pushNotifications": False},
            "defaultInputModes": ["application/json"], "defaultOutputModes": ["application/json"],
            "skills": self.skills or [{"id": self.id, "name": self.name, "description": self.description, "tags": [self.kind]}],
            "securitySchemes": {"localHmac": {"type": "http", "scheme": "bearer", "description": "short-lived HMAC token"}},
            "x-swarmpipe": {"agent_id": self.id, "kind": self.kind, "model_role": self.role, "scopes": sorted(self.scopes),
                            "tools": list(self.tools), "owner": self.owner},
        }

    def card_hash(self) -> str:
        return sha256_text(canonical_json(self.card()))[:16]

    @contextmanager
    def invoke(self, operation: str, **attrs):
        with self.svc.tracer.span(f"invoke_agent {self.id}.{operation}", "internal", {
                T.GEN_AI_OPERATION: "invoke_agent", T.GEN_AI_AGENT_NAME: self.name, T.GEN_AI_AGENT_ID: self.id,
                "swarmpipe.agent.version": self.version, **{f"swarmpipe.{k}": v for k, v in attrs.items()}}) as span:
            t0 = time.perf_counter()
            try:
                yield span
            finally:
                self.svc.metrics.observe("agent_invocation_ms", (time.perf_counter() - t0) * 1000, agent=self.id, op=operation)

    def tool_ctx(self, tenant: str, *, incident_id: str | None = None, run_id: str | None = None, dataset: str | None = None,
                 on_behalf_of: Identity | None = None) -> ToolContext:
        ident = self.identity.acting_for(on_behalf_of) if on_behalf_of else self.identity
        return ToolContext(self.svc, ident, tenant, self.id, incident_id, run_id, dataset)

    def call_tool(self, name: str, args: dict, ctx: ToolContext) -> ToolResult:
        return self.svc.tools.call(name, args, ctx, allowed=set(self.tools))

    def _spotlight(self) -> bool:
        v = self.svc.flags.get("guardrails.spotlighting")
        return self.svc.settings.guardrails.spotlighting if v is None else bool(v)

    def ask(self, prompt_id: str, *, task: str, context: dict, schema: type[BaseModel], tenant: str,
            untrusted: dict[str, str] | None = None, run_id: str | None = None, incident_id: str | None = None,
            version: str | None = None, purpose: str = "", temperature: float = 0.0, max_tokens: int = 900) -> LLMResponse:
        tpl = self.svc.prompts.get(prompt_id, version)
        blocks = [DataBlock("context", context, "trusted")] + [DataBlock(k, v, "untrusted") for k, v in (untrusted or {}).items()]
        messages = build_messages(tpl, task, blocks, self.svc.settings, schema, spotlight=self._spotlight())
        req = LLMRequest(role=self.role or "default", prompt_id=tpl.id, prompt_version=tpl.version, prompt_hash=tpl.hash,
                         messages=messages, schema=schema, temperature=temperature, max_output_tokens=max_tokens,
                         agent=f"agent:{self.id}", tenant=tenant, run_id=run_id, incident_id=incident_id,
                         purpose=purpose or prompt_id)
        return self.svc.llm.chat(req)

    def react(self, prompt_id: str, *, task: str, context: dict, tenant: str, run_id: str | None, incident_id: str | None,
              dataset: str | None = None, max_steps: int | None = None) -> dict:
        """ReAct loop over the JSON action protocol. Returns {finding, steps, tools_used, evidence_ids, stop_reason}."""
        max_steps = max_steps or self.svc.settings.budgets.agent_max_steps
        tpl = self.svc.prompts.get(prompt_id)
        ctx_block = {**context, "tools": list(self.tools),
                     "tool_specs": [{k: d[k] for k in ("name", "description", "input_schema")} for d in self.svc.tools.describe(list(self.tools))]}
        messages = build_messages(tpl, task, [DataBlock("context", ctx_block, "trusted")], self.svc.settings, ReactStep,
                                  spotlight=self._spotlight())
        tctx = self.tool_ctx(tenant, incident_id=incident_id, run_id=run_id, dataset=dataset)
        seen: dict[str, int] = {}
        tools_used: list[str] = []
        evidence: list[str] = []
        loops = 0
        for step in range(1, max_steps + 1):
            req = LLMRequest(role=self.role or "default", prompt_id=tpl.id, prompt_version=tpl.version, prompt_hash=tpl.hash,
                             messages=messages, schema=ReactStep, agent=f"agent:{self.id}", tenant=tenant, run_id=run_id,
                             incident_id=incident_id, purpose=f"react_step:{step}", max_output_tokens=700)
            resp = self.svc.llm.chat(req)
            st: ReactStep = resp.parsed
            messages = messages + [{"role": "assistant", "content": resp.text}]
            if st.action == "final":
                try:
                    finding = Finding.model_validate(st.answer or {})
                except ValidationError as exc:
                    messages.append({"role": "user", "content": f"REPAIR: your final answer is invalid ({str(exc)[:300]}). "
                                                                "Reply with a final action whose answer matches the Finding schema."})
                    continue
                return {"finding": finding.model_dump(), "steps": step, "tools_used": tools_used, "evidence_ids": evidence,
                        "stop_reason": "final"}
            sig = f"{st.tool}|{canonical_json(st.args)}"
            seen[sig] = seen.get(sig, 0) + 1
            if seen[sig] > 1:
                loops += 1
                self.svc.metrics.inc("agent_loops_detected_total", agent=self.id)
                span = self.svc.tracer.current()
                if span:
                    span.event("loop_detected", tool=st.tool, repeats=seen[sig])
                if loops >= 2:
                    return {"finding": None, "steps": step, "tools_used": tools_used, "evidence_ids": evidence, "stop_reason": "loop_detected"}
                messages.append({"role": "user", "content": f"LOOP_DETECTED: you already called {st.tool} with these arguments. "
                                                            "Use the earlier result or give your final answer."})
                continue
            result = self.call_tool(st.tool or "", st.args, tctx)
            tools_used.append(st.tool or "?")
            if result.ok and result.evidence_id:
                evidence.append(result.evidence_id)
            messages.append(tool_result_message(st.tool or "?", result.evidence_id, result.ok, result.content(), result.trust, self.svc.settings,
                                                spotlight=self._spotlight()))
        return {"finding": None, "steps": max_steps, "tools_used": tools_used, "evidence_ids": evidence, "stop_reason": "step_budget_exhausted"}
