"""Model gateway: the single, policy-controlled path from any agent to any model (routing, quotas,
guardrails, caching, metering).

Per call:  kill switch -> tenant quota -> run budget -> guardrails (PII redaction, secret-leak
check) -> context-window compaction -> cache -> route chain [model A, model B, ...] where each
model gets: circuit breaker -> bulkhead semaphore -> transport retries with backoff+jitter ->
JSON extraction + schema validation -> repair loop. If every model fails: LLMUnavailable, and the
calling agent degrades to its deterministic fallback. Every attempt is metered (tokens, cost,
latency) and traced with OpenTelemetry GenAI attributes."""
from __future__ import annotations

import json
import re
import threading
import time
from datetime import date

from swarmpipe.core.errors import BudgetExceeded, KillSwitchEngaged, LLMUnavailable, QuotaExceeded
from swarmpipe.core.util import backoff_delay, dumps, iso, loads, new_id, sha256_text
from swarmpipe.governance.guardrails import check_secret_leak, redact_for_model
from swarmpipe.llm.prompts import normalized_messages
from swarmpipe.llm.providers import ProviderBadRequest, ProviderError, ProviderRateLimited, make_provider
from swarmpipe.llm.tokens import count_messages
from swarmpipe.llm.types import LLMRequest, LLMResponse
from swarmpipe.observability import tracing as T

_FENCE = re.compile(r"```(?:json)?\s*(\{.*\})\s*```", re.S)


class CircuitBreaker:
    """closed -> (N consecutive failures) -> open -> (cooldown) -> half_open -> success: closed / failure: open"""

    def __init__(self, threshold: int, cooldown_s: float):
        self.threshold = threshold
        self.cooldown_s = cooldown_s
        self._s: dict[str, dict] = {}
        self._lock = threading.Lock()

    def _get(self, key: str) -> dict:
        return self._s.setdefault(key, {"state": "closed", "failures": 0, "opened_at": 0.0, "opens": 0})

    def allow(self, key: str) -> bool:
        with self._lock:
            s = self._get(key)
            if s["state"] == "open":
                if time.monotonic() - s["opened_at"] >= self.cooldown_s:
                    s["state"] = "half_open"
                    return True
                return False
            return True

    def success(self, key: str) -> None:
        with self._lock:
            s = self._get(key)
            s.update(state="closed", failures=0)

    def failure(self, key: str) -> bool:
        with self._lock:
            s = self._get(key)
            s["failures"] += 1
            if s["state"] == "half_open" or s["failures"] >= self.threshold:
                opened = s["state"] != "open"
                s.update(state="open", opened_at=time.monotonic())
                if opened:
                    s["opens"] += 1
                return opened
            return False

    def snapshot(self) -> dict:
        with self._lock:
            now = time.monotonic()
            return {k: {**v, "open_for_s": round(now - v["opened_at"], 1) if v["state"] == "open" else 0} for k, v in self._s.items()}

    def reset(self) -> None:
        with self._lock:
            self._s.clear()


def extract_json(text: str):
    """Tolerant JSON extraction: raw JSON, ```json fenced``` JSON, or the first {...} span."""
    t = (text or "").strip()
    try:
        return json.loads(t)
    except ValueError:
        pass
    m = _FENCE.search(t)
    if m:
        try:
            return json.loads(m.group(1))
        except ValueError:
            pass
    a, b = t.find("{"), t.rfind("}")
    if a != -1 and b > a:
        return json.loads(t[a:b + 1])
    raise ValueError("no JSON object found in model output")


class ModelGateway:
    def __init__(self, svc):
        self.svc = svc
        s = svc.settings.llm
        self.breaker = CircuitBreaker(s.breaker_failure_threshold, s.breaker_cooldown_s)
        self.bulkhead = threading.BoundedSemaphore(max(1, svc.settings.engine.llm_concurrency))
        self._providers: dict[str, object] = {}
        self._lock = threading.Lock()

    # ---- routing -----------------------------------------------------------------------
    def active_profile(self) -> str:
        return self.svc.flags.get("llm.active_profile") or self.svc.settings.llm.active_profile

    def route(self, role: str) -> list[str]:
        prof = self.svc.settings.llm.profiles.get(self.active_profile()) or {}
        chain = list(prof.get(role) or prof.get("default") or ["sim-small"])
        if self.svc.settings.llm.require_certification or self.svc.flags.get("llm.require_certification"):
            ok = {r["model"] for r in self.svc.db.query(
                "SELECT model FROM model_certifications WHERE role=? AND status='certified'", (role,))}
            chain = [m for m in chain if m in ok]
        return chain

    def provider(self, name: str):
        with self._lock:
            if name not in self._providers:
                self._providers[name] = make_provider(name, self.svc.settings.llm.providers[name], self.svc)
            return self._providers[name]

    # ---- budgets / quotas ----------------------------------------------------------------
    def _check_limits(self, req: LLMRequest) -> None:
        if self.svc.killswitch.engaged(agent=req.agent.replace("agent:", ""), tenant=req.tenant, llm=True):
            raise KillSwitchEngaged("LLM calls are disabled by the kill switch")
        tcfg = self.svc.settings.tenants.get(req.tenant, {})
        row = self.svc.db.query_one("SELECT llm_requests, cost_usd FROM quotas WHERE tenant=? AND day=?",
                                    (req.tenant, date.today().isoformat())) or {"llm_requests": 0, "cost_usd": 0.0}
        limit_req = int(self.svc.flags.get(f"quota.{req.tenant}.daily_llm_requests", tcfg.get("daily_llm_requests", 10 ** 9)))
        if row["llm_requests"] >= limit_req:
            raise QuotaExceeded(f"tenant {req.tenant} reached its daily LLM request quota ({limit_req})")
        if row["cost_usd"] >= float(tcfg.get("daily_usd", 10 ** 9)):
            raise QuotaExceeded(f"tenant {req.tenant} reached its daily LLM budget (${tcfg.get('daily_usd')})")
        if req.run_id:
            spent = self.svc.db.query_one("SELECT COALESCE(SUM(cost_usd),0) AS usd, COALESCE(SUM(input_tokens+output_tokens),0) AS tok "
                                          "FROM llm_calls WHERE run_id=?", (req.run_id,))
            b = self.svc.settings.budgets
            if spent["usd"] >= b.per_run_usd or spent["tok"] >= b.per_run_tokens:
                raise BudgetExceeded(f"run {req.run_id} exhausted its budget (${spent['usd']:.4f}, {spent['tok']} tokens)")

    # ---- guardrails / compaction -----------------------------------------------------------
    def _guard(self, messages: list[dict]) -> tuple[list[dict], int]:
        g = self.svc.settings.guardrails
        out, redactions = [], 0
        for m in messages:
            content = m["content"]
            if g.pii_redaction_for_llm:
                content, n = redact_for_model(content)
                redactions += n
            out.append({**m, "content": content})
        if g.secret_leak_check:
            check_secret_leak("\n".join(m["content"] for m in out), self.svc.secrets.known_values())
        return out, redactions

    @staticmethod
    def _compact(messages: list[dict], max_tokens: int) -> tuple[list[dict], bool]:
        msgs = [dict(m) for m in messages]
        changed = False
        while count_messages(msgs) > max_tokens:
            idx = max(range(1, len(msgs)), key=lambda i: len(msgs[i]["content"]), default=None)
            if idx is None or len(msgs[idx]["content"]) < 400:
                break
            c = msgs[idx]["content"]
            keep = len(c) // 3
            msgs[idx]["content"] = c[:keep] + "\n…[context compacted: middle removed]…\n" + c[-keep:]
            changed = True
        return msgs, changed

    # ---- main entry point --------------------------------------------------------------------
    def chat(self, req: LLMRequest) -> LLMResponse:
        chain = self.route(req.role)
        attrs = {T.GEN_AI_OPERATION: "chat", "swarmpipe.role": req.role, T.GEN_AI_AGENT_NAME: req.agent,
                 "swarmpipe.prompt": f"{req.prompt_id}.{req.prompt_version}", "swarmpipe.route": ",".join(chain),
                 "swarmpipe.run_id": req.run_id, "swarmpipe.incident_id": req.incident_id, T.GEN_AI_TEMPERATURE: req.temperature}
        with self.svc.tracer.span(f"chat {req.role}", "client", attrs) as span:
            self._check_limits(req)
            messages, redactions = self._guard(req.messages)
            if redactions:
                span.event("pii_redacted", count=redactions)
            if not chain:
                raise LLMUnavailable(f"no routable (certified) model for role {req.role}")
            cache_key = None
            if req.cacheable and req.temperature == 0 and self.svc.settings.llm.cache_enabled and not self.svc.flags.get("llm.cache_disabled"):
                cache_key = sha256_text(f"{chain[0]}|{req.schema.__name__ if req.schema else ''}|{normalized_messages(messages)}")
                hit = self.svc.db.query_one("SELECT * FROM llm_cache WHERE key=?", (cache_key,))
                if hit and time.time() - hit["created_at"] < self.svc.settings.llm.cache_ttl_s:
                    self.svc.db.execute("UPDATE llm_cache SET hits=hits+1 WHERE key=?", (cache_key,))
                    data = loads(hit["response"], {})
                    parsed = req.schema.model_validate(data["parsed"]) if req.schema else data["parsed"]
                    self._record(req, span, hit["model"], "cache", 0, 0, 0.0, 0.0, "ok", None, 0, cached=True)
                    span.set_attrs({"swarmpipe.cache": "hit", T.GEN_AI_RESPONSE_MODEL: hit["model"]})
                    self.svc.metrics.inc("llm_cache_hits_total", role=req.role)
                    return LLMResponse(data["text"], parsed, hit["model"], "cache", 0, 0, 0.0, 0.0, cached=True, call_id="cache")
            errors: list[str] = []
            tried: list[str] = []
            for model_id in chain:
                mcfg = self.svc.settings.llm.models.get(model_id)
                if not mcfg:
                    errors.append(f"{model_id}: not configured")
                    continue
                if not self.breaker.allow(model_id):
                    errors.append(f"{model_id}: circuit open")
                    span.event("circuit_open_skip", model=model_id)
                    continue
                tried.append(model_id)
                resp = self._try_model(req, model_id, mcfg, messages, span, errors)
                if resp is not None:
                    resp.fallback_chain = tried
                    if len(tried) > 1 or errors:
                        self.svc.metrics.inc("llm_fallbacks_total", role=req.role, served_by=model_id)
                        span.event("fallback", served_by=model_id, errors=errors[-3:])
                    if cache_key:
                        self.svc.db.execute("INSERT OR REPLACE INTO llm_cache(key, model, response, created_at, hits) VALUES(?,?,?,?,0)",
                                            (cache_key, model_id, dumps({"text": resp.text, "parsed": resp.parsed.model_dump() if hasattr(resp.parsed, "model_dump") else resp.parsed}), time.time()))
                    span.set_attrs({T.GEN_AI_RESPONSE_MODEL: model_id, T.GEN_AI_INPUT_TOKENS: resp.input_tokens,
                                    T.GEN_AI_OUTPUT_TOKENS: resp.output_tokens, "swarmpipe.cost_usd": resp.cost_usd,
                                    "swarmpipe.repairs": resp.repairs})
                    return resp
            self.svc.metrics.inc("llm_unavailable_total", role=req.role)
            raise LLMUnavailable(f"all models failed for role {req.role}: {'; '.join(errors[-4:])}", details={"errors": errors})

    def _try_model(self, req: LLMRequest, model_id: str, mcfg: dict, messages: list[dict], parent, errors: list[str]):
        prov_name = mcfg["provider"]
        provider = self.provider(prov_name)
        s = self.svc.settings.llm
        window = int(mcfg.get("context_window", 16000))
        msgs, compacted = self._compact(messages, int(window * 0.85) - req.max_output_tokens)
        if compacted:
            parent.event("context_compacted", model=model_id)
        repairs = 0
        attempt = 0
        transport_failures = 0
        while True:
            attempt += 1
            with self.svc.tracer.span(f"llm.call {model_id}", "client", {
                    T.GEN_AI_OPERATION: "chat", T.GEN_AI_PROVIDER: prov_name, T.GEN_AI_REQUEST_MODEL: mcfg.get("name"),
                    T.GEN_AI_MAX_TOKENS: req.max_output_tokens, "swarmpipe.attempt": attempt, "swarmpipe.repair": repairs}) as sp:
                t0 = time.perf_counter()
                try:
                    with self.bulkhead:
                        res = provider.complete(mcfg.get("name", model_id), msgs, temperature=req.temperature,
                                                max_tokens=req.max_output_tokens, json_mode=req.schema is not None)
                except ProviderError as exc:
                    lat = (time.perf_counter() - t0) * 1000
                    sp.error(exc)
                    self._record(req, sp, model_id, prov_name, 0, 0, 0.0, lat, "error", f"{type(exc).__name__}: {exc}", attempt)
                    errors.append(f"{model_id}: {type(exc).__name__}")
                    if isinstance(exc, ProviderBadRequest) or not exc.retryable:
                        self.breaker.failure(model_id)
                        return None
                    transport_failures += 1
                    if transport_failures > s.max_retries:
                        if self.breaker.failure(model_id):
                            self.svc.metrics.inc("llm_breaker_open_total", model=model_id)
                            self.svc.audit.record("system:gateway", "circuit_breaker.open", model_id, "open", {"errors": errors[-3:]})
                        return None
                    delay = exc.retry_after if isinstance(exc, ProviderRateLimited) else backoff_delay(transport_failures, 0.2, 3.0)
                    time.sleep(min(delay, 5.0))
                    continue
                cost = res.input_tokens / 1000 * float(mcfg.get("usd_per_1k_in", 0)) + res.output_tokens / 1000 * float(mcfg.get("usd_per_1k_out", 0))
                sp.set_attrs({T.GEN_AI_RESPONSE_MODEL: res.model, T.GEN_AI_INPUT_TOKENS: res.input_tokens,
                              T.GEN_AI_OUTPUT_TOKENS: res.output_tokens, "swarmpipe.cost_usd": round(cost, 8)})
                parsed, err = self._parse(res.text, req.schema)
                status = "ok" if err is None else "invalid_output"
                if err:
                    sp.event("validation_failed", error=err[:300])
                self._record(req, sp, model_id, prov_name, res.input_tokens, res.output_tokens, cost, res.latency_ms, status, err, attempt,
                             purpose="repair" if repairs else None)
            if err is None:
                self.breaker.success(model_id)
                return LLMResponse(res.text, parsed, model_id, prov_name, res.input_tokens, res.output_tokens, round(cost, 8),
                                   res.latency_ms, attempts=attempt, repairs=repairs, call_id=model_id)
            if repairs >= s.max_repairs:
                errors.append(f"{model_id}: invalid output after {repairs} repairs")
                self.breaker.failure(model_id)
                return None
            repairs += 1
            self.svc.metrics.inc("llm_repairs_total", model=model_id, role=req.role)
            msgs = msgs + [{"role": "assistant", "content": res.text[:4000]},
                           {"role": "user", "content": f"REPAIR: your previous output was invalid ({err[:500]}). Return ONLY one JSON object "
                                                       f"that matches the required schema. No prose, no code fences."}]

    @staticmethod
    def _parse(text: str, schema):
        if schema is None:
            return text, None
        try:
            data = extract_json(text)
        except ValueError as exc:
            return None, f"JSON parse error: {exc}"
        try:
            return schema.model_validate(data), None
        except Exception as exc:  # pydantic ValidationError
            return None, f"schema validation error: {str(exc)[:600]}"

    def _record(self, req: LLMRequest, span, model_id: str, provider: str, tin: int, tout: int, cost: float, latency: float,
                status: str, error: str | None, attempt: int, cached: bool = False, purpose: str | None = None) -> None:
        self.svc.db.insert("llm_calls", {
            "id": new_id("llm"), "ts": iso(), "trace_id": span.trace_id, "span_id": span.span_id, "run_id": req.run_id,
            "incident_id": req.incident_id, "tenant": req.tenant, "agent": req.agent, "role": req.role, "model": model_id,
            "provider": provider, "prompt_id": req.prompt_id, "prompt_version": req.prompt_version, "prompt_hash": req.prompt_hash,
            "input_tokens": tin, "output_tokens": tout, "cost_usd": round(cost, 8), "latency_ms": round(latency, 2),
            "cached": 1 if cached else 0, "status": status, "error": (error or "")[:1000] or None, "attempt": attempt,
            "purpose": purpose or req.purpose})
        if provider != "cache":
            day = date.today().isoformat()
            self.svc.db.execute("INSERT INTO quotas(tenant, day, llm_requests, cost_usd) VALUES(?,?,1,?) "
                                "ON CONFLICT(tenant, day) DO UPDATE SET llm_requests=llm_requests+1, cost_usd=cost_usd+excluded.cost_usd",
                                (req.tenant, day, cost))
        self.svc.metrics.inc("llm_calls_total", model=model_id, status=status, role=req.role)
        if not cached:
            self.svc.metrics.observe("llm_latency_ms", latency, model=model_id)
            self.svc.metrics.inc("llm_tokens_total", tin, model=model_id, direction="input")
            self.svc.metrics.inc("llm_tokens_total", tout, model=model_id, direction="output")
            self.svc.metrics.inc("llm_cost_usd_total", cost, tenant=req.tenant, agent=req.agent)

    def status(self) -> dict:
        prof = self.active_profile()
        roles = sorted(set((self.svc.settings.llm.profiles.get(prof) or {}).keys()))
        return {"active_profile": prof, "routes": {r: self.route(r) for r in roles}, "breakers": self.breaker.snapshot(),
                "models": self.svc.settings.llm.models}
