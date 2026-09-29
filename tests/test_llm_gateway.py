"""Model gateway: retries, repairs, fallback, circuit breaker, cache, budgets, quotas, redaction."""
import pytest

from swarmpipe.core.errors import LLMUnavailable, QuotaExceeded
from swarmpipe.llm.prompts import DataBlock, build_messages
from swarmpipe.llm.types import LLMRequest, RouterOut


def req(svc, role="router", **kw):
    tpl = svc.prompts.get("router")
    msgs = build_messages(tpl, "Classify.", [DataBlock("context", {"file_name": "a.csv", "sniff": {"kind_hint": "tabular"}}, "trusted"),
                                             DataBlock("first_lines", kw.pop("text", "id,amount\n1,2"), "untrusted")], svc.settings, RouterOut)
    return LLMRequest(role=role, prompt_id=tpl.id, prompt_version=tpl.version, prompt_hash=tpl.hash, messages=msgs, schema=RouterOut,
                      agent="agent:test", tenant=kw.pop("tenant", "default"), cacheable=kw.pop("cacheable", False), **kw)


def test_malformed_output_is_extracted_or_repaired(svc):
    svc.flags.set("chaos.llm_malformed_rate", 1.0)
    results = [svc.llm.chat(req(svc, text=f"id,amount\n{i},2")) for i in range(8)]
    assert all(r.parsed.kind == "tabular" for r in results)
    assert sum(r.repairs for r in results) >= 1                      # truncated JSON -> repair loop
    assert any(r.repairs == 0 and "```" in r.text for r in results)   # fenced JSON -> tolerant extraction


def test_outage_falls_back_then_breaker_opens(svc):
    svc.flags.set("chaos.llm_outage_models", ["sim-large"])
    for _ in range(3):
        r = svc.llm.chat(req(svc, role="diagnoser"))
        assert r.model == "sim-small"
    assert svc.llm.breaker.snapshot()["sim-large"]["state"] == "open"


def test_all_models_down_raises_llm_unavailable(svc):
    svc.flags.set("chaos.llm_outage_models", ["sim-small", "sim-large"])
    with pytest.raises(LLMUnavailable):
        svc.llm.chat(req(svc))


def test_cache_hit_costs_nothing(svc):
    a = svc.llm.chat(req(svc, cacheable=True))
    b = svc.llm.chat(req(svc, cacheable=True))
    assert not a.cached and b.cached and b.cost_usd == 0


def test_tenant_quota(svc):
    svc.flags.set("quota.acme.daily_llm_requests", 1)
    svc.llm.chat(req(svc, tenant="acme"))
    with pytest.raises(QuotaExceeded):
        svc.llm.chat(req(svc, tenant="acme"))


def test_pii_is_redacted_before_the_model_sees_it(svc):
    svc.llm.chat(req(svc, text="email jane.doe@example.com card 4111 1111 1111 1111"))
    seen = {}
    prov = svc.llm.provider("mock")
    orig = prov.complete

    def spy(model_name, messages, **kw):
        seen["text"] = "\n".join(m["content"] for m in messages)
        return orig(model_name, messages, **kw)

    prov.complete = spy
    svc.llm.chat(req(svc, text="email jane.doe@example.com card 4111 1111 1111 1111"))
    assert "jane.doe@example.com" not in seen["text"] and "[EMAIL]" in seen["text"] and "[CARD]" in seen["text"]


def test_unapproved_prompt_is_refused(svc, tmp_path):
    from swarmpipe.core.errors import GuardrailViolation

    tpl = svc.prompts.get("router")
    svc.prompts._templates[tpl.key].approved = False
    with pytest.raises(GuardrailViolation):
        svc.prompts.get("router")
