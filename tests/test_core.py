"""Core primitives: migrations, events (at-least-once), audit hash chain, flags, backoff."""
from swarmpipe.core.events import Dispatcher
from swarmpipe.core.util import backoff_delay


def test_migrations_are_idempotent(svc):
    assert svc.db.schema_version() >= 1
    assert svc.db.migrate() == 0


def test_event_delivery_is_at_least_once_and_skips_poison(svc):
    bus = svc.events
    seen = []
    calls = {"n": 0}

    def flaky(ev):
        calls["n"] += 1
        if ev["payload"]["k"] == 1 and calls["n"] == 1:
            raise RuntimeError("transient")
        if ev["payload"]["k"] == 2:
            raise RuntimeError("poison")
        seen.append(ev["payload"]["k"])

    d = Dispatcher(bus)
    d.subscribe("t", ["test.ev"], flaky)
    for k in (1, 2, 3):
        bus.publish("test.ev", {"k": k})
    for _ in range(6):
        d.poll_once()
    assert seen == [1, 3]  # 1 redelivered after a failure, 2 skipped after 3 failed deliveries


def test_audit_chain_detects_tampering(svc):
    for i in range(5):
        svc.audit.record("user:test", "demo.action", f"r{i}", "ok", {"i": i})
    assert svc.audit.verify()["ok"]
    svc.db.execute("UPDATE audit SET decision='forged' WHERE seq=3")
    res = svc.audit.verify()
    assert not res["ok"] and res["first_bad_seq"] == 3


def test_audit_chain_detects_deletion(svc):
    for i in range(4):
        svc.audit.record("user:test", "demo.action", f"r{i}", "ok")
    svc.db.execute("DELETE FROM audit WHERE seq=2")
    assert not svc.audit.verify()["ok"]


def test_runtime_flags_roundtrip(svc):
    svc.flags.set("chaos.llm_timeout_rate", 0.25)
    assert svc.flags.get("chaos.llm_timeout_rate") == 0.25
    svc.flags.clear("chaos.")
    assert svc.flags.get("chaos.llm_timeout_rate") is None


def test_backoff_grows_and_is_capped():
    assert 0.5 <= backoff_delay(1, 1.0, 20) <= 1.0
    assert backoff_delay(10, 1.0, 20) <= 20
