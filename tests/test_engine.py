"""Durable engine: checkpoints/resume, retries, permanent failure hooks, human interrupts,
idempotency, saga compensation, deferral and lease recovery - with toy workflows."""
from swarmpipe.core.errors import Deferred, PermanentError, RetryableError
from swarmpipe.core.util import iso
from swarmpipe.runtime.engine import RetryPolicy, Step, Workflow


def test_retry_then_success_reuses_checkpoints(svc):
    calls = {"a": 0, "b": 0}

    def a(ctx):
        calls["a"] += 1
        return {"v": 1}

    def b(ctx):
        calls["b"] += 1
        if calls["b"] < 3:
            raise RetryableError("flaky")
        return {"v": ctx.outputs["a"]["v"] + 1}

    svc.engine.register(Workflow("toy_retry", [Step("a", a), Step("b", b, retry=RetryPolicy(5, 0.01, 0.02))]))
    rid = svc.engine.submit("toy_retry", {}, "default")
    svc.engine.run_until_quiescent(timeout_s=30)
    run = svc.db.query_one("SELECT status FROM runs WHERE id=?", (rid,))
    assert run["status"] == "succeeded"
    assert calls == {"a": 1, "b": 3}  # step a was checkpointed once and never re-executed


def test_permanent_failure_runs_compensation_and_hook(svc):
    comp, hook = [], []
    svc.engine.register_compensator("undo_thing", lambda s, p: comp.append(p["x"]))

    def side_effect(ctx):
        ctx.add_compensation("undo_thing", {"x": 42}, "side_effect")
        return {}

    def boom(ctx):
        raise PermanentError("cannot continue")

    def on_failure(ctx, exc):
        hook.append(str(exc))
        return "dead_lettered"

    svc.engine.register(Workflow("toy_saga", [Step("side_effect", side_effect), Step("boom", boom)], on_failure=on_failure))
    rid = svc.engine.submit("toy_saga", {}, "default")
    svc.engine.run_until_quiescent(timeout_s=30)
    assert svc.db.query_one("SELECT status FROM runs WHERE id=?", (rid,))["status"] == "dead_lettered"
    assert comp == [42] and hook == ["cannot continue"]


def test_human_interrupt_survives_and_resumes(svc):
    def ask(ctx):
        aid = ctx.context.get("aid") or svc.approvals.request("action", tenant="default", subject="toy", risk="low", summary="toy", run_id=ctx.run_id)
        ctx.set("aid", aid)
        ap = svc.approvals.get(aid)
        if ap["status"] == "pending":
            ctx.wait(f"approval:{aid}")
        return {"decision": ap["status"]}

    svc.engine.register(Workflow("toy_human", [Step("ask", ask)]))
    rid = svc.engine.submit("toy_human", {}, "default")
    svc.engine.run_until_quiescent(timeout_s=30)
    assert svc.db.query_one("SELECT status, waiting_on FROM runs WHERE id=?", (rid,))["status"] == "waiting"
    aid = svc.approvals.list("pending")[0]["id"]
    svc.approvals.decide(aid, "approved", svc.identity.user("oncall"), comment="ok")
    svc.engine.run_until_quiescent(timeout_s=30)
    row = svc.db.query_one("SELECT status FROM runs WHERE id=?", (rid,))
    assert row["status"] == "succeeded"


def test_idempotency_key_prevents_double_side_effects(svc):
    effects = []

    def step(ctx):
        return ctx.idempotent("toy:publish", lambda: effects.append(1) or {"n": len(effects)})

    svc.engine.register(Workflow("toy_idem", [Step("s", step)]))
    for _ in range(3):
        svc.engine.submit("toy_idem", {}, "default")
    svc.engine.run_until_quiescent(timeout_s=30)
    assert effects == [1]


def test_deferred_does_not_consume_attempts(svc):
    state = {"n": 0}

    def step(ctx):
        state["n"] += 1
        if state["n"] < 4:
            raise Deferred("busy", delay_s=0.01)
        return {}

    svc.engine.register(Workflow("toy_defer", [Step("s", step, retry=RetryPolicy(1, 0.01, 0.01))]))
    rid = svc.engine.submit("toy_defer", {}, "default")
    svc.engine.run_until_quiescent(timeout_s=30)
    assert svc.db.query_one("SELECT status FROM runs WHERE id=?", (rid,))["status"] == "succeeded"


def test_reaper_recovers_expired_leases(svc):
    svc.engine.register(Workflow("toy_noop", [Step("s", lambda ctx: {})]))
    rid = svc.engine.submit("toy_noop", {}, "default")
    svc.db.execute("UPDATE runs SET status='running', lease_owner='dead-worker', lease_expires_at=? WHERE id=?", ("2000-01-01T00:00:00.000+00:00", rid))
    assert svc.engine.reap_expired_leases() == 1
    svc.engine.run_until_quiescent(timeout_s=30)
    row = svc.db.query_one("SELECT status, recovered_count FROM runs WHERE id=?", (rid,))
    assert row["status"] == "succeeded" and row["recovered_count"] == 1
    assert iso()
