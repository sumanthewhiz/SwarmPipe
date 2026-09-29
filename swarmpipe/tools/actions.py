"""The remediation action catalog: agents pick from a catalog of actions, never free-form actions.

Every action is parameterized and typed, has a policy risk tier (config/policies.yaml), a
compensation (saga rollback) and a verification against ground truth. Actions are exposed to the
Executor agent ONLY, as `act_*` tools behind the tool gateway, so read-only agents physically cannot
invoke them (privilege separation)."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from pydantic import BaseModel, Field

from swarmpipe.core.errors import SwarmError
from swarmpipe.core.util import loads
from swarmpipe.tools.gateway import ToolSpec


class NotifyParams(BaseModel):
    recipient: str = Field(min_length=1, max_length=200)
    subject: str = Field(default="", max_length=200)
    message: str = Field(default="", max_length=4000)


class ResendParams(BaseModel):
    recipient: str = Field(min_length=1, max_length=200)
    message: str = Field(default="", max_length=4000)


class VersionParams(BaseModel):
    version_id: str


class HoldParams(BaseModel):
    datasets: list[str] = Field(min_length=1, max_length=10)
    reason: str = ""


class RollbackParams(BaseModel):
    dataset: str
    to_version_id: str | None = None


class ReprocessParams(BaseModel):
    file_id: str
    dataset: str
    mapping: dict[str, str] = Field(min_length=1)


class UpdateContractParams(BaseModel):
    dataset: str
    add_columns: list[dict] = Field(default_factory=list)
    aliases: dict[str, list[str]] = Field(default_factory=dict)


class ForcePublishParams(BaseModel):
    version_id: str
    justification: str = Field(min_length=10, max_length=500)


@dataclass
class ActionSpec:
    name: str
    description: str
    params: type[BaseModel]
    execute: Callable
    compensate: Callable | None
    verify: Callable


RECIPIENT_HANDLES = ("owner", "dataset_owner", "source_owner", "security", "oncall")


def _owner(svc, dataset: str | None, recipient: str) -> str:
    """Agents only ever see recipient *handles*; the executor resolves them (data minimization)."""
    r = recipient.strip()
    if r in ("owner", "dataset_owner"):
        return (svc.context.owner(dataset) if dataset else None) or "oncall@contoso.example"
    if r == "source_owner":
        c = svc.contracts.active(dataset) if dataset else None
        return (c or {}).get("source_owner") or "oncall@contoso.example"
    if r == "security":
        return "security@contoso.example"
    if r == "oncall":
        return "oncall@contoso.example"
    if r.startswith("[") or ("@" not in r and not r.lower().startswith(("http://", "https://"))):
        raise SwarmError(f"invalid recipient {recipient!r}: use a handle ({', '.join(RECIPIENT_HANDLES)}), an email or an allowlisted URL",
                         code="INVALID_ARGUMENTS")
    return r


# ---- execute ---------------------------------------------------------------------------------
def x_notify(ctx, p: NotifyParams):
    svc = ctx.svc
    row = svc.notifier.send(ctx.tenant, _owner(svc, ctx.dataset, p.recipient), p.subject or "SwarmPipe notification", p.message,
                            incident_id=ctx.incident_id, sender=ctx.identity.principal)
    return {"notification_id": row["id"], "channel": row["channel"], "recipient": row["recipient"], "path": row["path"]}


def x_resend(ctx, p: ResendParams):
    svc = ctx.svc
    row = svc.notifier.send(ctx.tenant, _owner(svc, ctx.dataset, p.recipient), "[resend request] please re-deliver the extract", p.message,
                            incident_id=ctx.incident_id, sender=ctx.identity.principal)
    return {"notification_id": row["id"], "channel": row["channel"], "recipient": row["recipient"], "path": row["path"]}


def x_quarantine(ctx, p: VersionParams):
    svc = ctx.svc
    v = svc.publishing.get(p.version_id)
    if not v or v["tenant"] != ctx.tenant:
        raise KeyError(p.version_id)
    if v["status"] == "quarantined":
        return {"version_id": p.version_id, "already": True}
    res = svc.publishing.unpublish(p.version_id, ctx.identity.principal, "quarantined", f"quarantined by incident {ctx.incident_id}")
    return {**res, "previous_status": v["status"]}


def c_quarantine(ctx, p: VersionParams, result: dict):
    if result.get("already"):
        return None
    return ctx.svc.publishing.publish(p.version_id, ctx.identity.principal, fresh=False)


def x_hold(ctx, p: HoldParams):
    svc = ctx.svc
    for ds in p.datasets:
        svc.db.execute("INSERT INTO dataset_state(tenant, dataset, hold, hold_reason, hold_by) VALUES(?,?,1,?,?) "
                       "ON CONFLICT(tenant, dataset) DO UPDATE SET hold=1, hold_reason=excluded.hold_reason, hold_by=excluded.hold_by",
                       (ctx.tenant, ds, p.reason[:200], ctx.identity.principal))
    return {"held": p.datasets}


def x_release(ctx, p: HoldParams):
    svc = ctx.svc
    for ds in p.datasets:
        svc.db.execute("UPDATE dataset_state SET hold=0, hold_reason=NULL, hold_by=NULL WHERE tenant=? AND dataset=?", (ctx.tenant, ds))
        svc.events.publish("dataset.released", {"tenant": ctx.tenant, "dataset": ds}, ctx.tenant)
    return {"released": p.datasets}


def c_hold(ctx, p: HoldParams, result: dict):
    return x_release(ctx, p)


def c_release(ctx, p: HoldParams, result: dict):
    return x_hold(ctx, p)


def x_rollback(ctx, p: RollbackParams):
    svc = ctx.svc
    cur = svc.publishing.current(ctx.tenant, p.dataset)
    if not cur:
        raise SwarmError(f"{p.dataset} has no published version", code="NOT_FOUND")
    target = p.to_version_id
    if not target:
        prev = svc.publishing.previous_good(ctx.tenant, p.dataset, cur["version"])
        if not prev:
            raise SwarmError("no earlier good version to roll back to", code="CONFLICT")
        target = prev["id"]
    if target == cur["id"]:
        res = svc.publishing.restore_from_snapshot(cur["id"], ctx.identity.principal)
        return {"mode": "restore_snapshot", "version_id": cur["id"], **res}
    svc.publishing.publish(target, ctx.identity.principal, fresh=False)
    svc.db.update("dataset_versions", {"id": cur["id"]}, {"status": "rolled_back"})
    return {"mode": "repoint", "from_version_id": cur["id"], "to_version_id": target}


def c_rollback(ctx, p: RollbackParams, result: dict):
    if result.get("mode") == "repoint":
        return ctx.svc.publishing.publish(result["from_version_id"], ctx.identity.principal, fresh=False)
    return None


def _frame_for(svc, file_id: str, dataset: str) -> dict:
    run = svc.db.query_one("SELECT id FROM runs WHERE workflow='ingest_file' AND file_id=? ORDER BY created_at LIMIT 1", (file_id,))
    if not run:
        raise KeyError(f"no ingest run for file {file_id}")
    child = svc.db.query("SELECT input FROM runs WHERE parent_run_id=? AND workflow='ingest_dataset'", (run["id"],))
    for c in child:
        inp = loads(c["input"], {})
        if inp.get("dataset") == dataset or inp.get("dataset_hint") == dataset:
            return inp
    if child:
        return loads(child[0]["input"], {})
    raise KeyError(f"no frame for dataset {dataset} in file {file_id}")


def x_reprocess(ctx, p: ReprocessParams):
    svc = ctx.svc
    contract = svc.contracts.active(p.dataset)
    if not contract:
        raise SwarmError(f"no active contract for {p.dataset}", code="NOT_FOUND")
    names = {c["name"] for c in contract.get("columns", [])}
    bad = [t for t in p.mapping.values() if t not in names]
    if bad:
        raise SwarmError(f"mapping targets not in contract: {bad}", code="INVALID_ARGUMENTS")
    frame = _frame_for(svc, p.file_id, p.dataset)
    if not Path(frame.get("frame_path", "")).exists():
        raise SwarmError("the original raw frame is no longer available", code="GONE")
    child = svc.engine.submit("ingest_dataset", {**frame, "dataset": p.dataset, "mapping_override": p.mapping,
                                                 "reprocess_of": frame.get("parent_run_id"), "incident_id": ctx.incident_id},
                              tenant=ctx.tenant, dataset=p.dataset, file_id=p.file_id, parent_run_id=ctx.run_id,
                              incident_id=ctx.incident_id, priority=3)
    return {"child_run_id": child, "mapping": p.mapping}


def c_reprocess(ctx, p: ReprocessParams, result: dict):
    svc = ctx.svc
    v = svc.db.query_one("SELECT id, status FROM dataset_versions WHERE run_id=?", (result.get("child_run_id"),))
    if v and v["status"] in ("published", "published_override"):
        return svc.publishing.unpublish(v["id"], ctx.identity.principal, "rolled_back", "compensation of reprocess_with_mapping")
    return None


def x_update_contract(ctx, p: UpdateContractParams):
    svc = ctx.svc
    c = svc.contracts.active(p.dataset)
    if not c:
        raise SwarmError(f"no active contract for {p.dataset}", code="NOT_FOUND")
    from_v = c["version"]
    names = {col["name"] for col in c["columns"]}
    for col in p.add_columns:
        if col.get("name") and col["name"] not in names:
            c["columns"].append({"name": col["name"], "type": col.get("type", "string"), "required": False})
    for col in c["columns"]:
        if col["name"] in p.aliases:
            col["aliases"] = sorted(set(col.get("aliases", [])) | set(p.aliases[col["name"]]))
    v = svc.contracts.propose(p.dataset, c, ctx.identity.principal, f"incident {ctx.incident_id}: add {p.add_columns} aliases {p.aliases}")
    svc.contracts.activate(p.dataset, v, ctx.identity.describe())
    return {"dataset": p.dataset, "from_version": from_v, "to_version": v}


def c_update_contract(ctx, p: UpdateContractParams, result: dict):
    ctx.svc.contracts.revert_to(p.dataset, result["from_version"], ctx.identity.principal)
    return {"reverted_to": result["from_version"]}


def x_force_publish(ctx, p: ForcePublishParams):
    svc = ctx.svc
    v = svc.publishing.get(p.version_id)
    if not v or v["tenant"] != ctx.tenant:
        raise KeyError(p.version_id)
    return svc.publishing.force_publish(p.version_id, ctx.identity.describe(), p.justification)


def c_force_publish(ctx, p: ForcePublishParams, result: dict):
    return ctx.svc.publishing.unpublish(p.version_id, ctx.identity.principal, "rolled_back", "compensation of force_publish")


# ---- verify (ground truth, not the agent's claim) ---------------------------------------------
def v_notify(ctx, p, result):
    row = ctx.svc.db.query_one("SELECT status, path FROM notifications WHERE id=?", (result.get("notification_id"),))
    ok = bool(row and row["status"] == "sent" and row["path"] and Path(row["path"]).exists())
    return {"ok": ok, "detail": row}


def v_quarantine(ctx, p: VersionParams, result):
    v = ctx.svc.publishing.get(p.version_id)
    target = ctx.svc.wh.view_target(v["tenant"], v["dataset"])
    ok = v["status"] == "quarantined" and target != v["table_name"]
    return {"ok": ok, "detail": {"status": v["status"], "view_target": target}}


def v_hold(ctx, p: HoldParams, result):
    rows = ctx.svc.db.query("SELECT dataset, hold FROM dataset_state WHERE tenant=?", (ctx.tenant,))
    held = {r["dataset"] for r in rows if r["hold"]}
    return {"ok": set(p.datasets) <= held, "detail": {"held": sorted(held)}}


def v_release(ctx, p: HoldParams, result):
    rows = ctx.svc.db.query("SELECT dataset, hold FROM dataset_state WHERE tenant=?", (ctx.tenant,))
    held = {r["dataset"] for r in rows if r["hold"]}
    return {"ok": not (set(p.datasets) & held), "detail": {"held": sorted(held)}}


def v_rollback(ctx, p: RollbackParams, result):
    svc = ctx.svc
    vid = result.get("to_version_id") or result.get("version_id")
    v = svc.publishing.get(vid)
    target = svc.wh.view_target(ctx.tenant, p.dataset)
    checksum = svc.wh.checksum(v["table_name"])
    ok = target == v["table_name"] and checksum == v["checksum"]
    return {"ok": ok, "detail": {"view_target": target, "checksum": checksum, "recorded": v["checksum"]}}


def v_reprocess(ctx, p: ReprocessParams, result):
    svc = ctx.svc
    run = svc.db.query_one("SELECT status FROM runs WHERE id=?", (result.get("child_run_id"),))
    if not run:
        return {"ok": False, "detail": "child run missing"}
    if run["status"] not in ("succeeded", "failed", "dead_lettered", "quarantined", "cancelled", "skipped"):
        return {"ok": None, "pending": True, "wait_for": f"run:{result['child_run_id']}"}
    v = svc.db.query_one("SELECT status FROM dataset_versions WHERE run_id=?", (result["child_run_id"],))
    ok = run["status"] == "succeeded" and bool(v and v["status"] in ("published", "published_override"))
    return {"ok": ok, "detail": {"run_status": run["status"], "version_status": v["status"] if v else None}}


def v_update_contract(ctx, p: UpdateContractParams, result):
    c = ctx.svc.contracts.active(p.dataset)
    return {"ok": bool(c and c["version"] == result.get("to_version")), "detail": {"active_version": c["version"] if c else None}}


def v_force_publish(ctx, p: ForcePublishParams, result):
    v = ctx.svc.publishing.get(p.version_id)
    target = ctx.svc.wh.view_target(v["tenant"], v["dataset"])
    return {"ok": target == v["table_name"], "detail": {"view_target": target, "status": v["status"]}}


ACTIONS: dict[str, ActionSpec] = {
    "notify_owner": ActionSpec("notify_owner", "Send the incident summary and evidence to an owner (email/chat outbox; URLs must be allowlisted).",
                               NotifyParams, x_notify, None, v_notify),
    "request_resend": ActionSpec("request_resend", "Ask the source owner to re-deliver a complete, current extract.", ResendParams, x_resend, None, v_notify),
    "quarantine_version": ActionSpec("quarantine_version", "Un-publish a published version; consumers fall back to the previous good version.",
                                     VersionParams, x_quarantine, c_quarantine, v_quarantine),
    "hold_downstream": ActionSpec("hold_downstream", "Pause rebuilds of derived datasets so bad data does not propagate.", HoldParams, x_hold, c_hold, v_hold),
    "release_hold": ActionSpec("release_hold", "Release held datasets so they rebuild.", HoldParams, x_release, c_release, v_release),
    "rollback_dataset": ActionSpec("rollback_dataset", "Re-point a dataset to an earlier version, or restore the current version from its immutable snapshot.",
                                   RollbackParams, x_rollback, c_rollback, v_rollback),
    "reprocess_with_mapping": ActionSpec("reprocess_with_mapping", "Re-run a quarantined batch with a column rename mapping.",
                                         ReprocessParams, x_reprocess, c_reprocess, v_reprocess),
    "update_contract": ActionSpec("update_contract", "Create and activate a new contract version (add optional columns / aliases).",
                                  UpdateContractParams, x_update_contract, c_update_contract, v_update_contract),
    "force_publish": ActionSpec("force_publish", "Publish a quarantined version despite failed checks (like 'set to OK'). Highest risk.",
                                ForcePublishParams, x_force_publish, c_force_publish, v_force_publish),
}


def action_tools() -> list[ToolSpec]:
    def _handler(spec: ActionSpec):
        return lambda ctx, params: spec.execute(ctx, params)

    tools = []
    for name, spec in ACTIONS.items():
        tools.append(ToolSpec(f"act_{name}", "1.0", spec.description, spec.params, _handler(spec), f"action:{name}",
                              read_only=False, destructive=name in ("force_publish", "rollback_dataset", "quarantine_version", "update_contract"),
                              idempotent=name not in ("notify_owner", "request_resend", "reprocess_with_mapping"),
                              open_world=name in ("notify_owner", "request_resend"), rate_limit_per_min=30))
    return tools


def catalog_for_prompt(policies: dict) -> list[dict]:
    out = []
    for name, spec in ACTIONS.items():
        pol = policies.get("actions", {}).get(name, {})
        props = spec.params.model_json_schema().get("properties", {})
        out.append({"action": name, "risk": pol.get("risk"), "description": spec.description,
                    "params": {k: v.get("type", "any") for k, v in props.items()}})
    return out
