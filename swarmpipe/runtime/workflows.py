"""Workflow definitions: the deterministic skeletons that orchestrate the agent swarm.

  ingest_file     stage -> route (Router) -> read -> fan-out one child per sheet (hierarchical) -> finalize
  ingest_dataset  load -> privacy -> profile -> [onboard: human approval] -> contract_check -> transform
                  -> quality (circuit breaker) -> publish | quarantine -> lineage
  document        read -> guard (injection scan) -> summarize (Librarian) -> index -> archive
  derive          event-driven rebuild of derived datasets; blocked when inputs are held/incident-bound
  triage          open -> investigate (fan-out/fan-in) -> diagnose -> impact -> plan -> govern
                  -> execute (auto) -> await approvals (durable) -> verify -> learn -> close
"""
from __future__ import annotations

import re
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

from swarmpipe.agents.messaging import Blackboard
from swarmpipe.core.errors import Deferred, PermanentError
from swarmpipe.core.util import Clock, dumps, iso, loads, new_id, seconds_between, sha256_file, sha256_text
from swarmpipe.data.contracts import DATASET_RE
from swarmpipe.data.frames import load_frame, save_frame
from swarmpipe.data.profiling import profile_frame, schema_diff
from swarmpipe.data.readers import SniffResult, read_frames, read_text, sniff
from swarmpipe.data.transforms import TransformResult
from swarmpipe.governance.guardrails import scan_text
from swarmpipe.governance.policy import ActionContext
from swarmpipe.observability.logging import get_logger
from swarmpipe.runtime.engine import TERMINAL, RetryPolicy, Step, Workflow
from swarmpipe.signals import SEVERITIES
from swarmpipe.tools.actions import ACTIONS

log = get_logger("workflows")

CHECK_SIGNAL = {"schema": "schema_drift", "volume": "volume_anomaly", "freshness": "stale_data", "distribution": "distribution_shift",
                "lineage": "referential_break", "reconciliation": "reconciliation_break", "privacy": "pii_undeclared",
                "security": "injection_attempt", "quality": "quality_failure"}
RESEND_FIXABLE = {"truncated_extract", "stale_data_resent", "late_or_missing_delivery", "unit_or_scale_change", "data_quality_regression",
                  "referential_integrity_break", "schema_change_upstream", "duplicate_delivery"}


# ============================================================================== helpers
def _file(ctx) -> dict:
    f = ctx.svc.db.query_one("SELECT * FROM files WHERE id=?", (ctx.input["file_id"],))
    if not f:
        raise PermanentError(f"file record {ctx.input['file_id']} not found", code="NOT_FOUND")
    return f


def _move(src: Path, dst_dir: Path) -> Path:
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / src.name
    if dst.exists():
        dst = dst_dir / f"{src.stem}__{new_id('dup')[-8:]}{src.suffix}"
    shutil.move(str(src), str(dst))
    return dst


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def dataset_name_for(file_name: str, sheet: str | None = None) -> str:
    stem = file_name.rsplit(".", 1)[0].lower()
    stem = re.sub(r"[_\-\s]?\d{4}[-_]?\d{2}[-_]?\d{2}.*$", "", stem)
    stem = re.sub(r"[_\-\s]?(q[1-4]|v\d+|final|copy)$", "", stem)
    name = re.sub(r"[^a-z0-9]+", "_", stem).strip("_") or "dataset"
    if sheet:
        name = f"{name}_{re.sub(r'[^a-z0-9]+', '_', sheet.lower()).strip('_')}"
    if not name[0].isalpha():
        name = "ds_" + name
    return name[:40]


def acquire_lock(svc, name: str, owner: str, ttl_s: float = 60) -> bool:
    now = iso()
    with svc.db.tx():
        row = svc.db.query_one("SELECT owner, expires_at FROM locks WHERE name=?", (name,))
        if row and row["owner"] != owner and row["expires_at"] > now:
            return False
        svc.db.execute("INSERT OR REPLACE INTO locks(name, owner, expires_at) VALUES(?,?,?)", (name, owner, Clock.real_iso(ttl_s)))
    return True


def release_lock(svc, name: str, owner: str) -> None:
    svc.db.execute("DELETE FROM locks WHERE name=? AND owner=?", (name, owner))


def _signal_severity(check: dict) -> str | None:
    if check["status"] == "fail":
        return check["severity"] if check["severity"] in ("high", "critical") else "high"
    if check["status"] == "warn":
        if check["check_type"] in ("privacy", "security"):
            return "high"
        if check["check_type"] == "schema":
            return "warning"
        return "info"
    return None


# ============================================================================== ingest_file
def f_stage(ctx):
    svc = ctx.svc
    f = _file(ctx)
    path = Path(f["staged_path"])
    if not path.exists():
        raise PermanentError("staged file is missing", code="GONE")
    digest = sha256_file(path)
    if digest != f["sha256"]:
        raise PermanentError("file content changed after it was staged (integrity check failed)", code="INTEGRITY")
    ol = ctx.context.get("ol_run_id") or svc.lineage.new_run_uuid()
    ctx.set("ol_run_id", ol)
    svc.lineage.emit("START", ol_run_id=ol, job="ingest_file", tenant=ctx.tenant, run_id=ctx.run_id,
                     inputs=[{"namespace": "file://inbox", "name": f["original_name"]}])
    dup = svc.db.query_one("SELECT id, original_name, detected_at FROM files WHERE tenant=? AND sha256=? AND id != ? "
                           "AND status IN ('processed','partial','quarantined') ORDER BY detected_at LIMIT 1", (ctx.tenant, digest, f["id"]))
    if dup:
        dest = _move(path, svc.settings.data_path("archive", ctx.tenant, "duplicates"))
        svc.db.update("files", {"id": f["id"]}, {"status": "duplicate", "duplicate_of": dup["id"], "final_path": str(dest)})
        svc.signals.raise_signal(ctx.tenant, "duplicate_file", None, "info",
                                 f"{f['original_name']} is a duplicate delivery of {dup['original_name']} - skipped (idempotent ingestion)",
                                 {"duplicate_of": dup["id"], "sha256": digest}, run_id=ctx.run_id)
        svc.metrics.inc("ingest_files_total", outcome="skipped_duplicate", tenant=ctx.tenant)
        ctx.stop("skipped", "duplicate content (idempotent ingestion)", {"duplicate_of": dup["id"]})
    return {"path": str(path), "sha256": digest, "size": path.stat().st_size, "name": f["original_name"]}


def f_route(ctx):
    svc = ctx.svc
    st = ctx.outputs["stage"]
    sn = sniff(Path(st["path"]))
    decision = svc.agents.router.run(st["name"], sn, ctx.tenant, ctx.run_id)
    if decision["kind"] == "document":
        child = ctx.context.get("doc_child") or ctx.child("document", {"file_id": ctx.input["file_id"], "path": st["path"], "sniff": sn.to_dict()},
                                                           file_id=ctx.input["file_id"])
        ctx.set("doc_child", child)
        ctx.stop("handed_off", "free-text document handed to the document workflow", {"child_run_id": child, "route": decision})
    if decision["kind"] == "unsupported":
        raise PermanentError(f"unsupported content: {sn.reason}", code="UNSUPPORTED")
    return {"sniff": sn.to_dict(), "route": decision}


def _sniff_from(d: dict) -> SniffResult:
    return SniffResult(d["extension"], d["size"], d["kind_hint"], d.get("encoding"), d.get("delimiter"), d.get("header"),
                       d.get("sample_lines", []), d.get("est_lines", 0), d.get("reason", ""))


def f_read(ctx):
    svc = ctx.svc
    st = ctx.outputs["stage"]
    frames = read_frames(Path(st["path"]), _sniff_from(ctx.outputs["route"]["sniff"]))
    out_dir = svc.settings.data_path("processing", ctx.input["file_id"])
    out_dir.mkdir(parents=True, exist_ok=True)
    info = []
    for i, fr in enumerate(frames):
        p = out_dir / f"frame_{i}.frame"
        save_frame(fr.df, p)
        info.append({"index": i, "name": fr.name, "rows": len(fr.df), "columns": [str(c) for c in fr.df.columns], "path": str(p),
                     "bad_lines": fr.bad_lines[:20], "bad_line_count": len(fr.bad_lines)})
        if fr.bad_lines:
            svc.signals.raise_signal(ctx.tenant, "parse_warning", None, "info", f"{len(fr.bad_lines)} malformed lines skipped in {st['name']}",
                                     {"examples": fr.bad_lines[:3]}, run_id=ctx.run_id)
    return {"frames": info, "n_frames": len(info)}


def f_fanout(ctx):
    svc = ctx.svc
    children = ctx.context.get("children")
    if not children:
        name = ctx.outputs["stage"]["name"]
        frames = ctx.outputs["read"]["frames"]
        is_xl = name.lower().endswith((".xlsx", ".xls"))
        children = []
        for fr in frames:
            c = svc.contracts.match(name, fr["name"] if is_xl else None, fr["index"], len(frames))
            ds = c["dataset"] if c else dataset_name_for(name, fr["name"] if is_xl and len(frames) > 1 else None)
            children.append(ctx.child("ingest_dataset", {"file_id": ctx.input["file_id"], "frame_path": fr["path"], "frame_name": fr["name"],
                                                         "frame_index": fr["index"], "dataset": ds, "contract_matched": bool(c),
                                                         "file_name": name, "parent_run_id": ctx.run_id,
                                                         "bad_lines": fr["bad_line_count"]},
                                      dataset=ds, file_id=ctx.input["file_id"], priority=4))
        ctx.set("children", children)
    qs = ",".join("?" for _ in children)
    rows = svc.db.query(f"SELECT id, status, dataset FROM runs WHERE id IN ({qs})", children)
    if any(r["status"] not in TERMINAL for r in rows):
        ctx.wait("children", f"{len(rows)} dataset run(s) in progress")
    return {"children": [{"run_id": r["id"], "status": r["status"], "dataset": r["dataset"]} for r in rows]}


def f_finalize(ctx):
    svc = ctx.svc
    kids = ctx.outputs["fanout"]["children"]
    sts = [k["status"] for k in kids]
    if all(s == "succeeded" for s in sts):
        outcome = "published"
    elif all(s in ("quarantined", "rejected", "failed") for s in sts):
        outcome = "quarantined"
    else:
        outcome = "partial"
    f = _file(ctx)
    path = Path(f["staged_path"])
    dest = _move(path, svc.settings.data_path("archive" if outcome == "published" else "quarantine", ctx.tenant, _today())) if path.exists() else path
    svc.db.update("files", {"id": f["id"]}, {"status": {"published": "processed"}.get(outcome, outcome), "final_path": str(dest)})
    svc.lineage.emit("COMPLETE", ol_run_id=ctx.context.get("ol_run_id") or svc.lineage.new_run_uuid(), job="ingest_file", tenant=ctx.tenant,
                     run_id=ctx.run_id, inputs=[{"namespace": "file://inbox", "name": f["original_name"]}],
                     outputs=[{"namespace": f"swarmpipe://{ctx.tenant}", "name": k["dataset"]} for k in kids])
    secs = seconds_between(f["detected_at"], iso())
    svc.metrics.inc("ingest_files_total", outcome=outcome, tenant=ctx.tenant)
    if secs is not None:
        svc.metrics.observe("ingest_e2e_seconds", secs, outcome=outcome, tenant=ctx.tenant)
    ctx.set_result({"published": "succeeded"}.get(outcome, outcome))
    return {"summary": {"outcome": outcome, "children": kids, "final_path": str(dest), "seconds": secs}}


def f_on_failure(ctx, exc) -> str:
    svc = ctx.svc
    f = _file(ctx)
    path = Path(f["staged_path"])
    dest = None
    if path.exists():
        dest = _move(path, svc.settings.data_path("dlq", ctx.tenant))
        Path(str(dest) + ".reason.json").write_text(dumps({"file": f["original_name"], "run_id": ctx.run_id, "error": f"{type(exc).__name__}: {exc}",
                                                           "at": iso()}, indent=2), encoding="utf-8")
    svc.db.insert("dlq", {"id": new_id("dlq"), "run_id": ctx.run_id, "file_id": f["id"], "tenant": ctx.tenant,
                          "reason": getattr(exc, "code", type(exc).__name__), "error": str(exc)[:1000], "path": str(dest) if dest else None,
                          "created_at": iso(), "redriven_at": None})
    svc.db.update("files", {"id": f["id"]}, {"status": "dead_lettered", "final_path": str(dest) if dest else None})
    svc.signals.raise_signal(ctx.tenant, "pipeline_failure", None, "high", f"{f['original_name']} dead-lettered: {exc}"[:300],
                             {"error": str(exc)[:500], "code": getattr(exc, "code", None), "file": f["original_name"]}, run_id=ctx.run_id)
    svc.lineage.emit("FAIL", ol_run_id=ctx.context.get("ol_run_id") or svc.lineage.new_run_uuid(), job="ingest_file", tenant=ctx.tenant,
                     run_id=ctx.run_id, inputs=[{"namespace": "file://inbox", "name": f["original_name"]}],
                     run_facets={"errorMessage": {"message": str(exc)[:500], "programmingLanguage": "python"}})
    svc.metrics.inc("ingest_files_total", outcome="dead_lettered", tenant=ctx.tenant)
    secs = seconds_between(f["detected_at"], iso())
    if secs is not None:
        svc.metrics.observe("ingest_e2e_seconds", secs, outcome="dead_lettered", tenant=ctx.tenant)
    return "dead_lettered"


# ============================================================================== ingest_dataset
def _df(ctx) -> pd.DataFrame:
    if "df" not in ctx.cache:
        ctx.cache["df"] = load_frame(ctx.input["frame_path"])
    return ctx.cache["df"]


def _contract(ctx) -> dict | None:
    return ctx.svc.contracts.active(ctx.input["dataset"])


def d_load(ctx):
    ds = ctx.input["dataset"]
    if not DATASET_RE.match(ds):
        raise PermanentError(f"invalid dataset name {ds!r}", code="INVALID_DATASET")
    df = _df(ctx)
    ctx.svc.db.execute("INSERT INTO dataset_state(tenant, dataset, last_arrival_at) VALUES(?,?,?) "
                       "ON CONFLICT(tenant, dataset) DO UPDATE SET last_arrival_at=excluded.last_arrival_at", (ctx.tenant, ds, ctx.svc.clock.now_iso()))
    return {"rows": len(df), "columns": [str(c) for c in df.columns], "dataset": ds, "reprocess": bool(ctx.input.get("mapping_override"))}


def d_privacy(ctx):
    svc = ctx.svc
    res = svc.agents.privacy.run(_df(ctx), _contract(ctx), ctx.input["file_name"])
    header_hits = [h for h in res["injection_hits"] if h["row"] == "header"]
    if res["filename_injection"]["suspicious"] or header_hits:
        svc.signals.raise_signal(ctx.tenant, "injection_attempt", ctx.input["dataset"], "high",
                                 f"Instructions embedded in the file name or header of {ctx.input['file_name']}",
                                 {"file_name": ctx.input["file_name"], "filename_scan": res["filename_injection"], "header_hits": header_hits[:3]},
                                 run_id=ctx.run_id)
    res["injection_hits"] = res["injection_hits"][:50]
    return res


def d_profile(ctx):
    return ctx.svc.agents.profiler.run(_df(ctx), ctx.outputs["privacy"], ctx.tenant, ctx.run_id)


def d_onboard(ctx):
    svc = ctx.svc
    ds = ctx.input["dataset"]
    ap_id = ctx.context.get("onboard_approval")
    if not ap_id:
        prop = svc.agents.steward.propose_contract(ds, ctx.input["file_name"], _df(ctx), ctx.outputs["profile"]["profile"],
                                                   ctx.outputs["privacy"], ctx.tenant, ctx.run_id)
        version = svc.contracts.propose(ds, prop["contract"], "agent:steward", f"onboarding proposal from {ctx.input['file_name']}")
        c = prop["contract"]
        ap_id = svc.approvals.request(
            "contract_onboarding", tenant=ctx.tenant, subject=f"{ds}@v{version}", risk="medium", run_id=ctx.run_id,
            summary=(f"New dataset '{ds}' arrived in {ctx.input['file_name']}. Approve the proposed contract: {len(c['columns'])} columns, "
                     f"primary key {c.get('primary_key')}, classification {c.get('classification')}. Critic verdict: {prop['critique'].get('verdict')}"),
            payload={"dataset": ds, "version": version, "contract": c, "critique": prop["critique"]})
        ctx.set("onboard_approval", ap_id)
        ctx.set("onboard_version", version)
        ctx.wait(f"approval:{ap_id}", "a human must approve the proposed contract")
    ap = svc.approvals.get(ap_id)
    version = ctx.context.get("onboard_version")
    if ap["status"] == "pending":
        ctx.wait(f"approval:{ap_id}")
    if ap["status"] == "approved":
        svc.contracts.activate(ds, version, ap["decided_by"])
        return {"approved_by": ap["decided_by"], "version": version}
    svc.contracts.reject(ds, version, ap["decided_by"] or "system:expiry")
    ctx.stop("rejected", f"contract for new dataset {ds} was {ap['status']}")


def d_contract_check(ctx):
    svc = ctx.svc
    contract = _contract(ctx)
    if not contract:
        raise PermanentError(f"no active contract for {ctx.input['dataset']}", code="NO_CONTRACT")
    mo = ctx.input.get("mapping_override")
    if mo:
        df = _df(ctx).rename(columns={k: v for k, v in mo.items() if k in _df(ctx).columns})
        diff = schema_diff(contract, [str(c) for c in df.columns], svc.contracts.alias_map(contract))
        return {"dataset": contract["dataset"], "contract_version": contract["version"], "diff": diff, "mapping_override": mo}
    out = svc.agents.steward.check(_df(ctx), contract, ctx.tenant, ctx.run_id)
    out.pop("candidates", None)
    return out


def d_transform(ctx):
    svc = ctx.svc
    contract = _contract(ctx)
    tr, info = svc.agents.transformer.run(_df(ctx), contract, ctx.input.get("mapping_override"), ctx.tenant, ctx.run_id)
    ctx.cache["tr"] = tr
    p = svc.settings.data_path("processing", ctx.input["file_id"], f"{ctx.input['dataset']}_{ctx.run_id}_typed.frame")
    save_frame(tr.df, p)
    with svc.db.tx():
        svc.db.execute("DELETE FROM quarantine_rows WHERE run_id=? AND reason LIKE 'transform:%'", (ctx.run_id,))
        svc.db.executemany("INSERT INTO quarantine_rows(run_id, tenant, dataset, row_number, reason, data, created_at) VALUES(?,?,?,?,?,?,?)",
                           [(ctx.run_id, ctx.tenant, ctx.input["dataset"], r["row_number"], "transform:" + r["reason"], dumps(r["data"]), iso())
                            for r in tr.rejects[:5000]])
    return {"stats": tr.stats, "diff": tr.diff, "typed_path": str(p), **info}


def _tr(ctx) -> TransformResult:
    if "tr" in ctx.cache:
        return ctx.cache["tr"]
    o = ctx.outputs["transform"]
    return TransformResult(load_frame(o["typed_path"]), [], o["stats"], o["diff"])


def d_quality(ctx):
    svc = ctx.svc
    contract = _contract(ctx)
    ds = ctx.input["dataset"]
    outcome = svc.agents.assurance.run(contract, _tr(ctx), ctx.outputs["privacy"], ctx.tenant, ds)
    rows = [r.to_dict() for r in outcome.results]
    with svc.db.tx():
        svc.db.execute("DELETE FROM check_results WHERE run_id=?", (ctx.run_id,))
        svc.db.execute("DELETE FROM quarantine_rows WHERE run_id=? AND reason LIKE 'quality:%'", (ctx.run_id,))
        svc.db.executemany("INSERT INTO check_results(run_id, tenant, dataset, version_id, check_name, check_type, status, severity, observed, "
                           "expected, details, created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                           [(ctx.run_id, ctx.tenant, ds, None, r["name"], r["check_type"], r["status"], r["severity"], dumps(r["observed"]),
                             dumps(r["expected"]), dumps(r["details"]), iso()) for r in rows])
        svc.db.executemany("INSERT INTO quarantine_rows(run_id, tenant, dataset, row_number, reason, data, created_at) VALUES(?,?,?,?,?,?,?)",
                           [(ctx.run_id, ctx.tenant, ds, q["row_number"], "quality:" + q["reason"], dumps(q["data"]), iso())
                            for q in outcome.quarantined_rows[:5000]])
    kept = svc.settings.data_path("processing", ctx.input["file_id"], f"{ds}_{ctx.run_id}_kept.frame")
    save_frame(outcome.df, kept)
    by_type: dict[str, list[dict]] = {}
    for r in rows:
        sev = _signal_severity(r)
        if sev:
            by_type.setdefault(CHECK_SIGNAL.get(r["check_type"], "quality_failure"), []).append({**r, "_sev": sev})
    for stype, checks in by_type.items():
        sev = max((c["_sev"] for c in checks), key=SEVERITIES.index)
        names = [c["name"] for c in checks]
        summary = f"{ds}: {stype.replace('_', ' ')} - " + ", ".join(f"{c['name']} ({c['status']}: {str(c['observed'])[:60]})" for c in checks[:3])
        svc.signals.raise_signal(ctx.tenant, stype, ds, sev, summary,
                                 {"checks": [{k: c[k] for k in ("name", "status", "severity", "observed", "expected", "details")} for c in checks[:6]],
                                  "decision": outcome.decision, "file": ctx.input["file_name"], "names": names}, run_id=ctx.run_id)
    return {"decision": outcome.decision, "failed": [r.name for r in outcome.failed], "warnings": [r.name for r in outcome.warnings],
            "kept_path": str(kept), "quarantined_rows": len(outcome.quarantined_rows), "checks_total": len(rows)}


def d_publish(ctx):
    svc = ctx.svc
    contract = _contract(ctx)
    ds = ctx.input["dataset"]
    q = ctx.outputs["quality"]
    lock = f"dataset:{ctx.tenant}:{ds}"
    if not acquire_lock(svc, lock, ctx.run_id, ttl_s=120):
        raise Deferred(f"dataset {ds} is being published by another run", delay_s=0.5)
    try:
        df = load_frame(q["kept_path"])
        masked, mask_report = svc.agents.publisher.mask(df, contract, ctx.outputs["privacy"], ctx.tenant)
        content_hash = sha256_text(masked.to_csv(index=False))
        pii_cols = {c["name"] for c in contract.get("columns", []) if c.get("pii")}
        profile = profile_frame(masked, masked_columns=pii_cols)
        st = svc.db.query_one("SELECT hold, hold_reason FROM dataset_state WHERE tenant=? AND dataset=?", (ctx.tenant, ds)) or {}
        quarantined = q["decision"] != "publish"

        def do():
            v = svc.publishing.stage(tenant=ctx.tenant, dataset=ds, batch=masked, contract=contract, run_id=ctx.run_id,
                                     file_id=ctx.input["file_id"], content_hash=content_hash, profile=profile, quarantined=quarantined,
                                     note="quarantined by data assurance: " + ", ".join(q["failed"])[:250] if quarantined else "")
            status = "quarantined" if quarantined else ("held" if st.get("hold") else "published")
            if status == "published":
                svc.publishing.publish(v["id"], "agent:publisher")
            if quarantined:
                try:
                    svc.notifier.send(ctx.tenant, contract.get("owner", "owner@contoso.example"), f"[{ds}] batch quarantined",
                                      f"{ctx.input['file_name']} failed checks {q['failed']} and was NOT published. Consumers keep the last good "
                                      f"version. Incident triage has started.", sender="agent:publisher")
                except Exception:  # noqa: BLE001
                    pass
            return {"version_id": v["id"], "version": v["version"], "status": status, "table": v["table_name"]}

        res = ctx.idempotent(f"publish:{ctx.run_id}", do)
        if res["status"] == "published":
            ctx.add_compensation("unpublish_version", {"version_id": res["version_id"]}, "publish")
        if ctx.input.get("mapping_override") and res["status"] == "published":
            svc.audit.record("agent:publisher", "dataset.reprocessed", ds, "published", {"version_id": res["version_id"], "mapping": ctx.input["mapping_override"]})
        return {**res, "mask": mask_report, "content_hash": content_hash}
    finally:
        release_lock(svc, lock, ctx.run_id)


def d_lineage(ctx):
    svc = ctx.svc
    ds = ctx.input["dataset"]
    pub = ctx.outputs["publish"]
    tr = ctx.outputs["transform"]
    checks = svc.db.query("SELECT check_name, status FROM check_results WHERE run_id=?", (ctx.run_id,))
    contract = _contract(ctx) or {}
    mapping = {**(tr["stats"].get("mapping_used") or {})}
    fields = {tgt: {"inputFields": [{"namespace": "file://inbox", "name": ctx.input["file_name"], "field": src}]}
              for src, tgt in mapping.items()}
    facets = {
        "schema": {"fields": [{"name": c["name"], "type": c.get("type", "string")} for c in contract.get("columns", [])]},
        "dataQualityMetrics": {"rowCount": tr["stats"]["rows_out"], "rejected": tr["stats"]["rejected"]},
        "dataQualityAssertions": {"assertions": [{"assertion": c["check_name"], "success": c["status"] != "fail"} for c in checks]},
        "columnLineage": {"fields": fields},
        "version": {"datasetVersion": str(pub.get("version"))},
    }
    ol = svc.lineage.new_run_uuid()
    ev = "FAIL" if pub["status"] == "quarantined" else "COMPLETE"
    svc.lineage.emit(ev, ol_run_id=ol, job=f"ingest_dataset.{ds}", tenant=ctx.tenant, run_id=ctx.run_id,
                     inputs=[{"namespace": "file://inbox", "name": ctx.input["file_name"]}],
                     outputs=[{"namespace": f"swarmpipe://{ctx.tenant}", "name": ds, "facets": facets}],
                     run_facets={"errorMessage": {"message": "quarantined: " + ", ".join(ctx.outputs['quality']['failed']), "programmingLanguage": "python"}}
                     if ev == "FAIL" else None)
    if pub["status"] == "quarantined":
        ctx.set_result("quarantined")
    return {"summary": {"dataset": ds, "status": pub["status"], "version": pub.get("version"), "failed_checks": ctx.outputs["quality"]["failed"]}}


def d_on_failure(ctx, exc) -> str:
    ctx.svc.signals.raise_signal(ctx.tenant, "pipeline_failure", ctx.input.get("dataset"), "high",
                                 f"ingest of {ctx.input.get('dataset')} failed: {exc}"[:300], {"error": str(exc)[:500]}, run_id=ctx.run_id)
    return "failed"


# ============================================================================== document
def _text(ctx) -> str:
    if "text" not in ctx.cache:
        ctx.cache["text"] = read_text(Path(ctx.input["path"]), _sniff_from(ctx.input["sniff"]))
    return ctx.cache["text"]


def doc_read(ctx):
    if not Path(ctx.input["path"]).exists():
        raise PermanentError("document file is missing", code="GONE")
    t = _text(ctx)
    return {"chars": len(t), "lines": t.count("\n") + 1}


def doc_guard(ctx):
    svc = ctx.svc
    f = _file(ctx)
    rep = scan_text(_text(ctx))
    trust = "untrusted" if rep.suspicious else "unverified"
    if rep.suspicious:
        svc.signals.raise_signal(ctx.tenant, "injection_attempt", None, "high",
                                 f"Document {f['original_name']} contains instructions aimed at agents (score {rep.score:.2f})",
                                 {"file": f["original_name"], "findings": rep.findings[:5], "snippet": _text(ctx)[:300]}, run_id=ctx.run_id)
    return {"trust": trust, "injection": rep.to_dict()}


def doc_summarize(ctx):
    f = _file(ctx)
    return ctx.svc.agents.librarian.summarize(_text(ctx), f["original_name"], ctx.tenant, ctx.run_id)


def doc_index(ctx):
    svc = ctx.svc
    f = _file(ctx)
    s = ctx.outputs["summarize"]
    g = ctx.outputs["guard"]

    def do():
        doc_id = svc.knowledge.add_document(tenant=ctx.tenant, title=s["title"], text=_text(ctx), source=f"inbox/{f['original_name']}",
                                            trust=g["trust"], doc_type=s["doc_type"], file_id=f["id"], summary=s["summary"],
                                            flags={"injection": g["injection"]})
        ap = None
        if s["doc_type"] in ("runbook", "policy"):
            ap = svc.approvals.request("knowledge_promotion", tenant=ctx.tenant, subject=s["title"][:80],
                                       risk="high" if g["trust"] == "untrusted" else "medium",
                                       summary=(f"Promote '{s['title']}' ({s['doc_type']}) to TRUSTED knowledge so agents can rely on it. "
                                                + ("WARNING: the injection detector flagged this document." if g["trust"] == "untrusted" else "")),
                                       payload={"doc_id": doc_id, "summary": s["summary"], "injection": g["injection"]}, run_id=ctx.run_id)
        return {"doc_id": doc_id, "promotion_approval": ap}

    return ctx.idempotent(f"doc:{ctx.run_id}", do)


def doc_archive(ctx):
    svc = ctx.svc
    f = _file(ctx)
    path = Path(ctx.input["path"])
    dest = _move(path, svc.settings.data_path("archive", ctx.tenant, _today())) if path.exists() else path
    svc.db.update("files", {"id": f["id"]}, {"status": "processed", "final_path": str(dest)})
    svc.lineage.emit("COMPLETE", ol_run_id=svc.lineage.new_run_uuid(), job="document_ingest", tenant=ctx.tenant, run_id=ctx.run_id,
                     inputs=[{"namespace": "file://inbox", "name": f["original_name"]}],
                     outputs=[{"namespace": f"swarmpipe://{ctx.tenant}/knowledge", "name": ctx.outputs["index"]["doc_id"]}])
    svc.metrics.inc("ingest_files_total", outcome="document", tenant=ctx.tenant)
    secs = seconds_between(f["detected_at"], iso())
    if secs is not None:
        svc.metrics.observe("ingest_e2e_seconds", secs, outcome="document", tenant=ctx.tenant)
    return {"summary": {"doc_id": ctx.outputs["index"]["doc_id"], "trust": ctx.outputs["guard"]["trust"], "final_path": str(dest)}}


# ============================================================================== derive
def _derived_spec(svc, name: str) -> dict:
    spec = ((svc.settings.consumers or {}).get("derived") or {}).get(name)
    if not spec:
        raise PermanentError(f"unknown derived dataset {name}", code="NOT_FOUND")
    return spec


def dv_check(ctx):
    svc = ctx.svc
    name = ctx.input["dataset"]
    spec = _derived_spec(svc, name)
    st = svc.db.query_one("SELECT hold, hold_reason FROM dataset_state WHERE tenant=? AND dataset=?", (ctx.tenant, name)) or {}
    if st.get("hold"):
        svc.signals.raise_signal(ctx.tenant, "derive_blocked", name, "info", f"{name} rebuild skipped: dataset is on hold ({st.get('hold_reason')})",
                                 {"reason": "hold"}, run_id=ctx.run_id)
        ctx.stop("held", f"{name} is on hold: {st.get('hold_reason')}")
    versions = {}
    for inp in spec["inputs"]:
        cur = svc.publishing.current(ctx.tenant, inp)
        if not cur:
            ctx.stop("blocked", f"input {inp} is not published yet")
        bad = [i for i in svc.incidents.open_for_dataset(ctx.tenant, inp) if i["severity"] in ("high", "critical")]
        if bad:
            svc.signals.raise_signal(ctx.tenant, "derive_blocked", name, "info",
                                     f"Propagation prevented: {name} not rebuilt because input {inp} has open incident {bad[0]['id']}",
                                     {"input": inp, "incident_id": bad[0]["id"]}, run_id=ctx.run_id)
            ctx.stop("blocked", f"input {inp} has an open {bad[0]['severity']} incident")
        versions[inp] = cur["id"]
    return {"inputs": versions}


def dv_build(ctx):
    svc = ctx.svc
    name = ctx.input["dataset"]
    spec = _derived_spec(svc, name)
    sql = spec["sql"].format(**{inp: f'"{svc.wh.view_name(ctx.tenant, inp)}"' for inp in spec["inputs"]})
    df = pd.read_sql_query(sql, svc.wh.conn())
    first = spec["inputs"][0]
    base_rows = svc.wh.row_count(svc.wh.view_name(ctx.tenant, first))
    if len(df) < int(spec.get("min_rows", 1)):
        raise PermanentError(f"derived dataset {name} produced {len(df)} rows", code="EMPTY_RESULT")
    if base_rows is not None and len(df) != base_rows:
        svc.signals.raise_signal(ctx.tenant, "reconciliation_break", name, "high", f"{name} has {len(df)} rows but {first} has {base_rows}",
                                 {"rows": len(df), "input_rows": base_rows}, run_id=ctx.run_id)
    p = svc.settings.data_path("processing", "derived")
    p.mkdir(parents=True, exist_ok=True)
    path = p / f"{name}_{ctx.run_id}.frame"
    save_frame(df, path)
    return {"rows": len(df), "path": str(path), "input_rows": base_rows}


def dv_publish(ctx):
    svc = ctx.svc
    name = ctx.input["dataset"]

    def do():
        df = load_frame(ctx.outputs["build"]["path"])
        v = svc.publishing.stage(tenant=ctx.tenant, dataset=name, batch=df, contract=None, run_id=ctx.run_id, file_id=None,
                                 content_hash=sha256_text(df.to_csv(index=False)), profile=profile_frame(df), quarantined=False,
                                 note=f"derived from {ctx.outputs['check']['inputs']}")
        svc.publishing.publish(v["id"], "agent:publisher")
        return {"version_id": v["id"], "version": v["version"]}

    res = ctx.idempotent(f"derive:{ctx.run_id}", do)
    spec = _derived_spec(svc, name)
    svc.lineage.emit("COMPLETE", ol_run_id=svc.lineage.new_run_uuid(), job=f"derive.{name}", tenant=ctx.tenant, run_id=ctx.run_id,
                     inputs=[{"namespace": f"swarmpipe://{ctx.tenant}", "name": i} for i in spec["inputs"]],
                     outputs=[{"namespace": f"swarmpipe://{ctx.tenant}", "name": name,
                               "facets": {"dataQualityMetrics": {"rowCount": ctx.outputs["build"]["rows"]}}}])
    return {"summary": res}


# ============================================================================== triage
def _incident(ctx) -> dict:
    inc = ctx.svc.incidents.get(ctx.input["incident_id"])
    if not inc:
        raise PermanentError("incident not found", code="NOT_FOUND")
    return inc


def t_open(ctx):
    svc = ctx.svc
    inc = _incident(ctx)
    svc.incidents.update(inc["id"], status="triaging", triage_run_id=ctx.run_id)
    sigs = svc.signals.for_incident(inc["id"])
    ds = inc["dataset"]
    pre = {"contract_active": bool(svc.contracts.active(ds)) if ds else None,
           "lineage_edges": svc.db.scalar("SELECT COUNT(*) FROM lineage_edges", default=0),
           "has_published_version": bool(svc.publishing.current(inc["tenant"], ds)) if ds else None}
    Blackboard(svc, inc["id"]).post("agent:supervisor", "case_opened", {"signals": [{"type": s["type"], "severity": s["severity"], "summary": s["summary"]}
                                                                                    for s in sigs], "preconditions": pre})
    svc.audit.record("agent:supervisor", "triage.start", inc["id"], "started", {"signals": len(sigs)})
    return {"signals": [{"id": s["id"], "type": s["type"], "severity": s["severity"], "run_id": s["run_id"]} for s in sigs], "preconditions": pre}


def t_investigate(ctx):
    svc = ctx.svc
    inc = _incident(ctx)
    res = svc.agents.supervisor.investigate(inc, svc.signals.for_incident(inc["id"]), ctx.run_id)
    return {"results": [{"specialist": r["specialist"], "finding": r["finding"], "steps": r["steps"], "tools_used": r["tools_used"],
                         "stop_reason": r["stop_reason"], "degraded": r["degraded"]} for r in res]}


def t_diagnose(ctx):
    svc = ctx.svc
    inc = _incident(ctx)
    results = ctx.outputs["investigate"]["results"]
    d = svc.agents.supervisor.diagnose(inc, results, ctx.run_id)
    svc.incidents.update(inc["id"], diagnosis=d, diagnosed_at=svc.clock.now_iso(), status="diagnosed")
    secs = seconds_between(inc["first_signal_at"], svc.clock.now_iso())
    if secs is not None:
        svc.metrics.observe("triage_diagnosis_seconds", secs, tenant=inc["tenant"])
    cand = (svc.settings.features.get("shadow_candidates") or {}).get("diagnoser")
    if cand:
        try:
            cd = svc.agents.supervisor.diagnose(inc, results, ctx.run_id, version=cand.get("prompt_version"), shadow=True)
            svc.db.insert("shadow_comparisons", {"incident_id": inc["id"], "agent": "supervisor", "production": dumps(d), "candidate": dumps(cd),
                                                 "agreed": 1 if cd["root_cause_category"] == d["root_cause_category"] else 0, "created_at": iso()})
        except Exception as exc:  # noqa: BLE001
            log.warning("shadow candidate failed: %s", exc)
    return {"diagnosis": d}


def t_impact(ctx):
    inc = _incident(ctx)
    imp = ctx.svc.agents.impact.run(inc["tenant"], inc["dataset"])
    ctx.svc.incidents.update(inc["id"], impact=imp)
    return {"impact": imp}


def _facts(svc, inc: dict, diag: dict, imp: dict) -> dict:
    sigs = svc.signals.for_incident(inc["id"])
    runs = [s["run_id"] for s in sigs if s.get("run_id")]
    latest = runs[-1] if runs else None
    ds = inc["dataset"]
    facts = {"dataset": ds, "tenant": inc["tenant"], "owner": "owner", "source_owner": "source_owner",
             "recipient_handles": ["owner", "source_owner", "security", "oncall"],
             "owner_team": (svc.context.owner(ds) or "").split("@")[0] if ds else None,
             "injection_suspected": any(s["type"] in ("injection_attempt", "egress_blocked") for s in sigs)}
    c = svc.contracts.active(ds) if ds else None
    if c:
        facts["source_team"] = (c.get("source_owner") or "").split("@")[0]
    if runs:
        qs = ",".join("?" for _ in runs)
        qv = svc.db.query_one(f"SELECT id FROM dataset_versions WHERE run_id IN ({qs}) AND status='quarantined' ORDER BY version DESC LIMIT 1", runs)
        facts["quarantined_version_id"] = qv["id"] if qv else None
        run = svc.db.query_one("SELECT file_id FROM runs WHERE id=?", (latest,))
        facts["file_id"] = run["file_id"] if run else None
        cc = svc.db.query_one("SELECT output FROM steps WHERE run_id=? AND name='contract_check'", (latest,))
        cco = loads(cc["output"], {}) if cc else {}
        facts["mapping"] = cco.get("mapping_proposal") or {}
        facts["mapping_confidence"] = cco.get("mapping_confidence")
        facts["new_columns"] = (cco.get("diff") or {}).get("new_columns")
        facts["missing_required"] = (cco.get("diff") or {}).get("missing_required")
        facts["failed_checks"] = [r["check_name"] for r in svc.db.query("SELECT check_name FROM check_results WHERE run_id=? AND status='fail'", (latest,))]
    if ds:
        cur = svc.publishing.current(inc["tenant"], ds)
        facts["published_version_id"] = cur["id"] if cur else None
        prev = svc.publishing.previous_good(inc["tenant"], ds, cur["version"]) if cur else None
        facts["previous_version_id"] = prev["id"] if prev else None
    derived = set(((svc.settings.consumers or {}).get("derived") or {}).keys())
    facts["derived_downstream"] = [d["dataset"] for d in imp.get("downstream_datasets", []) if d["dataset"] in derived]
    return facts


def _collect_snippets(obj, out: list[str], depth: int = 0) -> list[str]:
    if depth > 6 or len(out) >= 8:
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in ("snippet", "preview") and isinstance(v, str):
                out.append(v)
            else:
                _collect_snippets(v, out, depth + 1)
    elif isinstance(obj, list):
        for v in obj:
            _collect_snippets(v, out, depth + 1)
    return out


def _evidence_snippets(svc, inc: dict) -> str:
    """Untrusted content the planner sees (data samples / suspicious cells). It is what makes indirect
    prompt injection possible - and why it is spotlighted and why the planner cannot act by itself."""
    parts = []
    sigs = svc.signals.for_incident(inc["id"])
    for s in sigs:
        if s["type"] in ("injection_attempt", "egress_blocked"):
            snips = _collect_snippets(s["details"], [])
            if s["details"].get("file_name"):
                snips.insert(0, f"file name: {s['details']['file_name']}")
            parts.append(dumps({"signal": s["type"], "snippets": snips[:5]}))
    runs = [s["run_id"] for s in sigs if s.get("run_id")]
    if runs:
        qs = ",".join("?" for _ in runs)
        for r in svc.db.query(f"SELECT reason, data FROM quarantine_rows WHERE run_id IN ({qs}) LIMIT 3", runs):
            parts.append(f"quarantined row ({r['reason']}): {r['data'][:400]}")
    return "\n".join(parts)[:2500]


def t_plan(ctx):
    svc = ctx.svc
    inc = _incident(ctx)
    diag = ctx.outputs["diagnose"]["diagnosis"]
    imp = ctx.outputs["impact"]["impact"]
    facts = _facts(svc, inc, diag, imp)
    plan = svc.agents.planner.plan(inc, diag, imp, facts, _evidence_snippets(svc, inc), ctx.run_id)
    ids = []
    for rank, p in enumerate(plan["proposals"], start=1):
        pid = new_id("prp")
        target = (p["params"].get("version_id") or p["params"].get("dataset") or inc["dataset"])
        svc.db.insert("proposals", {"id": pid, "incident_id": inc["id"], "tenant": inc["tenant"], "dataset": inc["dataset"], "action": p["action"],
                                    "params": dumps(p["params"]), "rank": rank, "rationale": p.get("rationale", "")[:1000],
                                    "citations": dumps(p.get("citations", [])), "risk": (svc.policy.spec(p["action"]) or {}).get("risk"),
                                    "blast_radius": imp.get("blast_radius", 0), "autonomy_level": None, "policy_effect": None, "policy_details": None,
                                    "status": "proposed", "proposed_by": "agent:planner", "approval_id": None, "executed_by": None,
                                    "on_behalf_of": None, "idempotency_key": None, "result": None, "verification": None, "created_at": iso(),
                                    "decided_at": None, "executed_at": None, "verified_at": None})
        svc.autonomy.record(inc["tenant"], p["action"], "proposed")
        ids.append(pid)
        _ = target
    for bad in plan["invalid"]:
        svc.db.insert("proposals", {"id": new_id("prp"), "incident_id": inc["id"], "tenant": inc["tenant"], "dataset": inc["dataset"],
                                    "action": str(bad.get("action")), "params": dumps(bad.get("params")), "rank": 99,
                                    "rationale": f"REJECTED BY VALIDATION: {bad['invalid_reason']} | {bad.get('rationale', '')}"[:1000],
                                    "citations": dumps(bad.get("citations", [])), "risk": None, "blast_radius": imp.get("blast_radius", 0),
                                    "autonomy_level": None, "policy_effect": "deny", "policy_details": dumps({"reasons": [bad["invalid_reason"]]}),
                                    "status": "invalid", "proposed_by": "agent:planner", "approval_id": None, "executed_by": None,
                                    "on_behalf_of": None, "idempotency_key": None, "result": None, "verification": None, "created_at": iso(),
                                    "decided_at": iso(), "executed_at": None, "verified_at": None})
        svc.audit.record("agent:planner", "proposal.invalid", inc["id"], "rejected", {"action": bad.get("action"), "reason": bad["invalid_reason"]})
    Blackboard(svc, inc["id"]).post("agent:planner", "plan", {"proposals": [p["action"] for p in plan["proposals"]],
                                                             "invalid": [b.get("action") for b in plan["invalid"]],
                                                             "critique": plan.get("critique"), "rounds": len(plan["rounds"])})
    return {"proposal_ids": ids, "invalid": [{"action": b.get("action"), "reason": b["invalid_reason"]} for b in plan["invalid"]],
            "rounds": len(plan["rounds"]), "critique": plan.get("critique"), "facts": facts}


def _proposals(svc, incident_id: str, status: tuple[str, ...] | None = None) -> list[dict]:
    rows = svc.db.query("SELECT * FROM proposals WHERE incident_id=? ORDER BY rank", (incident_id,))
    return [r for r in rows if status is None or r["status"] in status]


def t_govern(ctx):
    svc = ctx.svc
    inc = _incident(ctx)
    diag = ctx.outputs["diagnose"]["diagnosis"]
    imp = ctx.outputs["impact"]["impact"]
    facts = ctx.outputs["plan"]["facts"]
    c = svc.contracts.active(inc["dataset"]) if inc["dataset"] else None
    out = []
    for p in _proposals(svc, inc["id"], ("proposed",)):
        params = loads(p["params"], {})
        ac = ActionContext(tenant=inc["tenant"], action=p["action"], params=params, dataset=inc["dataset"],
                           target=inc["dataset"], blast_radius=int(imp.get("blast_radius", 0)), regulated_consumer=bool(imp.get("regulated")),
                           classification=(c or {}).get("classification"), diagnosis_confidence=diag.get("confidence"),
                           injection_suspected=bool(facts.get("injection_suspected")), incident_id=inc["id"])
        dec = svc.policy.evaluate(ac)
        status = {"deny": "denied", "inform_only": "informational", "recommend": "recommended", "require_approval": "awaiting_approval",
                  "auto_execute_notify": "approved_auto", "auto_execute": "approved_auto"}[dec.effect]
        fields = {"policy_effect": dec.effect, "policy_details": dumps(dec.to_dict()), "autonomy_level": dec.level, "status": status, "decided_at": iso()}
        if dec.effect == "require_approval":
            spec = svc.policy.spec(p["action"]) or {}
            ap = svc.approvals.request("action", tenant=inc["tenant"], subject=f"{p['action']} on {inc['dataset']}", risk=dec.risk,
                                       summary=f"{p['action']} {dumps(params)[:200]} - {p['rationale'][:300]} | policy: {'; '.join(dec.reasons)}",
                                       payload={"proposal_id": p["id"], "params": params, "annotation_required": bool(spec.get("annotation_required")),
                                                "diagnosis": diag.get("root_cause_category"), "confidence": diag.get("confidence")},
                                       proposal_id=p["id"], incident_id=inc["id"], run_id=ctx.run_id,
                                       requires_confirmation=inc["dataset"] if dec.typed_confirmation else None)
            fields["approval_id"] = ap
        svc.db.update("proposals", {"id": p["id"]}, fields)
        svc.audit.record("system:policy", "policy.evaluate", p["id"], dec.effect,
                         {"action": p["action"], "level": dec.level, "reasons": dec.reasons, "rules": dec.matched_rules, "incident_id": inc["id"]})
        out.append({"proposal_id": p["id"], "action": p["action"], "effect": dec.effect, "level": dec.level, "reasons": dec.reasons})
    return {"decisions": out}


def t_execute(ctx):
    svc = ctx.svc
    inc = _incident(ctx)
    done = []
    for p in _proposals(svc, inc["id"], ("approved_auto",)):
        p["params"] = loads(p["params"], {})
        res = svc.agents.executor.execute(p, inc["tenant"], inc["id"], ctx.run_id)
        done.append({"proposal_id": p["id"], "action": p["action"], "ok": res["ok"], "error": res.get("error")})
    notify = [d for d in done if d["ok"] and loads(svc.db.scalar("SELECT policy_details FROM proposals WHERE id=?", (d["proposal_id"],)), {}).get("effect") == "auto_execute_notify"]
    if notify:
        owner = svc.context.owner(inc["dataset"]) if inc["dataset"] else "oncall@contoso.example"
        try:
            svc.notifier.send(inc["tenant"], owner or "oncall@contoso.example", f"[{inc['dataset']}] SwarmPipe acted automatically",
                              "Executed: " + ", ".join(d["action"] for d in notify) + f". One-click rollback: swarmpipe actions rollback <proposal_id> "
                              f"(incident {inc['id']}).", incident_id=inc["id"], sender="agent:executor")
        except Exception:  # noqa: BLE001
            pass
    return {"executed": done}


def t_await(ctx):
    svc = ctx.svc
    inc = _incident(ctx)
    handled = []
    for p in _proposals(svc, inc["id"], ("awaiting_approval",)):
        ap = svc.approvals.get(p["approval_id"]) if p["approval_id"] else None
        if not ap or ap["status"] == "pending":
            continue
        handled.append(_apply_decision(svc, p, ap, inc, ctx.run_id))
    pending = _proposals(svc, inc["id"], ("awaiting_approval",))
    if pending:
        svc.incidents.update(inc["id"], status="awaiting_approval")
        ctx.wait(f"approval:{pending[0]['approval_id']}", f"{len(pending)} action(s) awaiting human approval")
    return {"handled": handled}


def _apply_decision(svc, p: dict, ap: dict, inc: dict, run_id: str | None) -> dict:
    if ap["status"] == "approved":
        svc.autonomy.record(inc["tenant"], p["action"], "approved")
        svc.db.update("proposals", {"id": p["id"]}, {"status": "approved"})
        user = svc.identity.user(ap["decided_by"].split(":", 1)[1]) if ap["decided_by"].startswith("user:") else None
        p["params"] = loads(p["params"], {}) if isinstance(p["params"], str) else p["params"]
        res = svc.agents.executor.execute(p, inc["tenant"], inc["id"], run_id, on_behalf_of=user)
        return {"proposal_id": p["id"], "action": p["action"], "decision": "approved", "ok": res["ok"]}
    status = "rejected" if ap["status"] == "rejected" else "expired"
    svc.db.update("proposals", {"id": p["id"]}, {"status": status})
    if status == "rejected":
        svc.autonomy.record(inc["tenant"], p["action"], "rejected")
    return {"proposal_id": p["id"], "action": p["action"], "decision": status}


def t_verify(ctx):
    svc = ctx.svc
    inc = _incident(ctx)
    results = []
    for p in _proposals(svc, inc["id"], ("executed",)):
        v = svc.agents.verifier.verify(p, inc["tenant"], inc["id"], ctx.run_id)
        if v.get("pending"):
            ctx.wait(v["wait_for"], f"verifying {p['action']}: waiting for {v['wait_for']}")
        results.append({"proposal_id": p["id"], "action": p["action"], "ok": v.get("ok"), "compensated": v.get("compensated", False)})
    return {"verifications": results}


def t_learn(ctx):
    svc = ctx.svc
    inc = _incident(ctx)
    diag = ctx.outputs["diagnose"]["diagnosis"]
    props = _proposals(svc, inc["id"])
    timings = {"diagnose_s": seconds_between(inc["first_signal_at"], inc.get("diagnosed_at"))}
    res = svc.agents.learner.learn(inc, diag, props, svc.signals.for_incident(inc["id"]), timings, ctx.run_id)
    svc.incidents.update(inc["id"], postmortem=res["postmortem"])
    return {"memory_id": res["memory_id"], "eval_candidate_id": res["eval_candidate_id"]}


def close_incident(svc, incident_id: str) -> dict:
    inc = svc.incidents.get(incident_id)
    diag = inc.get("diagnosis") or {}
    props = _proposals(svc, incident_id)
    verified = [p for p in props if p["status"] == "verified"]
    fixing = {"reprocess_with_mapping", "rollback_dataset", "update_contract", "force_publish"}
    open_human = [p for p in props if p["status"] in ("recommended", "informational")]
    if inc["status"] == "resolved":
        status = "resolved"
    elif diag.get("abstain"):
        status = "escalated"
    elif any(p["action"] in fixing for p in verified):
        status = "resolved"
    elif verified:
        status = "mitigated"
    elif open_human:
        status = "awaiting_human"
    else:
        status = "escalated"
    cost = svc.db.scalar("SELECT COALESCE(SUM(cost_usd),0) FROM llm_calls WHERE incident_id=?", (incident_id,), default=0.0)
    fields = {"status": status, "cost_usd": round(cost, 6)}
    if status == "resolved" and not inc.get("resolved_at"):
        fields["resolved_at"] = svc.clock.now_iso()
    svc.incidents.update(incident_id, **fields)
    try:
        path = svc.evidence.export(incident_id, "both")
    except Exception as exc:  # noqa: BLE001
        path = f"export failed: {exc}"
    svc.audit.record("agent:supervisor", "incident.close", incident_id, status, {"cost_usd": round(cost, 6), "evidence": str(path)})
    svc.metrics.inc("incidents_closed_total", status=status, tenant=inc["tenant"])
    return {"status": status, "cost_usd": round(cost, 6), "evidence_pack": str(path)}


def t_close(ctx):
    return {"summary": close_incident(ctx.svc, ctx.input["incident_id"])}


def _not_abstained(ctx) -> bool:
    return not ctx.outputs.get("diagnose", {}).get("diagnosis", {}).get("abstain", False)


# ============================================================================== after-the-fact human actions
def execute_recommendation(svc, proposal_id: str, user) -> dict:
    """L1: the agent recommended, a human executes (attributed to the human, measured as agreement)."""
    p = svc.db.query_one("SELECT * FROM proposals WHERE id=?", (proposal_id,))
    if not p or p["status"] not in ("recommended", "informational", "denied", "rejected", "expired"):
        raise ValueError("proposal is not executable by a human in its current state")
    if not (user.has_role("operator") and user.can(f"action:{p['action']}")):
        raise PermissionError(f"{user.principal} may not execute {p['action']}")
    inc = svc.incidents.get(p["incident_id"])
    top = svc.db.query_one("SELECT id, action FROM proposals WHERE incident_id=? AND status NOT IN ('invalid') ORDER BY rank LIMIT 1", (p["incident_id"],))
    p["params"] = loads(p["params"], {})
    res = svc.agents.executor.execute(p, inc["tenant"], inc["id"], None, on_behalf_of=user)
    if top:
        svc.autonomy.record(inc["tenant"], top["action"], "human_agreed" if top["id"] == proposal_id else "human_disagreed")
    fresh = svc.db.query_one("SELECT * FROM proposals WHERE id=?", (proposal_id,))
    v = svc.agents.verifier.verify(fresh, inc["tenant"], inc["id"], None) if res["ok"] else {"ok": False}
    close_incident(svc, inc["id"])
    return {"execution": res, "verification": v}


def rollback_proposal(svc, proposal_id: str, user) -> dict:
    """One-click rollback of an executed action (the L3 'act and notify' contract)."""
    p = svc.db.query_one("SELECT * FROM proposals WHERE id=?", (proposal_id,))
    if not p or p["status"] not in ("executed", "verified"):
        raise ValueError("only executed/verified actions can be rolled back")
    spec = ACTIONS[p["action"]]
    if not spec.compensate:
        raise ValueError(f"{p['action']} has no compensation")
    ctx = svc.agents.executor.tool_ctx(p["tenant"], incident_id=p["incident_id"], dataset=p["dataset"], on_behalf_of=user)
    result = loads(p["result"], {}) or {}
    comp = spec.compensate(ctx, spec.params.model_validate(loads(p["params"], {})), result.get("data") or {})
    svc.db.update("proposals", {"id": proposal_id}, {"status": "rolled_back", "verification": dumps({"rolled_back_by": user.principal, "result": comp})})
    svc.autonomy.record(p["tenant"], p["action"], "rolled_back")
    svc.audit.record(user.principal, "action.rollback", proposal_id, "rolled_back", {"action": p["action"], "result": comp})
    return {"rolled_back": proposal_id, "result": comp}


def handle_approval_decided(svc, approval_id: str) -> None:
    ap = svc.approvals.get(approval_id)
    if not ap:
        return
    if ap["kind"] == "knowledge_promotion" and ap["status"] == "approved":
        svc.knowledge.set_trust(ap["payload"]["doc_id"], "trusted", ap["decided_by"])
    if ap["run_id"]:
        run = svc.db.query_one("SELECT status FROM runs WHERE id=?", (ap["run_id"],))
        if run and run["status"] == "waiting":
            svc.engine.resume(ap["run_id"], f"approval {approval_id} {ap['status']}")
            return
    if ap["kind"] == "action" and ap["proposal_id"]:
        p = svc.db.query_one("SELECT * FROM proposals WHERE id=?", (ap["proposal_id"],))
        if p and p["status"] == "awaiting_approval":
            inc = svc.incidents.get(p["incident_id"])
            _apply_decision(svc, p, ap, inc, None)
            fresh = svc.db.query_one("SELECT * FROM proposals WHERE id=?", (p["id"],))
            if fresh["status"] == "executed":
                svc.agents.verifier.verify(fresh, inc["tenant"], inc["id"], None)
            close_incident(svc, inc["id"])


def on_dataset_published(svc, payload: dict) -> None:
    tenant, ds = payload["tenant"], payload["dataset"]
    if not svc.settings.features.get("derived_datasets", True):
        return
    for name, spec in ((svc.settings.consumers or {}).get("derived") or {}).items():
        if ds in spec.get("inputs", []) and name != ds:
            pending = svc.db.scalar("SELECT COUNT(*) FROM runs WHERE workflow='derive' AND tenant=? AND dataset=? AND status IN ('pending','retry_wait')",
                                    (tenant, name), default=0)
            if not pending:
                svc.engine.submit("derive", {"dataset": name, "trigger": ds}, tenant, dataset=name, priority=6, delay_s=0.5)
    if payload.get("rollback"):
        return
    for inc in svc.incidents.open_for_dataset(tenant, ds):
        diag = inc.get("diagnosis") or {}
        if inc["status"] in ("mitigated", "awaiting_human", "diagnosed", "escalated") and diag.get("root_cause_category") in RESEND_FIXABLE:
            svc.incidents.update(inc["id"], status="resolved", resolved_at=svc.clock.now_iso())
            svc.audit.record("system:verifier", "incident.auto_resolve", inc["id"], "resolved",
                             {"reason": f"a new version of {ds} passed all checks and was published", "version_id": payload.get("version_id")})
            held = [p for p in _proposals(svc, inc["id"], ("verified", "executed")) if p["action"] == "hold_downstream"]
            for h in held:
                params = loads(h["params"], {})
                pid = new_id("prp")
                svc.db.insert("proposals", {"id": pid, "incident_id": inc["id"], "tenant": tenant, "dataset": ds, "action": "release_hold",
                                            "params": dumps({"datasets": params.get("datasets", []), "reason": "source re-delivered good data"}),
                                            "rank": 50, "rationale": "Upstream recovered; release the downstream hold", "citations": "[]",
                                            "risk": "medium", "blast_radius": 0, "autonomy_level": None, "policy_effect": None,
                                            "policy_details": None, "status": "proposed", "proposed_by": "system:verifier", "approval_id": None,
                                            "executed_by": None, "on_behalf_of": None, "idempotency_key": None, "result": None,
                                            "verification": None, "created_at": iso(), "decided_at": None, "executed_at": None, "verified_at": None})
                dec = svc.policy.evaluate(ActionContext(tenant=tenant, action="release_hold", params=params, dataset=ds, target=ds, incident_id=inc["id"]))
                if dec.auto:
                    svc.db.update("proposals", {"id": pid}, {"status": "approved_auto", "policy_effect": dec.effect, "autonomy_level": dec.level})
                    p = svc.db.query_one("SELECT * FROM proposals WHERE id=?", (pid,))
                    p["params"] = loads(p["params"], {})
                    svc.agents.executor.execute(p, tenant, inc["id"], None)
                else:
                    ap = svc.approvals.request("action", tenant=tenant, subject=f"release_hold on {params.get('datasets')}", risk=dec.risk,
                                               summary=f"{ds} recovered (new good version published). Release the hold on {params.get('datasets')}?",
                                               payload={"proposal_id": pid}, proposal_id=pid, incident_id=inc["id"])
                    svc.db.update("proposals", {"id": pid}, {"status": "awaiting_approval", "approval_id": ap, "policy_effect": dec.effect,
                                                             "autonomy_level": dec.level, "policy_details": dumps(dec.to_dict())})


# ============================================================================== registration
def _unpublish_compensator(svc, params: dict) -> None:
    svc.publishing.unpublish(params["version_id"], "system:saga", "rolled_back", "saga compensation after a later step failed")


def build_workflows() -> list[Workflow]:
    agent = RetryPolicy(max_attempts=3, base_s=0.5, max_s=10)
    return [
        Workflow("ingest_file", [
            Step("stage", f_stage), Step("route", f_route, kind="agent", retry=agent), Step("read", f_read),
            Step("fanout", f_fanout), Step("finalize", f_finalize)],
            "One file from the watched folder: stage, route, read, fan out per sheet, archive.", on_failure=f_on_failure),
        Workflow("ingest_dataset", [
            Step("load", d_load), Step("privacy", d_privacy), Step("profile", d_profile, kind="agent", retry=agent),
            Step("onboard", d_onboard, kind="agent", when=lambda c: c.svc.contracts.active(c.input["dataset"]) is None),
            Step("contract_check", d_contract_check, kind="agent", retry=agent), Step("transform", d_transform, kind="agent", retry=agent),
            Step("quality", d_quality), Step("publish", d_publish), Step("lineage", d_lineage)],
            "One dataset batch (a CSV or one sheet): privacy, profile, contract, transform, data assurance, publish/quarantine.",
            on_failure=d_on_failure),
        Workflow("document", [Step("read", doc_read), Step("guard", doc_guard), Step("summarize", doc_summarize, kind="agent", retry=agent),
                              Step("index", doc_index), Step("archive", doc_archive)],
                 "Free-text document into the knowledge base (untrusted until promoted).", on_failure=f_on_failure),
        Workflow("derive", [Step("check", dv_check), Step("build", dv_build), Step("publish", dv_publish)],
                 "Event-driven rebuild of a derived dataset, blocked when inputs are held or incident-bound."),
        Workflow("triage", [
            Step("open", t_open), Step("investigate", t_investigate, kind="agent", retry=agent),
            Step("diagnose", t_diagnose, kind="agent", retry=agent), Step("impact", t_impact),
            Step("plan", t_plan, kind="agent", retry=agent, when=_not_abstained), Step("govern", t_govern, when=_not_abstained),
            Step("execute", t_execute, when=_not_abstained), Step("await_approvals", t_await, when=_not_abstained),
            Step("verify", t_verify, when=_not_abstained), Step("learn", t_learn, kind="agent", retry=agent), Step("close", t_close)],
            "Incident triage swarm: investigate, diagnose, assess impact, plan, govern, act, verify, learn."),
    ]


def register(svc) -> None:
    for wf in build_workflows():
        svc.engine.register(wf)
    svc.engine.register_compensator("unpublish_version", _unpublish_compensator)

    def on_signal(ev):
        sig = svc.signals.get(ev["payload"]["signal_id"])
        if not sig or sig["incident_id"]:
            return
        iid, is_new = svc.agents.correlator.on_signal(sig)
        if iid and is_new:
            inc = svc.incidents.get(iid)
            svc.engine.submit("triage", {"incident_id": iid}, sig["tenant"], dataset=inc["dataset"], incident_id=iid, priority=2,
                              trace_id=inc["trace_id"], delay_s=svc.settings.engine.triage_debounce_s)

    def on_approval(ev):
        handle_approval_decided(svc, ev["payload"]["approval_id"])

    def on_published(ev):
        on_dataset_published(svc, ev["payload"])

    def on_released(ev):
        on_dataset_published(svc, {**ev["payload"], "rollback": True})

    svc.dispatcher.subscribe("correlator", ["signal.raised"], on_signal)
    svc.dispatcher.subscribe("approvals", ["approval.decided"], on_approval)
    svc.dispatcher.subscribe("derive-trigger", ["dataset.published"], on_published)
    svc.dispatcher.subscribe("hold-release", ["dataset.released"], on_released)
