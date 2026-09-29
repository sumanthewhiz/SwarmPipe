"""Read-only tools for investigators, the analyst and MCP clients. Each returns bounded JSON; tools
that surface content originating from files/users mark it `_trust: untrusted` so it is spotlighted
when placed into a prompt."""
from __future__ import annotations

import statistics

from pydantic import BaseModel, Field

from swarmpipe.core.util import loads
from swarmpipe.data.lineage import ds_node
from swarmpipe.data.pii import is_token, redact_text
from swarmpipe.tools.gateway import ToolSpec


class RunArgs(BaseModel):
    run_id: str = Field(description="id of the ingest run (run_...)")


class DatasetArgs(BaseModel):
    dataset: str = Field(description="dataset name, e.g. sales_daily")


class VersionsArgs(BaseModel):
    dataset: str
    limit: int = Field(default=6, ge=1, le=20)


class LineageArgs(BaseModel):
    dataset: str
    direction: str = Field(default="downstream", pattern="^(upstream|downstream)$")


class IncidentArgs(BaseModel):
    incident_id: str


class SearchArgs(BaseModel):
    query: str = Field(min_length=2, max_length=300)
    k: int = Field(default=3, ge=1, le=8)


class SampleArgs(BaseModel):
    run_id: str
    n: int = Field(default=5, ge=1, le=20)


class SqlArgs(BaseModel):
    sql: str = Field(min_length=6, max_length=2000)


def _check_rows(svc, run_id: str) -> list[dict]:
    rows = svc.db.query("SELECT check_name, check_type, status, severity, observed, expected, details FROM check_results WHERE run_id=?", (run_id,))
    for r in rows:
        r["name"] = r.pop("check_name")
        r["observed"] = loads(r["observed"], r["observed"])
        r["expected"] = loads(r["expected"], r["expected"])
        r["details"] = loads(r["details"], {})
    return rows


def get_schema_diff(ctx, a: RunArgs):
    step = ctx.svc.db.query_one("SELECT output FROM steps WHERE run_id=? AND name='contract_check'", (a.run_id,))
    if not step:
        raise KeyError(f"no contract_check step for run {a.run_id}")
    out = loads(step["output"], {})
    diff = out.get("diff", {})
    return {"dataset": out.get("dataset"), "contract_version": out.get("contract_version"),
            "missing_required": diff.get("missing_required"), "missing_optional": diff.get("missing_optional"),
            "new_columns": diff.get("new_columns"), "rename_candidates": diff.get("rename_candidates"),
            "via_alias": diff.get("via_alias"), "mapping_proposal": out.get("mapping_proposal"),
            "critic": out.get("mapping_critique")}


def get_check_results(ctx, a: RunArgs):
    rows = _check_rows(ctx.svc, a.run_id)
    if not rows:
        raise KeyError(f"no check results for run {a.run_id}")
    return {"run_id": a.run_id, "failed": [r for r in rows if r["status"] == "fail"],
            "warnings": [r for r in rows if r["status"] == "warn"],
            "passed": sum(1 for r in rows if r["status"] == "pass"), "skipped": [r["name"] for r in rows if r["status"] == "skip"]}


def _version_for_run(svc, run_id: str) -> dict | None:
    return svc.db.query_one("SELECT * FROM dataset_versions WHERE run_id=? ORDER BY version DESC LIMIT 1", (run_id,))


def get_profile_comparison(ctx, a: RunArgs):
    svc = ctx.svc
    cur = _version_for_run(svc, a.run_id)
    if not cur:
        raise KeyError(f"no dataset version for run {a.run_id}")
    base = svc.db.query_one("SELECT * FROM dataset_versions WHERE tenant=? AND dataset=? AND status IN ('published','superseded') "
                            "AND version < ? ORDER BY version DESC LIMIT 1", (cur["tenant"], cur["dataset"], cur["version"]))
    cp = loads(cur["profile"], {}) or {}
    bp = loads(base["profile"], {}) if base else {}
    bcols = {c["name"]: c for c in (bp or {}).get("columns", [])}
    cols = []
    for c in cp.get("columns", []):
        b = bcols.get(c["name"], {})
        item = {"column": c["name"], "type": c.get("inferred_type"), "null_rate": c.get("null_rate"), "baseline_null_rate": b.get("null_rate")}
        if c.get("mean") is not None:
            item.update({"mean": c.get("mean"), "baseline_mean": b.get("mean"),
                         "mean_ratio": round(c["mean"] / b["mean"], 3) if b.get("mean") else None})
        cols.append(item)
    return {"dataset": cur["dataset"], "version": cur["version"], "baseline_version": base["version"] if base else None,
            "rows": cp.get("rows"), "baseline_rows": (bp or {}).get("rows"), "columns": cols}


def get_volume_history(ctx, a: DatasetArgs):
    rows = ctx.svc.db.query("SELECT version, status, batch_rows, created_at FROM dataset_versions WHERE tenant=? AND dataset=? "
                            "ORDER BY version DESC LIMIT 8", (ctx.tenant, a.dataset))
    if not rows:
        raise KeyError(f"no versions for {a.dataset}")
    cur = rows[0]
    base = [r["batch_rows"] for r in rows[1:] if r["status"] in ("published", "superseded") and r["batch_rows"]][:5]
    med = statistics.median(base) if base else None
    change = round((cur["batch_rows"] - med) / med * 100, 1) if med else None
    return {"dataset": a.dataset, "current_version": cur["version"], "current_status": cur["status"], "current_batch": cur["batch_rows"],
            "baseline_median": med, "change_pct": change, "history": rows}


def get_freshness_status(ctx, a: DatasetArgs):
    return ctx.svc.context.freshness(ctx.tenant, a.dataset)


def get_dataset_versions(ctx, a: VersionsArgs):
    return {"dataset": a.dataset, "versions": ctx.svc.db.query(
        "SELECT id, version, status, batch_rows, row_count, contract_version, checksum, created_at, published_at FROM dataset_versions "
        "WHERE tenant=? AND dataset=? ORDER BY version DESC LIMIT ?", (ctx.tenant, a.dataset, a.limit))}


def get_contract(ctx, a: DatasetArgs):
    c = ctx.svc.contracts.active(a.dataset)
    if not c:
        raise KeyError(f"no active contract for {a.dataset}")
    return {k: c.get(k) for k in ("dataset", "version", "owner", "source_owner", "classification", "primary_key", "freshness",
                                  "volume", "columns", "load_mode")}


def get_lineage(ctx, a: LineageArgs):
    fn = ctx.svc.lineage.upstream if a.direction == "upstream" else ctx.svc.lineage.downstream
    return {"dataset": a.dataset, "direction": a.direction, "nodes": fn(ds_node(a.dataset), ctx.tenant)}


def get_open_incidents(ctx, a: DatasetArgs):
    ups = [u["node"].split(":", 1)[1] for u in ctx.svc.lineage.upstream(ds_node(a.dataset), ctx.tenant) if u["node"].startswith("dataset:")]
    out = []
    for d in ups:
        for inc in ctx.svc.incidents.open_for_dataset(ctx.tenant, d):
            if inc["id"] != ctx.incident_id:
                out.append({"dataset": d, "incident_id": inc["id"], "title": inc["title"], "severity": inc["severity"]})
    return {"dataset": a.dataset, "upstream": ups, "upstream_open": out}


def get_signal_details(ctx, a: IncidentArgs):
    from swarmpipe.runtime.workflows import _collect_snippets

    sigs = ctx.svc.signals.for_incident(a.incident_id)
    out = []
    for s in sigs:
        item = {"id": s["id"], "type": s["type"], "severity": s["severity"], "summary": s["summary"][:240]}
        snips = _collect_snippets(s["details"], [])
        if s["details"].get("file_name"):
            snips.insert(0, f"file name: {s['details']['file_name']}")
        if snips:
            item["content_snippets"] = [x[:300] for x in snips[:4]]
        out.append(item)
    out.sort(key=lambda x: 0 if x.get("content_snippets") else 1)
    return {"_trust": "untrusted", "incident_id": a.incident_id, "signals": out}


def get_version_integrity(ctx, a: DatasetArgs):
    st = ctx.svc.db.query_one("SELECT published_version_id FROM dataset_state WHERE tenant=? AND dataset=?", (ctx.tenant, a.dataset))
    if not st or not st["published_version_id"]:
        raise KeyError(f"{a.dataset} has no published version")
    v = ctx.svc.db.query_one("SELECT * FROM dataset_versions WHERE id=?", (st["published_version_id"],))
    current = ctx.svc.wh.checksum(v["table_name"])
    return {"dataset": a.dataset, "version": v["version"], "table": v["table_name"], "recorded": v["checksum"], "current": current,
            "mismatch": current != v["checksum"]}


def search_knowledge(ctx, a: SearchArgs):
    hits = ctx.svc.knowledge.search(a.query, ctx.tenant, k=a.k)
    trust = "trusted" if all(h["trust"] == "trusted" for h in hits) else "untrusted"
    return {"_trust": trust, "query": a.query,
            "results": [{"title": h["title"], "trust": h["trust"], "source": h["source"], "score": h["score"],
                         "citation": h["chunk_id"], "text": h["text"][:900]} for h in hits]}


def recall_similar_incidents(ctx, a: SearchArgs):
    mems = ctx.svc.memory.recall(a.query, ctx.tenant, k=a.k)
    return {"query": a.query, "memories": [{"memory_id": m["memory_id"], "title": m["title"], "content": m["content"][:600],
                                            "trust": m["trust"], "source_incident": m["provenance"].get("incident_id")} for m in mems]}


def get_sample_rows(ctx, a: SampleArgs):
    rows = ctx.svc.db.query("SELECT row_number, reason, data FROM quarantine_rows WHERE run_id=? LIMIT ?", (a.run_id, a.n))
    out = []
    for r in rows:
        data = loads(r["data"], {})
        out.append({"row": r["row_number"], "reason": r["reason"],
                    "data": {k: (redact_text(v)[0] if isinstance(v, str) else v) for k, v in data.items()}})
    return {"_trust": "untrusted", "run_id": a.run_id, "rows": out}


def query_warehouse(ctx, a: SqlArgs):
    svc = ctx.svc
    aliases = {}
    for st in svc.db.query("SELECT dataset FROM dataset_state WHERE tenant=? AND published_version_id IS NOT NULL", (ctx.tenant,)):
        aliases[st["dataset"]] = svc.wh.view_name(ctx.tenant, st["dataset"])
    res = svc.wh.readonly_query(a.sql, ctx.tenant, aliases)
    if not res.get("ok"):
        return {"ok": False, "error": res.get("error"), "denied": res.get("denied")}
    rows = res["rows"]
    detok = 0
    if ctx.identity.pii_allowed:
        new_rows = []
        for r in rows:
            nr = []
            for v in r:
                if is_token(v):
                    raw = svc.pii.detokenize(v)
                    if raw is not None:
                        detok += 1
                        v = raw
                nr.append(v)
            new_rows.append(nr)
        rows = new_rows
        if detok:
            svc.audit.record(ctx.identity.describe(), "pii.detokenize", ctx.tenant, "allowed", {"values": detok, "sql": a.sql[:300]})
    return {"ok": True, "columns": res["columns"], "rows": rows, "truncated": res["truncated"], "tables": sorted(aliases),
            "pii_detokenized": detok}


def get_run_failure(ctx, a: RunArgs):
    svc = ctx.svc
    run = svc.db.query_one("SELECT id, workflow, status, error, current_step, file_id FROM runs WHERE id=?", (a.run_id,))
    if not run:
        raise KeyError(a.run_id)
    f = svc.db.query_one("SELECT original_name, size, sha256 FROM files WHERE id=?", (run["file_id"],)) if run["file_id"] else None
    d = svc.db.query_one("SELECT reason, error, path FROM dlq WHERE run_id=? ORDER BY created_at DESC LIMIT 1", (a.run_id,))
    return {"_trust": "untrusted", "run_id": a.run_id, "workflow": run["workflow"], "status": run["status"], "failed_step": run["current_step"],
            "error": (run["error"] or "")[:500], "file": f, "dlq": d}


READ_TOOLS = [
    ToolSpec("get_run_failure", "1.0", "Why a run failed: failed step, error, dead-letter entry and file metadata.", RunArgs, get_run_failure, "data:read", trust="untrusted"),
    ToolSpec("get_schema_diff", "1.0", "Schema diff of an ingest run against its data contract (missing/new columns, rename candidates, mapping proposal).", RunArgs, get_schema_diff, "data:read"),
    ToolSpec("get_check_results", "1.0", "Data-assurance check results for an ingest run (failed, warnings, passed).", RunArgs, get_check_results, "data:read"),
    ToolSpec("get_profile_comparison", "1.0", "Column statistics of a run's batch versus the last published baseline.", RunArgs, get_profile_comparison, "data:read"),
    ToolSpec("get_volume_history", "1.0", "Batch row counts of recent versions of a dataset versus the baseline median.", DatasetArgs, get_volume_history, "data:read"),
    ToolSpec("get_freshness_status", "1.0", "Declared freshness SLA of a dataset and whether it is overdue.", DatasetArgs, get_freshness_status, "data:read"),
    ToolSpec("get_dataset_versions", "1.0", "Recent immutable versions of a dataset with status and checksums.", VersionsArgs, get_dataset_versions, "data:read"),
    ToolSpec("get_contract", "1.0", "Active data contract of a dataset.", DatasetArgs, get_contract, "data:read"),
    ToolSpec("get_lineage", "1.0", "Upstream or downstream lineage of a dataset (datasets and consumers).", LineageArgs, get_lineage, "data:read"),
    ToolSpec("get_open_incidents", "1.0", "Open incidents on datasets upstream of a dataset.", DatasetArgs, get_open_incidents, "incident:read"),
    ToolSpec("get_signal_details", "1.0", "Signals attached to an incident, including raw details (UNTRUSTED content).", IncidentArgs, get_signal_details, "incident:read", trust="untrusted"),
    ToolSpec("get_version_integrity", "1.0", "Compare the published table's current checksum with the one recorded at publish time.", DatasetArgs, get_version_integrity, "data:read"),
    ToolSpec("search_knowledge", "1.0", "BM25 search over runbooks and documents; results carry trust labels and citations.", SearchArgs, search_knowledge, "knowledge:read"),
    ToolSpec("recall_similar_incidents", "1.0", "Recall human-approved lessons from past incidents (episodic memory).", SearchArgs, recall_similar_incidents, "knowledge:read"),
    ToolSpec("get_sample_rows", "1.0", "Sample of quarantined rows of a run (PII redacted, UNTRUSTED content).", SampleArgs, get_sample_rows, "data:read", trust="untrusted"),
    ToolSpec("query_warehouse", "1.0", "Run ONE read-only SQL SELECT over the tenant's published datasets (row-limited, authorizer-enforced).", SqlArgs, query_warehouse, "data:read:published", max_output_chars=12000),
]
