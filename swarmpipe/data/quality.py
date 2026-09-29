"""Data-assurance checks + circuit breaker. A job that "ended OK" is not necessarily correct, so the
checks run inside the workflow and, when a blocking check fails, execution halts before publishing.

Semantics:
  pass  - fine
  warn  - publish, but raise a signal (e.g. additive schema change, a few bad rows quarantined)
  fail  - circuit breaker: the batch is quarantined and NOT published; a signal opens an incident
Row-level violations below a rule's threshold are quarantined individually ("mostly" semantics);
above the threshold the whole batch fails (systemic problem, not noise)."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Callable

import numpy as np
import pandas as pd

from swarmpipe.data import safe_expr
from swarmpipe.data.profiling import column_profile, psi

SEVERITY_ORDER = ["info", "warning", "high", "critical"]


@dataclass
class CheckResult:
    name: str
    check_type: str
    status: str
    severity: str
    observed: object = None
    expected: object = None
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class QualityInput:
    contract: dict
    df: pd.DataFrame
    stats: dict
    raw_control_totals: dict
    diff: dict
    pii_findings: dict
    injection_hits: list
    baseline_batches: list[int]
    baseline_profile: dict | None
    reference_values: Callable[[str, str], set | None]
    today: date
    rejected_control_totals: dict = field(default_factory=dict)
    duplicate_control_totals: dict = field(default_factory=dict)


@dataclass
class QualityOutcome:
    results: list[CheckResult]
    df: pd.DataFrame
    quarantined_rows: list[dict]
    decision: str

    @property
    def failed(self) -> list[CheckResult]:
        return [r for r in self.results if r.status == "fail"]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if r.status == "warn"]


def _row_violation_check(name, ctype, mask: pd.Series, df: pd.DataFrame, threshold: float, severity: str,
                         expected, quarantine: list[dict], results: list[CheckResult], reason: str) -> pd.Series:
    """Evaluate a row-level rule: quarantine violators if rate <= threshold, else fail the batch."""
    n = len(df)
    bad = int(mask.sum())
    rate = bad / n if n else 0.0
    if bad == 0:
        results.append(CheckResult(name, ctype, "pass", severity, 0, expected))
        return pd.Series(False, index=df.index)
    if rate <= threshold:
        results.append(CheckResult(name, ctype, "warn", "warning", f"{bad} rows ({rate:.1%})", expected,
                                   {"action": "rows quarantined", "violation_rate": round(rate, 4)}))
        for idx in df.index[mask]:
            quarantine.append({"row_number": int(idx), "reason": reason, "data": {k: (None if pd.isna(v) else str(v)) for k, v in df.loc[idx].items()}})
        return mask
    results.append(CheckResult(name, ctype, "fail", severity, f"{bad} rows ({rate:.1%})", expected,
                               {"violation_rate": round(rate, 4), "threshold": threshold,
                                "examples": [str(v) for v in df.index[mask][:5]]}))
    return pd.Series(False, index=df.index)


def run_checks(q: QualityInput) -> QualityOutcome:
    c = q.contract
    df = q.df
    results: list[CheckResult] = []
    quarantine: list[dict] = []
    drop_mask = pd.Series(False, index=df.index)
    st = q.stats

    # --- schema -------------------------------------------------------------------------
    if q.diff.get("missing_required"):
        results.append(CheckResult("schema_required_columns", "schema", "fail", "critical",
                                   q.diff["missing_required"], "all required columns present",
                                   {"new_columns": q.diff.get("new_columns"), "rename_candidates": q.diff.get("rename_candidates")}))
    else:
        results.append(CheckResult("schema_required_columns", "schema", "pass", "critical", [], "all required columns present"))
    if q.diff.get("new_columns"):
        results.append(CheckResult("schema_new_columns", "schema", "warn", "warning", q.diff["new_columns"], "no unknown columns",
                                   {"note": "unknown columns are not published until the contract is updated"}))
    if q.diff.get("via_alias"):
        results.append(CheckResult("schema_alias_mapping", "schema", "pass", "info", q.diff["via_alias"], "aliases resolved via glossary"))

    rows_in = st.get("rows_in", 0)
    for col, n in (st.get("cast_failures") or {}).items():
        rate = n / rows_in if rows_in else 0
        status = "fail" if rate > 0.2 else "warn"
        results.append(CheckResult(f"type_compatibility:{col}", "schema", status, "high" if status == "fail" else "warning",
                                   f"{n} values ({rate:.1%}) not castable", "values match contract type",
                                   {"column": col, "failure_rate": round(rate, 4)}))

    if q.diff.get("missing_required"):
        return QualityOutcome(results, df, quarantine, "quarantine")

    # --- volume -------------------------------------------------------------------------
    vol = c.get("volume") or {}
    batch = st.get("rows_out", len(df))
    if vol.get("min_rows") is not None:
        ok = batch >= int(vol["min_rows"])
        results.append(CheckResult("row_count_min", "volume", "pass" if ok else "fail", "critical", batch, f">= {vol['min_rows']}"))
    if q.baseline_batches:
        base = float(np.median(q.baseline_batches))
        change = (batch - base) / base * 100 if base else 0.0
        if change < -float(vol.get("max_drop_pct", 60)):
            results.append(CheckResult("volume_vs_baseline", "volume", "fail", "critical", batch, f"~{base:.0f} rows",
                                       {"change_pct": round(change, 1), "baseline_batches": q.baseline_batches}))
        elif change > float(vol.get("max_growth_pct", 300)):
            results.append(CheckResult("volume_vs_baseline", "volume", "fail", "high", batch, f"~{base:.0f} rows",
                                       {"change_pct": round(change, 1), "baseline_batches": q.baseline_batches}))
        else:
            results.append(CheckResult("volume_vs_baseline", "volume", "pass", "critical", batch, f"~{base:.0f} rows",
                                       {"change_pct": round(change, 1)}))
    else:
        results.append(CheckResult("volume_vs_baseline", "volume", "skip", "info", batch, "baseline", {"reason": "no history yet (cold start)"}))

    # --- completeness / reject rate ---------------------------------------------------------
    rejected = st.get("rejected", 0)
    rrate = rejected / rows_in if rows_in else 0
    rmax = float(c.get("reject_rate_max", 0.05))
    results.append(CheckResult("reject_rate", "quality", "pass" if rrate <= rmax else "fail", "high",
                               f"{rejected} rows ({rrate:.1%})", f"<= {rmax:.0%}", {"required_nulls": st.get("required_nulls"),
                                                                                 "cast_failures": st.get("cast_failures")}))
    dups = st.get("duplicates_removed", 0)
    if dups:
        drate = dups / rows_in if rows_in else 0
        results.append(CheckResult("unique_primary_key", "quality", "warn" if drate <= 0.05 else "fail", "high",
                                   f"{dups} duplicate keys removed ({drate:.1%})", "unique primary key"))
    else:
        results.append(CheckResult("unique_primary_key", "quality", "pass", "high", 0, "unique primary key"))

    # --- column rules ---------------------------------------------------------------------
    for spec in c.get("columns", []):
        name = spec["name"]
        if name not in df.columns or df.empty:
            continue
        s = df[name]
        thr = float(spec.get("max_violation_rate", 0.02))
        if spec.get("accepted_values"):
            mask = s.notna() & ~s.astype(str).isin([str(v) for v in spec["accepted_values"]])
            drop_mask |= _row_violation_check(f"accepted_values:{name}", "quality", mask, df, thr, "high",
                                              spec["accepted_values"], quarantine, results, f"accepted_values:{name}")
        if spec.get("regex"):
            mask = s.notna() & ~s.astype(str).str.match(spec["regex"])
            drop_mask |= _row_violation_check(f"regex:{name}", "quality", mask, df, thr, "high", spec["regex"],
                                              quarantine, results, f"regex:{name}")
        if spec.get("min") is not None or spec.get("max") is not None:
            x = pd.to_numeric(s, errors="coerce").astype("float64")
            mask = pd.Series(False, index=df.index)
            if spec.get("min") is not None:
                mask |= x < float(spec["min"])
            if spec.get("max") is not None:
                mask |= x > float(spec["max"])
            mask = mask.fillna(False).astype(bool)
            drop_mask |= _row_violation_check(f"range:{name}", "quality", mask, df, thr, "high",
                                              [spec.get("min"), spec.get("max")], quarantine, results, f"range:{name}")
        ref = spec.get("references")
        if ref:
            values = q.reference_values(ref["dataset"], ref["column"])
            if values is None:
                results.append(CheckResult(f"referential:{name}", "lineage", "skip", "info", None, f"{ref['dataset']}.{ref['column']}",
                                           {"reason": "reference dataset not published yet"}))
            else:
                orphans = s.notna() & ~s.astype(str).isin(values)
                n_orphans = int(orphans.sum())
                rate = n_orphans / len(df) if len(df) else 0
                limit = float(ref.get("max_orphan_rate", 0.02))
                results.append(CheckResult(f"referential:{name}", "lineage", "pass" if rate <= limit else "fail", "high",
                                           f"{n_orphans} orphans ({rate:.1%})", f"<= {limit:.0%} in {ref['dataset']}.{ref['column']}",
                                           {"examples": s[orphans].astype(str).head(5).tolist()}))

    # --- business rules (safe expressions) ----------------------------------------------------
    for rule in c.get("rules", []) or []:
        try:
            ok = safe_expr.evaluate(rule["expr"], df).fillna(False).astype(bool)
            mask = ~ok
            drop_mask |= _row_violation_check(f"rule:{rule['name']}", "quality", mask, df, float(rule.get("max_violation_rate", 0.01)),
                                              rule.get("severity", "high"), rule["expr"], quarantine, results, f"rule:{rule['name']}")
        except Exception as exc:  # noqa: BLE001
            results.append(CheckResult(f"rule:{rule['name']}", "quality", "fail", "high", str(exc), rule["expr"], {"error": "rule evaluation failed"}))

    # --- data freshness (content, not arrival) -------------------------------------------------
    dfresh = c.get("data_freshness")
    if dfresh and dfresh.get("column") in df.columns and not df.empty:
        dates = pd.to_datetime(df[dfresh["column"]], errors="coerce").dropna()
        if len(dates):
            newest = dates.max().date()
            age = (q.today - newest).days
            limit = int(dfresh.get("max_age_days", 7))
            results.append(CheckResult("data_freshness", "freshness", "pass" if age <= limit else "fail", "high",
                                       f"newest {newest} ({age} days old)", f"<= {limit} days old",
                                       {"newest": str(newest), "oldest": str(dates.min().date()), "age_days": age}))

    # --- distribution drift -------------------------------------------------------------------
    drift = c.get("drift") or {}
    for col in drift.get("columns", []):
        if col not in df.columns:
            continue
        base = column_profile(q.baseline_profile, col)
        value = psi(base.get("bins") if base else None, df[col])
        if value is None:
            results.append(CheckResult(f"distribution_psi:{col}", "distribution", "skip", "info", None, "baseline histogram",
                                       {"reason": "no baseline"}))
            continue
        cur_mean = float(pd.to_numeric(df[col], errors="coerce").mean())
        details = {"psi": round(value, 4), "baseline_mean": base.get("mean") if base else None, "current_mean": round(cur_mean, 4)}
        if base and base.get("mean"):
            details["mean_ratio"] = round(cur_mean / base["mean"], 3) if base["mean"] else None
        if value >= float(drift.get("psi_fail", 0.25)):
            results.append(CheckResult(f"distribution_psi:{col}", "distribution", "fail", "high", round(value, 4), f"< {drift.get('psi_fail', 0.25)}", details))
        elif value >= float(drift.get("psi_warn", 0.1)):
            results.append(CheckResult(f"distribution_psi:{col}", "distribution", "warn", "warning", round(value, 4), f"< {drift.get('psi_warn', 0.1)}", details))
        else:
            results.append(CheckResult(f"distribution_psi:{col}", "distribution", "pass", "high", round(value, 4), f"< {drift.get('psi_warn', 0.1)}", details))

    # --- privacy & security -------------------------------------------------------------------
    declared = {s["name"] for s in c.get("columns", []) if s.get("pii")}
    undeclared = {k: v for k, v in (q.pii_findings or {}).items() if k not in declared}
    if undeclared:
        results.append(CheckResult("pii_undeclared", "privacy", "warn", "high", undeclared, "PII only in declared columns",
                                   {"action": "values tokenized before publish", "classification": c.get("classification")}))
    else:
        results.append(CheckResult("pii_undeclared", "privacy", "pass", "high", {}, "PII only in declared columns"))
    if q.injection_hits:
        rows = {h["row"] for h in q.injection_hits if isinstance(h.get("row"), int)}
        mask = df.index.isin(list(rows)) if rows else np.zeros(len(df), dtype=bool)
        mask = pd.Series(mask, index=df.index)
        for idx in df.index[mask]:
            quarantine.append({"row_number": int(idx), "reason": "suspicious_content",
                               "data": {k: (None if pd.isna(v) else str(v)) for k, v in df.loc[idx].items()}})
        drop_mask |= mask
        results.append(CheckResult("suspicious_content", "security", "warn", "high", f"{len(q.injection_hits)} cells",
                                   "no instructions embedded in data",
                                   {"hits": q.injection_hits[:5], "action": "rows quarantined; content treated as data"}))

    # --- reconciliation (revenue-assurance style) ------------------------------------------------
    kept = df.loc[~drop_mask]
    n_q = int(drop_mask.sum())
    total_accounted = len(kept) + n_q + rejected + dups
    results.append(CheckResult("reconciliation_rows", "reconciliation", "pass" if total_accounted == rows_in else "fail", "critical",
                               {"rows_in": rows_in, "published": len(kept), "quarantined": n_q, "rejected": rejected, "duplicates": dups},
                               "rows_in = published + quarantined + rejected + duplicates"))
    for col, src_total in (q.raw_control_totals or {}).items():
        if col not in df.columns:
            continue
        pub = float(pd.to_numeric(kept[col], errors="coerce").fillna(0).sum())
        qtot = float(pd.to_numeric(df.loc[drop_mask, col], errors="coerce").fillna(0).sum())
        rej = float((q.rejected_control_totals or {}).get(col, 0.0))
        dup = float((q.duplicate_control_totals or {}).get(col, 0.0))
        accounted = pub + qtot + rej + dup
        diff = abs(accounted - src_total)
        tol = max(0.01, abs(src_total) * 0.005)
        results.append(CheckResult(f"control_total:{col}", "reconciliation", "pass" if diff <= tol else "fail", "critical",
                                   round(accounted, 2), round(src_total, 2),
                                   {"published": round(pub, 2), "quarantined": round(qtot, 2), "rejected": round(rej, 2),
                                    "duplicates": round(dup, 2), "difference": round(diff, 4)}))

    decision = "quarantine" if any(r.status == "fail" for r in results) else "publish"
    return QualityOutcome(results, kept.reset_index(drop=True), quarantine, decision)
