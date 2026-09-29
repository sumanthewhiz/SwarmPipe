"""Contract-driven transformation: column mapping (exact / alias / approved override), typing,
row-level rejects, primary-key de-duplication and derived columns.

Rows that cannot be typed are never silently coerced: they are rejected with a reason and end up in
the quarantine table, and every count feeds the reconciliation checks (rows_in = rows_out +
rejected + duplicates_removed + quarantined)."""
from __future__ import annotations

import re
from dataclasses import dataclass, field

import pandas as pd

from swarmpipe.data import safe_expr
from swarmpipe.data.profiling import parse_dates, schema_diff

_TRUE = {"true", "yes", "y", "t", "1"}
_FALSE = {"false", "no", "n", "f", "0"}


@dataclass
class TransformResult:
    df: pd.DataFrame
    rejects: list[dict] = field(default_factory=list)
    stats: dict = field(default_factory=dict)
    diff: dict = field(default_factory=dict)
    renamed: pd.DataFrame | None = None


def _to_number(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(",", "", regex=False).str.strip(), errors="coerce").where(s.notna())


def cast_column(s: pd.Series, spec: dict, date_hint: str | None = None) -> tuple[pd.Series, pd.Series]:
    """Return (typed, failed_mask). failed = value present but not castable."""
    t = spec.get("type", "string")
    present = s.notna()
    if t == "int":
        num = _to_number(s)
        integral = num.notna() & (num.round() == num)
        typed = num.where(integral).round().astype("Int64")
    elif t == "float":
        typed = _to_number(s).astype("Float64")
    elif t == "date":
        fmts = list(spec.get("formats") or ["%Y-%m-%d"])
        if date_hint and date_hint not in fmts:
            fmts.append(date_hint)
        dt = parse_dates(s.astype(str).where(present), fmts)
        typed = dt.dt.strftime("%Y-%m-%d").where(dt.notna())
    elif t == "bool":
        low = s.astype(str).str.strip().str.lower()
        typed = pd.Series(pd.NA, index=s.index, dtype="boolean")
        typed = typed.mask(low.isin(_TRUE), True).mask(low.isin(_FALSE), False)
    else:
        typed = s.astype("str").where(present).str.strip()
        if spec.get("transform") == "upper":
            typed = typed.str.upper()
        elif spec.get("transform") == "lower":
            typed = typed.str.lower()
    failed = present & typed.isna()
    return typed, failed


def apply_contract(raw: pd.DataFrame, contract: dict, alias_map: dict[str, str],
                   mapping_override: dict[str, str] | None = None, date_hints: dict[str, str] | None = None) -> TransformResult:
    mapping_override = {k: v for k, v in (mapping_override or {}).items() if k in raw.columns}
    df = raw.rename(columns=mapping_override)
    diff = schema_diff(contract, list(df.columns), alias_map)
    df = df.rename(columns=diff["mapped"])
    cols = [c for c in contract.get("columns", [])]
    rows_in = len(df)
    out = pd.DataFrame(index=df.index)
    reject_reason = pd.Series("", index=df.index, dtype="str")
    cast_failures: dict[str, int] = {}
    required_nulls: dict[str, int] = {}
    for spec in cols:
        name = spec["name"]
        if name not in df.columns:
            out[name] = pd.Series(pd.NA, index=df.index, dtype="object")
            continue
        typed, failed = cast_column(df[name], spec, (date_hints or {}).get(name))
        out[name] = typed
        n_failed = int(failed.sum())
        if n_failed:
            cast_failures[name] = n_failed
            if spec.get("required"):
                reject_reason = reject_reason.mask(failed & (reject_reason == ""), f"cast_failed:{name}")
        if spec.get("required"):
            nulls = df[name].isna()
            if int(nulls.sum()):
                required_nulls[name] = int(nulls.sum())
                reject_reason = reject_reason.mask(nulls & (reject_reason == ""), f"required_null:{name}")
    rejected_mask = reject_reason != ""
    numeric_cols = [s["name"] for s in cols if s.get("type") in ("int", "float") and s["name"] in df.columns]
    raw_totals = {c: round(float(_to_number(df[c]).fillna(0).sum()), 4) for c in numeric_cols}
    rejected_totals = {c: round(float(_to_number(df.loc[rejected_mask, c]).fillna(0).sum()), 4) for c in numeric_cols}
    rejects = []
    if rejected_mask.any():
        raw_rows = raw.loc[rejected_mask]
        for idx, reason in reject_reason[rejected_mask].items():
            rejects.append({"row_number": int(idx) + 2, "reason": reason,
                            "data": {str(k): (None if pd.isna(v) else str(v)) for k, v in raw_rows.loc[idx].items()}})
    typed_df = out.loc[~rejected_mask].copy()
    pk = [c for c in contract.get("primary_key", []) if c in typed_df.columns]
    dup_removed = 0
    duplicate_totals = {c: 0.0 for c in numeric_cols}
    if pk and not typed_df.empty:
        dup_mask = typed_df.duplicated(subset=pk, keep="last")
        dup_removed = int(dup_mask.sum())
        if dup_removed:
            duplicate_totals = {c: round(float(pd.to_numeric(typed_df.loc[dup_mask, c], errors="coerce").fillna(0).sum()), 4)
                                for c in numeric_cols}
            typed_df = typed_df.loc[~dup_mask]
    for name, expr in (contract.get("derived_columns") or {}).items():
        typed_df[name] = safe_expr.evaluate(expr, typed_df)
    stats = {"rows_in": rows_in, "rows_out": len(typed_df), "rejected": int(rejected_mask.sum()),
             "duplicates_removed": dup_removed, "cast_failures": cast_failures, "required_nulls": required_nulls,
             "mapping_used": {**mapping_override, **diff["mapped"]}, "new_columns": diff["new_columns"],
             "missing_required": diff["missing_required"], "missing_optional": diff["missing_optional"],
             "raw_totals": raw_totals, "rejected_totals": rejected_totals, "duplicate_totals": duplicate_totals}
    return TransformResult(typed_df, rejects, stats, diff, df)


def date_failure_samples(renamed: pd.DataFrame, contract: dict, limit: int = 12) -> dict[str, list[str]]:
    """Date columns where >20% of values fail the contract formats (candidates for format inference)."""
    samples = {}
    for spec in contract.get("columns", []):
        if spec.get("type") != "date" or spec["name"] not in renamed.columns:
            continue
        s = renamed[spec["name"]].dropna().astype(str)
        _, failed = cast_column(s, spec)
        bad = s[failed]
        if len(bad) and len(bad) / max(1, len(s)) > 0.2:
            samples[spec["name"]] = bad.head(limit).tolist()
    return samples


_SAFE_ID = re.compile(r"[^a-z0-9_]")


def safe_identifier(name: str) -> str:
    return _SAFE_ID.sub("_", name.lower())
