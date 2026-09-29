"""Deterministic profiling, type inference, schema diff and distribution drift (PSI).

The five data-health signals: freshness, volume, schema, distribution/quality, lineage -
detection is always deterministic; models are only used to *explain* and *map meaning*."""
from __future__ import annotations

import difflib
import math
import re

import numpy as np
import pandas as pd

_INT_RE = re.compile(r"^[+-]?\d+(\.0+)?$")
_BOOL = {"true", "false", "yes", "no", "y", "n", "t", "f"}
_DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d", "%d.%m.%Y", "%Y%m%d", "%d %b %Y", "%b %d %Y"]


def parse_dates(s: pd.Series, formats: list[str]) -> pd.Series:
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    remaining = s.notna()
    for fmt in formats:
        if not remaining.any():
            break
        parsed = pd.to_datetime(s[remaining], format=fmt, errors="coerce")
        ok = parsed.notna()
        out.loc[parsed.index[ok]] = parsed[ok]
        remaining.loc[parsed.index[ok]] = False
    return out


def infer_type(s: pd.Series) -> tuple[str, float, str | None]:
    """Return (type, parse_rate, date_format)."""
    v = s.dropna().astype(str)
    if v.empty:
        return "string", 1.0, None
    n = len(v)
    if v.str.match(_INT_RE).sum() / n >= 0.95:
        return "int", float(v.str.match(_INT_RE).sum() / n), None
    num = pd.to_numeric(v.str.replace(",", "", regex=False), errors="coerce")
    if num.notna().sum() / n >= 0.95:
        return "float", float(num.notna().sum() / n), None
    if v.str.lower().isin(_BOOL).sum() / n >= 0.95:
        return "bool", 1.0, None
    sample = v.head(200)
    for fmt in _DATE_FORMATS:
        rate = pd.to_datetime(sample, format=fmt, errors="coerce").notna().mean()
        if rate >= 0.95:
            return "date", float(rate), fmt
    return "string", 1.0, None


def numeric_bins(values: pd.Series, bins: int = 10) -> dict | None:
    x = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    if len(x) < 20 or x.nunique() < 3:
        return None
    edges = np.unique(np.quantile(x, np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:
        return None
    inner = edges[1:-1].tolist()
    counts = np.histogram(x, bins=[-np.inf, *inner, np.inf])[0]
    return {"edges": inner, "props": (counts / counts.sum()).round(6).tolist()}


def psi(baseline: dict | None, values: pd.Series) -> float | None:
    """Population Stability Index of `values` against a stored baseline histogram."""
    if not baseline:
        return None
    x = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    if len(x) < 20:
        return None
    counts = np.histogram(x, bins=[-np.inf, *baseline["edges"], np.inf])[0]
    cur = counts / counts.sum()
    base = np.asarray(baseline["props"], dtype=float)
    eps = 1e-4
    cur = np.clip(cur, eps, None)
    base = np.clip(base, eps, None)
    return float(np.sum((cur - base) * np.log(cur / base)))


def profile_frame(df: pd.DataFrame, masked_columns: set[str] | None = None, samples: int = 5) -> dict:
    masked_columns = masked_columns or set()
    cols = []
    n = len(df)
    for c in df.columns:
        s = df[c]
        nonnull = s.dropna()
        t, rate, fmt = infer_type(s)
        info = {"name": str(c), "inferred_type": t, "parse_rate": round(rate, 4), "date_format": fmt,
                "null_rate": round(1 - len(nonnull) / n, 4) if n else 0.0,
                "distinct": int(nonnull.nunique()), "distinct_ratio": round(nonnull.nunique() / max(1, len(nonnull)), 4)}
        if t in ("int", "float"):
            x = pd.to_numeric(nonnull.astype(str).str.replace(",", "", regex=False), errors="coerce").dropna()
            if len(x):
                info.update({"min": float(x.min()), "max": float(x.max()), "mean": round(float(x.mean()), 4),
                             "std": round(float(x.std(ddof=0)), 4), "p50": float(x.median()), "sum": round(float(x.sum()), 4)})
                info["bins"] = numeric_bins(x)
        else:
            lens = nonnull.astype(str).str.len()
            if len(lens):
                info["avg_len"] = round(float(lens.mean()), 2)
        if str(c) in masked_columns:
            info["samples"] = ["<masked>"] * min(samples, len(nonnull))
            info["top_values"] = {}
        else:
            info["samples"] = [str(v)[:60] for v in nonnull.head(samples).tolist()]
            if t not in ("float",) and info["distinct"] <= 50:
                info["top_values"] = {str(k): int(v) for k, v in nonnull.value_counts().head(8).items()}
        cols.append(info)
    return {"rows": n, "columns": cols}


def column_profile(profile: dict | None, name: str) -> dict | None:
    for c in (profile or {}).get("columns", []):
        if c["name"] == name:
            return c
    return None


def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def schema_diff(contract: dict, observed: list[str], alias_map: dict[str, str]) -> dict:
    """Compare observed column names with the contract. alias_map: normalized alias -> contract column."""
    contract_cols = {c["name"]: c for c in contract.get("columns", [])}
    norm_to_contract = {_norm(k): k for k in contract_cols}
    mapped: dict[str, str] = {}
    via_alias: dict[str, str] = {}
    for obs in observed:
        key = _norm(obs)
        if key in norm_to_contract:
            mapped[obs] = norm_to_contract[key]
        elif key in alias_map and alias_map[key] in contract_cols and alias_map[key] not in mapped.values():
            mapped[obs] = alias_map[key]
            via_alias[obs] = alias_map[key]
    present = set(mapped.values())
    missing_required = [c for c, spec in contract_cols.items() if spec.get("required") and c not in present]
    missing_optional = [c for c, spec in contract_cols.items() if not spec.get("required") and c not in present]
    new_columns = [o for o in observed if o not in mapped]
    candidates = {}
    for miss in missing_required + missing_optional:
        scored = sorted(((difflib.SequenceMatcher(None, _norm(miss), _norm(o)).ratio(), o) for o in new_columns), reverse=True)
        candidates[miss] = [{"column": o, "name_similarity": round(r, 3)} for r, o in scored[:3] if r > 0.3]
    return {"mapped": mapped, "via_alias": via_alias, "missing_required": missing_required,
            "missing_optional": missing_optional, "new_columns": new_columns, "rename_candidates": candidates,
            "breaking": bool(missing_required), "additive": bool(new_columns) and not missing_required}


def safe_float(v) -> float | None:
    try:
        f = float(v)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None
