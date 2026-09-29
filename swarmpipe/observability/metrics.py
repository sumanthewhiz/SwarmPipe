"""Metrics (counters, summaries, gauges), SLOs with error budgets, and Prometheus exposition.

Points are buffered in memory and flushed to SQLite in batches (never one write per metric)."""
from __future__ import annotations

import threading
import time
from collections import defaultdict

from swarmpipe.core.util import dumps, loads

_KIND = "_k"


def _quantile(sorted_vals: list[float], q: float) -> float | None:
    if not sorted_vals:
        return None
    idx = min(len(sorted_vals) - 1, max(0, int(round(q * (len(sorted_vals) - 1)))))
    return sorted_vals[idx]


class Metrics:
    def __init__(self, db, flush_at: int = 100):
        self.db = db
        self.flush_at = flush_at
        self._buf: list[tuple] = []
        self._lock = threading.Lock()

    def _add(self, kind: str, name: str, value: float, labels: dict) -> None:
        lab = {k: str(v) for k, v in sorted(labels.items()) if v is not None}
        lab[_KIND] = kind
        with self._lock:
            self._buf.append((name, dumps(lab), float(value), time.time()))
            should_flush = len(self._buf) >= self.flush_at
        if should_flush:
            self.flush()

    def inc(self, name: str, value: float = 1.0, **labels) -> None:
        self._add("c", name, value, labels)

    def observe(self, name: str, value: float, **labels) -> None:
        self._add("s", name, value, labels)

    def gauge(self, name: str, value: float, **labels) -> None:
        self._add("g", name, value, labels)

    def flush(self) -> None:
        with self._lock:
            batch, self._buf = self._buf, []
        if batch:
            try:
                self.db.executemany("INSERT INTO metric_points(name, labels, value, ts) VALUES(?,?,?,?)", batch)
            except Exception:  # noqa: BLE001 - never let telemetry break the pipeline
                with self._lock:
                    self._buf = batch + self._buf

    # ---- queries ---------------------------------------------------------------------------
    def points(self, name: str, since_s: float = 86400, where: dict | None = None) -> list[dict]:
        self.flush()
        rows = self.db.query("SELECT labels, value, ts FROM metric_points WHERE name=? AND ts>=? ORDER BY ts",
                             (name, time.time() - since_s))
        out = []
        for r in rows:
            lab = loads(r["labels"], {})
            if where and any(lab.get(k) != str(v) for k, v in where.items()):
                continue
            out.append({"labels": lab, "value": r["value"], "ts": r["ts"]})
        return out

    def summary(self, name: str, since_s: float = 86400, where: dict | None = None) -> dict:
        vals = sorted(p["value"] for p in self.points(name, since_s, where))
        if not vals:
            return {"count": 0}
        return {"count": len(vals), "sum": round(sum(vals), 6), "avg": round(sum(vals) / len(vals), 6),
                "p50": _quantile(vals, 0.5), "p95": _quantile(vals, 0.95), "p99": _quantile(vals, 0.99), "max": vals[-1]}

    def totals(self, name: str, since_s: float = 86400, by: str | None = None) -> dict:
        agg: dict[str, float] = defaultdict(float)
        for p in self.points(name, since_s):
            agg[p["labels"].get(by, "all") if by else "all"] += p["value"]
        return dict(agg)

    def names(self, since_s: float = 86400) -> list[str]:
        self.flush()
        return [r["name"] for r in self.db.query("SELECT DISTINCT name FROM metric_points WHERE ts>=? ORDER BY name",
                                                 (time.time() - since_s,))]

    def prometheus(self, since_s: float = 86400) -> str:
        """Prometheus text exposition, computed from the last `since_s` seconds of points."""
        self.flush()
        rows = self.db.query("SELECT name, labels, value FROM metric_points WHERE ts>=?", (time.time() - since_s,))
        series: dict[tuple, list[float]] = defaultdict(list)
        kinds: dict[str, str] = {}
        for r in rows:
            lab = loads(r["labels"], {})
            kind = lab.pop(_KIND, "g")
            kinds[r["name"]] = kind
            series[(r["name"], tuple(sorted(lab.items())))].append(r["value"])
        lines: list[str] = []
        for name in sorted(kinds):
            metric = "swarmpipe_" + name.replace(".", "_").replace("-", "_")
            kind = kinds[name]
            lines.append(f"# TYPE {metric} {'counter' if kind == 'c' else 'summary' if kind == 's' else 'gauge'}")
            for (n, lab), vals in sorted(series.items()):
                if n != name:
                    continue
                base = ",".join(f'{k}="{str(v).replace(chr(92), chr(92) * 2).replace(chr(34), chr(92) + chr(34))}"'
                                for k, v in lab)
                if kind == "c":
                    lines.append(f"{metric}{{{base}}} {sum(vals)}")
                elif kind == "g":
                    lines.append(f"{metric}{{{base}}} {vals[-1]}")
                else:
                    sv = sorted(vals)
                    for q in (0.5, 0.95, 0.99):
                        ql = (base + "," if base else "") + f'quantile="{q}"'
                        lines.append(f"{metric}{{{ql}}} {_quantile(sv, q)}")
                    lines.append(f"{metric}_count{{{base}}} {len(sv)}")
                    lines.append(f"{metric}_sum{{{base}}} {round(sum(sv), 6)}")
        return "\n".join(lines) + "\n"


class SLOService:
    """Service-level objectives for the pipeline itself (the agent system needs SLOs too)."""

    def __init__(self, settings, metrics: Metrics):
        self.settings = settings
        self.metrics = metrics

    def evaluate(self) -> list[dict]:
        out = []
        for slo in self.settings.slos:
            window = float(slo.get("window_h", 24)) * 3600
            metric = slo["metric"]
            if metric == "ingest_outcome":
                pts = self.metrics.points("ingest_files_total", window)
                total = sum(p["value"] for p in pts)
                bad = sum(p["value"] for p in pts if p["labels"].get("outcome") == "dead_lettered")
            else:
                pts = self.metrics.points(metric, window)
                total = len(pts)
                bad = sum(1 for p in pts if p["value"] > float(slo.get("threshold", 0)))
            objective = float(slo["objective"])
            sli = None if total == 0 else (total - bad) / total
            allowed_bad = 1.0 - objective
            burn = None if total == 0 or allowed_bad <= 0 else (bad / total) / allowed_bad
            remaining = None if burn is None else max(0.0, 1.0 - burn)
            status = "no_data" if total == 0 else ("ok" if sli >= objective else "breached")
            if status == "ok" and burn is not None and burn > 0.8:
                status = "at_risk"
            out.append({**slo, "total": total, "bad": bad, "sli": sli, "burn_rate": burn,
                        "error_budget_remaining": remaining, "status": status})
        return out
