"""Periodic control-plane tasks: the monitors that make the orchestrator's *declared* expectations
observable (the orchestrator declares expected behavior up front), plus housekeeping.

  reaper            re-queue runs whose worker lease expired (crash recovery)
  freshness         each contract declares when data is due -> freshness_overdue signals
  out_of_band       published tables must still match the checksum recorded at publish time
                    ("changes that happen outside the pipeline, like a direct database write")
  approvals_expiry  pending approvals expire -> the waiting run resumes and escalates
  autonomy_review   evidence-based promotion recommendations for the autonomy ladder
  slo               evaluate SLOs / error budgets of the pipeline itself
  retention         telemetry is short-lived; audit is never purged
"""
from __future__ import annotations

import shutil
import threading
import time
from datetime import timedelta

from swarmpipe.core.util import iso, parse_iso
from swarmpipe.observability.logging import get_logger

log = get_logger("scheduler")


class Scheduler:
    def __init__(self, svc):
        self.svc = svc
        self.tasks = [("reaper", 5, self.reaper), ("freshness", 20, self.freshness), ("out_of_band", 30, self.out_of_band),
                      ("approvals_expiry", 30, self.approvals_expiry), ("autonomy_review", 60, self.autonomy_review),
                      ("slo", 30, self.slo), ("memory_expiry", 3600, self.memory_expiry), ("retention", 3600, self.retention)]
        self._last: dict[str, float] = {}

    def tick(self, force: bool = False) -> bool:
        did = False
        now = time.monotonic()
        for name, every, fn in self.tasks:
            if force or now - self._last.get(name, 0) >= every:
                self._last[name] = now
                try:
                    res = fn()
                    if isinstance(res, dict):
                        res = sum(v for v in res.values() if isinstance(v, int))
                    did = bool(res) or did
                except Exception:  # noqa: BLE001
                    log.exception("scheduled task %s failed", name)
        self.svc.metrics.flush()
        return did

    def run_task(self, name: str):
        for n, _, fn in self.tasks:
            if n == name:
                return fn()
        raise KeyError(name)

    def loop(self, stop: threading.Event) -> None:
        while not stop.is_set():
            self.tick()
            stop.wait(1.0)

    # ---- tasks ----------------------------------------------------------------------------
    def reaper(self) -> int:
        return self.svc.engine.reap_expired_leases()

    def freshness(self) -> int:
        svc = self.svc
        n = 0
        for st in svc.db.query("SELECT * FROM dataset_state"):
            c = svc.contracts.active(st["dataset"])
            if not c or not c.get("freshness") or not st.get("last_success_at"):
                continue
            fr = svc.context.freshness(st["tenant"], st["dataset"])
            if not fr.get("overdue"):
                continue
            alerted = parse_iso(st.get("freshness_alerted_at"))
            if alerted and (svc.clock.now() - alerted).total_seconds() / 60 < float(c["freshness"].get("expected_every_min", 60)):
                continue
            svc.signals.raise_signal(st["tenant"], "freshness_overdue", st["dataset"], "high",
                                     f"{st['dataset']} is overdue: last good load {fr['age_min']:.0f} min ago (SLA {fr['limit_min']:.0f} min)",
                                     {"freshness": fr})
            svc.db.execute("UPDATE dataset_state SET freshness_alerted_at=? WHERE tenant=? AND dataset=?",
                           (svc.clock.now_iso(), st["tenant"], st["dataset"]))
            n += 1
        return n

    def out_of_band(self) -> int:
        svc = self.svc
        n = 0
        for st in svc.db.query("SELECT * FROM dataset_state WHERE published_version_id IS NOT NULL"):
            v = svc.db.query_one("SELECT * FROM dataset_versions WHERE id=?", (st["published_version_id"],))
            if not v or not v["checksum"]:
                continue
            cur = svc.wh.checksum(v["table_name"])
            if cur is None or cur == v["checksum"]:
                continue
            key = f"oob.alerted.{v['table_name']}"
            if svc.flags.get(key) == cur:
                continue
            svc.flags.set(key, cur, by="system:oob-detector")
            svc.signals.raise_signal(st["tenant"], "out_of_band_change", st["dataset"], "critical",
                                     f"{st['dataset']} v{v['version']} changed outside the pipeline (checksum {v['checksum']} -> {cur})",
                                     {"table": v["table_name"], "version_id": v["id"], "recorded": v["checksum"], "current": cur})
            n += 1
        return n

    def approvals_expiry(self) -> int:
        return self.svc.approvals.expire_due()

    def autonomy_review(self) -> int:
        return len(self.svc.autonomy.review_all())

    def slo(self) -> int:
        svc = self.svc
        n = 0
        for s in svc.slos.evaluate():
            svc.metrics.gauge("slo_sli", s["sli"] if s["sli"] is not None else -1, slo=s["id"])
            if s["status"] == "breached" and s["total"] >= 5:
                key = f"slo.alerted.{s['id']}"
                last = svc.flags.get(key)
                if last and time.time() - float(last) < 3600:
                    continue
                svc.flags.set(key, time.time(), by="system:slo")
                svc.signals.raise_signal(svc.settings.default_tenant, "slo_breach", None, "warning",
                                         f"SLO {s['id']} breached: SLI {s['sli']:.3f} < {s['objective']} (burn rate {s['burn_rate']:.1f}x)",
                                         {"slo": s})
                n += 1
        return n

    def memory_expiry(self) -> int:
        return self.svc.memory.expire()

    def retention(self) -> dict:
        svc = self.svc
        r = svc.settings.retention
        tel_cut = time.time() - float(r.get("telemetry_days", 7)) * 86400
        met_cut = time.time() - float(r.get("metrics_days", 14)) * 86400
        iso_cut = iso(svc.clock.real_now() - timedelta(days=float(r.get("telemetry_days", 7))))
        out = {
            "spans": svc.db.execute("DELETE FROM spans WHERE end_ts < ?", (tel_cut,)).rowcount,
            "metric_points": svc.db.execute("DELETE FROM metric_points WHERE ts < ?", (met_cut,)).rowcount,
            "tool_calls": svc.db.execute("DELETE FROM tool_calls WHERE ts < ?", (iso_cut,)).rowcount,
            "llm_cache": svc.db.execute("DELETE FROM llm_cache WHERE created_at < ?", (time.time() - svc.settings.llm.cache_ttl_s,)).rowcount,
            "events": svc.db.execute("DELETE FROM events WHERE created_at < ? AND id < (SELECT COALESCE(MIN(last_id),0) FROM event_offsets)",
                                     (iso_cut,)).rowcount,
        }
        removed = 0
        proc = svc.settings.data_path("processing")
        for f in svc.db.query("SELECT id FROM files WHERE status IN ('processed','duplicate') AND detected_at < ?", (iso_cut,)):
            d = proc / f["id"]
            if d.exists():
                shutil.rmtree(d, ignore_errors=True)
                removed += 1
        out["processing_dirs"] = removed
        if any(out.values()):
            log.info("retention: %s (audit log is never purged)", out)
        return out
