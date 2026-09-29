"""Versioned publishing: stage -> publish (blue/green view swap) -> supersede / quarantine / rollback,
with immutable snapshots of every version (the "WORM" copy used to restore a tampered table)."""
from __future__ import annotations

import pandas as pd

from swarmpipe.core.util import dumps, iso, loads, new_id
from swarmpipe.data.warehouse import Warehouse

LIVE = ("published", "published_override")


class PublishingService:
    def __init__(self, svc):
        self.svc = svc

    @property
    def wh(self) -> Warehouse:
        return self.svc.wh

    def next_version(self, tenant: str, dataset: str) -> int:
        return self.svc.db.scalar("SELECT MAX(version) FROM dataset_versions WHERE tenant=? AND dataset=?", (tenant, dataset), default=0) + 1

    def get(self, version_id: str) -> dict | None:
        v = self.svc.db.query_one("SELECT * FROM dataset_versions WHERE id=?", (version_id,))
        if v:
            v["schema"] = loads(v["schema"], [])
            v["profile"] = loads(v["profile"], {})
        return v

    def current(self, tenant: str, dataset: str) -> dict | None:
        st = self.svc.db.query_one("SELECT published_version_id FROM dataset_state WHERE tenant=? AND dataset=?", (tenant, dataset))
        if not st or not st["published_version_id"]:
            return None
        return self.get(st["published_version_id"])

    def baseline(self, tenant: str, dataset: str, n: int = 5) -> list[dict]:
        rows = self.svc.db.query("SELECT * FROM dataset_versions WHERE tenant=? AND dataset=? AND status IN ('published','superseded','published_override') "
                                 "ORDER BY version DESC LIMIT ?", (tenant, dataset, n))
        for r in rows:
            r["profile"] = loads(r["profile"], {})
        return rows

    def previous_good(self, tenant: str, dataset: str, before_version: int) -> dict | None:
        return self.svc.db.query_one("SELECT * FROM dataset_versions WHERE tenant=? AND dataset=? AND version < ? "
                                     "AND status IN ('superseded','published_override','published') ORDER BY version DESC LIMIT 1",
                                     (tenant, dataset, before_version))

    def stage(self, *, tenant: str, dataset: str, batch: pd.DataFrame, contract: dict | None, run_id: str, file_id: str | None,
              content_hash: str, profile: dict, quarantined: bool, note: str = "") -> dict:
        version = self.next_version(tenant, dataset)
        table = Warehouse.table_name(tenant, dataset, version, quarantined=quarantined)
        full = batch
        if contract and contract.get("load_mode") == "append" and not quarantined:
            cur = self.current(tenant, dataset)
            prev = self.wh.read(cur["table_name"]) if cur else None
            if prev is not None and not prev.empty:
                part = contract.get("partition_column")
                if part and part in batch.columns and part in prev.columns:
                    prev = prev[~prev[part].astype(str).isin(batch[part].astype(str).unique())]
                full = pd.concat([prev.astype(object), batch.astype(object)], ignore_index=True)
                pk = [c for c in contract.get("primary_key", []) if c in full.columns]
                if pk:
                    full = full.drop_duplicates(subset=pk, keep="last")
        self.wh.write_table(table, full)
        self.wh.snapshot(table)
        vid = new_id("dsv")
        row = {"id": vid, "tenant": tenant, "dataset": dataset, "version": version, "run_id": run_id, "file_id": file_id,
               "content_hash": content_hash, "batch_rows": len(batch), "row_count": len(full), "table_name": table,
               "schema": dumps([{"name": c, "dtype": str(batch[c].dtype) if c in batch.columns else "object"} for c in full.columns]),
               "profile": dumps(profile), "status": "quarantined" if quarantined else "staged",
               "contract_version": contract.get("version") if contract else None, "checksum": self.wh.checksum(table),
               "warnings": 0, "created_at": iso(), "published_at": None, "note": note}
        self.svc.db.insert("dataset_versions", row)
        return row

    def _set_state(self, tenant: str, dataset: str, version_id: str | None, fresh: bool) -> None:
        now = self.svc.clock.now_iso()
        self.svc.db.execute(
            "INSERT INTO dataset_state(tenant, dataset, published_version_id, last_success_at, last_arrival_at) VALUES(?,?,?,?,?) "
            "ON CONFLICT(tenant, dataset) DO UPDATE SET published_version_id=excluded.published_version_id, "
            "last_success_at=CASE WHEN ? THEN excluded.last_success_at ELSE dataset_state.last_success_at END, "
            "freshness_alerted_at=CASE WHEN ? THEN NULL ELSE dataset_state.freshness_alerted_at END",
            (tenant, dataset, version_id, now if fresh else None, now, 1 if fresh else 0, 1 if fresh else 0))

    def publish(self, version_id: str, by: str, status: str = "published", fresh: bool = True) -> dict:
        v = self.get(version_id)
        cur = self.current(v["tenant"], v["dataset"])
        self.wh.swap_view(v["tenant"], v["dataset"], v["table_name"])
        with self.svc.db.tx():
            if cur and cur["id"] != version_id:
                self.svc.db.update("dataset_versions", {"id": cur["id"]}, {"status": "superseded"})
            self.svc.db.update("dataset_versions", {"id": version_id}, {"status": status, "published_at": iso()})
        self._set_state(v["tenant"], v["dataset"], version_id, fresh)
        self.svc.audit.record(by, "dataset.publish", f"{v['tenant']}/{v['dataset']}@v{v['version']}", status,
                              {"version_id": version_id, "previous": cur["id"] if cur else None, "table": v["table_name"]})
        self.svc.events.publish("dataset.published", {"tenant": v["tenant"], "dataset": v["dataset"], "version_id": version_id,
                                                      "version": v["version"]}, v["tenant"])
        self.svc.metrics.inc("dataset_publish_total", dataset=v["dataset"], status=status)
        return {"version_id": version_id, "version": v["version"], "previous_version_id": cur["id"] if cur else None}

    def unpublish(self, version_id: str, by: str, new_status: str, reason: str) -> dict:
        """Take a version out of service; consumers fall back to the previous good version."""
        v = self.get(version_id)
        cur = self.current(v["tenant"], v["dataset"])
        restored = None
        if cur and cur["id"] == version_id:
            prev = self.previous_good(v["tenant"], v["dataset"], v["version"])
            if prev:
                self.wh.swap_view(v["tenant"], v["dataset"], prev["table_name"])
                self.svc.db.update("dataset_versions", {"id": prev["id"]}, {"status": "published"})
                self._set_state(v["tenant"], v["dataset"], prev["id"], fresh=False)
                restored = prev["id"]
            else:
                self.wh.drop_view(v["tenant"], v["dataset"])
                self.svc.db.execute("UPDATE dataset_state SET published_version_id=NULL WHERE tenant=? AND dataset=?",
                                    (v["tenant"], v["dataset"]))
        self.svc.db.update("dataset_versions", {"id": version_id}, {"status": new_status, "note": reason[:300]})
        self.svc.audit.record(by, "dataset.unpublish", f"{v['tenant']}/{v['dataset']}@v{v['version']}", new_status,
                              {"version_id": version_id, "restored_version_id": restored, "reason": reason})
        if restored:
            self.svc.events.publish("dataset.published", {"tenant": v["tenant"], "dataset": v["dataset"], "version_id": restored,
                                                          "rollback": True}, v["tenant"])
        return {"version_id": version_id, "status": new_status, "restored_version_id": restored}

    def restore_from_snapshot(self, version_id: str, by: str) -> dict:
        v = self.get(version_id)
        before = self.wh.checksum(v["table_name"])
        if before == v["checksum"]:
            return {"restored": False, "reason": "checksum already matches the recorded value"}
        if not self.wh.restore(v["table_name"]):
            return {"restored": False, "reason": "no snapshot available"}
        after = self.wh.checksum(v["table_name"])
        self.svc.audit.record(by, "dataset.restore_snapshot", v["table_name"], "restored" if after == v["checksum"] else "mismatch",
                              {"before": before, "after": after, "recorded": v["checksum"]})
        return {"restored": after == v["checksum"], "before": before, "after": after, "recorded": v["checksum"]}

    def force_publish(self, version_id: str, by: str, justification: str) -> dict:
        v = self.get(version_id)
        if v["status"] != "quarantined":
            raise ValueError(f"version {version_id} is {v['status']}, not quarantined")
        live_table = Warehouse.table_name(v["tenant"], v["dataset"], v["version"], quarantined=False)
        self.wh.copy_table(v["table_name"], live_table)
        self.wh.snapshot(live_table)
        self.svc.db.update("dataset_versions", {"id": version_id}, {"table_name": live_table, "checksum": self.wh.checksum(live_table),
                                                                    "note": f"FORCE PUBLISHED by {by}: {justification}"[:300]})
        return self.publish(version_id, by, status="published_override")
