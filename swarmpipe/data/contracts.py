"""Data contracts as code, versioned in the state DB.

YAML files in config/contracts/ are the reviewed source of truth ("GitOps"): at startup any file
whose version is newer than the DB's latest is imported as the active version. Runtime changes
proposed by agents create `proposed` versions that only become active through a human approval,
and can be reverted (the compensation for the `update_contract` action)."""
from __future__ import annotations

import copy
import fnmatch
import re
from pathlib import Path

import yaml

from swarmpipe.core.util import dumps, iso, loads

DATASET_RE = re.compile(r"^[a-z][a-z0-9_]{1,40}$")


def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


class ContractStore:
    def __init__(self, svc):
        self.svc = svc

    # ---- sync / read -----------------------------------------------------------------------
    def sync_from_files(self) -> list[str]:
        imported = []
        cdir = self.svc.settings.resolve(self.svc.settings.paths.contracts_dir)
        for path in sorted(Path(cdir).glob("*.y*ml")):
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            ds, ver = data.get("dataset"), int(data.get("version", 1))
            if not ds or not DATASET_RE.match(ds):
                continue
            latest = self.svc.db.scalar("SELECT MAX(version) FROM contract_versions WHERE dataset=?", (ds,), default=0)
            if ver > latest:
                with self.svc.db.tx():
                    self.svc.db.execute("UPDATE contract_versions SET status='superseded' WHERE dataset=? AND status='active'", (ds,))
                    self.svc.db.insert("contract_versions", {"dataset": ds, "version": ver, "contract": dumps(data), "status": "active",
                                                             "created_at": iso(), "created_by": f"file:{path.name}",
                                                             "approved_by": "git-review", "change_note": "imported from contracts-as-code"})
                imported.append(f"{ds}@v{ver}")
        return imported

    def _row_to_contract(self, row: dict | None) -> dict | None:
        if not row:
            return None
        c = loads(row["contract"], {})
        c["version"] = row["version"]
        c["_status"] = row["status"]
        return c

    def active(self, dataset: str) -> dict | None:
        row = self.svc.db.query_one("SELECT * FROM contract_versions WHERE dataset=? AND status='active' ORDER BY version DESC LIMIT 1", (dataset,))
        return self._row_to_contract(row)

    def get(self, dataset: str, version: int) -> dict | None:
        return self._row_to_contract(self.svc.db.query_one("SELECT * FROM contract_versions WHERE dataset=? AND version=?", (dataset, version)))

    def versions(self, dataset: str) -> list[dict]:
        rows = self.svc.db.query("SELECT dataset, version, status, created_at, created_by, approved_by, change_note FROM contract_versions "
                                 "WHERE dataset=? ORDER BY version DESC", (dataset,))
        return rows

    def all_active(self) -> list[dict]:
        rows = self.svc.db.query("SELECT * FROM contract_versions WHERE status='active' ORDER BY dataset")
        return [self._row_to_contract(r) for r in rows]

    # ---- matching / aliases -------------------------------------------------------------------
    def match(self, filename: str, sheet: str | None, sheet_index: int = 0, n_sheets: int = 1) -> dict | None:
        """Map a file (and sheet) to a contract. Multi-sheet workbooks need an exact `sheet` match
        (or a sheet-less contract for the first sheet); single-sheet files take the best candidate."""
        name = filename.lower()
        best, best_score = None, -1
        for c in self.all_active():
            if not any(fnmatch.fnmatch(name, p.lower()) for p in c.get("match", [])):
                continue
            want = str(c.get("sheet") or "").lower()
            if want and sheet and sheet.lower() == want:
                score = 3
            elif not want:
                score = 2
            elif c["dataset"] in name:
                score = 1
            else:
                score = 0
            if n_sheets > 1 and not (score == 3 or (score == 2 and sheet_index == 0)):
                continue
            if score > best_score:
                best, best_score = c, score
        return best

    def alias_map(self, contract: dict) -> dict[str, str]:
        """normalized alias -> contract column, from the contract and the business glossary."""
        out: dict[str, str] = {}
        terms = self.svc.settings.glossary.get("terms", {})
        for col in contract.get("columns", []):
            name = col["name"]
            for a in col.get("aliases", []) + terms.get(name, {}).get("aliases", []):
                out.setdefault(_norm(a), name)
        return out

    # ---- lifecycle --------------------------------------------------------------------------
    def propose(self, dataset: str, contract: dict, by: str, note: str) -> int:
        if not DATASET_RE.match(dataset):
            raise ValueError(f"invalid dataset name {dataset!r}")
        body = copy.deepcopy(contract)
        body.pop("_status", None)
        body["dataset"] = dataset
        latest = self.svc.db.scalar("SELECT MAX(version) FROM contract_versions WHERE dataset=?", (dataset,), default=0)
        version = latest + 1
        body["version"] = version
        self.svc.db.insert("contract_versions", {"dataset": dataset, "version": version, "contract": dumps(body), "status": "proposed",
                                                 "created_at": iso(), "created_by": by, "approved_by": None, "change_note": note})
        self.svc.audit.record(by, "contract.propose", f"{dataset}@v{version}", "proposed", {"note": note})
        return version

    def activate(self, dataset: str, version: int, by: str) -> None:
        with self.svc.db.tx():
            self.svc.db.execute("UPDATE contract_versions SET status='superseded' WHERE dataset=? AND status='active'", (dataset,))
            self.svc.db.execute("UPDATE contract_versions SET status='active', approved_by=? WHERE dataset=? AND version=?", (by, dataset, version))
        self.svc.audit.record(by, "contract.activate", f"{dataset}@v{version}", "active")
        self.svc.lineage.seed_static_edges()

    def reject(self, dataset: str, version: int, by: str) -> None:
        self.svc.db.execute("UPDATE contract_versions SET status='rejected', approved_by=? WHERE dataset=? AND version=?", (by, dataset, version))
        self.svc.audit.record(by, "contract.reject", f"{dataset}@v{version}", "rejected")

    def revert_to(self, dataset: str, version: int, by: str) -> None:
        self.activate(dataset, version, by)

    def export_yaml(self, dataset: str) -> str:
        c = self.active(dataset) or {}
        c.pop("_status", None)
        return yaml.safe_dump(c, sort_keys=False, allow_unicode=True)
