"""The watched-folder trigger (the mock data source).

Polling (not OS notifications) on purpose: it works the same on local disks, USB drives and network
shares, where change notifications are unreliable. A file is admitted only after its size and
mtime are stable for N polls and it can be moved (i.e. the copy has finished). Admission is
backpressured: when too much work is queued, new files wait in the inbox."""
from __future__ import annotations

import fnmatch
import re
import shutil
import threading
from pathlib import Path

from swarmpipe.core.util import iso, new_id, sha256_file
from swarmpipe.observability.logging import get_logger

log = get_logger("watcher")
_TENANT_RE = re.compile(r"^[a-z][a-z0-9_-]{0,30}$")


class FolderWatcher:
    def __init__(self, svc):
        self.svc = svc
        self._cand: dict[str, tuple[int, float, int]] = {}
        self._lock = threading.Lock()

    @property
    def inbox(self) -> Path:
        return self.svc.settings.inbox

    def tenant_for(self, path: Path) -> str | None:
        rel = path.relative_to(self.inbox)
        if len(rel.parts) == 1:
            return self.svc.settings.default_tenant
        t = rel.parts[0].lower()
        return t if _TENANT_RE.match(t) else None

    def _ignored(self, p: Path) -> bool:
        return any(fnmatch.fnmatch(p.name, g) for g in self.svc.settings.watcher.ignore_globs)

    def poll_once(self) -> int:
        w = self.svc.settings.watcher
        if not self.inbox.exists():
            self.inbox.mkdir(parents=True, exist_ok=True)
        depth = self.svc.engine.queue_depth()
        self.svc.metrics.gauge("queue_depth", depth)
        if depth >= w.backpressure_high_watermark:
            self.svc.metrics.gauge("watcher_backpressure", 1)
            return 0
        present, ready = set(), []
        with self._lock:
            for p in list(self.inbox.glob("*")) + list(self.inbox.glob("*/*")):
                if not p.is_file() or self._ignored(p):
                    continue
                key = str(p)
                present.add(key)
                try:
                    st = p.stat()
                except OSError:
                    continue
                prev = self._cand.get(key)
                stable = prev[2] + 1 if prev and prev[0] == st.st_size and prev[1] == st.st_mtime else 0
                self._cand[key] = (st.st_size, st.st_mtime, stable)
                if stable >= w.stability_polls:
                    ready.append((st.st_mtime, p))
            for k in list(self._cand):
                if k not in present:
                    self._cand.pop(k, None)
        admitted = 0
        for _, p in sorted(ready):
            if self.svc.engine.queue_depth() >= w.backpressure_high_watermark:
                break
            if self.admit(p):
                admitted += 1
                with self._lock:
                    self._cand.pop(str(p), None)
        self.svc.metrics.gauge("watcher_backpressure", 0)
        return admitted

    def admit(self, p: Path) -> bool:
        svc = self.svc
        w = svc.settings.watcher
        tenant = self.tenant_for(p)
        if tenant is None:
            log.warning("ignoring %s: sub-folder is not a valid tenant name", p)
            return False
        fid = new_id("file")
        reason = None
        if p.suffix.lower() not in [e.lower() for e in w.extensions]:
            reason = "unsupported_extension"
        elif p.stat().st_size > w.max_file_mb * 1024 * 1024:
            reason = "too_large"
        try:
            digest = sha256_file(p)
        except OSError:
            return False
        if reason:
            dest_dir = svc.settings.data_path("dlq", tenant)
            dest_dir.mkdir(parents=True, exist_ok=True)
            try:
                dest = Path(shutil.move(str(p), str(dest_dir / f"{fid}__{p.name}")))
            except OSError:
                return False
            svc.db.insert("files", {"id": fid, "tenant": tenant, "original_name": p.name, "original_path": str(p), "staged_path": None,
                                    "size": dest.stat().st_size, "sha256": digest, "detected_at": iso(), "status": "dead_lettered",
                                    "run_id": None, "duplicate_of": None, "final_path": str(dest)})
            svc.db.insert("dlq", {"id": new_id("dlq"), "run_id": None, "file_id": fid, "tenant": tenant, "reason": reason,
                                  "error": f"rejected at intake: {reason}", "path": str(dest), "created_at": iso(), "redriven_at": None})
            svc.signals.raise_signal(tenant, "pipeline_failure", None, "info", f"{p.name} rejected at intake ({reason})", {"file": p.name})
            svc.metrics.inc("ingest_files_total", outcome="dead_lettered", tenant=tenant)
            return True
        staged_dir = svc.settings.data_path("processing", fid)
        staged_dir.mkdir(parents=True, exist_ok=True)
        try:
            staged = Path(shutil.move(str(p), str(staged_dir / p.name)))
        except OSError as exc:
            log.info("file %s not movable yet (%s); will retry", p.name, exc)
            return False
        svc.db.insert("files", {"id": fid, "tenant": tenant, "original_name": p.name, "original_path": str(p), "staged_path": str(staged),
                                "size": staged.stat().st_size, "sha256": digest, "detected_at": iso(), "status": "staged",
                                "run_id": None, "duplicate_of": None, "final_path": None})
        run = svc.engine.submit("ingest_file", {"file_id": fid}, tenant, file_id=fid, priority=5)
        svc.db.update("files", {"id": fid}, {"run_id": run})
        svc.metrics.inc("files_detected_total", tenant=tenant, ext=p.suffix.lower())
        svc.audit.record("system:watcher", "file.admitted", p.name, "staged", {"file_id": fid, "tenant": tenant, "sha256": digest[:16], "run_id": run})
        log.info("admitted %s (tenant %s) -> run %s", p.name, tenant, run)
        return True

    def loop(self, stop: threading.Event) -> None:
        while not stop.is_set():
            try:
                self.poll_once()
            except Exception:  # noqa: BLE001
                log.exception("watcher poll failed")
            stop.wait(self.svc.settings.watcher.poll_interval_s)
