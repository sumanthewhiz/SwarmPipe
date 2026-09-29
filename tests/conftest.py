import shutil
import tempfile
from pathlib import Path

import pytest

from swarmpipe.app import Services
from swarmpipe.config import PROJECT_ROOT, load_settings
from swarmpipe.core.util import remove_tree

FAST = {"watcher": {"stability_polls": 0}, "engine": {"triage_debounce_s": 0.5, "backoff_base_s": 0.02, "backoff_max_s": 0.1, "workers": 1},
        "tracing": {"export_jsonl": False}}


def make_svc(tmp: Path, extra: dict | None = None) -> Services:
    ov = {**FAST, "paths": {"data_dir": str(tmp / "data"), "inbox": str(tmp / "inbox")}}
    for k, v in (extra or {}).items():
        ov[k] = {**ov.get(k, {}), **v} if isinstance(v, dict) else v
    svc = Services(load_settings(PROJECT_ROOT, ov), console_logs=False, log_level="ERROR")
    svc.bootstrap()
    return svc


def drive(svc: Services, *scenario_names: str, scheduler: bool = False, tenant: str | None = None) -> None:
    from swarmpipe import scenarios

    for name in scenario_names:
        scenarios.drop(name, svc.settings.inbox, tenant, svc=svc)
        for _ in range(4):
            if svc.watcher.poll_once() == 0:
                break
        svc.engine.run_until_quiescent(timeout_s=180, scheduler=svc.scheduler if scheduler else None)
        if scheduler:
            svc.scheduler.tick(force=True)
            svc.engine.run_until_quiescent(timeout_s=180)


def approve_all(svc: Services) -> None:
    admin = svc.identity.user("admin")
    for _ in range(4):
        pend = svc.approvals.list("pending")
        if not pend:
            return
        for a in pend:
            svc.approvals.decide(a["id"], "approved", admin, comment="approved by the test suite", confirm_text=a.get("requires_confirmation"))
        svc.engine.run_until_quiescent(timeout_s=180)


@pytest.fixture
def svc(tmp_path):
    s = make_svc(tmp_path)
    yield s
    s.close()


@pytest.fixture(scope="module")
def baseline_dir():
    d = Path(tempfile.mkdtemp(prefix="swarmpipe_test_base_"))
    s = make_svc(d)
    drive(s, "reference", "history")
    s.close()
    yield d
    remove_tree(d)


@pytest.fixture
def base_svc(baseline_dir, tmp_path):
    shutil.copytree(baseline_dir / "data", tmp_path / "data")
    s = make_svc(tmp_path)
    yield s
    s.close()
