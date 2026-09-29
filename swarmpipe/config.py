"""Configuration loading: YAML files + env overrides + programmatic overrides (used by evals/tests).

Env override convention: SWARMPIPE__<SECTION>__<KEY>=<yaml value>, e.g.
    SWARMPIPE__LLM__ACTIVE_PROFILE=ollama
    SWARMPIPE__PATHS__INBOX=D:/DropZone
"""
from __future__ import annotations

import copy
import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class _Cfg(BaseModel):
    model_config = ConfigDict(extra="allow")


class PathsCfg(_Cfg):
    inbox: str = "data/inbox"
    data_dir: str = "data"
    contracts_dir: str = "config/contracts"
    prompts_dir: str = "prompts"
    knowledge_dir: str = "knowledge"
    evals_dir: str = "evals"


class WatcherCfg(_Cfg):
    poll_interval_s: float = 1.0
    stability_polls: int = 2
    extensions: list[str] = [".csv", ".tsv", ".txt", ".xlsx", ".xls"]
    ignore_globs: list[str] = ["~$*", "*.tmp", "*.partial", "*.crdownload", ".*"]
    max_file_mb: float = 50
    backpressure_high_watermark: int = 40


class EngineCfg(_Cfg):
    workers: int = 4
    lease_s: float = 30
    heartbeat_s: float = 5
    default_max_attempts: int = 3
    backoff_base_s: float = 1.0
    backoff_max_s: float = 20.0
    step_timeout_s: float = 240
    llm_concurrency: int = 2
    triage_debounce_s: float = 3
    idle_sleep_s: float = 0.25


class LlmCfg(_Cfg):
    active_profile: str = "offline"
    request_timeout_s: float = 120
    max_retries: int = 2
    max_repairs: int = 2
    cache_enabled: bool = True
    cache_ttl_s: float = 86400
    breaker_failure_threshold: int = 3
    breaker_cooldown_s: float = 30
    require_certification: bool = False
    providers: dict[str, dict] = Field(default_factory=lambda: {"mock": {"type": "mock"}})
    models: dict[str, dict] = Field(default_factory=dict)
    profiles: dict[str, dict[str, list[str]]] = Field(default_factory=dict)


class GuardrailsCfg(_Cfg):
    spotlighting: bool = True
    injection_detection: bool = True
    pii_redaction_for_llm: bool = True
    secret_leak_check: bool = True
    max_untrusted_chars: int = 3500
    egress_allowlist: list[str] = ["localhost", "127.0.0.1"]


class BudgetsCfg(_Cfg):
    per_run_usd: float = 0.25
    per_run_tokens: int = 150000
    agent_max_steps: int = 6


class IncidentsCfg(_Cfg):
    min_severity: str = "warning"
    correlation_window_min: float = 30
    diagnosis_min_confidence: float = 0.6


class TracingCfg(_Cfg):
    sample_rate: float = 1.0
    always_keep_errors: bool = True
    export_jsonl: bool = True


class Settings(_Cfg):
    app: dict = Field(default_factory=lambda: {"name": "SwarmPipe", "default_tenant": "default"})
    paths: PathsCfg = Field(default_factory=PathsCfg)
    watcher: WatcherCfg = Field(default_factory=WatcherCfg)
    engine: EngineCfg = Field(default_factory=EngineCfg)
    llm: LlmCfg = Field(default_factory=LlmCfg)
    prompts: dict = Field(default_factory=lambda: {"pins": {}, "enforce_lock": True})
    guardrails: GuardrailsCfg = Field(default_factory=GuardrailsCfg)
    budgets: BudgetsCfg = Field(default_factory=BudgetsCfg)
    incidents: IncidentsCfg = Field(default_factory=IncidentsCfg)
    tenants: dict[str, dict] = Field(default_factory=lambda: {"default": {}})
    users: dict[str, dict] = Field(default_factory=dict)
    tracing: TracingCfg = Field(default_factory=TracingCfg)
    retention: dict = Field(default_factory=lambda: {"telemetry_days": 7, "metrics_days": 14})
    slos: list[dict] = Field(default_factory=list)
    features: dict = Field(default_factory=dict)
    mcp: dict = Field(default_factory=lambda: {"act_as_user": "oncall"})
    web: dict = Field(default_factory=lambda: {"host": "127.0.0.1", "port": 8765})

    # Populated by load_settings (not from swarmpipe.yaml)
    root: str = str(PROJECT_ROOT)
    policies: dict = Field(default_factory=dict)
    consumers: dict = Field(default_factory=dict)
    glossary: dict = Field(default_factory=dict)

    # ---- path helpers -------------------------------------------------------------------
    def resolve(self, p: str | Path) -> Path:
        p = Path(p)
        return p if p.is_absolute() else (Path(self.root) / p).resolve()

    @property
    def data_dir(self) -> Path:
        return self.resolve(self.paths.data_dir)

    @property
    def inbox(self) -> Path:
        return self.resolve(self.paths.inbox)

    def data_path(self, *parts: str) -> Path:
        return self.data_dir.joinpath(*parts)

    @property
    def state_db(self) -> Path:
        return self.data_path("state", "swarmpipe.db")

    @property
    def warehouse_db(self) -> Path:
        return self.data_path("state", "warehouse.db")

    @property
    def default_tenant(self) -> str:
        return self.app.get("default_tenant", "default")

    def ensure_dirs(self) -> None:
        for sub in ("state", "processing", "archive", "quarantine", "dlq", "outbox", "exports", "logs", "traces"):
            self.data_path(sub).mkdir(parents=True, exist_ok=True)
        self.inbox.mkdir(parents=True, exist_ok=True)


def _deep_merge(base: dict, extra: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in (extra or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def _env_overrides(prefix: str = "SWARMPIPE__") -> dict:
    out: dict[str, Any] = {}
    for key, raw in os.environ.items():
        if not key.startswith(prefix):
            continue
        parts = [p.lower() for p in key[len(prefix):].split("__") if p]
        if not parts:
            continue
        try:
            value = yaml.safe_load(raw)
        except yaml.YAMLError:
            value = raw
        cur = out
        for p in parts[:-1]:
            cur = cur.setdefault(p, {})
        cur[parts[-1]] = value
    return out


def _read_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_settings(root: str | Path | None = None, overrides: dict | None = None,
                  config_file: str | Path | None = None) -> Settings:
    root_path = Path(root or os.environ.get("SWARMPIPE_HOME") or PROJECT_ROOT).resolve()
    cfg_path = Path(config_file) if config_file else root_path / "config" / "swarmpipe.yaml"
    raw = _read_yaml(cfg_path)
    raw = _deep_merge(raw, _env_overrides())
    raw = _deep_merge(raw, overrides or {})
    raw["root"] = str(root_path)
    raw["policies"] = _read_yaml(root_path / "config" / "policies.yaml")
    raw["consumers"] = _read_yaml(root_path / "config" / "consumers.yaml")
    raw["glossary"] = _read_yaml(root_path / "config" / "glossary.yaml")
    return Settings.model_validate(raw)
