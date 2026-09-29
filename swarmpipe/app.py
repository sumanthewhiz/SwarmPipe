"""Composition root: builds every service once (dependency injection by hand, no globals) and runs
the background runtime (watcher, workers, event dispatcher, scheduler).

Control plane vs data plane: registry, identity, policy, model/tool gateways, evals and
audit are control-plane services; the engine, workers, warehouse and knowledge base are the data
plane. Everything shares one state DB here; in a real deployment each would be its own service."""
from __future__ import annotations

import threading

from swarmpipe.agents.messaging import MessageBus
from swarmpipe.agents.registry import AgentRegistry
from swarmpipe.config import Settings, load_settings
from swarmpipe.core.db import Database
from swarmpipe.core.events import Dispatcher, EventBus
from swarmpipe.core.util import Clock, RuntimeFlags
from swarmpipe.data.contracts import ContractStore
from swarmpipe.data.knowledge import KnowledgeBase
from swarmpipe.data.lineage import ContextGraph, LineageService
from swarmpipe.data.pii import PiiVault
from swarmpipe.data.publishing import PublishingService
from swarmpipe.data.warehouse import Warehouse
from swarmpipe.governance.approvals import ApprovalService
from swarmpipe.governance.audit import AuditLog
from swarmpipe.governance.autonomy import AutonomyManager
from swarmpipe.governance.evidence import EvidenceService
from swarmpipe.governance.identity import IdentityService, SecretsBroker
from swarmpipe.governance.killswitch import KillSwitch
from swarmpipe.governance.policy import PolicyEngine
from swarmpipe.llm.gateway import ModelGateway
from swarmpipe.llm.prompts import PromptRegistry
from swarmpipe.memory import MemoryStore
from swarmpipe.observability.logging import get_logger, release_log_dir, setup_logging
from swarmpipe.observability.metrics import Metrics, SLOService
from swarmpipe.observability.tracing import Tracer
from swarmpipe.runtime import workflows
from swarmpipe.runtime.engine import Engine
from swarmpipe.runtime.scheduler import Scheduler
from swarmpipe.runtime.watcher import FolderWatcher
from swarmpipe.signals import IncidentService, Notifier, SignalService
from swarmpipe.tools.actions import action_tools
from swarmpipe.tools.catalog import READ_TOOLS
from swarmpipe.tools.gateway import ToolGateway

log = get_logger("app")


class Services:
    def __init__(self, settings: Settings, console_logs: bool = True, log_level: str = "INFO"):
        settings.ensure_dirs()
        setup_logging(settings.data_path("logs"), level=log_level, console=console_logs)
        self.settings = settings
        self.db = Database(settings.state_db)
        self.db.migrate()
        self.wh = Warehouse(settings.warehouse_db)
        self.flags = RuntimeFlags(self.db)
        self.clock = Clock(self.flags)
        self.tracer = Tracer(self.db, settings)
        self.metrics = Metrics(self.db)
        self.slos = SLOService(settings, self.metrics)
        self.audit = AuditLog(self.db)
        self.events = EventBus(self.db)
        self.dispatcher = Dispatcher(self.events)
        self.secrets = SecretsBroker(settings)
        self.identity = IdentityService(settings, self.secrets)
        self.killswitch = KillSwitch(self.db, self.audit)
        self.contracts = ContractStore(self)
        self.lineage = LineageService(self)
        self.context = ContextGraph(self)
        self.knowledge = KnowledgeBase(self)
        self.memory = MemoryStore(self)
        self.pii = PiiVault(self.db, self.secrets.derive_key("pii-tokens"))
        self.publishing = PublishingService(self)
        self.prompts = PromptRegistry(settings)
        self.llm = ModelGateway(self)
        self.tools = ToolGateway(self)
        for spec in READ_TOOLS + action_tools():
            self.tools.register(spec)
        self.policy = PolicyEngine(self)
        self.autonomy = AutonomyManager(self)
        self.approvals = ApprovalService(self)
        self.evidence = EvidenceService(self)
        self.signals = SignalService(self)
        self.incidents = IncidentService(self)
        self.notifier = Notifier(self)
        self.bus = MessageBus(self)
        self.agents = AgentRegistry(self)
        self.engine = Engine(self)
        workflows.register(self)
        self.scheduler = Scheduler(self)
        self.watcher = FolderWatcher(self)

    def bootstrap(self) -> dict:
        if not self.prompts.lock_path.exists():
            self.prompts.write_lock()
        out = {"contracts_imported": self.contracts.sync_from_files(), "static_lineage_edges": self.lineage.seed_static_edges(),
               "knowledge_docs_added": self.knowledge.seed_from_dir(), "agents_registered": self.agents.register_all(),
               "unapproved_prompts": self.prompts.unapproved()}
        for consumer in ("correlator", "approvals", "derive-trigger", "hold-release"):
            if self.db.query_one("SELECT 1 FROM event_offsets WHERE consumer=?", (consumer,)) is None:
                self.events.ack(consumer, 0)
        return out

    def close(self) -> None:
        """Release every OS handle this instance holds (DB connections on all threads, the log file) so the
        data folder can be moved or deleted - on Windows open handles block both."""
        try:
            self.metrics.flush()
        except Exception:  # noqa: BLE001
            log.debug("metrics flush on close failed", exc_info=True)
        self.db.close_all()
        self.wh.close_all()
        release_log_dir(self.settings.data_path("logs"))


def build_services(root=None, overrides: dict | None = None, console_logs: bool = True, log_level: str = "INFO") -> Services:
    svc = Services(load_settings(root, overrides), console_logs=console_logs, log_level=log_level)
    svc.bootstrap_result = svc.bootstrap()
    return svc


class Runtime:
    """Background threads: 1 watcher, N workers, 1 event dispatcher, 1 scheduler."""

    def __init__(self, svc: Services, workers: int | None = None, watch: bool = True):
        self.svc = svc
        self.n_workers = workers or svc.settings.engine.workers
        self.watch = watch
        self.threads: list[threading.Thread] = []
        self.stop_event = svc.engine.stop_event

    def _dispatch_loop(self):
        while not self.stop_event.is_set():
            try:
                if self.svc.dispatcher.poll_once() == 0:
                    self.stop_event.wait(0.2)
            except Exception:  # noqa: BLE001
                log.exception("dispatcher error")
                self.stop_event.wait(1.0)

    def start(self) -> "Runtime":
        specs = [("dispatcher", self._dispatch_loop), ("scheduler", lambda: self.svc.scheduler.loop(self.stop_event))]
        if self.watch:
            specs.append(("watcher", lambda: self.svc.watcher.loop(self.stop_event)))
        for i in range(self.n_workers):
            specs.append((f"worker-{i + 1}", lambda i=i: self.svc.engine.worker_loop(f"worker-{i + 1}")))
        for name, target in specs:
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self.threads.append(t)
        log.info("runtime started: %d workers, watching %s", self.n_workers, self.svc.settings.inbox)
        return self

    def stop(self, timeout: float = 10.0) -> None:
        self.stop_event.set()
        for t in self.threads:
            t.join(timeout / max(1, len(self.threads)))
        self.svc.metrics.flush()
        log.info("runtime stopped")
