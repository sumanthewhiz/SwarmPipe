"""Agent registry & catalog (inventory: every agent, tool and model is registered, versioned and
owned). Cards are A2A-style Agent Cards; the card hash detects silent changes."""
from __future__ import annotations

from swarmpipe.agents.pipeline_agents import (ContractStewardAgent, CriticAgent, DataAssuranceAgent, PrivacyGuardAgent,
                                              ProfilerAgent, PublisherAgent, RouterAgent, TransformerAgent)
from swarmpipe.agents.service_agents import AnalystAgent, JudgeAgent, LibrarianAgent
from swarmpipe.agents.triage_agents import (SPECIALIST_TOOLS, CorrelatorAgent, ExecutorAgent, ImpactAnalyzerAgent, InvestigatorAgent,
                                            LearnerAgent, RemediationPlannerAgent, TriageSupervisorAgent, VerifierAgent)
from swarmpipe.core.util import dumps, iso


class AgentRegistry:
    def __init__(self, svc):
        self.svc = svc
        self.router = RouterAgent(svc)
        self.privacy = PrivacyGuardAgent(svc)
        self.profiler = ProfilerAgent(svc)
        self.steward = ContractStewardAgent(svc)
        self.critic = CriticAgent(svc)
        self.transformer = TransformerAgent(svc)
        self.assurance = DataAssuranceAgent(svc)
        self.publisher = PublisherAgent(svc)
        self.correlator = CorrelatorAgent(svc)
        self.supervisor = TriageSupervisorAgent(svc)
        self.investigators = {sp: InvestigatorAgent(svc, sp) for sp in SPECIALIST_TOOLS}
        self.impact = ImpactAnalyzerAgent(svc)
        self.planner = RemediationPlannerAgent(svc)
        self.executor = ExecutorAgent(svc)
        self.verifier = VerifierAgent(svc)
        self.learner = LearnerAgent(svc)
        self.analyst = AnalystAgent(svc)
        self.librarian = LibrarianAgent(svc)
        self.judge = JudgeAgent(svc)

    def all(self) -> list:
        return [self.router, self.privacy, self.profiler, self.steward, self.critic, self.transformer, self.assurance, self.publisher,
                self.correlator, self.supervisor, *self.investigators.values(), self.impact, self.planner, self.executor,
                self.verifier, self.learner, self.analyst, self.librarian, self.judge]

    def get(self, agent_id: str):
        return next((a for a in self.all() if a.id == agent_id), None)

    def register_all(self) -> int:
        n = 0
        for a in self.all():
            row = self.svc.db.query_one("SELECT card_hash FROM agent_registry WHERE agent_id=? AND version=?", (a.id, a.version))
            h = a.card_hash()
            if row and row["card_hash"] == h:
                continue
            self.svc.db.execute(
                "INSERT INTO agent_registry(agent_id, version, status, owner, card, card_hash, registered_at) VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(agent_id, version) DO UPDATE SET card=excluded.card, card_hash=excluded.card_hash, registered_at=excluded.registered_at",
                (a.id, a.version, "active", a.owner, dumps(a.card()), h, iso()))
            if row:
                self.svc.audit.record("system:registry", "agent.card_changed", a.id, "updated", {"from": row["card_hash"], "to": h})
            n += 1
        return n

    def cards(self) -> list[dict]:
        out = []
        for a in self.all():
            c = a.card()
            ks = self.svc.killswitch.engaged(agent=a.id)
            c["x-swarmpipe"]["status"] = "suspended" if ks else "active"
            c["x-swarmpipe"]["card_hash"] = a.card_hash()
            if a.role:
                c["x-swarmpipe"]["model_route"] = self.svc.llm.route(a.role)
            out.append(c)
        return out
