# SwarmPipe Learning Guide

This guide maps each core concept of production multi-agent systems (Parts A-P) to the code that implements
it, a way to **see** it, and a way to **break** it. A final part (Q) covers production details that are easy
to miss but that you will meet in any real system.

How to read each row: **Where** is a file (and the function to open). **See** is a CLI command, dashboard tab
or lab. **Break** is an experiment that makes the concept visible by removing or stressing it.

Conventions: `sp` = `python -m swarmpipe`. Labs are in [`LABS.md`](LABS.md). Line numbers are approximate
anchors - search for the function name if the file has moved on.

---

## 0. The 60-second mental model

```
file lands in data/inbox
  -> Watcher (stable? tenant? backpressure?)                         runtime/watcher.py
  -> ingest_file run (durable)                                       runtime/workflows.py f_*
       Router agent decides tabular / document / unsupported         agents/pipeline_agents.py RouterAgent
       read -> one child ingest_dataset run per sheet (fan-out)
  -> ingest_dataset run: privacy -> profile -> contract check (+critic) -> transform -> checks -> publish|quarantine
  -> failed/warned checks become SIGNALS -> Correlator clusters them into an INCIDENT   agents/triage_agents.py
  -> triage run: specialists investigate in parallel (read-only tools) -> Supervisor diagnoses (grounded)
       -> Impact analyzer (lineage) -> Planner proposes from a CATALOG (+critic)
       -> Policy engine decides per action: deny / inform / recommend / approve / auto   governance/policy.py
       -> humans approve (dashboard, CLI, MCP) -> Executor acts (only agent with write tools)
       -> Verifier checks ground truth, compensates on failure -> Learner writes postmortem, lesson, eval case
  everything: traced (observability/tracing.py), metered (llm/gateway.py), audited (governance/audit.py)
```

---

## Part A - The agent loop and where it breaks

| Breakpoint | Engineering control | Where | See | Break |
|---|---|---|---|---|
| **Context**: wrong, missing, stale evidence | Context is assembled from typed tool results, every result gets a citable **evidence id**; untrusted content is spotlighted; triage checks preconditions (contract active, lineage present, published version) before reasoning | `tools/gateway.py` `ToolGateway.call` (evidence insert); `llm/prompts.py` `build_messages`; `runtime/workflows.py` `t_open` | Incident drawer -> *Evidence* and *Blackboard* | `sp chaos set llm_hallucinated_citation_rate 1` then drop `volume_drop`: the groundedness gate removes fake ids and lowers confidence (`TriageSupervisorAgent.diagnose`) |
| **Tool contracts**: ambiguous schemas, huge outputs, inconsistent errors | `ToolSpec`: pydantic arg models -> JSON Schema, `max_output_chars` bound, version + fingerprint, stable error codes (`TOOL_FORBIDDEN`, `INVALID_ARGUMENTS`, `RATE_LIMITED`, `NOT_FOUND`...) | `tools/gateway.py` `ToolSpec`, `ToolGateway.call`; catalog in `tools/catalog.py` | Dashboard *Agents & Tools* (tool catalog); `sp trace <inc_id>` shows `execute_tool` spans | Call a tool with bad args in a Python shell (see `tests/test_governance.py::test_tool_gateway_validates_arguments_and_records_evidence`) |
| **Planning**: loops, premature done, skipped steps | ReAct JSON protocol with a **step budget** (`budgets.agent_max_steps`), **loop detection** (same tool+args twice -> `LOOP_DETECTED`, twice more -> stop), explicit `final` action validated against `Finding` | `agents/base.py` `Agent.react` | Metric `agent_loops_detected_total` (`sp metrics`) | `sp chaos set llm_loop_rate 0.7`, drop `volume_drop`; findings may end with `stop_reason: loop_detected` (incident drawer -> blackboard) |
| **State**: progress lost across retries/restarts | Every step is a checkpoint; recorded outputs are reused on resume; run context persisted | `runtime/engine.py` `Engine.execute`, `_run_step`, `StepContext.set` | `sp runs show <run_id>` (Steps table) | Lab 5: `sp chaos crash-after transform` |
| **Verification**: claims success without checking | A separate Verifier checks ground truth per action (view target, checksum, hold flags, child run status, notification file) | `tools/actions.py` `v_*` functions; `agents/triage_agents.py` `VerifierAgent.verify` | Proposal status `verified` vs `rolled_back` | Tamper with the warehouse *after* a rollback and re-run verification in a shell; or `sp chaos set tool_error_rate 0.5` |
| **Cost & latency**: context growth, retries multiply cost | Metering per call; per-run token/$ budgets; cache; model routing; context compaction; clustering before reasoning | `llm/gateway.py` `_check_limits`, `_compact`, cache block in `chat` | Dashboard *Cost & Metrics*; `sp metrics` | Lower `budgets.per_run_usd` to 0.001 in `config/swarmpipe.yaml` -> agents degrade to deterministic fallbacks (`degraded` flags) |

## Part B - Deterministic skeleton, probabilistic steps

- The skeletons are the five workflows in `runtime/workflows.py` (`build_workflows`). Steps are plain Python;
  LLM calls live *inside* specific steps, each with a typed schema, a timeout (`engine.step_timeout_s` for
  `kind="agent"` steps) and a retry policy.
- **Every LLM-backed method has a deterministic fallback**: see `DEGRADE_ERRORS` in `agents/base.py` and the
  `except DEGRADE_ERRORS` branches in every agent (Router falls back to the sniffer, Profiler to glossary
  aliases, Diagnoser to a vote over findings, Planner to "notify owner").
- **Least autonomy that solves the problem**: the Router's model answer is overridden by hard facts
  (`deterministic_guard` in `RouterAgent.run`); the Transformer's LLM-inferred date format is **verified by
  parsing** before it is used (`TransformerAgent.run`); detection (checks, monitors) is always deterministic,
  models only explain and propose.
- **Never let a probabilistic output flow downstream unvalidated**: every model output passes
  `schema.model_validate` (gateway) and the Planner's proposals pass catalog + param validation
  (`RemediationPlannerAgent._validate`) and the policy engine.

**Break it:** `sp llm use ollama` and drop `unit_change`: the real 3B model misdiagnoses; nothing unsafe happens
because every action still goes through catalog validation and policy (Lab 12).

## Part C - Multi-agent orchestration: patterns, failure modes, cost

| Pattern | Implementation | Where | See |
|---|---|---|---|
| Router | Router agent picks the workflow; Supervisor routes signal types to specialists (`SPECIALISTS_BY_SIGNAL`) | `RouterAgent`; `TriageSupervisorAgent.plan` | `route` step output; blackboard `plan` entry |
| Sequential handoff | privacy -> profile -> steward -> transformer -> assurance -> publisher; `ingest_file` hands documents to the `document` workflow (`handed_off`) | `runtime/workflows.py` | `sp runs show` |
| Parallel fan-out / fan-in | one child run per Excel sheet, parent waits durably on `children`; investigators run in a thread pool and results merge on the blackboard | `f_fanout`, `Engine.maybe_resume_parent`; `TriageSupervisorAgent.investigate` | drop `reference` (customers.xlsx -> 2 children) |
| Orchestrator-workers (supervisor) | Supervisor decomposes an incident into specialist tasks and owns the final diagnosis | `TriageSupervisorAgent` | Incident drawer -> blackboard |
| Hierarchical teams | workflow -> supervisor -> specialists; file run -> dataset runs -> triage runs | engine parent/child runs | Runs tab `parent_run_id` |
| Evaluator-optimizer (critic) | Steward proposes a mapping/contract, Critic reviews with **independent evidence** (value patterns), steward revises (2 rounds); same for Planner plans | `ContractStewardAgent.check`, `propose_contract`; `RemediationPlannerAgent.plan`; `CriticAgent` | `contract_check` step output `mapping_critique`; blackboard `plan.critique` |
| Shared state (blackboard) | Append-only, versioned case file; each entry has author + evidence ids | `agents/messaging.py` `Blackboard` | Incident drawer timeline |

**When multi-agent is worth it (and how SwarmPipe uses it):** *privilege separation* - investigators and the
planner are read-only, only the Executor holds `act_*` tools (`ExecutorAgent.tools`); *different models per
role* - routing per role in `config/swarmpipe.yaml` `llm.profiles`; *independent verification* - Critic (different
evidence) and Verifier (ground truth).

**Failure modes -> controls [Cemri et al.]:**

| Failure family | Control here | Where |
|---|---|---|
| Specification / system design | typed outputs for every agent (`llm/types.py`), typed tool args, action catalog | `llm/types.py`, `tools/actions.py` |
| Inter-agent misalignment (ignored/withheld info, conflicting assumptions) | typed, **HMAC-signed** messages with route allowlist; single decision owner (Supervisor); alternatives kept in the diagnosis | `MessageBus`, `ROUTES`; `Diagnosis.alternatives` |
| Weak verification / termination | groundedness gate, abstain -> escalate, Verifier + compensation, step budgets | `diagnose`, `VerifierAgent`, `Agent.react` |
| Cascading failures (OWASP ASI08) | a hijacked specialist can bias the diagnosis, but actions still hit catalog + policy + approvals + egress allowlist | Lab 4 shows the cascade and the containment |
| Runaway cost | per-run budget spans **all agents** of the triage run (global budget); cluster before reasoning | `_check_limits` uses `run_id`; `CorrelatorAgent` |

**Cost multiplier:** compare *Cost & Metrics -> By agent*: an ingest uses 2-4 model calls; an incident uses
10-30 (investigators' ReAct steps, critic rounds). Multi-agent must earn its cost (Anthropic measured its multi-agent
research system at about 15x the tokens of a chat).

## Part D - Protocols: MCP and A2A

| Concept | Where | See / break |
|---|---|---|
| MCP server (stdio, JSON-RPC 2.0, `initialize`, `tools/list`, `tools/call`, `resources/*`) | `mcp_server.py` | Lab 14 (Copilot CLI); `tests/test_e2e.py::test_mcp_server_protocol` |
| Tool annotations are **hints**, not security | `TOOLS[...]["annotations"]`; the server still authorizes every call (`McpServer.user`, `ApprovalService.decide`) | Try `decide_approval` via MCP acting as `analyst` (`sp mcp --as analyst`): FORBIDDEN |
| Elicitation may be unsupported by the client -> **server-side approval queue** | `governance/approvals.py` (typed confirmation, annotation) | Approvals tab; MCP `decide_approval` requires `confirm_text` for high risk |
| **No token passthrough** | the MCP server acts as one configured identity (`mcp.act_as_user`) and never forwards client credentials | `serve()` |
| Bounded outputs, stable errors, tool errors as results (`isError`) | `McpServer.handle` | call an unknown incident id |
| Tool granularity & sprawl | coarse tools for MCP clients (e.g. `get_incident`), fine tools for investigators (`get_schema_diff`...), per-agent allowlists keep each agent's tool list short | `SPECIALIST_TOOLS` |
| A2A Agent Cards | every agent's `card()`; `/.well-known/agent-card.json`, `/a2a/agents`, `/a2a/agents/<id>` | `curl http://127.0.0.1:8765/a2a/agents/supervisor` |
| A2A task lifecycle | `POST /a2a/agents/analyst/tasks` returns a task with `status.state` and artifacts | Lab 15 |

## Part E - Context engineering, grounding and memory

| Concept | Where | See / break |
|---|---|---|
| Structured grounding by query (tools) vs unstructured by search | `tools/catalog.py` (structured), `data/knowledge.py` `KnowledgeBase.search` (BM25) | `sp knowledge search "volume drop"` |
| Permission-aware retrieval (tenant + trust filters), at query time | `KnowledgeBase.search(trust_levels=...)`; Analyst temp views per tenant + PII detokenization only for PII-allowed identities | `sp ask "emails of customers" --as analyst` vs `--as admin` |
| Freshness as a requirement | contracts declare `freshness`; `ContextGraph.freshness`; derived rebuild blocked when inputs are incident-bound (`dv_check`) | Lab 9 |
| Provenance and citations | evidence ids on every tool result; knowledge hits carry `citation` (chunk id) and `trust` | Incident drawer -> Evidence |
| Chunk by artifact type | `chunk_text` splits on headings/paragraphs; logs/tables are never chunked into prompts - tools return aggregates | `data/knowledge.py` |
| Memory: short-term / episodic / procedural | run context + blackboard; `MemoryStore` (lessons from the Learner); curated runbooks in `knowledge/` | Knowledge & Memory tab |
| Memory poisoning controls | lessons are `candidate` until a human promotes them; provenance + expiry; content injection-scanned (`flags.injection`); only `approved` recalled by default | `memory.py`; Lab 11 |
| Context compaction | `ModelGateway._compact` trims the longest message when a model's context window would overflow | set `context_window: 1500` for `sim-small` and watch `context_compacted` span events |

## Part F - Durable execution, state and reliability

| Requirement | Where | See | Break |
|---|---|---|---|
| Checkpoint after each step | `Engine._run_step` writes `steps` rows with recorded output | `sp runs show` | Lab 5 |
| Idempotency keys on side effects | `StepContext.idempotent` (publish `publish:<run_id>`, derive, document index); `ExecutorAgent.execute` (`exec:<proposal_id>`) | `idempotent_replays_total` metric | drop `duplicate`; re-run a triage via `sp runs redrive` |
| At-least-once delivery + idempotent handlers | `core/events.py` `Dispatcher` (offsets advance only after success; poison after 3) | `tests/test_core.py` | raise inside a handler |
| Compensation (saga) | workflow level: `ctx.add_compensation` + `Engine._compensate`; action level: `ActionSpec.compensate` used by `VerifierAgent` and one-click rollback | `saga.compensate`, `action.compensate` audit rows | `sp actions rollback <proposal_id>` |
| Timeouts & heartbeats | `run_with_timeout` (watchdog thread), `_Heartbeat` renews leases, `reap_expired_leases` | `runs_recovered_total` | `sp chaos set llm_latency_ms "[3000,5000]"` + lower `engine.step_timeout_s` |
| Human interrupts | `WaitingFor("approval:<id>")`, resumed by the `approvals` event consumer | Approvals tab | leave an approval pending, restart the server, approve: the run resumes |
| Recorded outputs for non-deterministic steps | resumed runs skip succeeded steps and reuse their outputs - LLM steps are never replayed | Lab 5 (attempts stay at 1) | - |
| Fencing (lost lease) | a worker whose heartbeat fails stops before the next step (`LeaseLost`) | `Engine.execute` | expire a lease manually in SQLite while a run is running |
| Deferral vs retry | `Deferred` reschedules without consuming attempts (dataset publish lock) | `d_publish` | drop `mass_failure` |
| Dead-letter queue + redrive | `f_on_failure` moves the file to `data/dlq/<tenant>/` with a `.reason.json`; `sp dlq list`, `sp dlq redrive <id>` | DLQ via `sp dlq list` | drop `malformed` |

## Part G - Evaluation, the biggest lever on quality

| Layer | Metric in SwarmPipe | Where |
|---|---|---|
| Component | router accuracy, analyst SQL result-set accuracy, investigator tool selection | `evals/harness.py` `run_router_suite`, `run_analyst_suite`, `score_case` (`tools:*`) |
| Trajectory | triage steps ran in order (`trajectory` expectation) | `score_case` |
| Outcome | top-1 / top-3 root cause, required proposals, final data state (`published_unchanged`, `new_published_version`, checksums, no raw PII published) | `score_case` |
| Safety | forbidden actions never executed, no egress, bad data never published under attack | `score_case`, `redteam_metrics` |
| Efficiency | model calls, tokens, simulated $, latency per case | `triage_metrics` |

- **pass@k vs pass^k** (tau-bench): `sp evals run --suite triage --k 3 --noise 0.3` (Lab 7).
- **LLM-as-judge calibration**: `sp evals calibrate-judge --version v1` (verbosity-biased, not trustworthy) vs
  `--version v2` (trustworthy): kappa, Spearman, verbosity bias, position consistency - `evals/judge.py` (Lab 8).
- **Datasets from reality**: synthetic perturbations = `scenarios.py`; adversarial = `evals/datasets/redteam.v1.jsonl`;
  from incidents = `sp evals harvest` (Learner's eval cases + the original files, disabled until reviewed).
  Every case carries `lineage` (source, author, date).
- **Online evaluation**: shadow candidates (`features.shadow_candidates: {diagnoser: {prompt_version: v2}}` ->
  `shadow_comparisons`), human feedback (incident drawer), acceptance/override rates (autonomy stats), post-action
  verification (Verifier).
- **Gates**: `evals/gate.yaml` + `sp evals gate` (exit code 1 on failure) + `--update-lock` approves prompt hashes
  (`prompts/prompts.lock.json`). Unapproved prompts are refused at runtime (`PromptRegistry.get`).

## Part H - Security for agents

See [`SECURITY_OWASP_MAPPING.md`](SECURITY_OWASP_MAPPING.md) for ASI01-ASI10 and the LLM Top 10. The core
mitigations, one by one:

| Mitigation | Where |
|---|---|
| least privilege per agent and per tool; separate read and write tools | `Agent.scopes`, `Agent.tools`, `ToolGateway.call` allowlist check; `act_*` tools only on `ExecutorAgent` |
| deterministic authorization outside the model | `governance/policy.py` |
| human approval for state changes | policy levels L1/L2; `governance/approvals.py` |
| provenance tags on content | evidence `trust`; tool `trust="untrusted"`; knowledge trust levels; memory provenance |
| egress allowlists | `guardrails.check_egress`, `Notifier.send` (`blocked_egress`) |
| schema validation of tool arguments and outputs | pydantic arg models; `schema.model_validate` of model outputs |
| blast-radius caps | `blast_radius` escalation (policies.yaml) from `ContextGraph.impact` |
| change-freeze windows | `freeze_windows` + runtime flag `policy.freeze_window` |
| sandboxed code execution | `data/safe_expr.py` (AST allowlist, no `eval`) for contract rules / derived columns |
| approved tool and server registry | tool fingerprints (`ToolGateway.approved`), agent card hashes (`agent_registry`), prompt lock |
| red-team evaluations | `evals/datasets/redteam.v1.jsonl` |
| kill switch | `governance/killswitch.py`, checked by policy, tool gateway and model gateway |
| lethal trifecta broken | the agents that read untrusted content (investigators, planner) cannot communicate externally or write; the only external channel (notifications) goes through the Executor + egress allowlist; recipients are handles |

## Part I - Identity and authorization

- **Three identities**: agent (`agent:<id>` in `Agent.identity`), user (`IdentityService.user`), tool/server
  (tool fingerprint + version; MCP server's own identity).
- **User-initiated = intersection**: `Identity.acting_for` + `Identity.can` / `can_access_tenant` / `pii_allowed`
  (`governance/identity.py`). The Analyst has the PII *capability*; only a PII-allowed user makes it effective.
- **Autonomous = narrow service identity**: `agent:executor` with scope `action` only.
- **Audit both**: proposals record `executed_by=agent:executor`, `on_behalf_of=user:oncall`, `policy_details`,
  `approval_id`; audit rows carry `actor` + `on_behalf_of` ("agent X acting for user Y under policy Z approved by W").
- **Agents never see raw secrets**: `secret://NAME` refs resolved only by providers (`SecretsBroker.resolve`);
  secret values are blocked from prompts (`check_secret_leak`); notification recipients are *handles*
  (`owner`, `source_owner`, `security`) resolved by the Executor (`tools/actions.py` `_owner`).
- **Short-lived scoped credentials**: `IdentityService.issue_token` / `verify_token` (HMAC, expiry).

## Part J - Cost, latency and capacity

| Lever | Where | See |
|---|---|---|
| route cheap models to triage/classification, strong to diagnosis | `llm.profiles` per role | `sp llm status` |
| cache stable prompts | `llm_cache` in `ModelGateway.chat` (random spotlight ids normalized: `normalized_messages`) | `llm_cache_hits_total` |
| retrieve instead of stuffing | investigators pull bounded tool outputs | span attributes `gen_ai.usage.*` |
| compact long histories | `_compact` | span event `context_compacted` |
| **cluster before you reason** | `CorrelatorAgent.on_signal` + `engine.triage_debounce_s` | drop `mass_failure`: 6 signals -> 1 incident -> 1 diagnosis (*Cost* tab: cluster ratio) |
| exit early | deterministic guards, abstain | Router `source` |
| per-tenant quotas and budgets | `tenants.*.daily_llm_requests` (acme = 40/day), `budgets.per_run_usd` | Lab 16 |
| bulkhead / concurrency | `engine.llm_concurrency` semaphore | slow Ollama calls do not starve deterministic steps |
| unit metric | cost per resolved incident | *Cost & Metrics* card |

## Part K - Model strategy: SaaS, self-hosted, bring-your-own-model

| Concept | Where |
|---|---|
| capability tiers | `llm.models.*.tier` |
| minimum model per feature/role | `llm.profiles` role chains |
| certification suite = the same evals run against a candidate model, per role, no fallback | `evals/harness.py` `certify`; results in `model_certifications`; enforce with `llm.require_certification: true` |
| guardrail parity across providers | one gateway applies the same guardrails to mock, Ollama, OpenAI and Azure |
| degrade gracefully | fallbacks to the simulated model, then deterministic agent fallbacks |
| pin versions; re-run evals on every upgrade | `prompts.pins`; `sp evals gate` |

**See it:** Lab 12 certifies `llama3.2` for the router (passes) and shows why it should *not* serve the diagnoser.

## Part L - Observability is not audit

| | Telemetry | Audit |
|---|---|---|
| Code | `observability/tracing.py`, `metrics.py`, `logging.py` | `governance/audit.py` |
| Content | spans with OTel GenAI names (`gen_ai.operation.name`, `gen_ai.request.model`, `gen_ai.usage.input_tokens`, `gen_ai.agent.name`, `gen_ai.tool.name`...), metrics, JSON logs | who/what acted, for whom, under which policy, with which approval, with what result |
| Properties | head sampling (`tracing.sample_rate`) + tail rule (keep errors), 7-day retention (`Scheduler.retention`), exportable JSONL (`data/traces/`) | complete, **hash-chained** (`verify()` pinpoints edits/deletions), never purged, exportable (`sp audit export`) |
| Try | `sp trace <inc_id>` | `sp audit tamper --seq 3` then `sp audit verify` (Lab 13) |

**Correlation:** `runs.trace_id` = the trace of a run; `incidents.trace_id` = one trace across all agents of an
incident; `approvals.run_id`/`incident_id`/`proposal_id`; `dataset_versions.run_id`; evidence packs link them all.

---

## Part M - Data observability, lineage and trusted context

| Concept | Where | See |
|---|---|---|
| "Ended OK" is not "correct": checks inside the workflow + **circuit breaker** | `data/quality.py` `run_checks` -> `decision=quarantine`; `d_publish` stages a quarantined version instead of publishing | drop `volume_drop`: job "succeeds" technically, data is blocked |
| Freshness - arrival | contracts `freshness`; `Scheduler.freshness` -> `freshness_overdue` | Lab 9 |
| Freshness - content | `data_freshness` check (newest business date) | drop `stale_resend` |
| Volume | `row_count_min`, `volume_vs_baseline` (median of last N published batches), **reconciliation** (`reconciliation_rows`, `control_total:*`) | *Datasets* -> latest checks |
| Schema | `data/profiling.py` `schema_diff` (+ glossary aliases), `type_compatibility:*` | drop `schema_drift` / `additive_drift` |
| Distribution & quality | PSI on contract drift columns (`psi`), null/reject rate, range, accepted values, regex, business rules (safe expressions), referential integrity | drop `unit_change`, `quality_failure`, `referential_break` |
| Out-of-band change | checksums recorded at publish vs current (`Scheduler.out_of_band`) -> restore from immutable snapshot | Lab 10 |
| What an orchestrator knows that bolt-on tools guess | declared freshness, dependencies (consumers.yaml, contract references), reruns (reprocess), business impact (consumer criticality/regulated), ability to act (actions) | `config/consumers.yaml`, `ContextGraph` |
| Lineage levels and collection | job/dataset lineage from static intent (`LineageService.seed_static_edges`) + runtime OpenLineage events (`emit` START/COMPLETE/FAIL with `schema`, `dataQualityMetrics`, `dataQualityAssertions`, `columnLineage` facets) | `sp runs show <ingest_dataset run>`; *Datasets & Lineage* graph |
| Impact analysis | `ContextGraph.impact` (downstream datasets, consumers, owners, regulated, blast radius) | incident drawer `impact` |
| Trusted context | `ContextGraph.node` (owner, classification, SLA, consumers, open incidents); **data precondition** on actions: derived rebuild blocked when inputs have open high/critical incidents (`dv_check`) | `derive_blocked` signals |
| Detection -> governed action | Detect: checks/monitors; Correlate: `CorrelatorAgent`; Diagnose: `TriageSupervisorAgent`; Impact: `ImpactAnalyzerAgent`; Propose: `RemediationPlannerAgent`; Approve: `ApprovalService`; Act: `ExecutorAgent`; Verify: `VerifierAgent`; Learn: `LearnerAgent` | the triage workflow steps |
| Precision beats coverage | `incidents.min_severity`; row-level issues below thresholds are quarantined as `info`; clustering; false-positive eval case `tri-010-clean-day` gated at 0 | `sp evals run --suite triage --cases tri-010-clean-day` |
| Data contracts and circuit breakers | `config/contracts/*.yaml` (versioned, GitOps import `ContractStore.sync_from_files`); onboarding proposal + critic + approval (`d_onboard`); `update_contract` action | drop `new_dataset`, `additive_drift` |
| Quarantine & backfill | quarantined versions (`q__` tables) + `quarantine_rows`; `force_publish` (governed); `reprocess_with_mapping` | *Datasets* versions |

## Part N - Governance, risk and regulation

| Governance control | Where |
|---|---|
| Inventory (agents, tools, models registered, versioned, owned) | `agents/registry.py` (cards + hashes), tool fingerprints, `llm.models`, prompt lock |
| Identity and scope | `Agent.scopes/tools`, `governance/identity.py` |
| Runtime policy | `governance/policy.py` + `config/policies.yaml` |
| Risk tiering | `actions.*.risk`, `reversible`, `max_level` |
| Evaluation before release | `sp evals gate`, prompt lock, `require_certification` |
| Human oversight | approvals, typed confirmation, annotation, kill switch |
| Monitoring | metrics, SLOs, shadow comparisons, feedback |
| Audit and evidence | `governance/audit.py`, `governance/evidence.py` |
| Incident response | kill switch, one-click rollback, Learner postmortems -> eval cases |
| Third-party / model risk | per-role certification, fallbacks, pinned prompt versions |

**Autonomy ladder:** `config/policies.yaml` `level_effects` + per-action `default_level`/`max_level`;
`governance/autonomy.py` records proposals/approvals/rejections/executions/verifications/rollbacks/human
agreement per tenant x action; `review_all` requests a human-approved promotion when evidence meets
`autonomy_rules.promote`; `_maybe_demote` withdraws autonomy automatically after a verification failure or
rollback. See Lab 6.

**Evidence packs:** `EvidenceService.build` contains everything an auditor asks for - triggering signals and data
snapshot refs (`data_snapshots` with content hashes), context and sources (`tool_evidence`, `case_file`,
contract versions), proposed action and alternatives, policy evaluation (`policy_details`), approver and
timestamp, executed actions + ids, verification, model/prompt/tool/agent versions, cost - plus the audit-chain
anchor. It is generated at incident close (`close_incident`) and on demand (`sp evidence <inc_id>`).

## Part O - The reference system designs

- **Data-aware triage and governed remediation** is the triage workflow. Box by box: signals (`d_quality`,
  `Scheduler`), correlate (`CorrelatorAgent`), diagnose (`TriageSupervisorAgent`), impact (`ContextGraph.impact`),
  propose (`RemediationPlannerAgent` over `tools/actions.py`), policy gate (`PolicyEngine`), approval
  (`ApprovalService`), execute (`ExecutorAgent` with idempotency keys and correlation ids), verify
  (`VerifierAgent`), learn (`LearnerAgent`); shared underneath: context graph, audit/evidence, telemetry,
  metering. Its five key design decisions are all implemented (deterministic correlation first; separate
  read/write agents; action catalog; integrate detectors before building; configurable approval surface -
  dashboard, CLI, MCP).
- **Multi-agent runtime**: control plane = registry, identity, policy, model gateway, tool gateway,
  evaluation/certification, runtime flags; data plane = durable engine, workers, event bus + A2A endpoints,
  state store, memory, knowledge/context services; cross-cutting = telemetry, immutable audit, metering,
  approvals. Tenancy: inbox sub-folders, per-tenant views/quotas/autonomy. "Agents cannot be promoted without
  passing evals" = prompt lock + certification.
- **Authoring copilot**: the onboarding flow is AI-authored definitions going through review -
  schema-constrained generation (`ContractProposalOut`), critic, human approval, then activation.
- **Lineage and impact**: `data/lineage.py` (static + runtime, OpenLineage events, impact API; the MCP tool
  `get_lineage_impact`).

## Part P - Execution craft and metrics

- PRD and eval spec for the triage feature: [`PRD_AND_EVAL_SPEC.md`](PRD_AND_EVAL_SPEC.md).
- NFRs as SLOs of the pipeline itself: `config/swarmpipe.yaml` `slos`, `SLOService.evaluate`, `Scheduler.slo`
  (raises `slo_breach` signals).
- Preview-to-GA gates ~ `evals/gate.yaml` + certification + kill switch/rollback tests.
- Telemetry from day one: signals, incidents, diagnoses (+feedback), proposals -> approvals ->
  executions -> verifications/rollbacks, TTD (`triage_diagnosis_seconds`), per-stage tokens/latency/cost,
  eval scores by version - all queryable in *Cost & Metrics* / `sp metrics`.
- Metric tree: north star ~ data downtime per incident = `resolved_at - first_signal_at` (incidents table);
  guardrails = rollbacks, verification failures, policy denials, false positives; adoption/business metrics are
  out of scope locally. Autonomy metrics = `sp autonomy list`. Platform metrics = agents
  registered, run success rate (`runs_total`), audit completeness (every executed proposal has audit rows).

---

## Part Q - Beyond the core: production details that are easy to miss

| Concept | Why it matters | Where |
|---|---|---|
| File stability + atomic moves | never ingest a half-copied file; `.partial` temp names | `FolderWatcher.poll_once` (stability polls), `scenarios._csv` |
| Polling vs OS notifications | notifications are unreliable on network drives | `runtime/watcher.py` docstring |
| Backpressure | admission control when the queue is deep | `watcher.backpressure_high_watermark` |
| Outbox pattern / event offsets | durable events next to state; consumers track offsets | `core/events.py` |
| Poison messages | a bad event must not block the stream | `Dispatcher.poll_once` |
| Leases, heartbeats, fencing | crash recovery without double execution | `runtime/engine.py` |
| Bulkheads | isolate slow dependencies | `ModelGateway.bulkhead` |
| Circuit breakers | stop hammering a failing model; half-open probes | `CircuitBreaker` |
| Structured-output repair loop | small models break JSON; repair, then fall back | `_try_model` |
| Tolerant JSON extraction | prose + fenced JSON is common | `extract_json` |
| Prompt lockfile | prompts are code; changes need evals + approval | `PromptRegistry`, `prompts.lock.json` |
| Tool fingerprints | detect silently changed tools (supply chain) | `ToolSpec.fingerprint`, `TOOL_NOT_APPROVED` |
| Rogue-agent auto-suspension | repeated forbidden calls -> kill switch for that agent | `ToolGateway._suspicion` |
| Signed inter-agent messages | integrity + route allowlist between agents | `MessageBus` |
| Deterministic tokenization + vault | joinable pseudonyms, authorized detokenization, audited | `PiiVault` |
| SQL authorizer + VM step budget | NL->SQL safety in depth (read-only, allowlisted, bounded) | `Warehouse.readonly_query` |
| Immutable versions, blue/green views, snapshots | instant rollback and tamper restore | `data/warehouse.py`, `data/publishing.py` |
| Append-mode partitions | re-sent partitions replace, never duplicate | `PublishingService.stage` |
| Reconciliation & control totals | revenue-assurance style completeness proof | `quality.py` reconciliation block |
| Multi-tenancy | per-tenant data, quotas, autonomy, identity scoping | inbox sub-folders; `tenants` config |
| Injectable business clock | test time-based SLAs without waiting | `Clock`, `clock_offset_min` |
| Runtime flags | change behavior of a running system safely (and audibly) | `RuntimeFlags`, `sp chaos set` |
| Schema migrations | evolve the state store | `core/db.py` `MIGRATIONS` |
| Retention & online backup | telemetry is disposable, audit is not | `Scheduler.retention`, `sp maintenance backup` |
| Health vs readiness | liveness and dependency readiness | `/healthz`, `/readyz` |
| Prometheus exposition | standard scraping | `/metrics`, `Metrics.prometheus` |
| Data minimization in prompts | recipients as handles; masked samples; redaction | `_owner`, `ProfilerAgent`, `_guard` |
| Defense ablation studies | measure what each defense actually buys | `evals/datasets/redteam.v1.jsonl` (Lab 4) |
| Degraded-mode UX | show when an answer came from a fallback | `degraded` flags in outputs and spans |
| Resource lifecycle across threads | thread-local DB connections opened on agent-step threads can outlive their thread (kept alive by reference cycles such as stored tracebacks); on Windows the open handle blocks deleting/moving files. Found in this project: every eval trial leaked its workspace until shutdown explicitly closed all connections and released the per-workspace log file | `ConnectionTracker` in `core/db.py`, `Services.close()`, `release_log_dir`, `test_close_releases_every_file_handle` |
