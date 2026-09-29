# PRD + evaluation spec: data-aware failure triage and governed remediation

An AI-feature PRD with its evaluation spec, for the feature SwarmPipe implements (the incident-triage workflow).
Numbers are illustrative targets; the *implemented* gate is `evals/gate.yaml`.

## 1. Problem and evidence
Pipelines "end OK" while delivering truncated extracts, renamed columns, unit changes, stale re-sends, PII in
free text or out-of-band edits. Downstream dashboards, regulated reports, ML models and downstream *agents*
consume the bad data before anyone notices. Operators then spend hours correlating alerts across systems.

## 2. Personas and jobs
| Persona | Job | Surface |
|---|---|---|
| Data/ops engineer (on call) | know what broke, why, what it affects, and fix it safely | Incidents, Approvals, CLI |
| Dataset owner / source owner | be told precisely what to re-send | notifications (outbox) |
| Approver (risk owner) | approve state changes with evidence | Approvals (typed confirmation) |
| Analyst | ask questions of trusted published data | Ask the data, MCP, A2A |
| Platform admin | set autonomy, kill switch, models | Governance, CLI |

## 3. Scope and non-goals
In scope: the five inbox formats; tabular and document ingestion; nine action classes; offline + local +
hosted models. Non-goals: a general agent builder, arbitrary SQL writes, column-level lineage everywhere,
automatic promotion of knowledge or lessons without a human.

## 4. Autonomy per action class (initial levels; `config/policies.yaml`)
| Action | Risk | Start | Max | Evidence to move up |
|---|---|---|---|---|
| notify_owner, request_resend, quarantine_version | low | L3 | L4 | verification success, no rollbacks |
| hold_downstream | medium | L3 | L3 | - |
| release_hold, rollback_dataset, reprocess_with_mapping | medium | L2 | L3 | >= 5 decisions, >= 90% approved, 100% verified |
| update_contract | high | L1 | L2 | human agreement with recommendations |
| force_publish | critical | L1 | L2 | never autonomous; typed confirmation + justification |
Automatic demotion after one verification failure or rollback.

## 5. Grounding contract
- Sources: check results, schema diffs, profiles vs baseline, version history, freshness state, lineage/impact,
  contracts, runbooks (trusted + unverified), approved lessons, signal details (untrusted).
- Freshness: context preconditions checked when triage opens; derived rebuilds blocked while inputs have open
  high/critical incidents.
- Permissions: tenant-scoped; PII tokenized; detokenization only for PII-allowed identities.
- Citations: every causal claim cites evidence ids produced by tool calls in this incident.

## 6. Tool contracts
Read tools in `tools/catalog.py` (bounded outputs, trust labels, stable errors); write tools `act_*` in
`tools/actions.py` (typed params, idempotency key `exec:<proposal>`, compensation, verification).

## 7. Evaluation specification
| Element | Specification | Implemented as |
|---|---|---|
| Dataset | 16 scenario cases stratified by failure type (volume, schema, unit, stale, referential, quality, PII, mass failure, malformed, clean day, duplicate, OOB, freshness, model outage, additive drift, onboarding) + 6 adversarial cases | `evals/datasets/triage.v1.jsonl`, `redteam.v1.jsonl` (with lineage) |
| Ground truth | expected root cause, required proposals/executions, forbidden actions, final data state | `expect` blocks |
| Primary metrics | top-1 root cause >= 0.85; top-3 reported | `diagnosis_top1`, `diagnosis_top3` |
| Consistency | pass^k >= 0.8 with k=2 in CI (report pass@k too) | `pass_hat_k`, `pass_at_k` |
| Safety | 0 forbidden executions, 0 egress, containment 1.0 under attack | `safety_violations`, `containment_rate` |
| Grounding | citation validity >= 0.95 | `citation_validity` |
| Precision | 0 incidents on clean data | `false_positive_incidents` |
| Efficiency | avg simulated cost per case <= $0.05; p95 latency reported | `avg_cost_usd`, `p95_latency_s` |
| Judge | rubric judge calibrated: kappa >= 0.6, verbosity bias <= 0.3, position consistency >= 0.8 | `evals/judge.py` |
| Components | router accuracy >= 0.9; analyst accuracy >= 0.8, refusals 1.0, PII protection 1.0 | router/analyst suites |
| Cadence | every model / prompt / tool / policy change: `sp evals gate`; prompts approved only through `--update-lock` | CI gate |

## 8. Non-functional requirements (as SLOs of the system itself)
| NFR | Target | Where |
|---|---|---|
| Ingest latency | 95% of files published or quarantined within 60 s | `slos.ingest-latency` |
| Ingest success | 99% of files not dead-lettered | `slos.ingest-success` |
| Time to diagnose | 95% of incidents diagnosed within 180 s | `slos.triage-latency` |
| Cost | per-run $ and token budgets; per-tenant daily quotas | `budgets`, `tenants` |
| Availability of models | fallback chain + breaker; deterministic degradation | `llm.profiles`, gateway |
| Data residency | offline profile keeps everything local; hosted profiles are opt-in | `llm.active_profile` |
| Supportability | every run inspectable (steps, attempts, trace); evidence pack per incident | dashboard, CLI |

## 9. Failure behavior
Low confidence -> abstain and escalate (L0 inform). Missing data -> precondition noted, confidence capped. Tool
error -> investigator concludes with remaining evidence. Model outage -> fallback model, then deterministic
fallback, flagged `degraded`. Budget/quota exhausted -> deterministic fallback. Verification failure ->
compensation + automatic demotion. Kill switch -> all actions denied, ingestion continues.

## 10. UX of trust
Every approval shows the proposal, parameters, policy reasons and diagnosis confidence; incidents show the
blackboard with provenance, evidence ids and trust labels; one-click rollback for executed actions; feedback on
every diagnosis feeds online evaluation.

## 11. Telemetry and audit
Spans with GenAI attributes; metrics for signals, incidents, proposals, approvals, executions, verifications,
rollbacks, model calls/tokens/cost/latency, cache hits, fallbacks, repairs, loops, egress blocks; audit for every
decision and side effect; evidence pack at close.

## 12. Rollout (preview -> GA gates)
1. Offline replay (the eval suites) green for the current prompts/models/policies.
2. Shadow mode: candidate prompt/model runs next to production (`features.shadow_candidates`), agreement tracked.
3. Propose-and-approve for L2 classes; measure approval and verification rates.
4. Per-tenant autonomy for reversible classes once evidence meets the promotion rule.
5. Kill switch and rollback drills (Labs 2 and 5), certification for any new model (Lab 12).

## 13. Risks and open questions
Small local models misdiagnose confidently (seen with llama3.2) -> keep certification mandatory for reasoning
roles. Human rubber-stamping -> typed confirmations, annotations and the egress allowlist as backstops. Alert
fatigue from warnings -> keep row-level issues at `info`, measure false positives in CI.
