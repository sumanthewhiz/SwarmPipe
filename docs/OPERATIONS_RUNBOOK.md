# Operations runbook: running the agent system itself

The pipeline watches data; this runbook is about watching (and fixing) the pipeline and its agents.

## Daily health
| Check | How | Healthy |
|---|---|---|
| Liveness / readiness | `GET /healthz`, `GET /readyz` | all threads alive (dispatcher, scheduler, watcher, workers) |
| Queue and backpressure | dashboard *Overview* (queue depth), metric `watcher_backpressure` | depth drains; backpressure 0 |
| SLOs | `sp slo` | `ok`; burn rate < 1 |
| Stuck runs | `sp runs list --status running` / `--status waiting` | running runs have fresh leases; waiting runs have a pending approval or child |
| Pending approvals | `sp approvals list` | none older than their TTL (they expire and escalate) |
| DLQ | `sp dlq list` | understood entries only |
| Audit integrity | `sp audit verify` | OK |
| Model health | `sp llm status` | breakers closed |
| Cost | dashboard *Cost & Metrics*, `quotas` | within tenant budgets |

## Alerts -> response
| Signal / symptom | Meaning | Response |
|---|---|---|
| `slo_breach` signal | the pipeline itself is too slow or failing | inspect traces of slow runs (`sp trace`), model latency, queue depth |
| `rogue_agent` signal | an agent was auto-suspended after forbidden tool calls | read its tool calls (`tool_calls` table / audit `tool.denied`), fix prompt/config, then `sp killswitch off --scope agent:<id>` |
| `egress_blocked` signal | something tried to send data to a non-allowlisted host | treat as a security incident; evidence pack; check for injected content |
| breaker open for a model | provider down or failing validation | traffic already falls back; check the provider; `sp llm use offline` if needed |
| many `degraded: true` outputs | quotas/budgets exhausted or no routable model | raise quota consciously or accept degraded advice |
| runs `recovered_count > 0` | a worker died; leases expired and runs resumed | check logs (`data/logs/swarmpipe.jsonl`) for the crash cause |

## Kill switch
`sp killswitch on --reason "<why>"` stops every agent action and model call immediately (policy, tool gateway
and model gateway all check it); deterministic ingestion continues and bad data is still quarantined.
Narrower scopes: `--scope agent:planner`, `--scope tenant:acme`, `--scope action:rollback_dataset`, `--scope llm`.

## An AI-proposed action caused a problem
1. **Contain**: kill switch for the action class (`--scope action:<class>`) or globally.
2. **Roll back**: `sp actions rollback <proposal_id>` (compensation); verify the data state.
3. **Communicate**: evidence pack (`sp evidence <incident_id>`) to the affected owners.
4. **Learn**: blameless postmortem (the Learner's draft is on the incident); add the case to
   `evals/datasets/*.jsonl` (or `sp evals harvest` and review/enable it).
5. **Adjust**: the rollback already demoted the action class automatically; confirm with `sp autonomy list`;
   tighten `config/policies.yaml` if needed; re-run `sp evals gate`.

## Changing a prompt, model, tool or policy
1. Make the change on a branch.
2. `sp evals gate` (all suites; exit 1 fails CI).
3. For prompts: `sp evals gate --update-lock` approves the new hashes only if the gate passes; a running server
   refuses unapproved prompts and picks up approved ones within seconds.
4. For a new model: `sp evals certify --model <id> --roles <roles>`; optionally set `llm.require_certification: true`.
5. Optional shadow period: `features.shadow_candidates: {diagnoser: {prompt_version: v2}}` and watch agreement.

## Crash and recovery
Workers hold 30 s leases renewed every 5 s. After a crash, restart `sp run`; the reaper re-queues expired runs
and they resume from their last checkpoint (Lab 5). Side effects are idempotent, so partially completed steps
are safe to retry.

## Data corrections
Never edit warehouse tables directly: the out-of-band detector will raise a critical incident and propose
restoring the snapshot. Route corrections through the inbox (a re-sent file replaces its partition) or through a
governed action.

## Backup, retention, reset
- Online backup: `sp maintenance backup` (SQLite backup API; safe while running).
- Retention: telemetry 7 days, metrics 14 days (hourly task, or `sp maintenance retention`); audit is never purged.
- Reset a lab environment: `sp reset --yes` moves `data/` to a `.trash-data-*` folder (recoverable), then `sp init`.

## Where things are
| What | Path |
|---|---|
| State / warehouse / snapshots | `data/state/swarmpipe.db`, `warehouse.db`, `snapshots.db` |
| Logs (JSON, with trace/run/incident ids) | `data/logs/swarmpipe.jsonl` |
| Trace export (OTLP-like JSONL) | `data/traces/spans-YYYYMMDD.jsonl` |
| Notifications (simulated email/chat/webhook) | `data/outbox/<channel>/*.json` |
| Evidence packs | `data/exports/evidence/<incident>.json/.html` |
| Dead letters | `data/dlq/<tenant>/` (+ `.reason.json`) |
| Archive / quarantine | `data/archive/<tenant>/<date>/`, `data/quarantine/<tenant>/<date>/` |
