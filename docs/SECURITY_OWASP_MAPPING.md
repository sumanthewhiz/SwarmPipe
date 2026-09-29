# Security mapping: OWASP Top 10 for Agentic Applications (2026) and LLM Top 10 (2025)

Principle used throughout: **assume prompt injection will sometimes succeed, and design so that a
fooled model still cannot act.** The red-team suite measures exactly that: `containment_rate` must be 1.0 while
`model_fooled_rate` is allowed to be high (`sp evals run --suite redteam`).

## Agentic Top 10

| Risk | Where it could happen here | Controls (code) | Evidence (test / eval / lab) |
|---|---|---|---|
| **ASI01 Agent goal hijack** | instructions in cells, file names, documents, knowledge hits steer an investigator or the planner | spotlighting (`llm/prompts.py` `build_messages`, `SPOTLIGHT_RULE`); injection detection (`governance/guardrails.py`); `injection-suspected` escalation (`config/policies.yaml`); critic review (`CriticAgent`); catalog-only planning (`RemediationPlannerAgent._validate`) | `red-001..006`; Lab 4 |
| **ASI02 Tool misuse and exploitation** | an agent calls a tool outside its role or with crafted args | per-agent allowlists + scopes + tenant checks + typed args + rate limits (`tools/gateway.py` `ToolGateway.call`); write tools only on the Executor | `tests/test_governance.py::test_tool_gateway_*` |
| **ASI03 Identity and privilege abuse** | a user or agent gains more than it should via delegation | intersection semantics (`Identity.acting_for`, `can`, `pii_allowed`); approver roles; typed confirmation; short-lived tokens | `tests/test_security.py::test_delegation_uses_intersection`, analyst PII cases |
| **ASI04 Agentic supply chain** | a tool definition, prompt or agent card changes silently | tool fingerprints (`ToolSpec.fingerprint`, `TOOL_NOT_APPROVED`); prompt lockfile (`PromptRegistry`, `prompts.lock.json`); agent card hashes + change audit (`AgentRegistry.register_all`); per-role model certification | `tests/test_llm_gateway.py::test_unapproved_prompt_is_refused`; Lab 7 |
| **ASI05 Unexpected code execution** | contract rules / derived columns / LLM-suggested logic | no `eval`/`exec`: AST allowlist interpreter (`data/safe_expr.py`); SQL only through a read-only connection with an authorizer and VM-step budget (`Warehouse.readonly_query`); Transformer only accepts a *verified* date format string | `tests/test_security.py::test_safe_expressions_reject_code`; analyst refusal cases |
| **ASI06 Memory and context poisoning** | poisoned documents or lessons become trusted grounding | knowledge trust levels (`unverified`/`untrusted` until promoted); injection flags shown to approvers; lessons are `candidate` until promoted (`MemoryStore`), provenance + expiry; architecture still blocks harmful actions if a human promotes poison | `red-005`, `red-006`; Lab 11 |
| **ASI07 Insecure inter-agent communication** | a forged or tampered finding/task between agents | HMAC-signed `AgentMessage` + recipient check + route allowlist (`agents/messaging.py`) | `tests/test_security.py::test_signed_inter_agent_messages` |
| **ASI08 Cascading failures** | one hijacked specialist corrupts the diagnosis, the plan, then actions | independent critic evidence; single decision owner; blast-radius and global auto-action caps; circuit breaker on data; holds on derived datasets; cooldown on state-changing repeats | Lab 4 shows the cascade and its containment |
| **ASI09 Human-agent trust exploitation** | a persuasive agent gets a human to approve a harmful action | evidence and policy reasons shown with every approval; typed confirmation + written justification for critical actions (`ApprovalService.decide`); `force_publish` capped at L2; egress allowlist even after approval | Lab 4 step 5 (careless approval still blocked) |
| **ASI10 Rogue agents** | an agent repeatedly tries forbidden tools | repeated forbidden/unknown tool calls auto-engage `agent:<id>` kill switch + `rogue_agent` signal (`ToolGateway._suspicion`) | `tests/test_governance.py::test_tool_gateway_least_privilege_and_rogue_containment` |

## LLM Top 10 (2025)

| Risk | Controls here |
|---|---|
| LLM01 Prompt injection | see ASI01; plus data is never concatenated into the system prompt - instructions (system) and data (user blocks) are separated |
| LLM02 Sensitive information disclosure | PII detection (`data/pii.py`), tokenization before publish, redaction before every model call (`ModelGateway._guard`), detokenization only for PII-allowed identities (audited), recipient handles instead of emails in prompts |
| LLM03 Supply chain | ASI04 controls |
| LLM04 Data and model poisoning | ASI06 controls; eval cases harvested from incidents are disabled until reviewed (`harvest`) |
| LLM05 Improper output handling | schema validation of every output; catalog + params validation; SQL authorizer; outputs never executed as code |
| LLM06 Excessive agency | least-privilege tools, policy engine, autonomy ladder, approvals, kill switch |
| LLM07 System prompt leakage | no secrets in prompts (`check_secret_leak`); secrets referenced as `secret://` handles |
| LLM08 Vector and embedding weaknesses | retrieval is permission-trimmed by tenant and trust level (`KnowledgeBase.search`) |
| LLM09 Misinformation | groundedness gate (citations must be real evidence ids), abstain-and-escalate, calibrated judge, human feedback |
| LLM10 Unbounded consumption | per-run token/$ budgets, per-tenant daily quotas, step budgets, loop detection, bulkhead, SQL VM-step budget, bounded tool outputs |

## The lethal trifecta (Willison)

| Leg | Who has it | How it is broken |
|---|---|---|
| Access to private data | investigators, planner, analyst | read-only, scoped, tenant-trimmed |
| Exposure to untrusted content | investigators, planner, librarian | spotlighted, flagged, bounded |
| Ability to communicate externally | **only the Executor** (notifications) | recipients are handles; URLs must pass the egress allowlist; every send is audited |

No single agent holds all three legs.
