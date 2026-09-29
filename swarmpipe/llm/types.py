"""Typed contracts for model calls: request/response envelopes and every agent's output schema.

Output schemas are pydantic models: they are rendered into the prompt as JSON Schema, and every
model response is validated against them (enums, ranges, required fields). A response that fails
validation triggers the gateway's repair loop, then fallback, then deterministic degradation."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from pydantic import BaseModel, Field

Category = Literal[
    "schema_change_upstream", "truncated_extract", "duplicate_delivery", "unit_or_scale_change",
    "data_quality_regression", "late_or_missing_delivery", "stale_data_resent", "pii_exposure",
    "malicious_content", "referential_integrity_break", "out_of_band_modification", "malformed_input", "pipeline_bug", "unknown"]
CATEGORIES = list(Category.__args__)  # type: ignore[attr-defined]


class RouterOut(BaseModel):
    kind: Literal["tabular", "document", "unsupported"]
    confidence: float = Field(ge=0, le=1)
    reason: str = ""


class ColumnSemantic(BaseModel):
    name: str
    semantic_type: Literal["identifier", "email", "phone", "person_name", "date", "currency_amount", "quantity", "price",
                           "category", "free_text", "code", "boolean", "number", "unknown"]
    glossary_term: str | None = None
    is_pii: bool = False
    confidence: float = Field(ge=0, le=1, default=0.5)


class SemanticTypingOut(BaseModel):
    columns: list[ColumnSemantic]


class MappingItem(BaseModel):
    source_column: str
    target_column: str
    confidence: float = Field(ge=0, le=1)
    rationale: str = ""


class MappingOut(BaseModel):
    mappings: list[MappingItem] = Field(default_factory=list)
    unmapped: list[str] = Field(default_factory=list)


class ContractColumnProposal(BaseModel):
    name: str
    type: Literal["string", "int", "float", "date", "bool"]
    required: bool = False
    unique: bool = False
    pii: str | None = None
    description: str = ""


class ContractProposalOut(BaseModel):
    dataset: str
    description: str
    primary_key: list[str]
    classification: Literal["public", "internal", "confidential", "restricted"]
    columns: list[ContractColumnProposal]
    freshness_expected_every_min: int | None = None
    rationale: str = ""


class CritiqueOut(BaseModel):
    verdict: Literal["approve", "revise", "reject"]
    score: float = Field(ge=0, le=1)
    issues: list[str] = Field(default_factory=list)
    suggestions: list[str] = Field(default_factory=list)


class DateFormatOut(BaseModel):
    format: str
    confidence: float = Field(ge=0, le=1)
    rationale: str = ""


class ReactStep(BaseModel):
    action: Literal["call_tool", "final"]
    tool: str | None = None
    args: dict = Field(default_factory=dict)
    thought: str = ""
    answer: dict | None = None


class Finding(BaseModel):
    category: Category
    summary: str
    confidence: float = Field(ge=0, le=1)
    evidence_ids: list[str] = Field(default_factory=list)


class Alternative(BaseModel):
    category: Category
    confidence: float = Field(ge=0, le=1)


class Diagnosis(BaseModel):
    root_cause_category: Category
    summary: str
    confidence: float = Field(ge=0, le=1)
    citations: list[str] = Field(default_factory=list)
    alternatives: list[Alternative] = Field(default_factory=list)
    abstain: bool = False
    next_checks: list[str] = Field(default_factory=list)


class ProposedAction(BaseModel):
    action: str
    params: dict = Field(default_factory=dict)
    rationale: str
    expected_outcome: str = ""
    citations: list[str] = Field(default_factory=list)


class PlanOut(BaseModel):
    proposals: list[ProposedAction]
    notes: str = ""


class SqlOut(BaseModel):
    sql: str | None = None
    explanation: str = ""
    refuse: bool = False
    refusal_reason: str = ""


class DocSummaryOut(BaseModel):
    title: str
    doc_type: Literal["runbook", "policy", "report", "note", "other"]
    summary: str
    entities: list[str] = Field(default_factory=list)


class PostmortemOut(BaseModel):
    summary: str
    root_cause: str
    what_went_well: list[str] = Field(default_factory=list)
    what_to_improve: list[str] = Field(default_factory=list)
    lesson: str
    eval_case: dict = Field(default_factory=dict)


class JudgeOut(BaseModel):
    score: int = Field(ge=1, le=5)
    correctness: int = Field(ge=1, le=5)
    grounding: int = Field(ge=1, le=5)
    actionability: int = Field(ge=1, le=5)
    rationale: str = ""


class PairwiseOut(BaseModel):
    winner: Literal["A", "B", "tie"]
    rationale: str = ""


SCHEMAS: dict[str, type[BaseModel]] = {
    "RouterOut": RouterOut, "SemanticTypingOut": SemanticTypingOut, "MappingOut": MappingOut,
    "ContractProposalOut": ContractProposalOut, "CritiqueOut": CritiqueOut, "DateFormatOut": DateFormatOut,
    "ReactStep": ReactStep, "Finding": Finding, "Diagnosis": Diagnosis, "PlanOut": PlanOut, "SqlOut": SqlOut,
    "DocSummaryOut": DocSummaryOut, "PostmortemOut": PostmortemOut, "JudgeOut": JudgeOut, "PairwiseOut": PairwiseOut,
}


@dataclass
class LLMRequest:
    role: str
    prompt_id: str
    prompt_version: str
    prompt_hash: str
    messages: list[dict]
    schema: type[BaseModel] | None = None
    temperature: float = 0.0
    max_output_tokens: int = 900
    agent: str = "unknown"
    tenant: str = "default"
    run_id: str | None = None
    incident_id: str | None = None
    purpose: str = ""
    cacheable: bool = True


@dataclass
class LLMResponse:
    text: str
    parsed: Any
    model: str
    provider: str
    input_tokens: int
    output_tokens: int
    cost_usd: float
    latency_ms: float
    cached: bool = False
    attempts: int = 1
    repairs: int = 0
    fallback_chain: list[str] = field(default_factory=list)
    call_id: str = ""
    degraded: bool = False
