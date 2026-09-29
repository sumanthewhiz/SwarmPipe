"""Guardrails (OWASP LLM01/LLM02/LLM06, ASI01/ASI02/ASI06).

Layered, deterministic defenses around every model call and tool call:
  1. Injection detection: heuristic scoring of instructions hidden in data (file names, cells,
     documents, tool outputs). It *flags*; it never "cleans" silently.
  2. Spotlighting: untrusted content is wrapped in randomized DATA boundaries and the system
     prompt says content inside them is data, never instructions (see llm/prompts.py).
  3. PII redaction: prompts are redacted before they leave the process.
  4. Secret-leak check: prompts containing known secret values are blocked.
  5. Egress allowlist: outbound network destinations must be allowlisted.
Detection is imperfect by design; the architecture assumes injection will sometimes succeed and
makes sure a fooled model still cannot act (policy engine + action catalog + approvals).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from urllib.parse import urlparse

import pandas as pd

from swarmpipe.core.errors import GuardrailViolation
from swarmpipe.data.pii import redact_text

_PATTERNS: list[tuple[str, float, re.Pattern]] = [
    ("override_instructions", 0.9, re.compile(r"\b(ignore|forget|override)\s+(all\s+|any\s+|the\s+|your\s+)?(previous|prior|above|earlier|existing)\s+(instructions|prompts?|rules|guidelines)", re.I)),
    ("disregard", 0.8, re.compile(r"\bdisregard\s+(all\s+|the\s+|any\s+)?(previous|prior|above|your|safety)\b", re.I)),
    ("role_reassignment", 0.5, re.compile(r"\byou\s+are\s+now\b|\bact\s+as\s+(an?\s+)?(admin|administrator|system|root)\b", re.I)),
    ("system_prompt_reference", 0.55, re.compile(r"\b(system|developer)\s+(prompt|message|instructions?)\b", re.I)),
    ("fake_markup", 0.7, re.compile(r"</?\s*(system|assistant|instructions?|im_start|im_end)\s*>|\[\[\s*system\s*\]\]", re.I)),
    ("privileged_mode", 0.6, re.compile(r"\b(maintenance|admin|god|developer|jailbreak|dan)\s+mode\b", re.I)),
    ("tool_invocation", 0.5, re.compile(r"\b(call|invoke|execute|run|use)\s+(the\s+)?(tool|function|action)\b", re.I)),
    ("dangerous_action_request", 0.8, re.compile(r"\bforce[_\s-]?publish\b|\bmark\s+(all|every|each)\b.{0,40}\b(ok|success|passed|valid)\b|\bset\s+(all\s+|every\s+)?(jobs?|datasets?|checks?)\s+to\s+ok\b|\bskip\s+(all\s+)?(checks|validation)\b", re.I)),
    ("exfiltration", 0.8, re.compile(r"\b(send|post|upload|exfiltrate|forward|email|copy)\b.{0,60}\bhttps?://", re.I)),
    ("concealment", 0.5, re.compile(r"\bdo\s+not\s+(tell|inform|alert|notify|log|mention)\b", re.I)),
    ("new_instructions", 0.5, re.compile(r"\b(new|updated|real)\s+instructions?\b", re.I)),
    ("destructive_request", 0.6, re.compile(r"\b(delete|drop|truncate|wipe)\s+(all\s+|the\s+|every\s+)?(tables?|datasets?|data|records)\b", re.I)),
]


@dataclass
class InjectionReport:
    score: float = 0.0
    findings: list[dict] = field(default_factory=list)

    @property
    def suspicious(self) -> bool:
        return self.score >= 0.6

    def to_dict(self) -> dict:
        return {"score": round(self.score, 3), "suspicious": self.suspicious, "findings": self.findings}


def scan_text(text: str) -> InjectionReport:
    rep = InjectionReport()
    if not text or not isinstance(text, str):
        return rep
    keep = 1.0
    for pid, weight, pat in _PATTERNS:
        m = pat.search(text)
        if m:
            keep *= (1.0 - weight)
            rep.findings.append({"pattern": pid, "weight": weight, "match": m.group(0)[:120]})
    rep.score = 1.0 - keep
    return rep


def scan_frame(df: pd.DataFrame, max_cells: int = 50000) -> list[dict]:
    """Scan string cells for injected instructions. Returns [{row, column, score, patterns, snippet}]."""
    hits: list[dict] = []
    budget = max_cells
    for col in df.columns:
        s = df[col].dropna()
        if s.empty:
            continue
        s = s.astype(str)
        s = s[s.str.len() >= 16]
        for idx, v in s.head(budget).items():
            rep = scan_text(v)
            if rep.suspicious:
                hits.append({"row": int(idx) if str(idx).isdigit() else str(idx),
                             "column": str(col), "score": round(rep.score, 3),
                             "patterns": [f["pattern"] for f in rep.findings], "snippet": v[:160]})
        budget -= min(budget, len(s))
        if budget <= 0:
            break
    for col in df.columns:
        rep = scan_text(str(col))
        if rep.suspicious:
            hits.append({"row": "header", "column": str(col), "score": round(rep.score, 3),
                         "patterns": [f["pattern"] for f in rep.findings], "snippet": str(col)[:160]})
    return hits


def redact_for_model(text: str) -> tuple[str, int]:
    return redact_text(text)


def check_secret_leak(text: str, known_secret_values: list[str]) -> None:
    for v in known_secret_values:
        if v and v in text:
            raise GuardrailViolation("prompt contains a secret value; refusing to send it to a model",
                                     code="SECRET_LEAK")


def check_egress(url: str, allowlist: list[str]) -> tuple[bool, str]:
    host = (urlparse(url).hostname or "").lower()
    for allowed in allowlist:
        a = allowed.lower()
        if host == a or host.endswith("." + a):
            return True, host
    return False, host
