"""PII detection, classification and masking/tokenization (defense against sensitive information
disclosure).

- Detection is deterministic (regex + validators such as Luhn), never delegated to a model.
- Published tables contain tokens (`tok_email_ab12...`) instead of raw values; the raw value lives in
  a vault table and is only detokenized for identities with PII access (audited, permission-trimmed
  at query time). Tokens are deterministic per value, so joins across datasets still work.
- Nothing raw is ever sent to a model: `redact_text` is applied to every prompt (guardrails).
"""
from __future__ import annotations

import hashlib
import hmac
import re
import threading

import pandas as pd

from swarmpipe.core.util import iso

PII_PATTERNS: dict[str, re.Pattern] = {
    "email": re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
    "card": re.compile(r"\b(?:\d[ -]?){12,18}\d\b"),
    "phone": re.compile(r"(?:\+91[\s-]?)?\b[6-9]\d{4}[\s-]?\d{5}\b|\+\d{1,3}[\s-]?\d{3}[\s-]?\d{3}[\s-]?\d{3,4}\b"),
    "pan": re.compile(r"\b[A-Z]{5}\d{4}[A-Z]\b"),
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
}
_TOKEN_RE = re.compile(r"^tok_[a-z_]+_[0-9a-f]{12}$")


def luhn_ok(number: str) -> bool:
    digits = [int(c) for c in number if c.isdigit()]
    if not 13 <= len(digits) <= 19:
        return False
    total, parity = 0, len(digits) % 2
    for i, d in enumerate(digits):
        if i % 2 == parity:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def find_pii(text: str) -> list[tuple[str, str]]:
    """Return [(pii_type, matched_text)] found in a free-text value."""
    if not text or not isinstance(text, str):
        return []
    found: list[tuple[str, str]] = []
    for kind, pat in PII_PATTERNS.items():
        for m in pat.finditer(text):
            value = m.group(0)
            if kind == "card" and not luhn_ok(value):
                continue
            if kind == "phone" and luhn_ok(value):
                continue
            found.append((kind, value))
    return found


def redact_text(text: str) -> tuple[str, int]:
    """Replace PII in free text with typed placeholders. Returns (text, n_redactions)."""
    if not text:
        return text, 0
    n = 0
    out = text
    for kind, value in sorted(find_pii(text), key=lambda kv: -len(kv[1])):
        if value in out:
            out = out.replace(value, f"[{kind.upper()}]")
            n += 1
    return out, n


def is_token(value) -> bool:
    return isinstance(value, str) and bool(_TOKEN_RE.match(value))


def scan_columns(df: pd.DataFrame, sample: int = 2000) -> dict[str, dict]:
    """Column-level PII scan: whole-column PII (e.g. an email column) vs PII embedded in free text."""
    results: dict[str, dict] = {}
    for col in df.columns:
        s = df[col].dropna()
        if s.empty:
            continue
        s = s.astype(str)
        if len(s) > sample:
            s = s.sample(sample, random_state=7)
        hits: dict[str, int] = {}
        full: dict[str, int] = {}
        for v in s:
            if is_token(v):
                continue
            for kind, value in find_pii(v):
                hits[kind] = hits.get(kind, 0) + 1
                if value.strip() == v.strip():
                    full[kind] = full.get(kind, 0) + 1
        if not hits:
            continue
        kind = max(hits, key=hits.get)
        n = len(s)
        results[col] = {
            "pii_type": kind,
            "match_rate": round(hits[kind] / n, 4),
            "full_match_rate": round(full.get(kind, 0) / n, 4),
            "values_with_pii": hits[kind],
            "mode": "column" if full.get(kind, 0) / n >= 0.6 else "embedded",
            "all_types": hits,
        }
    return results


class PiiVault:
    """Deterministic tokenization with a restricted vault for authorized detokenization."""

    def __init__(self, db, key: bytes):
        self.db = db
        self.key = key
        self._pending: dict[str, tuple] = {}
        self._lock = threading.Lock()

    def token_for(self, value: str, pii_type: str, tenant: str) -> str:
        digest = hmac.new(self.key, f"{tenant}|{value}".encode("utf-8"), hashlib.sha256).hexdigest()[:12]
        token = f"tok_{pii_type}_{digest}"
        with self._lock:
            self._pending.setdefault(token, (token, tenant, pii_type, value, iso()))
        return token

    def flush(self) -> None:
        with self._lock:
            rows, self._pending = list(self._pending.values()), {}
        if rows:
            self.db.executemany("INSERT OR IGNORE INTO pii_vault(token, tenant, pii_type, value, created_at) VALUES(?,?,?,?,?)", rows)

    def mask_series(self, s: pd.Series, pii_type: str, method: str, tenant: str) -> pd.Series:
        cache: dict[str, str] = {}

        def _one(v):
            if v is None or (isinstance(v, float) and pd.isna(v)) or v is pd.NA:
                return v
            v = str(v)
            if is_token(v):
                return v
            if v in cache:
                return cache[v]
            if method == "redact":
                out = f"[REDACTED:{pii_type}]"
            elif method == "last4":
                digits = "".join(c for c in v if c.isdigit())
                out = "****" + digits[-4:]
            else:
                out = self.token_for(v, pii_type, tenant)
            cache[v] = out
            return out

        out = s.map(_one)
        self.flush()
        return out

    def mask_embedded(self, s: pd.Series, tenant: str) -> tuple[pd.Series, int]:
        """Tokenize PII found inside free-text values (e.g. a card number typed into 'notes')."""
        count = 0

        def _one(v):
            nonlocal count
            if not isinstance(v, str):
                return v
            out = v
            for kind, value in find_pii(v):
                out = out.replace(value, self.token_for(value, kind, tenant))
                count += 1
            return out

        res = s.map(_one)
        self.flush()
        return res, count

    def detokenize(self, token: str) -> str | None:
        row = self.db.query_one("SELECT value FROM pii_vault WHERE token=?", (token,))
        return row["value"] if row else None
