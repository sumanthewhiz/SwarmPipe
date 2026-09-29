"""Service agents: the Analyst (NL -> governed SQL, the "ask your data" surface), the Librarian
(document ingestion for grounding) and the Judge (LLM-as-judge used by the eval harness)."""
from __future__ import annotations

import re

from swarmpipe.agents.base import DEGRADE_ERRORS, Agent
from swarmpipe.governance.guardrails import scan_text
from swarmpipe.llm.types import DocSummaryOut, JudgeOut, PairwiseOut, SqlOut

_SELECT = re.compile(r"^\s*(with|select)\b", re.I)


class AnalystAgent(Agent):
    id, name, role, kind = "analyst", "Analyst", "analyst", "llm"
    description = "Answers natural-language questions with read-only SQL over published datasets, through the semantic layer."
    scopes = frozenset({"data:read:published"})
    tools = ("query_warehouse",)
    pii_access = True  # capability only: effective PII access still requires the USER to have it (intersection)
    skills = [{"id": "ask_data", "name": "Ask a data question", "description": "NL question -> governed SQL -> answer with sources",
               "tags": ["analytics", "nl2sql"], "examples": ["total revenue by region", "top 5 products by units sold"]}]

    def _tables(self, tenant: str, user) -> list[dict]:
        svc = self.svc
        out = []
        derived = (svc.settings.consumers or {}).get("derived") or {}
        for st in svc.db.query("SELECT dataset FROM dataset_state WHERE tenant=? AND published_version_id IS NOT NULL ORDER BY dataset", (tenant,)):
            ds = st["dataset"]
            c = svc.contracts.active(ds)
            if c:
                cols = [{"name": x["name"], "type": x.get("type", "string"), "pii": bool(x.get("pii"))} for x in c.get("columns", [])]
            elif ds in derived:
                df = svc.wh.read(svc.wh.view_name(tenant, ds), limit=1)
                cols = [{"name": n, "type": "any", "pii": False} for n in (df.columns if df is not None else [])]
            else:
                continue
            out.append({"name": ds, "columns": cols, "pii_masked": not user.pii_allowed})
        return out

    def ask_question(self, question: str, user, tenant: str) -> dict:
        svc = self.svc
        ident = self.identity.acting_for(user)
        with self.invoke("ask", tenant=tenant) as span:
            if not ident.can("data:read:published") or not ident.can_access_tenant(tenant):
                svc.audit.record(ident.describe(), "analyst.ask", tenant, "forbidden", {"question": question[:200]})
                return {"refused": True, "reason": f"{user.principal} may not read published data of tenant {tenant}"}
            flags = scan_text(question).to_dict() if scan_text(question).findings else None
            tables = self._tables(tenant, user)
            if not tables:
                return {"refused": True, "reason": "no published datasets yet"}
            glossary = svc.settings.glossary or {}
            ctx = {"question": question, "tables": tables, "metrics": glossary.get("metrics", {}), "dimensions": glossary.get("dimensions", {})}
            attempts = []
            for attempt in range(2):
                try:
                    resp = self.ask("analyst_sql", task="Write one read-only SQL query that answers the question.", context=ctx,
                                    schema=SqlOut, tenant=tenant, purpose=f"nl2sql_{attempt + 1}")
                except DEGRADE_ERRORS as exc:
                    return {"refused": True, "reason": f"analyst unavailable ({type(exc).__name__}): {exc}", "degraded": True}
                out: SqlOut = resp.parsed
                if out.refuse or not out.sql:
                    svc.audit.record(ident.describe(), "analyst.ask", tenant, "refused", {"question": question[:200], "reason": out.refusal_reason})
                    return {"refused": True, "reason": out.refusal_reason or "cannot answer from governed data", "model": resp.model}
                sql = out.sql.strip().rstrip(";")
                if not _SELECT.match(sql) or ";" in sql:
                    attempts.append({"sql": sql, "error": "only a single SELECT statement is allowed"})
                    ctx["previous_attempt"] = attempts[-1]
                    continue
                tctx = self.tool_ctx(tenant, on_behalf_of=user)
                res = self.call_tool("query_warehouse", {"sql": sql}, tctx)
                data = res.data if res.ok else None
                if res.ok and isinstance(data, dict) and data.get("ok"):
                    span.set_attrs({"swarmpipe.sql": sql[:300], "swarmpipe.rows": len(data["rows"])})
                    svc.audit.record(ident.describe(), "analyst.ask", tenant, "answered",
                                     {"question": question[:200], "sql": sql[:500], "rows": len(data["rows"]), "evidence_id": res.evidence_id})
                    return {"refused": False, "question": question, "sql": sql, "explanation": out.explanation, "columns": data["columns"],
                            "rows": data["rows"], "truncated": data["truncated"], "tables": data["tables"], "evidence_id": res.evidence_id,
                            "pii_detokenized": data.get("pii_detokenized", 0), "model": resp.model, "answer": self._summarize(data),
                            "question_flags": flags, "acting_as": ident.describe()}
                err = (data or {}).get("error") if isinstance(data, dict) else (res.error or {}).get("message")
                attempts.append({"sql": sql, "error": err, "denied": (data or {}).get("denied") if isinstance(data, dict) else None})
                ctx["previous_attempt"] = attempts[-1]
            svc.audit.record(ident.describe(), "analyst.ask", tenant, "failed", {"question": question[:200], "attempts": attempts})
            return {"refused": True, "reason": "query failed after repair", "attempts": attempts}

    @staticmethod
    def _summarize(data: dict) -> str:
        cols, rows = data["columns"], data["rows"]
        if not rows:
            return "No rows matched."
        if len(rows) == 1 and len(cols) == 1:
            return f"{cols[0]} = {rows[0][0]}"
        head = "; ".join(", ".join(f"{c}={v}" for c, v in zip(cols, r)) for r in rows[:5])
        return f"{len(rows)} row(s){' (truncated)' if data.get('truncated') else ''}: {head}"


class LibrarianAgent(Agent):
    id, name, role, kind = "librarian", "Librarian", "librarian", "llm"
    description = "Summarizes and classifies ingested documents for the knowledge base (untrusted until a human promotes them)."
    scopes = frozenset({"knowledge:write"})

    def summarize(self, text: str, file_name: str, tenant: str, run_id: str) -> dict:
        with self.invoke("summarize", file=file_name):
            try:
                known = sorted({c["dataset"] for c in self.svc.contracts.all_active()})
                resp = self.ask("librarian", task="Summarize and classify this document.", context={"file_name": file_name, "known_datasets": known},
                                untrusted={"document": text}, schema=DocSummaryOut, tenant=tenant, run_id=run_id)
                return {**resp.parsed.model_dump(), "model": resp.model, "degraded": False}
            except DEGRADE_ERRORS:
                first = next((ln.strip("# ").strip() for ln in text.splitlines() if ln.strip()), file_name)
                return {"title": first[:90], "doc_type": "runbook" if "runbook" in text.lower() else "note",
                        "summary": text[:200], "entities": [], "model": None, "degraded": True}


class JudgeAgent(Agent):
    id, name, role, kind = "judge", "Judge", "judge", "llm"
    description = "LLM-as-judge for explanation quality; must be calibrated against human labels before its scores are trusted."
    scopes = frozenset()

    def score(self, candidate: str, reference: dict, version: str | None = None, tenant: str = "default") -> dict:
        resp = self.ask("judge", task="Score the candidate explanation.", context={"rubric": ["correctness", "grounding", "actionability"],
                                                                                   "reference": reference, "candidate": candidate},
                        schema=JudgeOut, tenant=tenant, version=version, purpose="judge")
        return {**resp.parsed.model_dump(), "model": resp.model}

    def pairwise(self, a: str, b: str, reference: dict, tenant: str = "default") -> dict:
        resp = self.ask("judge_pairwise", task="Which explanation is better?", context={"reference": reference, "A": a, "B": b},
                        schema=PairwiseOut, tenant=tenant, purpose="judge_pairwise")
        return resp.parsed.model_dump()
