"""Pipeline agents: the data-plane swarm that ingests one file.

Router (LLM, cheap tier, router pattern) -> PrivacyGuard (deterministic) -> Profiler (stats +
LLM semantic typing) -> ContractSteward (+ Critic: evaluator-optimizer) -> Transformer (+ verified
LLM date-format inference) -> DataAssurance (deterministic checks, circuit breaker) -> Publisher.
"""
from __future__ import annotations

import difflib
import re
from datetime import datetime

import pandas as pd

from swarmpipe.agents.base import DEGRADE_ERRORS, Agent
from swarmpipe.data.pii import redact_text, scan_columns
from swarmpipe.data.profiling import infer_type, profile_frame, schema_diff
from swarmpipe.data.quality import QualityInput, run_checks
from swarmpipe.data.transforms import apply_contract, date_failure_samples
from swarmpipe.governance.guardrails import scan_frame, scan_text
from swarmpipe.llm.types import ContractProposalOut, CritiqueOut, DateFormatOut, MappingOut, RouterOut, SemanticTypingOut


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


class RouterAgent(Agent):
    id, name, role, kind = "router", "Router", "router", "hybrid"
    description = "Classifies each arriving file (tabular / document / unsupported) and routes it to the right workflow."
    scopes = frozenset({"data:read"})

    def run(self, file_name: str, sniff, tenant: str, run_id: str) -> dict:
        det = {"tabular": "tabular", "document": "document"}.get(sniff.kind_hint, "unsupported")
        with self.invoke("route", file=file_name) as span:
            try:
                resp = self.ask("router", task="Classify this arriving file.",
                                context={"file_name": file_name, "extension": sniff.extension, "size": sniff.size, "sniff": sniff.to_dict()},
                                untrusted={"first_lines": "\n".join(sniff.sample_lines[:8]), "file_name": file_name},
                                schema=RouterOut, tenant=tenant, run_id=run_id)
                out: RouterOut = resp.parsed
                kind, source = out.kind, "llm"
                if sniff.kind_hint in ("binary", "unknown"):
                    kind, source = "unsupported", "deterministic_guard"
                elif sniff.extension in (".xlsx", ".xls") and kind != "tabular":
                    kind, source = "tabular", "deterministic_guard"
                elif kind != det and out.confidence < 0.8:
                    kind, source = det, "llm_overridden_low_confidence"
                result = {"kind": kind, "confidence": out.confidence, "source": source, "model": resp.model, "reason": out.reason}
            except DEGRADE_ERRORS as exc:
                result = {"kind": det, "confidence": 0.7, "source": "deterministic_fallback", "model": None, "reason": str(exc)[:200],
                          "degraded": True}
            span.set_attrs({"swarmpipe.route": result["kind"], "swarmpipe.route_source": result["source"]})
            return result


class PrivacyGuardAgent(Agent):
    id, name, kind = "privacy_guard", "Privacy & Content Guard", "deterministic"
    description = "Deterministic PII detection/classification and prompt-injection scanning of file names and cells."
    scopes = frozenset({"data:read"})

    def run(self, df: pd.DataFrame, contract: dict | None, file_name: str) -> dict:
        with self.invoke("scan") as span:
            pii = scan_columns(df)
            declared = {c["name"] for c in (contract or {}).get("columns", []) if c.get("pii")}
            mapped_decl = set()
            for col in df.columns:
                if _norm(col) in {_norm(d) for d in declared}:
                    mapped_decl.add(col)
            undeclared = {k: v for k, v in pii.items() if k not in mapped_decl}
            hits = scan_frame(df)
            fname = scan_text(file_name)
            masked = sorted(mapped_decl | {k for k, v in pii.items() if v["mode"] == "column"})
            span.set_attrs({"swarmpipe.pii_columns": len(pii), "swarmpipe.injection_hits": len(hits)})
            return {"pii": pii, "undeclared": undeclared, "injection_hits": hits, "filename_injection": fname.to_dict(),
                    "masked_columns": masked}


class ProfilerAgent(Agent):
    id, name, role, kind = "profiler", "Profiler", "profiler", "hybrid"
    description = "Statistical profiling plus LLM semantic typing against the business glossary."
    scopes = frozenset({"data:read"})

    def run(self, df: pd.DataFrame, privacy: dict, tenant: str, run_id: str) -> dict:
        with self.invoke("profile", rows=len(df)) as span:
            prof = profile_frame(df, masked_columns=set(privacy.get("masked_columns", [])))
            pii = privacy.get("pii", {})
            cols = [{"name": c["name"], "inferred_type": c["inferred_type"], "null_rate": c["null_rate"],
                     "distinct_ratio": c["distinct_ratio"], "avg_len": c.get("avg_len"), "pii_type": (pii.get(c["name"]) or {}).get("pii_type"),
                     "samples": [redact_text(str(s))[0] for s in c.get("samples", [])][:4]} for c in prof["columns"]]
            terms = {k: {"aliases": v.get("aliases", []), "description": v.get("description", "")}
                     for k, v in (self.svc.settings.glossary.get("terms") or {}).items()}
            degraded = False
            try:
                resp = self.ask("profiler", task="Assign semantic types and glossary terms to these columns.",
                                context={"columns": cols, "glossary_terms": terms}, schema=SemanticTypingOut, tenant=tenant, run_id=run_id)
                sem = {c.name: c.model_dump() for c in resp.parsed.columns}
            except DEGRADE_ERRORS:
                degraded = True
                alias = {_norm(a): t for t, s in terms.items() for a in [t, *s["aliases"]]}
                sem = {c["name"]: {"name": c["name"], "semantic_type": "unknown", "glossary_term": alias.get(_norm(c["name"])),
                                   "is_pii": bool(c["pii_type"]), "confidence": 0.3} for c in cols}
            for c in prof["columns"]:
                s = sem.get(c["name"], {})
                c["semantic_type"] = s.get("semantic_type")
                c["glossary_term"] = s.get("glossary_term")
                c["is_pii"] = bool(s.get("is_pii")) or c["name"] in pii
                c["pii_type"] = (pii.get(c["name"]) or {}).get("pii_type")
            span.set_attrs({"swarmpipe.degraded": degraded})
            return {"profile": prof, "degraded": degraded}


class CriticAgent(Agent):
    id, name, role, kind = "critic", "Critic", "critic", "llm"
    description = "Independent evaluator (evaluator-optimizer pattern) for mappings, contract proposals and remediation plans."
    scopes = frozenset({"data:read"})

    def review(self, subject_type: str, proposal: dict, evidence: dict, tenant: str, run_id: str | None = None,
               incident_id: str | None = None) -> dict:
        if self.svc.flags.get("feature.critic_review", True) is False:
            return {"verdict": "skipped", "score": None, "issues": [], "suggestions": [], "reason": "critic disabled (ablation)"}
        with self.invoke("review", subject=subject_type):
            try:
                resp = self.ask("critic", task=f"Critique this {subject_type.replace('_', ' ')}.",
                                context={"subject_type": subject_type, "proposal": proposal, "evidence": evidence,
                                         "criteria": ["supported by independent evidence", "consistent", "safe", "minimal"]},
                                schema=CritiqueOut, tenant=tenant, run_id=run_id, incident_id=incident_id)
                return {**resp.parsed.model_dump(), "model": resp.model}
            except DEGRADE_ERRORS as exc:
                return {"verdict": "unavailable", "score": None, "issues": [], "suggestions": [], "reason": str(exc)[:200]}


class ContractStewardAgent(Agent):
    id, name, role, kind = "steward", "Contract Steward", "steward", "hybrid"
    description = "Detects schema drift against the data contract, proposes rename mappings and new contracts (reviewed by the Critic)."
    scopes = frozenset({"data:read", "contract:propose"})

    def _candidates(self, df: pd.DataFrame, contract: dict, diff: dict) -> list[dict]:
        specs = {c["name"]: c for c in contract.get("columns", [])}
        terms = self.svc.settings.glossary.get("terms", {})
        out = []
        for miss in diff["missing_required"] + diff["missing_optional"]:
            spec = specs[miss]
            aliases = {_norm(a) for a in [*spec.get("aliases", []), *terms.get(miss, {}).get("aliases", [])]}
            for new in diff["new_columns"]:
                s = df[new].dropna().astype(str).head(200)
                t, _, _ = infer_type(df[new])
                compat = {"string": True, "int": t in ("int",), "float": t in ("int", "float"), "date": t == "date",
                          "bool": t == "bool"}.get(spec.get("type", "string"), True)
                rate = None
                if spec.get("regex") and len(s):
                    rate = round(float(s.str.match(spec["regex"]).mean()), 3)
                out.append({"source_column": new, "target_column": miss, "name_similarity": round(difflib.SequenceMatcher(None, _norm(miss), _norm(new)).ratio(), 3),
                            "glossary_alias": _norm(new) in aliases, "type_compatible": bool(compat), "regex_match_rate": rate,
                            "inferred_type": t, "target_type": spec.get("type"), "samples": [redact_text(v)[0] for v in s.head(3).tolist()]})
        return out

    def check(self, df: pd.DataFrame, contract: dict, tenant: str, run_id: str) -> dict:
        with self.invoke("contract_check", dataset=contract["dataset"]) as span:
            diff = schema_diff(contract, list(df.columns), self.svc.contracts.alias_map(contract))
            out = {"dataset": contract["dataset"], "contract_version": contract.get("version"), "diff": diff}
            if not (diff["missing_required"] or (diff["missing_optional"] and diff["new_columns"])) or not diff["new_columns"]:
                return out
            cands = self._candidates(df, contract, diff)
            missing = [{"name": m, "type": next(c.get("type") for c in contract["columns"] if c["name"] == m),
                        "regex": next(c.get("regex") for c in contract["columns"] if c["name"] == m)}
                       for m in diff["missing_required"] + diff["missing_optional"]]
            feedback: list[str] = []
            proposal, critique = None, None
            for round_ in range(2):
                try:
                    resp = self.ask("steward_mapping", task="Propose column mappings for this schema drift.",
                                    context={"dataset": contract["dataset"], "missing_columns": missing, "candidates": cands,
                                             "critic_feedback": feedback or None},
                                    schema=MappingOut, tenant=tenant, run_id=run_id, purpose=f"mapping_round_{round_ + 1}")
                    proposal = resp.parsed.model_dump()
                except DEGRADE_ERRORS:
                    best = {}
                    for c in sorted(cands, key=lambda c: (-(c["regex_match_rate"] or 0), -c["name_similarity"])):
                        if c["target_column"] not in best.values() and c["source_column"] not in best and \
                                (c["glossary_alias"] or (c["regex_match_rate"] or 0) >= 0.9):
                            best[c["source_column"]] = c["target_column"]
                    proposal = {"mappings": [{"source_column": s, "target_column": t, "confidence": 0.7, "rationale": "deterministic fallback"}
                                             for s, t in best.items()], "unmapped": [], "degraded": True}
                critique = self.svc.agents.critic.review("column_mapping", proposal, {"candidates": cands}, tenant, run_id)
                if critique["verdict"] != "revise":
                    break
                feedback = critique.get("issues", [])
            accepted = [m for m in proposal.get("mappings", []) if m.get("confidence", 0) >= 0.5]
            out["mapping_proposal"] = {m["source_column"]: m["target_column"] for m in accepted}
            out["mapping_confidence"] = min((m["confidence"] for m in accepted), default=0.0)
            out["mapping_critique"] = critique
            out["candidates"] = cands
            span.set_attrs({"swarmpipe.mappings": len(accepted), "swarmpipe.critic_verdict": critique.get("verdict") if critique else None})
            return out

    def propose_contract(self, dataset: str, file_name: str, df: pd.DataFrame, profile: dict, privacy: dict, tenant: str, run_id: str) -> dict:
        with self.invoke("propose_contract", dataset=dataset):
            pii = privacy.get("pii", {})
            cols = [{"name": c["name"], "inferred_type": c["inferred_type"], "null_rate": c["null_rate"], "distinct_ratio": c["distinct_ratio"],
                     "pii_type": (pii.get(c["name"]) or {}).get("pii_type"), "samples": [redact_text(str(s))[0] for s in c.get("samples", [])][:3]}
                    for c in profile["columns"]]
            feedback: list[str] = []
            prop, critique = None, None
            for round_ in range(2):
                try:
                    resp = self.ask("steward_contract", task="Propose a data contract for this new dataset.",
                                    context={"dataset": dataset, "file_name": file_name, "columns": cols, "critic_feedback": feedback or None},
                                    schema=ContractProposalOut, tenant=tenant, run_id=run_id)
                    prop = resp.parsed.model_dump()
                except DEGRADE_ERRORS:
                    prop = {"dataset": dataset, "description": f"Onboarded from {file_name}", "primary_key": [],
                            "classification": "confidential" if pii else "internal",
                            "columns": [{"name": c["name"], "type": {"int": "int", "float": "float", "date": "date"}.get(c["inferred_type"], "string"),
                                         "required": c["null_rate"] == 0, "unique": False, "pii": c["pii_type"]} for c in cols],
                            "freshness_expected_every_min": None, "degraded": True}
                critique = self.svc.agents.critic.review("contract_proposal", prop, {"columns": cols}, tenant, run_id)
                if critique["verdict"] != "revise":
                    break
                feedback = critique.get("issues", [])
            stem = re.sub(r"[_\-]?\d{4}[-_]?\d{2}[-_]?\d{2}.*$|[_\-]?q\d.*$", "", file_name.rsplit(".", 1)[0].lower())
            contract = {
                "dataset": dataset, "description": prop.get("description"), "owner": "unassigned@contoso.example",
                "source_owner": "unknown@contoso.example", "match": [f"{stem}*{file_name[file_name.rfind('.'):].lower()}"],
                "classification": prop.get("classification"), "load_mode": "replace", "primary_key": prop.get("primary_key") or [],
                "volume": {"min_rows": 1, "max_drop_pct": 80, "max_growth_pct": 500, "baseline_versions": 3},
                "columns": [{k: v for k, v in {"name": c["name"], "type": c["type"], "required": c.get("required", False),
                                                "unique": c.get("unique") or None, "pii": c.get("pii"),
                                                "mask": "tokenize" if c.get("pii") else None}.items() if v is not None}
                            for c in prop.get("columns", [])],
            }
            if prop.get("freshness_expected_every_min"):
                contract["freshness"] = {"expected_every_min": prop["freshness_expected_every_min"], "grace_min": 120}
            return {"contract": contract, "critique": critique, "proposal": prop}


class TransformerAgent(Agent):
    id, name, role, kind = "transformer", "Transformer", "transformer", "hybrid"
    description = "Applies the contract (mapping, typing, rejects, dedupe); infers unknown date formats and VERIFIES them before use."
    scopes = frozenset({"data:read"})

    def run(self, df: pd.DataFrame, contract: dict, mapping_override: dict | None, tenant: str, run_id: str):
        with self.invoke("transform", dataset=contract["dataset"]) as span:
            alias_map = self.svc.contracts.alias_map(contract)
            tr = apply_contract(df, contract, alias_map, mapping_override)
            info = {"date_format_fixes": {}, "rejected_fixes": {}}
            if self.svc.settings.features.get("llm_date_format_inference", True) and tr.renamed is not None:
                samples = date_failure_samples(tr.renamed, contract)
                fixes = {}
                for col, vals in samples.items():
                    spec = next(c for c in contract["columns"] if c["name"] == col)
                    try:
                        resp = self.ask("date_format", task=f"Infer the date format of column {col}.",
                                        context={"column": col, "samples": vals, "contract_formats": spec.get("formats", [])},
                                        schema=DateFormatOut, tenant=tenant, run_id=run_id)
                        fmt = resp.parsed.format
                    except DEGRADE_ERRORS:
                        continue
                    ok = 0
                    for v in vals:
                        try:
                            datetime.strptime(str(v).strip(), fmt)
                            ok += 1
                        except ValueError:
                            pass
                    rate = ok / len(vals) if vals else 0
                    if rate >= 0.95:
                        fixes[col] = fmt
                        info["date_format_fixes"][col] = {"format": fmt, "verified_parse_rate": round(rate, 3)}
                    else:
                        info["rejected_fixes"][col] = {"format": fmt, "verified_parse_rate": round(rate, 3)}
                if fixes:
                    tr = apply_contract(df, contract, alias_map, mapping_override, date_hints=fixes)
            span.set_attrs({"swarmpipe.rows_in": tr.stats["rows_in"], "swarmpipe.rows_out": tr.stats["rows_out"],
                            "swarmpipe.rejected": tr.stats["rejected"]})
            return tr, info


class DataAssuranceAgent(Agent):
    id, name, kind = "data_assurance", "Data Assurance", "deterministic"
    description = "Runs contract checks (volume, schema, quality, freshness, drift, reconciliation, privacy) and trips the circuit breaker."
    scopes = frozenset({"data:read"})

    def run(self, contract: dict, tr, privacy: dict, tenant: str, dataset: str):
        with self.invoke("checks", dataset=dataset) as span:
            vol = contract.get("volume") or {}
            base = self.svc.publishing.baseline(tenant, dataset, int(vol.get("baseline_versions", 5)))
            wh = self.svc.wh

            def ref_values(ds: str, col: str):
                return wh.column_values(wh.view_name(tenant, ds), col)

            st = tr.stats
            raw_totals = {c: st["raw_totals"][c] for c in contract.get("control_totals", []) if c in st.get("raw_totals", {})}
            q = QualityInput(contract=contract, df=tr.df, stats=st, raw_control_totals=raw_totals, diff=tr.diff,
                             pii_findings=privacy.get("undeclared", {}), injection_hits=[h for h in privacy.get("injection_hits", []) if h["row"] != "header"],
                             baseline_batches=[b["batch_rows"] for b in base if b.get("batch_rows")],
                             baseline_profile=base[0]["profile"] if base else None, reference_values=ref_values,
                             today=self.svc.clock.today(), rejected_control_totals=st.get("rejected_totals", {}),
                             duplicate_control_totals=st.get("duplicate_totals", {}))
            outcome = run_checks(q)
            span.set_attrs({"swarmpipe.decision": outcome.decision, "swarmpipe.failed_checks": len(outcome.failed),
                            "swarmpipe.warnings": len(outcome.warnings)})
            return outcome


class PublisherAgent(Agent):
    id, name, kind = "publisher", "Publisher", "deterministic"
    description = "Masks PII per contract, stages immutable versions, publishes (blue/green) or quarantines."
    scopes = frozenset({"warehouse:write"})

    def mask(self, df: pd.DataFrame, contract: dict, privacy: dict, tenant: str) -> tuple[pd.DataFrame, dict]:
        out = df.copy()
        report = {"tokenized_columns": [], "embedded_tokens": 0}
        for spec in contract.get("columns", []):
            if spec.get("pii") and spec["name"] in out.columns:
                out[spec["name"]] = self.svc.pii.mask_series(out[spec["name"]], spec["pii"], spec.get("mask", "tokenize"), tenant)
                report["tokenized_columns"].append(spec["name"])
        for col in (privacy.get("undeclared") or {}):
            target = next((c["name"] for c in contract.get("columns", []) if _norm(c["name"]) == _norm(col)), None)
            if target and target in out.columns and target not in report["tokenized_columns"]:
                out[target], n = self.svc.pii.mask_embedded(out[target], tenant)
                report["embedded_tokens"] += n
        return out, report
