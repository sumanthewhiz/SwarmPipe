"""Knowledge base for grounding: documents -> chunks -> BM25 lexical retrieval with
trust labels and citations.

Trust levels: `trusted` (human-curated: seeded runbooks or promoted documents), `unverified`
(ingested from the watched folder), `untrusted` (ingested and flagged by the injection detector).
Retrieval is permission-aware (tenant + allowed trust levels) and every hit carries its provenance
so agents can cite it and prompts can spotlight anything not trusted."""
from __future__ import annotations

import math
import re
from collections import Counter
from pathlib import Path

from swarmpipe.core.util import dumps, iso, loads, new_id, sha256_text

_WORD = re.compile(r"[a-z0-9_]+")
STOPWORDS = set("""a an the and or of to in on for with by is are was were be been it this that these those as at from
into if then else when while do does did not no yes can could should would will may might must you your we our they
their he she his her them its about over under than so such via per any all each every which who whom what where how""".split())


def tokenize(text: str) -> list[str]:
    toks = [t for t in _WORD.findall((text or "").lower()) if t not in STOPWORDS and len(t) > 1]
    out = []
    for t in toks:
        out.append(t)
        if "_" in t:
            out.extend(p for p in t.split("_") if len(p) > 1 and p not in STOPWORDS)
    return out


def chunk_text(text: str, max_chars: int = 1200) -> list[str]:
    parts = [p.strip() for p in re.split(r"\n\s*\n|\n(?=#)", text or "") if p.strip()]
    chunks, cur = [], ""
    for p in parts:
        if len(cur) + len(p) + 2 <= max_chars:
            cur = f"{cur}\n\n{p}" if cur else p
        else:
            if cur:
                chunks.append(cur)
            while len(p) > max_chars:
                chunks.append(p[:max_chars])
                p = p[max_chars:]
            cur = p
    if cur:
        chunks.append(cur)
    return chunks


def bm25_rank(query: str, docs: list[tuple[str, list[str]]], k: int = 5, k1: float = 1.5, b: float = 0.75) -> list[tuple[str, float]]:
    q = tokenize(query)
    if not q or not docs:
        return []
    n = len(docs)
    avgdl = sum(len(t) for _, t in docs) / n or 1.0
    df = Counter()
    for _, terms in docs:
        df.update(set(terms))
    scores = []
    for doc_id, terms in docs:
        tf = Counter(terms)
        dl = len(terms) or 1
        s = 0.0
        for term in q:
            if term not in tf:
                continue
            idf = math.log(1 + (n - df[term] + 0.5) / (df[term] + 0.5))
            s += idf * tf[term] * (k1 + 1) / (tf[term] + k1 * (1 - b + b * dl / avgdl))
        if s > 0:
            scores.append((doc_id, s))
    scores.sort(key=lambda x: -x[1])
    return scores[:k]


class KnowledgeBase:
    def __init__(self, svc):
        self.svc = svc

    def add_document(self, *, tenant: str, title: str, text: str, source: str, trust: str, doc_type: str = "note",
                     file_id: str | None = None, summary: str | None = None, flags: dict | None = None) -> str:
        doc_id = new_id("doc")
        self.svc.db.insert("documents", {"id": doc_id, "tenant": tenant, "file_id": file_id, "title": title, "doc_type": doc_type,
                                         "summary": summary, "trust": trust, "status": "active", "source": source,
                                         "flags": dumps(flags or {}), "created_at": iso()})
        rows = []
        for i, ch in enumerate(chunk_text(text)):
            terms = tokenize(f"{title} {ch}")
            rows.append((f"{doc_id}#{i}", doc_id, tenant, i, ch, dumps(terms), len(terms)))
        self.svc.db.executemany("INSERT INTO chunks(id, doc_id, tenant, seq, text, terms, n_terms) VALUES(?,?,?,?,?,?,?)", rows)
        return doc_id

    def seed_from_dir(self) -> int:
        kdir = Path(self.svc.settings.resolve(self.svc.settings.paths.knowledge_dir))
        added = 0
        for p in sorted(kdir.glob("*.md")) + sorted(kdir.glob("*.txt")):
            text = p.read_text(encoding="utf-8")
            source = f"knowledge/{p.name}#{sha256_text(text)[:12]}"
            if self.svc.db.scalar("SELECT COUNT(*) FROM documents WHERE source=?", (source,), default=0):
                continue
            self.svc.db.execute("UPDATE documents SET status='superseded' WHERE source LIKE ? AND status='active'",
                                (f"knowledge/{p.name}#%",))
            title = next((ln.lstrip("# ").strip() for ln in text.splitlines() if ln.strip()), p.stem)
            self.add_document(tenant="*", title=title, text=text, source=source, trust="trusted", doc_type="runbook",
                              summary=None, flags={"curated": True})
            added += 1
        return added

    def search(self, query: str, tenant: str, k: int = 4, trust_levels: tuple[str, ...] = ("trusted", "unverified")) -> list[dict]:
        qs = ",".join("?" for _ in trust_levels)
        rows = self.svc.db.query(
            f"SELECT c.id, c.doc_id, c.text, c.terms, d.title, d.trust, d.source, d.doc_type FROM chunks c JOIN documents d ON d.id=c.doc_id "
            f"WHERE d.status='active' AND d.tenant IN (?, '*') AND d.trust IN ({qs})", [tenant, *trust_levels])
        by_id = {r["id"]: r for r in rows}
        ranked = bm25_rank(query, [(r["id"], loads(r["terms"], [])) for r in rows], k=k)
        return [{"chunk_id": cid, "doc_id": by_id[cid]["doc_id"], "title": by_id[cid]["title"], "trust": by_id[cid]["trust"],
                 "source": by_id[cid]["source"], "doc_type": by_id[cid]["doc_type"], "score": round(score, 3),
                 "text": by_id[cid]["text"]} for cid, score in ranked]

    def documents(self, tenant: str | None = None) -> list[dict]:
        if tenant:
            rows = self.svc.db.query("SELECT * FROM documents WHERE tenant IN (?, '*') ORDER BY created_at DESC", (tenant,))
        else:
            rows = self.svc.db.query("SELECT * FROM documents ORDER BY created_at DESC")
        for r in rows:
            r["flags"] = loads(r["flags"], {})
        return rows

    def set_trust(self, doc_id: str, trust: str, by: str) -> None:
        self.svc.db.update("documents", {"id": doc_id}, {"trust": trust})
        self.svc.audit.record(by, "knowledge.set_trust", doc_id, trust)
