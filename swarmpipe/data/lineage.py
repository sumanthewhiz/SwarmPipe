"""Lineage (OpenLineage run events + a lineage graph) and the context graph.

Static lineage ("intent") comes from contracts (references), derived-dataset definitions and the
consumer catalog; runtime lineage ("reality") comes from OpenLineage-style START/COMPLETE/FAIL
events emitted by every pipeline run. Impact analysis walks the graph downstream to consumers,
owners and regulated outputs - the blast radius used by the policy engine."""
from __future__ import annotations

import uuid
from collections import deque

from swarmpipe.core.util import dumps, iso, loads, parse_iso

PRODUCER = "https://github.com/local/swarmpipe"
SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"
CRIT_ORDER = ["low", "medium", "high", "critical"]


def ds_node(dataset: str) -> str:
    return f"dataset:{dataset}"


class LineageService:
    def __init__(self, svc):
        self.svc = svc

    @staticmethod
    def new_run_uuid() -> str:
        return str(uuid.uuid4())

    def emit(self, event_type: str, *, ol_run_id: str, job: str, tenant: str, run_id: str | None = None,
             inputs: list[dict] | None = None, outputs: list[dict] | None = None,
             run_facets: dict | None = None, job_facets: dict | None = None) -> dict:
        ns = f"swarmpipe://{tenant}"
        event = {
            "eventType": event_type, "eventTime": iso(), "producer": PRODUCER, "schemaURL": SCHEMA_URL,
            "run": {"runId": ol_run_id, "facets": {"swarmpipe": {"run_id": run_id, "tenant": tenant}, **(run_facets or {})}},
            "job": {"namespace": ns, "name": job, "facets": job_facets or {}},
            "inputs": inputs or [], "outputs": outputs or [],
        }
        self.svc.db.insert("lineage_events", {"event_type": event_type, "event_time": event["eventTime"], "run_id": run_id,
                                              "job_name": job, "payload": dumps(event)})
        if event_type == "COMPLETE":
            for i in inputs or []:
                for o in outputs or []:
                    self.add_edge(self._node(i), self._node(o), "runtime", tenant)
        return event

    @staticmethod
    def _node(ds: dict) -> str:
        name = ds.get("name", "")
        if ds.get("namespace", "").startswith("file://"):
            return f"file:{name}"
        return ds_node(name)

    def add_edge(self, src: str, dst: str, kind: str, tenant: str) -> None:
        now = iso()
        self.svc.db.execute(
            "INSERT INTO lineage_edges(src, dst, kind, tenant, first_seen, last_seen) VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(src, dst, kind, tenant) DO UPDATE SET last_seen=excluded.last_seen", (src, dst, kind, tenant, now, now))

    def seed_static_edges(self) -> int:
        """Rebuild the declared ("intent") part of the graph from contracts + consumer catalog."""
        db = self.svc.db
        edges: list[tuple[str, str, str]] = []
        for c in self.svc.contracts.all_active():
            for col in c.get("columns", []):
                ref = col.get("references")
                if ref:
                    edges.append((ds_node(ref["dataset"]), ds_node(c["dataset"]), "references"))
        cons = self.svc.settings.consumers or {}
        for name, d in (cons.get("derived") or {}).items():
            for inp in d.get("inputs", []):
                edges.append((ds_node(inp), ds_node(name), "derives"))
        for cns in cons.get("consumers") or []:
            for dep in cns.get("depends_on", []):
                edges.append((ds_node(dep), f"consumer:{cns['id']}", "consumed_by"))
        with db.tx():
            db.execute("DELETE FROM lineage_edges WHERE tenant='*' AND kind IN ('references','derives','consumed_by')")
            now = iso()
            for s, d, k in edges:
                db.execute("INSERT OR IGNORE INTO lineage_edges(src, dst, kind, tenant, first_seen, last_seen) VALUES(?,?,?,?,?,?)",
                           (s, d, k, "*", now, now))
        return len(edges)

    def edges(self, tenant: str) -> list[dict]:
        return self.svc.db.query("SELECT * FROM lineage_edges WHERE tenant IN (?, '*')", (tenant,))

    def _walk(self, start: str, tenant: str, direction: str, depth: int) -> list[dict]:
        adj: dict[str, list[tuple[str, str]]] = {}
        for e in self.edges(tenant):
            a, b = (e["src"], e["dst"]) if direction == "down" else (e["dst"], e["src"])
            if direction == "down" and a.startswith("file:"):
                continue
            adj.setdefault(a, []).append((b, e["kind"]))
        seen = {start}
        out = []
        q = deque([(start, 0)])
        while q:
            node, d = q.popleft()
            if d >= depth:
                continue
            for nxt, kind in adj.get(node, []):
                if nxt in seen:
                    continue
                seen.add(nxt)
                out.append({"node": nxt, "depth": d + 1, "via": kind, "from": node})
                q.append((nxt, d + 1))
        return out

    def downstream(self, node: str, tenant: str, depth: int = 6) -> list[dict]:
        return self._walk(node, tenant, "down", depth)

    def upstream(self, node: str, tenant: str, depth: int = 6) -> list[dict]:
        return self._walk(node, tenant, "up", depth)

    def graph(self, tenant: str) -> dict:
        edges = self.edges(tenant)
        nodes = {}
        for e in edges:
            for n in (e["src"], e["dst"]):
                nodes.setdefault(n, {"id": n, "type": n.split(":", 1)[0], "label": n.split(":", 1)[1]})
        for n in nodes.values():
            if n["type"] == "dataset":
                st = self.svc.db.query_one("SELECT * FROM dataset_state WHERE tenant=? AND dataset=?", (tenant, n["label"]))
                n["published_version_id"] = st["published_version_id"] if st else None
                n["hold"] = bool(st and st["hold"])
                n["open_incidents"] = self.svc.db.scalar(
                    "SELECT COUNT(*) FROM incidents WHERE tenant=? AND dataset=? AND status NOT IN ('resolved','closed')",
                    (tenant, n["label"]), default=0)
        return {"nodes": list(nodes.values()), "edges": [{"src": e["src"], "dst": e["dst"], "kind": e["kind"]} for e in edges]}

    def events(self, run_id: str | None = None, limit: int = 50) -> list[dict]:
        if run_id:
            rows = self.svc.db.query("SELECT * FROM lineage_events WHERE run_id=? ORDER BY id", (run_id,))
        else:
            rows = self.svc.db.query("SELECT * FROM lineage_events ORDER BY id DESC LIMIT ?", (limit,))
        for r in rows:
            r["payload"] = loads(r["payload"], {})
        return rows


class ContextGraph:
    """Trusted context for agents: meaning (owners, SLAs, policies, consumers) on top of lineage."""

    def __init__(self, svc):
        self.svc = svc

    def consumer(self, cid: str) -> dict | None:
        for c in (self.svc.settings.consumers or {}).get("consumers", []):
            if c["id"] == cid:
                return c
        return None

    def owner(self, dataset: str) -> str | None:
        c = self.svc.contracts.active(dataset)
        if c:
            return c.get("owner")
        d = ((self.svc.settings.consumers or {}).get("derived") or {}).get(dataset)
        return d.get("owner") if d else None

    def impact(self, tenant: str, dataset: str) -> dict:
        down = self.svc.lineage.downstream(ds_node(dataset), tenant)
        consumers, datasets = [], []
        for d in down:
            kind, name = d["node"].split(":", 1)
            if kind == "consumer":
                c = self.consumer(name) or {"id": name}
                consumers.append({**c, "depth": d["depth"], "via": d["from"]})
            elif kind == "dataset":
                datasets.append({"dataset": name, "depth": d["depth"], "via": d["via"], "owner": self.owner(name)})
        owners = sorted({o for o in [self.owner(dataset)] + [c.get("owner") for c in consumers] + [d["owner"] for d in datasets] if o})
        crit = "low"
        for c in consumers:
            if CRIT_ORDER.index(c.get("criticality", "low")) > CRIT_ORDER.index(crit):
                crit = c.get("criticality", "low")
        return {"dataset": dataset, "downstream_datasets": datasets, "consumers": consumers, "owners": owners,
                "regulated": any(c.get("regulated") for c in consumers), "max_criticality": crit,
                "blast_radius": len(consumers) + len(datasets)}

    def freshness(self, tenant: str, dataset: str) -> dict:
        c = self.svc.contracts.active(dataset) or {}
        sla = c.get("freshness") or {}
        st = self.svc.db.query_one("SELECT * FROM dataset_state WHERE tenant=? AND dataset=?", (tenant, dataset)) or {}
        now = self.svc.clock.now()
        last = parse_iso(st.get("last_success_at"))
        out = {"dataset": dataset, "expected_every_min": sla.get("expected_every_min"), "grace_min": sla.get("grace_min"),
               "last_success_at": st.get("last_success_at"), "last_arrival_at": st.get("last_arrival_at")}
        if last and sla.get("expected_every_min"):
            age = (now - last).total_seconds() / 60
            limit = float(sla["expected_every_min"]) + float(sla.get("grace_min") or 0)
            out.update({"age_min": round(age, 1), "limit_min": limit, "overdue": age > limit})
        else:
            out.update({"age_min": None, "overdue": False})
        return out

    def node(self, tenant: str, dataset: str) -> dict:
        c = self.svc.contracts.active(dataset)
        st = self.svc.db.query_one("SELECT * FROM dataset_state WHERE tenant=? AND dataset=?", (tenant, dataset)) or {}
        up = [u for u in self.svc.lineage.upstream(ds_node(dataset), tenant) if u["node"].startswith("dataset:")]
        incidents = self.svc.db.query("SELECT id, status, severity, title FROM incidents WHERE tenant=? AND dataset=? "
                                      "AND status NOT IN ('resolved','closed') ORDER BY created_at DESC", (tenant, dataset))
        return {"dataset": dataset, "contract_version": c.get("version") if c else None,
                "owner": self.owner(dataset), "source_owner": c.get("source_owner") if c else None,
                "classification": c.get("classification") if c else None,
                "freshness": self.freshness(tenant, dataset), "state": st,
                "upstream": [u["node"].split(":", 1)[1] for u in up], "impact": self.impact(tenant, dataset),
                "open_incidents": incidents}
