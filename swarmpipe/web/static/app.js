// SwarmPipe operator dashboard - vanilla JS, no build step, works offline.
const TABS = [
  ["overview", "Overview"], ["lab", "Scenarios & Chaos"], ["runs", "Runs"], ["incidents", "Incidents"], ["approvals", "Approvals"],
  ["data", "Datasets & Lineage"], ["agents", "Agents & Tools"], ["governance", "Governance"], ["cost", "Cost & Metrics"],
  ["evals", "Evals"], ["ask", "Ask the data"], ["knowledge", "Knowledge & Memory"],
];
let current = location.hash.slice(1) || "overview";
const $ = (s) => document.querySelector(s);
const esc = (v) => (v === null || v === undefined) ? "" : String(v).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const pill = (v) => `<span class="pill s-${esc(v)}">${esc(v)}</span>`;
const short = (v, n = 80) => { const s = typeof v === "string" ? v : JSON.stringify(v); return s && s.length > n ? esc(s.slice(0, n)) + "…" : esc(s); };
const fmt = (n, d = 4) => (n === null || n === undefined) ? "" : (typeof n === "number" ? (Math.abs(n) < 1 && n !== 0 ? n.toFixed(d) : Math.round(n * 100) / 100) : n);
const user = () => $("#user").value;

async function api(path, opts = {}) {
  const res = await fetch(path, { ...opts, headers: { "Content-Type": "application/json", "X-User": user(), ...(opts.headers || {}) } });
  const txt = await res.text();
  let data; try { data = JSON.parse(txt); } catch { data = txt; }
  if (!res.ok) { const msg = (data && data.error && (data.error.message || data.error.code)) || data.detail || txt; toast(msg, true); throw new Error(msg); }
  return data;
}
const post = (p, body) => api(p, { method: "POST", body: JSON.stringify(body || {}) });
function toast(msg, bad) { const t = $("#toast"); t.textContent = msg; t.className = "toast" + (bad ? " bad" : ""); setTimeout(() => t.className = "toast hidden", 3500); }
function table(rows, cols, opts = {}) {
  if (!rows || !rows.length) return `<div class="muted">${opts.empty || "nothing yet"}</div>`;
  const head = cols.map((c) => `<th>${esc(c.label || c.key || c)}</th>`).join("");
  const body = rows.map((r, i) => `<tr class="${opts.onclick ? "click" : ""}" ${opts.onclick ? `onclick="${opts.onclick}(${JSON.stringify(r[opts.idKey || "id"]).replace(/"/g, "&quot;")})"` : ""}>` +
    cols.map((c) => { const k = c.key || c; const v = r[k]; return `<td>${c.render ? c.render(v, r, i) : short(v, c.max || 90)}</td>`; }).join("") + "</tr>").join("");
  return `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}
function openDrawer(title, html) { $("#drawer-title").innerHTML = title; $("#drawer-body").innerHTML = html; $("#drawer").classList.remove("hidden"); }
function closeDrawer() { $("#drawer").classList.add("hidden"); }

// ------------------------------------------------------------------ waterfall
function waterfall(spans) {
  if (!spans || !spans.length) return `<div class="muted">no spans for this trace (sampled out or still running)</div>`;
  const t0 = Math.min(...spans.map((s) => s.start_ts)), t1 = Math.max(...spans.map((s) => s.end_ts || s.start_ts));
  const total = Math.max(t1 - t0, 0.001);
  const byParent = {}; spans.forEach((s) => (byParent[s.parent_span_id || "root"] ||= []).push(s));
  const ids = new Set(spans.map((s) => s.span_id));
  const order = []; const walk = (s, d) => { order.push([s, d]); (byParent[s.span_id] || []).sort((a, b) => a.start_ts - b.start_ts).forEach((c) => walk(c, d + 1)); };
  spans.filter((s) => !s.parent_span_id || !ids.has(s.parent_span_id)).sort((a, b) => a.start_ts - b.start_ts).forEach((s) => walk(s, 0));
  return `<div class="wf">` + order.map(([s, d]) => {
    const a = s.attributes || {};
    const cls = s.status === "error" ? "err" : s.name.startsWith("llm.call") || s.name.startsWith("chat") ? "llm" : s.name.startsWith("execute_tool") ? "tool" : s.name.startsWith("invoke_agent") ? "agent" : "";
    const left = ((s.start_ts - t0) / total) * 100, width = Math.max(((s.end_ts || s.start_ts) - s.start_ts) / total * 100, 0.3);
    const extra = ["gen_ai.request.model", "gen_ai.usage.input_tokens", "gen_ai.usage.output_tokens", "swarmpipe.cost_usd", "gen_ai.tool.name", "swarmpipe.evidence_id", "swarmpipe.route", "swarmpipe.root_cause"]
      .filter((k) => a[k] !== undefined).map((k) => `${k.split(".").pop()}=${a[k]}`).join(" ");
    const title = esc(JSON.stringify(a).slice(0, 900));
    return `<div class="wf-row" title="${title}"><div class="wf-bar ${cls}" style="left:${left}%;width:${width}%"></div>` +
      `<span class="wf-label" style="padding-left:${d * 12}px">${esc(s.name)} <span class="muted">${(s.duration_ms || 0).toFixed(0)}ms ${esc(extra)}</span></span></div>`;
  }).join("") + `</div><div class="muted">bars: <span class="pill">workflow/step</span> <span class="pill" style="color:#d29922">agent</span> <span class="pill" style="color:#a371f7">model call</span> <span class="pill" style="color:#3fb950">tool call</span> <span class="pill s-error">error</span> - hover for OpenTelemetry GenAI attributes</div>`;
}

// ------------------------------------------------------------------ views
const views = {};

views.overview = async () => {
  const o = await api("/api/overview");
  const runs = o.runs.reduce((m, r) => (m[r.status] = (m[r.status] || 0) + r.n, m), {});
  return `
  <div class="note">Drop <b>.csv / .tsv / .txt / .xlsx / .xls</b> files into <code>${esc(o.inbox)}</code> (sub-folder = tenant), or use <b>Scenarios & Chaos</b>. Every file becomes a durable run; failures become incidents that the agent swarm triages under policy.</div>
  <div class="grid">
    <div class="card"><div class="k">queue depth</div><div class="v">${o.queue_depth}</div></div>
    <div class="card"><div class="k">runs succeeded</div><div class="v">${runs.succeeded || 0}</div></div>
    <div class="card"><div class="k">quarantined / DLQ</div><div class="v">${(runs.quarantined || 0)} / ${(runs.dead_lettered || 0)}</div></div>
    <div class="card"><div class="k">open incidents</div><div class="v">${o.open_incidents}</div></div>
    <div class="card"><div class="k">pending approvals</div><div class="v">${o.pending_approvals}</div></div>
    <div class="card"><div class="k">published datasets</div><div class="v">${o.datasets}</div></div>
    <div class="card"><div class="k">LLM calls (cached)</div><div class="v">${o.llm.calls} <span class="muted" style="font-size:12px">(${o.llm.cached})</span></div></div>
    <div class="card"><div class="k">LLM cost (simulated $)</div><div class="v">${fmt(o.llm.usd, 4)}</div></div>
    <div class="card"><div class="k">tokens</div><div class="v">${o.llm.tokens}</div></div>
    <div class="card"><div class="k">profile / clock offset</div><div class="v" style="font-size:15px">${esc(o.profile)} / ${o.clock_offset_min || 0}m</div></div>
  </div>
  <div class="row"><div class="col"><h2>SLOs of the pipeline itself</h2>${table(o.slos, [{ key: "id" }, { key: "description", max: 60 }, { key: "sli", render: (v) => fmt(v, 3) }, { key: "objective" }, { key: "burn_rate", render: (v) => fmt(v, 2) }, { key: "status", render: pill }])}
  <h2>Kill switches</h2><div class="flex">${o.kill_switches.length ? o.kill_switches.map((k) => pill(k.scope) + " " + esc(k.reason)).join(" ") : '<span class="muted">none engaged</span>'}
  <button class="btn danger" onclick="killswitch(true)">engage GLOBAL kill switch</button><button class="btn secondary" onclick="killswitch(false)">release</button></div>
  <h2>Model circuit breakers</h2>${table(Object.entries(o.breakers).map(([k, v]) => ({ model: k, ...v })), ["model", { key: "state", render: pill }, "failures", "opens", "open_for_s"], { empty: "no model called yet" })}</div>
  <div class="col"><h2>Recent signals</h2>${table(o.recent_signals, [{ key: "severity", render: pill }, "type", "dataset", { key: "summary", max: 70 }, { key: "incident_id", render: (v) => v ? `<a href="#" onclick="showIncident('${v}');return false">${v}</a>` : '<span class="muted">(below incident threshold)</span>' }])}
  <h2>Runs</h2>${table(o.runs, ["workflow", { key: "status", render: pill }, "n"])}</div></div>`;
};
async function killswitch(on) { await post("/api/killswitch", { scope: "global", enabled: on, reason: on ? "engaged from dashboard" : "released" }); toast(on ? "GLOBAL kill switch engaged: all agent actions and LLM calls stop" : "kill switch released"); render(); }

views.lab = async () => {
  const [sc, ch] = await Promise.all([api("/api/scenarios"), api("/api/chaos")]);
  const flags = ch.flags;
  const quick = [
    ["Spotlighting OFF", "guardrails.spotlighting", false], ["Critic OFF", "feature.critic_review", false],
    ["30% LLM timeouts", "chaos.llm_timeout_rate", 0.3], ["30% malformed JSON", "chaos.llm_malformed_rate", 0.3],
    ["sim-large outage", "chaos.llm_outage_models", ["sim-large"]], ["10% wrong answers", "chaos.llm_wrong_answer_rate", 0.1],
    ["hallucinated citations", "chaos.llm_hallucinated_citation_rate", 0.5], ["tool loops", "chaos.llm_loop_rate", 0.6],
    ["tool errors 30%", "chaos.tool_error_rate", 0.3], ["crash after 'transform'", "chaos.crash_after_step", "transform"],
    ["+26h business clock", "clock_offset_min", 1560], ["change freeze ON", "policy.freeze_window", true],
    ["acme quota = 5 req/day", "quota.acme.daily_llm_requests", 5],
  ];
  return `<div class="note">Scenarios write deterministic synthetic files into the watched folder (or change runtime chaos flags). Run <b>baseline</b> first, then any failure scenario, then watch <b>Runs</b> and <b>Incidents</b>.</div>
  <div class="flex"><label>tenant <input id="lab-tenant" placeholder="default" size="8"></label></div>
  ${table(sc, [{ key: "name", render: (v) => `<button class="btn" onclick="drop('${v}')">drop ${esc(v)}</button>` }, { key: "description", max: 120 }, { key: "teaches", max: 80 }])}
  <h2>Chaos & runtime flags</h2><div class="flex">${quick.map(([l, k, v]) => `<button class="btn secondary" onclick='setFlag(${JSON.stringify(k)}, ${JSON.stringify(v)})'>${esc(l)}</button>`).join("")}
  <button class="btn danger" onclick="clearChaos()">clear all</button></div>
  <div class="flex" style="margin-top:8px"><input id="flag-k" placeholder="key e.g. chaos.llm_latency_ms" size="36"><input id="flag-v" placeholder='JSON value e.g. [200, 900]' size="24"><button class="btn" onclick="setFlagForm()">set</button></div>
  <h3>Active flags</h3><pre>${esc(JSON.stringify(flags, null, 2))}</pre><h3>Simulator defaults</h3><pre>${esc(JSON.stringify(ch.defaults, null, 2))}</pre>`;
};
async function drop(n) { const t = $("#lab-tenant").value.trim() || null; const r = await post(`/api/scenarios/${n}`, { tenant: t }); toast(`dropped ${r.dropped.join(", ") || "(runtime change)"}`); }
async function setFlag(k, v) { await post("/api/chaos", { key: k, value: v }); toast(`${k} = ${JSON.stringify(v)}`); render(); }
async function setFlagForm() { let v = $("#flag-v").value; try { v = JSON.parse(v); } catch { } await setFlag($("#flag-k").value, v); }
async function clearChaos() { await post("/api/chaos/clear"); toast("cleared"); render(); }

views.runs = async () => {
  const st = window.runFilter || "";
  const rows = await api("/api/runs" + (st ? `?status=${st}` : ""));
  return `<div class="flex">filter: ${["", "running", "waiting", "retry_wait", "succeeded", "quarantined", "dead_lettered", "failed", "blocked"].map((s) => `<button class="btn secondary" onclick="window.runFilter='${s}';render()">${s || "all"}</button>`).join("")}</div>
  ${table(rows, [{ key: "id" }, "workflow", { key: "status", render: pill }, "tenant", "dataset", "current_step", "attempt", { key: "recovered_count", label: "recovered" }, "waiting_on", { key: "created_at", render: (v) => esc((v || "").slice(11, 19)) }, { key: "error", max: 60 }], { onclick: "showRun" })}`;
};
async function showRun(id) {
  const d = await api(`/api/runs/${id}`);
  const tr = await api(`/api/traces/${d.run.trace_id}`);
  openDrawer(`Run ${id} ${pill(d.run.status)}`, `<div class="kv"><div>workflow</div><div>${esc(d.run.workflow)}</div><div>tenant / dataset</div><div>${esc(d.run.tenant)} / ${esc(d.run.dataset)}</div>
  <div>waiting on</div><div>${esc(d.run.waiting_on)}</div><div>recovered from crashes</div><div>${d.run.recovered_count}</div><div>parent</div><div>${d.run.parent_run_id ? `<a href="#" onclick="showRun('${d.run.parent_run_id}');return false">${d.run.parent_run_id}</a>` : ""}</div><div>error</div><div>${esc(d.run.error)}</div></div>
  <h3>Steps = durable checkpoints (recorded outputs are reused on resume)</h3>${table(d.steps, ["name", { key: "status", render: pill }, "attempt", "kind", { key: "duration_ms", render: (v) => fmt(v) }, { key: "error", max: 80 }, { key: "output", max: 110 }])}
  <h3>Attempts (retries with backoff)</h3>${table(d.attempts, ["step", "attempt", { key: "status", render: pill }, { key: "duration_ms", render: (v) => fmt(v) }, { key: "error", max: 90 }])}
  ${d.children.length ? `<h3>Children (fan-out)</h3>${table(d.children, ["id", "workflow", { key: "status", render: pill }, "dataset"], { onclick: "showRun" })}` : ""}
  ${d.checks.length ? `<h3>Data assurance checks</h3>${table(d.checks, [{ key: "status", render: pill }, "check_name", "check_type", { key: "severity", render: pill }, { key: "observed", max: 60 }, { key: "expected", max: 60 }])}` : ""}
  ${d.llm_calls.length ? `<h3>Model calls</h3>${table(d.llm_calls, ["agent", "model", "prompt_id", "prompt_version", { key: "status", render: pill }, "input_tokens", "output_tokens", { key: "cost_usd", render: (v) => fmt(v, 5) }, { key: "latency_ms", render: (v) => fmt(v) }, "cached", "purpose"])}` : ""}
  ${d.lineage.length ? `<h3>OpenLineage events</h3><pre>${esc(JSON.stringify(d.lineage.map((e) => e.payload), null, 1)).slice(0, 6000)}</pre>` : ""}
  <h3>Trace</h3>${waterfall(tr.spans)}`);
}

views.incidents = async () => {
  const rows = await api("/api/incidents");
  return table(rows, [{ key: "id" }, { key: "status", render: pill }, { key: "severity", render: pill }, "tenant", "dataset", { key: "signal_count", label: "signals" }, "root_cause", { key: "confidence", render: (v) => fmt(v, 2) }, { key: "cost_usd", render: (v) => fmt(v, 5) }, { key: "title", max: 70 }], { onclick: "showIncident", empty: "no incidents - drop a failure scenario" });
};
async function showIncident(id) {
  const d = await api(`/api/incidents/${id}`);
  const inc = d.incident, dx = inc.diagnosis || {}, imp = inc.impact || {};
  const tr = await api(`/api/traces/${inc.trace_id}`);
  const props = d.proposals.map((p) => ({ ...p, reasons: (p.policy_details && p.policy_details.reasons) || [] }));
  const actBtn = (v, p) => {
    let b = "";
    if (["recommended", "informational", "denied", "rejected"].includes(p.status) && p.status !== "invalid") b += `<button class="btn secondary" onclick="execProp('${p.id}','${id}')">execute as me</button> `;
    if (["executed", "verified"].includes(p.status)) b += `<button class="btn danger" onclick="rollbackProp('${p.id}','${id}')">rollback</button>`;
    return b;
  };
  openDrawer(`Incident ${id} ${pill(inc.status)} ${pill(inc.severity)}`, `
  <div class="kv"><div>title</div><div>${esc(inc.title)}</div><div>tenant / dataset</div><div>${esc(inc.tenant)} / ${esc(inc.dataset)}</div>
  <div>diagnosis</div><div><b>${esc(dx.root_cause_category)}</b> (confidence ${fmt(dx.confidence, 2)}${dx.abstain ? ", ABSTAINED -> escalated" : ""}${dx.degraded ? ", degraded" : ""}) ${esc(dx.summary)}</div>
  <div>grounding</div><div>${dx.grounded ? pill("ok") : pill("warn")} citations: ${esc((dx.citations || []).join(", "))} ${dx.ungrounded_citations ? `<span class="s-fail">removed hallucinated: ${esc(dx.ungrounded_citations.join(", "))}</span>` : ""}</div>
  <div>alternatives</div><div>${esc((dx.alternatives || []).map((a) => `${a.category} (${a.confidence})`).join(", "))}</div>
  <div>impact (blast radius)</div><div>${imp.blast_radius ?? ""} - consumers: ${esc((imp.consumers || []).map((c) => c.name || c.id).join(", "))} ${imp.regulated ? pill("regulated") : ""}</div>
  <div>cost</div><div>${d.llm.calls} model calls, ${d.llm.tokens} tokens, $${fmt(d.llm.usd, 5)}</div>
  <div>evidence pack</div><div><a href="/api/evidence/${id}" target="_blank">JSON evidence pack</a></div></div>
  <h3>Signals (clustered into this incident)</h3>${table(d.signals, [{ key: "severity", render: pill }, "type", "dataset", { key: "summary", max: 110 }])}
  <h3>Blackboard - the shared case file (append-only, with provenance)</h3><div class="timeline">${d.blackboard.map((b) => `<div class="tl-item"><span class="who">${esc(b.author)}</span> <span class="pill">${esc(b.kind)}</span> <span class="muted">v${b.version} ${esc(b.created_at.slice(11, 19))}</span><br>${short(b.content, 380)} ${b.evidence_ids.length ? `<span class="muted">evidence: ${esc(b.evidence_ids.join(", "))}</span>` : ""}</div>`).join("")}</div>
  <h3>Proposals -> policy decisions -> execution -> verification</h3>${table(props, ["rank", "action", { key: "status", render: pill }, { key: "policy_effect", render: pill }, "autonomy_level", { key: "params", max: 70 }, { key: "reasons", render: (v) => esc((v || []).join(" | ")).slice(0, 220) }, "executed_by", "on_behalf_of", { key: "id", label: "", render: actBtn }])}
  ${d.approvals.length ? `<h3>Approvals</h3>${table(d.approvals, ["kind", { key: "status", render: pill }, "subject", "decided_by", "comment"])}` : ""}
  <h3>Evidence (every tool result gets a citable id)</h3>${table(d.evidence, ["id", "tool", { key: "trust", render: pill }, "created_by", { key: "content", max: 140 }])}
  ${inc.postmortem ? `<h3>Postmortem (Learner agent)</h3><pre>${esc(JSON.stringify(inc.postmortem, null, 2))}</pre>` : ""}
  <h3>Feedback (online evaluation)</h3><div class="flex">rating <select id="fb-r"><option>5</option><option>4</option><option>3</option><option>2</option><option>1</option></select> correct root cause (if wrong) <input id="fb-c" size="24"> <input id="fb-t" placeholder="comment" size="30"> <button class="btn" onclick="feedback('${id}')">send</button>
  <button class="btn secondary" onclick="resolveInc('${id}')">mark resolved</button></div>
  ${d.feedback.length ? table(d.feedback, ["user", "rating", "correct_category", "comment"]) : ""}
  <h3>Trace (one trace across every agent of this incident)</h3>${waterfall(tr.spans)}`);
}
async function execProp(pid, iid) { await post(`/api/proposals/${pid}/execute`); toast("executed (attributed to you)"); showIncident(iid); }
async function rollbackProp(pid, iid) { await post(`/api/proposals/${pid}/rollback`); toast("rolled back"); showIncident(iid); }
async function feedback(iid) { await post(`/api/incidents/${iid}/feedback`, { rating: +$("#fb-r").value, correct_category: $("#fb-c").value || null, comment: $("#fb-t").value }); toast("thanks - feedback feeds online evals"); showIncident(iid); }
async function resolveInc(iid) { await post(`/api/incidents/${iid}/resolve`, { note: "resolved from dashboard" }); toast("resolved"); showIncident(iid); }

views.approvals = async () => {
  const rows = await api("/api/approvals?status=" + (window.apAll ? "all" : "pending"));
  return `<div class="note">The server-side approval queue: agents cannot act on these until a human with the <b>approver</b> role decides. High-risk actions require typing the dataset name and a written justification. Current user: <b>${esc(user())}</b>.</div>
  <button class="btn secondary" onclick="window.apAll=!window.apAll;render()">${window.apAll ? "show pending" : "show all"}</button>
  ${table(rows, ["kind", { key: "status", render: pill }, { key: "risk", render: pill }, "tenant", "subject", { key: "summary", max: 180 }, "decided_by",
    { key: "id", label: "decide", render: (v, r) => r.status !== "pending" ? "" : `<div class="flex">${r.requires_confirmation ? `<input id="cf-${v}" placeholder="type: ${esc(r.requires_confirmation)}" size="16">` : ""}<input id="cm-${v}" placeholder="comment / justification" size="22"><button class="btn ok" onclick="decide('${v}','approved')">approve</button><button class="btn danger" onclick="decide('${v}','rejected')">reject</button></div>` }], { empty: "no approvals waiting" })}`;
};
async function decide(id, decision) {
  const cf = document.getElementById("cf-" + id), cm = document.getElementById("cm-" + id);
  await post(`/api/approvals/${id}/decide`, { decision, comment: cm ? cm.value : "", confirm_text: cf ? cf.value : null });
  toast(`${decision} - the waiting workflow resumes`); render();
}

views.data = async () => {
  const [ds, g] = await Promise.all([api("/api/datasets"), api("/api/lineage?tenant=" + (window.lineTenant || "default"))]);
  return `${table(ds, ["tenant", "dataset", { key: "current", label: "version", render: (v) => v ? `v${v.version} (${v.row_count} rows)` : "-" }, "versions", "contract_version", { key: "classification", render: pill },
    { key: "hold", render: (v) => v ? pill("held") : "" }, { key: "freshness", label: "freshness", render: (v) => v && v.age_min !== null ? (v.overdue ? pill("breached") : pill("ok")) + ` ${fmt(v.age_min)}m / ${fmt(v.limit_min)}m` : "" }, { key: "last_success_at", render: (v) => esc((v || "").slice(0, 19)) }], { onclick: "showDataset", idKey: "dataset" })}
  <h2>Lineage & context graph (static intent + runtime OpenLineage)</h2>${lineageSvg(g)}`;
};
function lineageSvg(g) {
  const nodes = g.nodes.filter((n) => n.type !== "file"), ids = new Set(nodes.map((n) => n.id));
  const edges = g.edges.filter((e) => ids.has(e.src) && ids.has(e.dst) && e.kind !== "runtime");
  const level = {}; nodes.forEach((n) => level[n.id] = 0);
  for (let i = 0; i < nodes.length; i++) edges.forEach((e) => { if (level[e.dst] < level[e.src] + 1) level[e.dst] = level[e.src] + 1; });
  const cols = {}; nodes.forEach((n) => (cols[level[n.id]] ||= []).push(n));
  const W = 230, H = 46, pos = {};
  Object.entries(cols).forEach(([l, ns]) => ns.forEach((n, i) => pos[n.id] = { x: 20 + l * W, y: 20 + i * H }));
  const maxY = Math.max(...Object.values(pos).map((p) => p.y), 0) + 60, maxX = Math.max(...Object.values(pos).map((p) => p.x), 0) + 230;
  const color = (n) => n.type === "consumer" ? "#a371f7" : n.open_incidents ? "#f85149" : n.hold ? "#d29922" : n.published_version_id ? "#3fb950" : "#8a99a8";
  return `<svg width="${maxX}" height="${maxY}" style="background:#161d26;border:1px solid #2a3542;border-radius:6px">
  <defs><marker id="arr" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#4a5a6a"/></marker></defs>
  ${edges.map((e) => { const a = pos[e.src], b = pos[e.dst]; return `<line x1="${a.x + 180}" y1="${a.y + 12}" x2="${b.x}" y2="${b.y + 12}" stroke="#4a5a6a" marker-end="url(#arr)"><title>${esc(e.kind)}</title></line>`; }).join("")}
  ${nodes.map((n) => { const p = pos[n.id]; return `<g><rect x="${p.x}" y="${p.y}" width="180" height="24" rx="5" fill="#1d2631" stroke="${color(n)}"/><text x="${p.x + 8}" y="${p.y + 16}">${esc(n.label.slice(0, 26))}</text><title>${esc(JSON.stringify(n))}</title></g>`; }).join("")}
  </svg><div class="muted">green = published, amber = on hold, red = open incident, purple = consumer (dashboards, regulated reports, ML models, downstream agents)</div>`;
}
async function showDataset(ds) {
  const d = await api(`/api/datasets/${ds}?tenant=default`);
  const c = d.context;
  openDrawer(`Dataset ${ds}`, `<div class="kv"><div>owner / source</div><div>${esc(c.owner)} / ${esc(c.source_owner)}</div><div>classification</div><div>${pill(c.classification)}</div>
  <div>freshness SLA</div><div>${esc(JSON.stringify(c.freshness))}</div><div>upstream</div><div>${esc(c.upstream.join(", "))}</div>
  <div>impact</div><div>blast radius ${c.impact.blast_radius}; consumers ${esc(c.impact.consumers.map((x) => x.name || x.id).join(", "))}</div></div>
  <h3>Immutable versions (publish = view swap; rollback = re-point)</h3>${table(d.versions, ["version", { key: "status", render: pill }, "batch_rows", "row_count", "contract_version", "checksum", { key: "note", max: 60 }])}
  <h3>Latest checks</h3>${table(d.last_checks, [{ key: "status", render: pill }, "check_name", "check_type", { key: "severity", render: pill }, { key: "observed", max: 60 }, { key: "expected", max: 50 }])}
  <h3>Sample of the published view (PII stays tokenized in storage)</h3>${d.sample ? table(d.sample.rows.map((r) => Object.fromEntries(d.sample.columns.map((c2, i) => [c2, r[i]]))), d.sample.columns) : '<div class="muted">not published or not permitted for this user</div>'}
  <h3>Data contract v${d.contract ? d.contract.version : "-"}</h3><pre>${esc(JSON.stringify(d.contract, null, 2))}</pre>
  <h3>Contract versions</h3>${table(d.contract_versions, ["version", { key: "status", render: pill }, "created_by", "approved_by", { key: "change_note", max: 80 }])}`);
}

views.agents = async () => {
  const [cards, tools] = await Promise.all([api("/api/agents"), api("/api/tools")]);
  return `<div class="note">The swarm's registry: every agent has an identity, scopes, an explicit tool allowlist, a model route and a card hash (A2A Agent Card at <code>/a2a/agents/&lt;id&gt;</code>). Read-only agents cannot see write tools; only the Executor holds <code>act_*</code> tools.</div>
  ${table(cards.map((c) => ({ ...c["x-swarmpipe"], name: c.name, description: c.description })), [{ key: "agent_id" }, "name", { key: "kind", render: pill }, { key: "status", render: pill }, { key: "model_route", render: (v) => esc((v || []).join(" -> ")) }, { key: "scopes", render: (v) => esc(v.join(", ")) }, { key: "tools", render: (v) => esc(v.join(", ")), max: 200 }, { key: "stats", label: "llm calls / $", render: (v) => v ? `${v.calls} / ${fmt(v.usd, 5)}` : "" }, "tool_calls", { key: "description", max: 110 }])}
  <h2>Tool catalog (MCP-style contracts)</h2>${table(tools, ["name", "version", "scope", { key: "annotations", render: (a) => ["readOnlyHint", "destructiveHint", "idempotentHint", "openWorldHint"].filter((k) => a[k]).map((k) => pill(k)).join(" ") }, { key: "description", max: 130 }])}`;
};

views.governance = async () => {
  const [au, audit, ks] = await Promise.all([api("/api/autonomy"), api("/api/audit?limit=80"), api("/api/killswitch")]);
  return `<h2>Autonomy ladder (per tenant x action class; earned with evidence, withdrawn automatically)</h2>
  ${table(au.stats.filter((s) => s.tenant === "default" || s.proposals), ["tenant", "action", { key: "risk", render: pill }, { key: "level", render: (v, r) => `<b>${v}</b> / max ${r.max_level}` }, "proposals", "approved", "rejected", "executed", "verified_ok", "verified_fail", "rolled_back", "human_agreed", "human_disagreed",
    { key: "action", label: "set level (admin)", render: (v, r) => `<select onchange="setLevel('${r.tenant}','${v}',this.value)"><option></option>${["L0", "L1", "L2", "L3", "L4"].map((l) => `<option>${l}</option>`).join("")}</select>` }])}
  <h3>Autonomy history</h3>${table(au.history, ["ts", "tenant", "action_class", "from_level", "to_level", "changed_by", { key: "reason", max: 90 }])}
  <h2>Policy simulator (deterministic authorization, outside the model)</h2><div class="flex">
  action <select id="ps-a">${["notify_owner", "request_resend", "quarantine_version", "hold_downstream", "release_hold", "rollback_dataset", "reprocess_with_mapping", "update_contract", "force_publish", "drop_table"].map((a) => `<option>${a}</option>`).join("")}</select>
  confidence <input id="ps-c" value="0.9" size="4"> blast radius <input id="ps-b" value="1" size="3"> <label><input type="checkbox" id="ps-i"> injection suspected</label> <label><input type="checkbox" id="ps-r"> regulated consumer</label>
  <button class="btn" onclick="simulate()">evaluate</button></div><pre id="ps-out">-</pre>
  <h2>Kill switches</h2>${table(ks, ["scope", "reason", "set_by", "set_at"], { empty: "none engaged" })}
  <h2>Tamper-evident audit log <button class="btn secondary" onclick="verifyAudit()">verify hash chain</button> <span id="av"></span></h2>
  ${table(audit, ["seq", { key: "ts", render: (v) => esc(v.slice(11, 19)) }, "actor", "on_behalf_of", "action", { key: "resource", max: 40 }, { key: "decision", render: pill }, { key: "details", max: 110 }, { key: "hash", render: (v) => esc(v.slice(0, 10)) }])}`;
};
async function setLevel(t, a, l) { if (!l) return; await post("/api/autonomy", { tenant: t, action: a, level: l, reason: "set from dashboard" }); toast(`${a} -> ${l}`); render(); }
async function simulate() { const r = await post("/api/policy/simulate", { action: $("#ps-a").value, dataset: "sales_daily", confidence: +$("#ps-c").value, blast_radius: +$("#ps-b").value, injection: $("#ps-i").checked, regulated: $("#ps-r").checked }); $("#ps-out").textContent = JSON.stringify(r, null, 2); }
async function verifyAudit() { const r = await api("/api/audit/verify"); $("#av").innerHTML = r.ok ? pill("ok") + ` ${r.checked} records chain-verified` : pill("fail") + ` ${esc(r.reason)} at seq ${r.first_bad_seq}`; }

views.cost = async () => {
  const [c, m] = await Promise.all([api("/api/cost"), api("/api/metrics")]);
  return `<div class="grid"><div class="card"><div class="k">cost per resolved/mitigated incident</div><div class="v">$${fmt(c.cost_per_resolved_incident, 5)}</div></div>
  <div class="card"><div class="k">signals -> incidents (cluster before you reason)</div><div class="v">${c.cluster_ratio.signals} -> ${c.cluster_ratio.incidents}</div></div></div>
  <div class="row"><div class="col"><h2>By agent</h2>${table(c.by_agent, ["agent", "calls", "tin", "tout", { key: "usd", render: (v) => fmt(v, 5) }, "avg_ms", "cached"])}</div>
  <div class="col"><h2>By model / status</h2>${table(c.by_model, ["model", { key: "status", render: pill }, "calls", { key: "usd", render: (v) => fmt(v, 5) }, "avg_ms"])}
  <h2>Tenant quotas (per day)</h2>${table(c.by_tenant_day, ["tenant", "day", "llm_requests", { key: "usd", render: (v) => fmt(v, 5) }])}</div></div>
  <h2>Per incident</h2>${table(c.per_incident, ["id", "dataset", { key: "status", render: pill }, "calls", { key: "usd", render: (v) => fmt(v, 5) }], { onclick: "showIncident" })}
  <h2>Metrics (last 24h) <a href="/metrics" target="_blank" class="muted">Prometheus exposition</a></h2>${table(m, ["metric", "count", { key: "sum", render: (v) => fmt(v, 3) }, { key: "p50", render: (v) => fmt(v, 3) }, { key: "p95", render: (v) => fmt(v, 3) }, { key: "max", render: (v) => fmt(v, 3) }])}`;
};

views.evals = async () => {
  const e = await api("/api/evals");
  const l = e.latest;
  let latest = '<div class="muted">no report yet - run <code>swarmpipe evals run</code> or <code>swarmpipe evals gate</code></div>';
  if (l) {
    latest = `<div class="kv"><div>generated</div><div>${esc(l.generated_at)}</div><div>config</div><div>${esc(JSON.stringify(l.config))}</div><div>gate</div><div>${l.gate ? (l.gate.passed ? pill("pass") : pill("fail")) : ""}</div></div>
    ${Object.entries(l.suites || {}).map(([name, s]) => `<h3>${esc(name)}</h3>${table(Object.entries(s.metrics || {}).map(([k, v]) => ({ metric: k, value: typeof v === "number" ? fmt(v, 3) : JSON.stringify(v) })), ["metric", "value"])}
    ${s.cases ? table(s.cases, ["case_id", { key: "pass_at_k", label: "pass@k", render: (v) => v ? pill("pass") : pill("fail") }, { key: "pass_hat_k", label: "pass^k", render: (v) => v ? pill("pass") : pill("fail") }, { key: "failures", render: (v) => esc((v || []).join("; ")), max: 200 }]) : ""}`).join("")}
    ${l.gate ? `<h3>Gate thresholds</h3>${table(l.gate.checks, ["suite", "metric", "op", "threshold", { key: "value", render: (v) => fmt(v, 3) }, { key: "passed", render: (v) => v ? pill("pass") : pill("fail") }])}` : ""}`;
  }
  return `<h2>Latest offline evaluation</h2>${latest}
  <h2>Eval runs</h2>${table(e.runs, ["id", "suite", "started_at", { key: "passed", render: (v) => v ? pill("pass") : pill("fail") }, { key: "summary", max: 160 }])}
  <h2>Model certifications (a model may only serve a role it passed)</h2>${table(e.certifications, ["model", "role", { key: "status", render: pill }, { key: "scores", max: 120 }, "certified_at"])}
  <h2>Online evaluation</h2><h3>Shadow candidates (agreement with production)</h3>${table(e.shadow, ["agent", "n", "agreed"], { empty: "enable features.shadow_candidates to compare a candidate prompt" })}
  <h3>Human feedback</h3>${table(e.feedback, ["incident_id", "user", "rating", "correct_category", "comment"])}
  <h3>Eval-case candidates harvested from incidents (the feedback -> eval flywheel)</h3>${table(e.candidates, ["id", "incident_id", { key: "status", render: pill }, { key: "case_json", max: 160 }])}`;
};

views.ask = async () => `<div class="note">The Analyst agent turns your question into ONE read-only SQL query over published data via the semantic layer (governed metrics & dimensions). It acts as <b>agent:analyst acting for user:${esc(user())}</b>: permissions are the intersection. PII columns are tokenized; only users with PII access see raw values (audited). Try switching "acting as".</div>
  <div class="flex"><input id="q" size="70" placeholder="e.g. total revenue by region" value="total revenue by region"><button class="btn" onclick="ask()">ask</button></div>
  <div class="flex muted">examples: ${["total revenue by region", "top 5 product by units sold", "number of orders by channel", "average order value", "emails of customers", "how many rows in vendors", "delete all sales rows"].map((x) => `<a href="#" onclick="$('#q').value='${x}';ask();return false">${x}</a>`).join(" · ")}</div><div id="ans"></div>`;
async function ask() {
  const r = await post("/api/ask", { question: $("#q").value, tenant: "default" });
  $("#ans").innerHTML = r.refused ? `<div class="note">refused: ${esc(r.reason)}</div>` : `<h3>${esc(r.answer)}</h3><div class="kv"><div>SQL</div><div><code>${esc(r.sql)}</code></div><div>acting as</div><div>${esc(r.acting_as)}</div><div>tables</div><div>${esc(r.tables.join(", "))}</div><div>PII detokenized</div><div>${r.pii_detokenized}</div><div>evidence</div><div>${esc(r.evidence_id)}</div></div>${table(r.rows.map((row) => Object.fromEntries(r.columns.map((c, i) => [c, row[i]]))), r.columns)}`;
}

views.knowledge = async () => {
  const [docs, mem] = await Promise.all([api("/api/knowledge"), api("/api/memory")]);
  return `<div class="note">Grounding sources. Curated runbooks are <b>trusted</b>; documents dropped into the inbox are <b>unverified</b> (or <b>untrusted</b> if the injection detector fired) until a human promotes them. Lessons proposed by the Learner agent are candidates until approved - memory-poisoning defense.</div>
  <div class="flex"><input id="kq" size="50" placeholder="search the knowledge base (BM25)"><button class="btn" onclick="ksearch()">search</button></div><div id="kres"></div>
  <h2>Documents</h2>${table(docs, ["title", { key: "trust", render: pill }, "doc_type", "tenant", "source", { key: "summary", max: 100 }])}
  <h2>Episodic memory</h2>${table(mem, ["title", { key: "status", render: pill }, { key: "trust", render: pill }, { key: "content", max: 140 }, { key: "flags", max: 60 },
    { key: "id", label: "", render: (v, r) => r.status === "candidate" ? `<button class="btn ok" onclick="memDecide('${v}',true)">promote</button> <button class="btn danger" onclick="memDecide('${v}',false)">reject</button>` : "" }])}`;
};
async function ksearch() { const r = await api(`/api/knowledge?q=${encodeURIComponent($("#kq").value)}`); $("#kres").innerHTML = table(r, [{ key: "score" }, { key: "trust", render: pill }, "title", { key: "text", max: 220 }]); }
async function memDecide(id, ok) { await post(`/api/memory/${id}/decide`, { approve: ok }); toast(ok ? "promoted: agents may now recall it" : "rejected"); render(); }

// ------------------------------------------------------------------ shell
async function render() {
  document.querySelectorAll("nav button").forEach((b) => b.classList.toggle("active", b.dataset.tab === current));
  try { $("#main").innerHTML = await views[current](); } catch (e) { $("#main").innerHTML = `<div class="note">error: ${esc(e.message)}</div>`; }
}
function init() {
  $("#tabs").innerHTML = TABS.map(([k, l]) => `<button data-tab="${k}">${l}</button>`).join("");
  document.querySelectorAll("nav button").forEach((b) => b.onclick = () => { current = b.dataset.tab; location.hash = current; render(); });
  api("/api/llm").then((s) => { $("#profile").innerHTML = Object.keys(s.routes ? { offline: 1, ollama: 1, azure: 1, openai: 1 } : {}).map((p) => `<option ${p === s.active_profile ? "selected" : ""}>${p}</option>`).join(""); });
  $("#profile").onchange = async (e) => { await post("/api/llm/profile", { profile: e.target.value }); toast(`LLM profile -> ${e.target.value}`); };
  $("#user").onchange = render;
  render();
  setInterval(() => { if ($("#auto").checked && $("#drawer").classList.contains("hidden") && !["ask", "lab", "governance"].includes(current)) render(); }, 4000);
}
init();
