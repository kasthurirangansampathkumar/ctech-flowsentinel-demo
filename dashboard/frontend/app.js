const API = "";
let CATALOG = [];

/* ---------------- Execution mode (Demo Mode vs Production Mode) ---------------- */
function isLiveMode() {
  return localStorage.getItem("flowsentinel_live_mode") === "true";
}

function demoModeHeader() {
  return { "X-Demo-Mode": isLiveMode() ? "live" : "visual" };
}

function onModeToggle() {
  const checked = document.getElementById("live-mode-toggle").checked;
  localStorage.setItem("flowsentinel_live_mode", checked ? "true" : "false");
  applyModeUI();
  refreshAll(); // switch Summary between demo sample data and real Composer/BigQuery immediately
}

function applyModeUI() {
  const live = isLiveMode();
  document.getElementById("live-mode-toggle").checked = live;
  document.getElementById("mode-label-compact").textContent = live ? "Production Mode" : "Demo Mode";
  document.getElementById("mode-toggle-compact").classList.toggle("live", live);
  document.getElementById("demo-mode-panel").style.display = live ? "none" : "flex";
  document.getElementById("production-mode-panel").style.display = live ? "flex" : "none";
}

async function syncFromGithub(btn) {
  btn.disabled = true; btn.textContent = "Syncing…";
  const res = await adminPost("/api/tickets/sync-github");
  btn.disabled = false; btn.textContent = "🔄 Sync from GitHub";
  document.getElementById("sync-confirm").textContent = res.error
    ? `⚠️ ${res.error}`
    : `✅ ${res.synced.length} new ticket(s) synced from ${res.total_open_issues} open incident issue(s).`;
  await refreshTickets();
}

async function j(path, opts = {}) {
  const headers = { ...(opts.headers || {}), ...demoModeHeader() };
  const res = await fetch(API + path, { ...opts, headers });
  return res.json();
}

function adminKey() {
  let key = sessionStorage.getItem("flowsentinel_admin_key");
  if (!key) {
    key = window.prompt("Admin key required for Live GCP Mode (ask whoever deployed the dashboard):") || "";
    sessionStorage.setItem("flowsentinel_admin_key", key);
  }
  return key;
}

async function adminPost(path, body) {
  // Only Live GCP Mode actions are actually gated server-side -- skip the
  // key prompt entirely in Visual Demo mode so it never interrupts the flow.
  const authHeaders = isLiveMode() ? { "X-Admin-Key": adminKey() } : {};
  const res = await fetch(API + path, {
    method: "POST",
    headers: { ...authHeaders, ...demoModeHeader(), ...(body ? { "Content-Type": "application/json" } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (res.status === 401) sessionStorage.removeItem("flowsentinel_admin_key");
  return res.json();
}

function badge(text, color) { return `<span class="pill ${color}">${text}</span>`; }

function levelColor(level) {
  return { autonomous: "green", assisted: "amber", advisory: "grey" }[level] || "grey";
}
function statusColor(status) {
  return { resolved: "green", working: "amber", awaiting_approval: "amber", advisory: "grey", needs_attention: "red", new: "grey", rolled_back: "grey" }[status] || "grey";
}
function statusLabel(status) {
  return { resolved: "Resolved", working: "Working…", awaiting_approval: "Awaiting Approval", advisory: "Advisory", needs_attention: "Needs Attention", new: "New", rolled_back: "Rolled Back" }[status] || status;
}

/* ---------------- Tabs ---------------- */
function initTabs() {
  document.querySelectorAll("nav.tabs button").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll("nav.tabs button").forEach(b => b.classList.remove("active"));
      document.querySelectorAll(".view").forEach(v => v.classList.remove("active"));
      btn.classList.add("active");
      document.getElementById(`view-${btn.dataset.view}`).classList.add("active");
    });
  });
}

/* ---------------- Summary view ---------------- */
async function loadScorecard() {
  const el = document.getElementById("scorecard");
  const data = await j("/api/summary/pipelines");
  const tbody = document.querySelector("#pipeline-runs-table tbody");
  if (data.error) {
    el.innerHTML = `<div class="empty">⚠️ ${data.error}</div>`;
    tbody.innerHTML = `<tr><td colspan="3" class="empty">⚠️ ${data.error}</td></tr>`;
    return;
  }
  const sc = data.scorecard;
  el.innerHTML = `
    <div class="scorecard-tile"><div class="metric">${sc.total_pipelines}</div><div class="metric-label">Pipelines Monitored</div></div>
    <div class="scorecard-tile ok"><div class="metric">${sc.in_sla}</div><div class="metric-label">In SLA</div></div>
    <div class="scorecard-tile ${sc.breaching ? "warn" : ""}"><div class="metric">${sc.breaching}</div><div class="metric-label">Breaching SLA</div></div>
  `;

  tbody.innerHTML = data.pipelines.length ? data.pipelines.map(p => `
    <tr>
      <td>${p.airflow_url
        ? `<a href="${p.airflow_url}" target="_blank" title="Open in Airflow (Cloud Composer)"><b>${p.name}</b></a>`
        : `<b>${p.name}</b>`}</td>
      <td><div class="run-dots">${p.runs.map(r => `<div class="run-dot ${r}" title="${r}"></div>`).join("")}</div></td>
      <td>${badge(p.in_sla ? "In SLA" : "Breach", p.in_sla ? "green" : "red")}</td>
    </tr>
  `).join("") : `<tr><td colspan="3" class="empty">No pipelines found.</td></tr>`;
}

async function loadTableFreshness() {
  const data = await j("/api/summary/tables");
  const tbody = document.querySelector("#table-freshness-table tbody");
  if (data.error) {
    tbody.innerHTML = `<tr><td colspan="4" class="empty">⚠️ ${data.error}</td></tr>`;
    return;
  }
  tbody.innerHTML = data.tables.length ? data.tables.map(t => `
    <tr>
      <td><span class="mono" style="font-family:ui-monospace,monospace">${t.table}</span></td>
      <td>${t.most_recent_date}</td>
      <td>${t.days_since} day${t.days_since === 1 ? "" : "s"} ago</td>
      <td>${badge(t.in_sla ? "In SLA" : "Non-SLA", t.in_sla ? "green" : "red")}</td>
    </tr>
  `).join("") : `<tr><td colspan="4" class="empty">No tables found.</td></tr>`;
}

/* ---------------- Ops Overview (unchanged behavior) ---------------- */
async function loadHealth() {
  try {
    const h = await j("/api/health");
    document.getElementById("health-sub").textContent = `${h.project} · ${h.repo}`;
    document.getElementById("prod-repo-name").textContent = h.repo;
  } catch { document.getElementById("health-sub").textContent = "backend unreachable"; }
}

async function loadPipelines() {
  const el = document.getElementById("pipelines");
  const data = await j("/api/pipelines");
  if (data.error) { el.innerHTML = `<div class="empty">⚠️ ${data.error}</div>`; return; }
  if (!data.environments.length) { el.innerHTML = `<div class="empty">No Composer environments found.</div>`; return; }
  el.innerHTML = data.environments.map(e => `
    <div class="row"><span>${e.status_icon} ${e.name}</span>${badge(e.state, e.state === "RUNNING" ? "green" : "red")}</div>
  `).join("") + `<div class="empty" style="margin-top:8px">Watching DAG: <b>${data.dag_watch}</b></div>`;
}

async function loadFeedStatus() {
  const el = document.getElementById("feed-status");
  const data = await j("/api/feed-status");
  if (data.error) { el.innerHTML = `<div class="empty">⚠️ ${data.error}</div>`; return; }
  if (!data.feeds.length) { el.innerHTML = `<div class="empty">No feeds configured.</div>`; return; }
  el.innerHTML = data.feeds.map(f => `
    <div class="row" style="flex-direction:column; align-items:stretch; gap:4px">
      <div style="display:flex; justify-content:space-between">
        <span><b>${f.key}</b> — ${f.description}</span>
        ${badge(f.file_present ? "ON TIME" : "SLA BREACH", f.file_present ? "green" : "red")}
      </div>
      <div style="color:var(--faint); font-size:12px">${f.path} · deadline ${f.sla_deadline} · DAG ${f.dag_state || "n/a"}</div>
    </div>
  `).join("");
}

async function loadIncidents() {
  const el = document.getElementById("incidents");
  const data = await j("/api/incidents");
  if (data.error) { el.innerHTML = `<div class="empty">⚠️ ${data.error}</div>`; return; }
  if (!data.issues.length) {
    el.innerHTML = `<div class="empty">No incidents yet. ${isLiveMode() ? "Waiting on real GitHub Issues." : "Inject a failure and classify it on the Ticket Board."}</div>`;
    return;
  }
  el.innerHTML = data.issues.map(i => `
    <div class="row">${i.url ? `<a href="${i.url}" target="_blank">#${i.number} ${i.title}</a>` : `<span>#${i.number} ${i.title}</span>`}${badge(i.state, i.state === "open" ? "red" : "green")}</div>
  `).join("");
}

async function loadPRs() {
  const el = document.getElementById("prs");
  const data = await j("/api/pull-requests");
  if (data.error) { el.innerHTML = `<div class="empty">⚠️ ${data.error}</div>`; return; }
  if (!data.pull_requests.length) {
    el.innerHTML = `<div class="empty">No PRs awaiting approval. ${isLiveMode() ? "Waiting on a real open GitHub PR." : "Nothing sitting in Awaiting Approval on the Ticket Board."}</div>`;
    return;
  }
  el.innerHTML = data.pull_requests.map(p => `
    <div class="row">${p.url ? `<a href="${p.url}" target="_blank">#${p.number} ${p.title}</a>` : `<span>#${p.number} ${p.title}</span>`}
      <button class="btn" onclick="${p.kind === "ticket" ? `approveTicketFromSummary('${p.number}', this)` : `approvePr(${p.number}, this)`}">Approve &amp; Auto-Deploy</button></div>
  `).join("");
}

async function approvePr(number, btn) {
  btn.disabled = true; btn.textContent = "Merging…";
  try {
    const res = await adminPost(`/api/approve-pr/${number}`);
    btn.textContent = res.merged ? "✅ Merged" : "❌ Failed";
    if (res.merged) setTimeout(loadPRs, 1500);
  } catch { btn.textContent = "❌ Failed"; }
}

async function approveTicketFromSummary(id, btn) {
  btn.disabled = true; btn.textContent = "Merging…";
  await adminPost(`/api/tickets/${id}/approve`);
  btn.textContent = "✅ Merged";
  refreshTickets();
  setTimeout(loadPRs, 1200);
}

async function loadReadout() {
  const el = document.getElementById("readout-text");
  const tag = document.getElementById("readout-tag");
  const data = await j("/api/summary/readout");
  el.textContent = data.text || "Unable to generate a read-out right now.";
  tag.textContent = data.engine === "gemini" ? "🧠 Gemini" : "📋 Template";
  tag.className = `readout-tag ${data.engine === "gemini" ? "gemini" : "template"}`;
}

/* ---------------- Demo Console ---------------- */
async function loadCatalog() {
  const data = await j("/api/failure-catalog");
  CATALOG = data.failures || [];
  const select = document.getElementById("demo-category");
  select.innerHTML = `<option value="auto">Let the Segmenting Agent classify it</option>` +
    CATALOG.map(f => `<option value="${f.key}">${f.label} (${f.automation_level})</option>`).join("");
  select.addEventListener("change", () => {
    const f = CATALOG.find(c => c.key === select.value);
    document.getElementById("demo-placeholder-hint").textContent = f ? `Example: "${f.placeholder}"` : "";
  });
}

async function submitTicket() {
  const category = document.getElementById("demo-category").value;
  const pipeline = document.getElementById("demo-pipeline").value;
  const table = document.getElementById("demo-table").value;
  const message = document.getElementById("demo-message").value;
  const res = await j("/api/tickets", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ category, pipeline, table, message }),
  });
  document.getElementById("demo-message").value = "";
  document.getElementById("demo-confirm").textContent =
    `✅ Ticket #${res.id} created ("${res.title}"). Go to the Ticket Board to categorize and work it.`;
  await refreshTickets();
  return res;
}

async function resetDemo() {
  await j("/api/demo/reset", { method: "POST" });
  document.getElementById("demo-confirm").textContent = "";
  await refreshTickets();
}

async function beginDemo(btn) {
  btn.disabled = true; btn.textContent = "Injecting…";
  const res = await j("/api/demo/seed", { method: "POST" });
  btn.disabled = false; btn.textContent = "🎬 Begin Demo";
  document.getElementById("demo-confirm").textContent =
    `✅ ${res.created.length} failures injected. Go to the Ticket Board and run the Issue Segment Agent.`;
  await refreshTickets();
}

async function classifyAll(btn) {
  btn.disabled = true; btn.textContent = "Classifying…";
  const res = await j("/api/tickets/classify-all", { method: "POST" });
  btn.disabled = false; btn.textContent = "🧭 Run Issue Segment Agent";
  await refreshTickets();
  return res;
}

async function classifyOne(id, btn) {
  btn.disabled = true; btn.textContent = "Classifying…";
  await j(`/api/tickets/${id}/classify`, { method: "POST" });
  await refreshTickets();
}

async function startFix(id, btn) {
  btn.disabled = true; btn.textContent = "Working…";
  await adminPost(`/api/tickets/${id}/start-fix`);
  await refreshTickets();
}

async function startAnalysis(id, btn) {
  btn.disabled = true; btn.textContent = "Analyzing…";
  await adminPost(`/api/tickets/${id}/start-analysis`);
  await refreshTickets();
}

async function approveTicket(id, btn) {
  btn.disabled = true; btn.textContent = "Applying…";
  await adminPost(`/api/tickets/${id}/approve`);
  await refreshTickets();
}

async function addGuidance(id, btn) {
  const note = window.prompt("Guidance for the agent (e.g. a constraint, a correction, extra context):");
  if (!note) return;
  btn.disabled = true;
  await adminPost(`/api/tickets/${id}/guidance`, { note });
  btn.disabled = false;
  await refreshTickets();
}

async function requestChanges(id, btn) {
  const note = window.prompt("What should the agent do differently? This re-runs the analysis with your note.");
  if (!note) return;
  btn.disabled = true; btn.textContent = "Re-analyzing…";
  await adminPost(`/api/tickets/${id}/request-changes`, { note });
  await refreshTickets();
}

async function rollbackTicket(id, btn) {
  if (!window.confirm("Roll back this ticket's fix? This reverts the applied change.")) return;
  btn.disabled = true; btn.textContent = "Rolling back…";
  await adminPost(`/api/tickets/${id}/rollback`);
  await refreshTickets();
}

function ticketAction(t) {
  switch (t.status) {
    case "new":
      return `<button class="btn" onclick="classifyOne('${t.id}', this)">Categorize</button>`;
    case "queued_autonomous":
      return `<button class="btn" onclick="startFix('${t.id}', this)">Start Fix</button>`;
    case "queued_assisted":
      return `<button class="btn" onclick="startAnalysis('${t.id}', this)">Start Analysis</button>`
           + `<button class="btn ghost" onclick="addGuidance('${t.id}', this)">💬 Add Guidance</button>`;
    case "awaiting_approval":
      return `<button class="btn" onclick="approveTicket('${t.id}', this)">Approve &amp; Apply Fix</button>`
           + `<button class="btn ghost" onclick="requestChanges('${t.id}', this)">✏️ Request Changes</button>`;
    case "needs_attention":
      return `<span style="font-size:12px;color:var(--red)">Needs a human look</span>`;
    case "resolved":
      return t.rollback_available
        ? `<button class="btn ghost" onclick="rollbackTicket('${t.id}', this)">↩️ Rollback</button>`
        : "";
    default:
      return "";
  }
}

function renderTicket(t) {
  return `
    <div class="ticket">
      <div class="ticket-head">
        <div>
          <div class="ticket-title">${t.title}</div>
          <div class="ticket-meta">#${t.id}${t.category_label ? ` · ${t.category_label}` : ""}
            ${t.confidence ? ` · confidence ${(t.confidence * 100).toFixed(0)}%` : ""}</div>
        </div>
        ${t.automation_level ? badge(t.automation_level, levelColor(t.automation_level)) : ""}
      </div>
      <div class="ticket-desc">${t.description}</div>
      ${t.log && t.log.length ? `<div class="ticket-log">${t.log.join("\n\n")}</div>` : ""}
      <div class="ticket-actions">
        ${ticketAction(t)}
        ${t.github_issue_url ? `<a href="${t.github_issue_url}" target="_blank" style="font-size:12px">GitHub Issue ↗</a>` : ""}
      </div>
    </div>
  `;
}

const BUCKETS = {
  new: "col-new", queued_autonomous: "col-autonomous", queued_assisted: "col-assisted",
  awaiting_approval: "col-assisted", advisory: "col-advisory",
  resolved: "col-resolved", needs_attention: "col-resolved", rolled_back: "col-resolved",
};
const COUNTS = {
  "col-new": "count-new", "col-autonomous": "count-autonomous", "col-assisted": "count-assisted",
  "col-advisory": "count-advisory", "col-resolved": "count-resolved",
};

async function refreshTickets() {
  const data = await j("/api/tickets");
  const tickets = data.tickets || [];
  const grouped = { "col-new": [], "col-autonomous": [], "col-assisted": [], "col-advisory": [], "col-resolved": [] };
  tickets.forEach(t => grouped[BUCKETS[t.status] || "col-new"].push(t));

  Object.entries(grouped).forEach(([colId, items]) => {
    const el = document.getElementById(colId);
    el.innerHTML = items.length ? items.map(renderTicket).join("") : `<div class="empty">Nothing here.</div>`;
    document.getElementById(COUNTS[colId]).textContent = items.length;
  });
}

function refreshAll() {
  loadHealth(); loadReadout(); loadScorecard(); loadTableFreshness();
  loadPipelines(); loadFeedStatus(); loadIncidents();
  loadPRs(); refreshTickets();
}

initTabs();
applyModeUI();
loadCatalog();
refreshAll();
setInterval(refreshAll, 10000);
