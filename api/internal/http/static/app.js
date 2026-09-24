/* ABI dashboard — batch (Phase 0) + live replay (Phase 1) */
"use strict";

const state = {
  range: "all",
  latest: null,
  dataStart: null,
  dataEnd: null,
};

const BRL = new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" });
const NUM = new Intl.NumberFormat("en-US");

const PALETTE = ["#4da3ff", "#56d364", "#f0883e", "#7b61ff", "#f85149", "#3fb6c9"];

const charts = {};

async function getJSON(path) {
  const res = await fetch(path);
  if (!res.ok) {
    throw new Error(`${path} -> HTTP ${res.status}`);
  }
  return res.json();
}

function rangeParams() {
  if (state.range === "all" || !state.latest) return {};
  const to = new Date(state.latest);
  const from = new Date(to);
  from.setDate(from.getDate() - (parseInt(state.range, 10) - 1));
  const fmt = (d) =>
    `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
  return { from: fmt(from), to: fmt(to) };
}

function query(path) {
  const p = new URLSearchParams(rangeParams()).toString();
  return path + (p ? "?" + p : "");
}

function rangeLabel() {
  if (state.range === "all") return "all time";
  return `last ${state.range} days`;
}

async function load() {
  try {
    const [summary, revenue, orders, cats] = await Promise.all([
      getJSON(query("/api/v1/summary")),
      getJSON(query("/api/v1/revenue/daily")),
      getJSON(query("/api/v1/orders/daily")),
      getJSON(query("/api/v1/categories/top?metric=revenue&limit=10")),
    ]);

    const d = summary.data;
    state.latest = d.data_end ? new Date(d.data_end + "T00:00:00") : new Date();
    state.dataStart = d.data_start;
    state.dataEnd = d.data_end;

    renderKpis(d);
    renderMeta(summary);
    renderCharts(revenue.data, orders.data, cats.data);
    initForecastPicker(cats.data);
    document.querySelectorAll("[data-range-label]").forEach((el) => (el.textContent = rangeLabel()));
  } catch (err) {
    document.getElementById("kpis").innerHTML =
      `<div class="kpi"><span class="kpi-label">Error</span>` +
      `<span class="kpi-value">${err.message}</span></div>`;
  }
}

function kpi(label, value, hint) {
  return `<div class="kpi"><span class="kpi-label">${label}</span>` +
    `<span class="kpi-value">${value}</span>` +
    `<span class="kpi-hint">${hint}</span></div>`;
}

function renderKpis(d) {
  document.getElementById("kpis").innerHTML = [
    kpi("Revenue", BRL.format(d.revenue), `Data range ${d.data_start} → ${d.data_end}`),
    kpi("Orders", NUM.format(d.orders), "Non-lost orders with payment"),
    kpi("Avg order value", BRL.format(d.aov), "Revenue ÷ orders"),
    kpi("Active customers", NUM.format(d.customers), "Distinct unique customers"),
    kpi("Top category", d.top_category || "—", "By revenue, all time"),
  ].join("");
}

function renderMeta(env) {
  const cached = env.meta && env.meta.generated_at
    ? new Date(env.meta.generated_at).toLocaleString()
    : "—";
  document.getElementById("metaList").innerHTML = [
    `<li>Generated: <b>${cached}</b></li>`,
    `<li>Latency: <b>${env.meta.took_ms.toFixed(2)} ms</b></li>`,
    `<li>Data: <b>${state.dataStart} → ${state.dataEnd}</b></li>`,
    `<li>Window: <b>${rangeLabel()}</b></li>`,
    `<li>Stack: <b>MinIO → dbt → Go API</b></li>`,
  ].join("");
  document.getElementById("refreshTime").textContent = `Last refreshed ${new Date().toLocaleTimeString()}`;
}

function chartDefaults() {
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { labels: { color: "#8b949e", boxWidth: 12 } },
      tooltip: { mode: "index", intersect: false },
    },
    scales: {
      x: { ticks: { color: "#8b949e" }, grid: { color: "#21262d" } },
      y: { ticks: { color: "#8b949e" }, grid: { color: "#21262d" } },
    },
  };
}

function ensureChart(id, config) {
  if (!charts[id]) {
    const el = document.getElementById(id);
    if (!el) return null;
    charts[id] = new Chart(el, config);
  }
  return charts[id];
}

function renderCharts(revenue, orders, cats) {
  // Revenue line
  const rev = ensureChart("chartRevenue", {
    type: "line",
    data: { labels: [], datasets: [{ label: "Revenue (BRL)", data: [], borderColor: PALETTE[0], backgroundColor: "rgba(77,163,255,0.12)", fill: true, tension: 0.25, pointRadius: 0 }] },
    options: chartDefaults(),
  });
  if (rev) {
    rev.data.labels = revenue.map((p) => p.date);
    rev.data.datasets[0].data = revenue.map((p) => p.value);
    rev.update();
  }

  // Orders bar
  const ord = ensureChart("chartOrders", {
    type: "bar",
    data: { labels: [], datasets: [
      { label: "Orders", data: [], backgroundColor: PALETTE[4] },
      { label: "Delivered", data: [], backgroundColor: PALETTE[1] },
    ] },
    options: chartDefaults(),
  });
  if (ord) {
    ord.data.labels = orders.map((p) => p.date);
    ord.data.datasets[0].data = orders.map((p) => p.orders);
    ord.data.datasets[1].data = orders.map((p) => p.delivered);
    ord.update();
  }

  // Categories horizontal bar (sorted desc)
  const sorted = [...cats].sort((a, b) => b.revenue - a.revenue);
  const cat = ensureChart("chartCategories", {
    type: "bar",
    data: { labels: [], datasets: [{ label: "Revenue (BRL)", data: [], backgroundColor: PALETTE }] },
    options: { indexAxis: "y", ...chartDefaults() },
  });
  if (cat) {
    cat.data.labels = sorted.map((c) => c.category);
    cat.data.datasets[0].data = sorted.map((c) => c.revenue);
    cat.update();
  }
}

document.querySelectorAll("#range button").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll("#range button").forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");
    state.range = btn.dataset.range;
    load();
  });
});

document.getElementById("refresh").addEventListener("click", load);

/* ------------------------------------------------------------------ */
/* Phase 1 · live replay tiles (SSE)                                  */
/* ------------------------------------------------------------------ */

const live = {
  buckets: [], // recent 1-minute buckets (ascending) for the sparkline
  anomaly: null,
  es: null,
  updated: false, // true once the stream pushes data
};

function liveChart() {
  return ensureChart("chartLive", {
    type: "line",
    data: {
      labels: [],
      datasets: [
        {
          label: "Revenue (BRL)",
          data: [],
          borderColor: PALETTE[1],
          backgroundColor: "rgba(86,211,100,0.12)",
          fill: true,
          tension: 0.3,
          pointRadius: 0,
          spanGaps: true,
        },
        {
          label: "Orders",
          data: [],
          borderColor: PALETTE[0],
          borderDash: [4, 4],
          pointRadius: 0,
          yAxisID: "y2",
        },
      ],
    },
    options: {
      ...chartDefaults(),
      scales: {
        ...chartDefaults().scales,
        y2: {
          position: "right",
          grid: { drawOnChartArea: false },
          ticks: { color: "#8b949e" },
        },
      },
      plugins: { legend: { labels: { color: "#8b949e", boxWidth: 12 } } },
    },
  });
}

function renderLive(u) {
  const section = document.getElementById("liveSection");
  if (section && section.hidden) section.hidden = false;

  const cur = u.current || {};
  const badge = document.getElementById("liveBadge");

  const at = cur.bucket_start ? `${cur.bucket_start} UTC` : "—";
  document.getElementById("liveRevenue").textContent =
    cur.bucket_start ? BRL.format(cur.revenue || 0) : "—";
  document.getElementById("liveRevenueAt").textContent = at;
  document.getElementById("liveOrders").textContent =
    cur.bucket_start ? NUM.format(cur.orders || 0) : "—";
  document.getElementById("liveOrdersAt").textContent = at;
  document.getElementById("liveSessions").textContent =
    cur.bucket_start ? NUM.format(cur.active_sessions || 0) : "—";
  document.getElementById("liveSpeed").textContent =
    `speed ×${NUM.format(u.speed_multiplier || 0)} · ${u.status || "replay"}`;
  document.getElementById("liveState").textContent = `(${u.status || "replay"})`;

  badge.textContent = cur.bucket_start ? "live" : "warming up";
  badge.classList.toggle("offline", !cur.bucket_start);
  badge.classList.toggle("ok", !!cur.bucket_start);

  // Snapshots arrive on the first push; afterwards only `current` streams.
  if (u.snapshot && u.snapshot.length) {
    live.buckets = u.snapshot.slice(-60);
  } else if (cur.bucket_start) {
    const last = live.buckets[live.buckets.length - 1];
    const curStart = new Date(cur.bucket_start).getTime();
    const lastStart = last ? new Date(last.bucket_start).getTime() : null;
    if (lastStart === curStart) {
      live.buckets[live.buckets.length - 1] = cur;
    } else {
      live.buckets.push(cur);
      if (live.buckets.length > 60) live.buckets.shift();
    }
  }

  const chart = liveChart();
  if (chart) {
    chart.data.labels = live.buckets.map((b) => b.bucket_start.slice(11, 16));
    chart.data.datasets[0].data = live.buckets.map((b) => b.revenue);
    chart.data.datasets[1].data = live.buckets.map((b) => b.orders);
    chart.update();
  }

  if (u.anomaly) {
    showAnomaly(u.anomaly);
  }
}

function showAnomaly(a) {
  live.anomaly = a;
  const banner = document.getElementById("anomalyBanner");
  banner.hidden = false;
  const rateBased = (a.detector || "") === "model";
  const fmt = (v) => (String(a.metric).endsWith("_rate")
    ? `${(v * 100).toFixed(1)}%`
    : BRL.format(v));
  document.getElementById("anomalyTitle").textContent =
    `${a.metric} anomaly · ${rateBased ? "model rate" : "statistical"} · ${a.severity}`;
  document.getElementById("anomalyDetail").textContent =
    `bucket ${a.bucket_start} · observed ${fmt(a.observed)} vs expected ${fmt(a.expected)}` +
    (rateBased ? " · >3× baseline" : ` · z=${a.z_score.toFixed(2)}`);
}

async function refreshOpenAnomalies() {
  try {
    const res = await fetch("/api/v1/anomalies");
    if (!res.ok) return;
    const env = await res.json();
    const list = env.data || [];
    if (list.length) showAnomaly(list[0]);
  } catch {
    /* offline — banner stays hidden */
  }
}

async function connectLive() {
  const section = document.getElementById("liveSection");
  const badge = document.getElementById("liveBadge");

  // First paint from the REST read surface (no SSE dependency).
  try {
    const res = await fetch("/api/v1/realtime/metrics?limit=60");
    if (res.ok) {
      const env = await res.json();
      const buckets = (env.data && env.data.buckets) || [];
      if (buckets.length) {
        section.hidden = false;
        live.buckets = buckets;
        renderLive({
          current: buckets[buckets.length - 1],
          snapshot: buckets,
          speed_multiplier: 0,
          status: "replay",
        });
      }
    }
  } catch {
    /* server not up yet */
  }

  // Then subscribe to the SSE stream for continuous updates.
  if (live.es) {
    live.es.close();
    live.es = null;
  }
  badge.textContent = "connecting…";
  badge.classList.add("offline");
  badge.classList.remove("ok");

  const es = new EventSource("/api/v1/stream/metrics");
  live.es = es;

  es.addEventListener("metrics", (ev) => {
    try {
      renderLive(JSON.parse(ev.data));
    } catch (err) {
      console.warn("bad live frame", err);
    }
  });
  es.addEventListener("open", () => {
    badge.textContent = "connected";
    badge.classList.remove("offline");
  });
  es.onerror = () => {
    badge.textContent = "offline";
    badge.classList.add("offline");
    badge.classList.remove("ok");
  };
}

document.getElementById("dismissAnomaly").addEventListener("click", async () => {
  if (!live.anomaly || !live.anomaly.id) return;
  try {
    const res = await fetch(`/api/v1/anomalies/${live.anomaly.id}/dismiss`, { method: "POST" });
    if (res.ok) {
      live.anomaly = null;
      document.getElementById("anomalyBanner").hidden = true;
    }
  } catch {
    /* keep banner on failure */
  }
});

/* ------------------------------------------------------------------ */
/* Phase 3 · model panels: forecast band, churn board, fraud feed      */
/* ------------------------------------------------------------------ */

const risk = { fraudTimer: null };

function shortID(id) {
  if (!id) return "—";
  return id.length > 14 ? `${id.slice(0, 12)}…` : id;
}

// Thresholds mirror the registry's recommended_thresholds (fraud 0.795,
// bot 0.5, churn 0.5 are the active rows); anything under is low for display.
function scoreClass(v) {
  if (v >= 0.7) return "high";
  if (v >= 0.4) return "mid";
  return "low";
}

function pct(v) { return `${(v * 100).toFixed(1)}%`; }

function sourceOf(p) {
  return (p.metadata && p.metadata.source) || "batch";
}

function initForecastPicker(cats) {
  const sel = document.getElementById("forecastCat");
  if (!sel || sel.options.length) return;
  (cats || []).slice(0, 8).forEach((c) => {
    const opt = document.createElement("option");
    opt.value = c.category;
    opt.textContent = c.category;
    sel.appendChild(opt);
  });
  if (!sel.options.length) return;
  sel.addEventListener("change", () => loadForecast(sel.value));
  loadForecast(sel.value);
}

async function loadForecast(category) {
  try {
    const env = await getJSON(`/api/v1/forecast/${encodeURIComponent(category)}`);
    renderForecast(env.data || {});
  } catch (err) {
    document.getElementById("forecastBand").hidden = true;
    const chart = charts.chartForecast;
    if (chart) {
      chart.data.labels = [];
      chart.data.datasets.forEach((ds) => { ds.data = []; });
      chart.update();
    }
  }
}

function renderForecast(d) {
  const series = d.series || [];
  const ord = (d.orders || [])[0];
  const rev = (d.revenue || [])[0];

  const band = document.getElementById("forecastBand");
  const items = [];
  if (ord) {
    items.push(`<div class="band-item"><span class="band-k">Orders forecast</span>` +
      `<span class="band-v">${NUM.format(Math.round(ord.prediction))}</span>` +
      `<span class="band-h">±MAE band ${NUM.format(Math.round(ord.lower_bound))}–${NUM.format(Math.round(ord.upper_bound))}</span></div>`);
  }
  if (rev) {
    items.push(`<div class="band-item"><span class="band-k">Revenue forecast</span>` +
      `<span class="band-v">${BRL.format(rev.prediction)}</span>` +
      `<span class="band-h">±MAE band ${BRL.format(rev.lower_bound)}–${BRL.format(rev.upper_bound)} · wMAPE ${pct(rev.wmape || 0)}</span></div>`);
  }
  const horizon = (ord && ord.week_start) || (rev && rev.week_start) || "";
  if (horizon && items.length) {
    items.push(`<div class="band-item"><span class="band-k">Horizon</span>` +
      `<span class="band-v">${horizon}</span>` +
      `<span class="band-h">weekly forecast · backtest series</span></div>`);
  }
  band.innerHTML = items.join("");
  band.hidden = items.length === 0;

  const chart = ensureChart("chartForecast", {
    type: "line",
    data: {
      labels: [],
      datasets: [
        { label: "Orders · forecast", data: [], borderColor: PALETTE[0], tension: 0.25, pointRadius: 0, fill: false },
        { label: "Orders · actual", data: [], borderColor: PALETTE[0], borderDash: [5, 4], pointStyle: "rectRot", fill: false },
        { label: "Revenue · forecast (BRL)", data: [], borderColor: PALETTE[1], tension: 0.25, pointRadius: 0, fill: false, yAxisID: "y2" },
        { label: "Revenue · actual (BRL)", data: [], borderColor: PALETTE[1], borderDash: [5, 4], pointStyle: "rectRot", fill: false, yAxisID: "y2" },
      ],
    },
    options: {
      ...chartDefaults(),
      scales: {
        ...chartDefaults().scales,
        y2: { position: "right", grid: { drawOnChartArea: false }, ticks: { color: "#8b949e" } },
      },
    },
  });
  if (chart) {
    chart.data.labels = series.map((p) => p.week_start);
    chart.data.datasets[0].data = series.map((p) => p.orders);
    chart.data.datasets[1].data = series.map((p) => p.orders_actual);
    chart.data.datasets[2].data = series.map((p) => p.revenue);
    chart.data.datasets[3].data = series.map((p) => p.revenue_actual);
    chart.update();
  }
}

async function loadModelRisk() {
  try {
    const env = await getJSON("/api/v1/predictions?model=churn_risk&limit=50");
    const rows = (env.data || [])
      .slice()
      .sort((a, b) => b.prediction - a.prediction)
      .slice(0, 12);
    const tbody = document.getElementById("churnBoard");
    tbody.innerHTML = rows.map((p) =>
      `<tr><td class="id-cell">${shortID(p.entity_id)}</td>` +
      `<td class="score-cell"><span class="score ${scoreClass(p.prediction)}">${pct(p.prediction)}</span></td>` +
      `<td class="score-cell">${(p.confidence * 100).toFixed(0)}%</td>` +
      `<td class="time-cell">${(p.predicted_at || "").slice(0, 16).replace("T", " ")}</td></tr>`
    ).join("");
  } catch {
    document.getElementById("churnBoard").innerHTML =
      `<tr><td colspan="4" class="muted-cell">churn board unavailable</td></tr>`;
  }
}

async function loadFraudFeed() {
  let rows = null;
  try {
    const env = await getJSON("/api/v1/predictions?model=fraud_risk&source=stream_score&limit=25");
    rows = env.data || [];
  } catch { /* fall through to unfiltered */ }
  if (!rows || !rows.length) {
    try {
      const env = await getJSON("/api/v1/predictions?model=fraud_risk&limit=25");
      rows = env.data || [];
    } catch { rows = null; }
  }
  const tbody = document.querySelector("#fraudFeed tbody");
  if (!rows || !rows.length) {
    tbody.innerHTML = `<tr><td colspan="5" class="muted-cell">no stream-scored fraud predictions yet</td></tr>`;
    return;
  }
  tbody.innerHTML = rows.map((p) =>
    `<tr><td class="id-cell">${shortID(p.entity_id)}</td>` +
    `<td class="score-cell"><span class="score ${scoreClass(p.prediction)}">${pct(p.prediction)}</span></td>` +
    `<td class="score-cell">${(p.confidence * 100).toFixed(0)}%</td>` +
    `<td><span class="src-badge ${sourceOf(p)}">${sourceOf(p)}</span></td>` +
    `<td class="time-cell">${(p.predicted_at || "").slice(0, 16).replace("T", " ")}</td></tr>`
  ).join("");
}

function startFraudFeed() {
  loadFraudFeed();
  if (!risk.fraudTimer) risk.fraudTimer = setInterval(loadFraudFeed, 15000);
}

connectLive();
refreshOpenAnomalies();
loadModelRisk();
startFraudFeed();

/* ------------------------------------------------------------------ */
/* Phase 4 · governance: approval queue, action history, model health  */
/* ------------------------------------------------------------------ */

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

function statusPill(s) {
  return `<span class="status-pill pill-${escapeHtml(s || "unknown")}">${escapeHtml(s || "unknown")}</span>`;
}

async function decide(id, reject, reason, input) {
  const verb = reject ? "reject" : "approve";
  let res;
  try {
    res = await fetch(`/api/v1/actions/${id}/${verb}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ reason }),
    });
  } catch (err) {
    if (input) { input.placeholder = "server unreachable"; input.classList.add("bad-input"); }
    return;
  }
  if (res.ok) {
    if (input) { input.value = ""; input.classList.remove("bad-input"); }
    loadApprovalQueue();
    loadActionHistory();
    return;
  }
  let msg = `HTTP ${res.status}`;
  try { msg = (await res.json()).error || msg; } catch { /* keep msg */ }
  if (input) { input.placeholder = msg; input.classList.add("bad-input"); }
}

async function loadApprovalQueue() {
  const tbody = document.getElementById("approvalQueue");
  let rows = [];
  try {
    const env = await getJSON("/api/v1/actions?status=pending&limit=50");
    rows = env.data || [];
  } catch { /* fall through */ }
  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="7" class="muted-cell">no pending actions — approval queue is clear</td></tr>`;
    return;
  }
  tbody.innerHTML = rows.map((a) =>
    `<tr data-id="${a.id}">` +
    `<td class="id-cell">#${a.id}</td>` +
    `<td>${escapeHtml(a.action)}</td>` +
    `<td class="id-cell">${shortID(a.entity)}</td>` +
    `<td>${statusPill(a.risk_tier)}</td>` +
    `<td class="id-cell">${escapeHtml(a.rule)}</td>` +
    `<td class="time-cell">${(a.created_at || "").slice(0, 16).replace("T", " ")}</td>` +
    `<td class="action-cell">` +
    `<input class="reason-input" placeholder="reason…" />` +
    `<button class="btn-action approve" data-decision="approve">Approve</button>` +
    `<button class="btn-action reject" data-decision="reject">Reject</button>` +
    `</td></tr>`
  ).join("");
}

async function loadActionHistory() {
  const tbody = document.getElementById("actionHistory");
  let rows = [];
  try {
    const env = await getJSON("/api/v1/actions?limit=100");
    rows = env.data || [];
  } catch { /* fall through */ }
  rows = rows.filter((a) => a.status !== "pending").slice(0, 25);
  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="7" class="muted-cell">no decided actions yet</td></tr>`;
    return;
  }
  tbody.innerHTML = rows.map((a) =>
    `<tr data-id="${a.id}">` +
    `<td class="id-cell">#${a.id}</td>` +
    `<td>${escapeHtml(a.action)}</td>` +
    `<td class="id-cell">${shortID(a.entity)}</td>` +
    `<td>${statusPill(a.status)}</td>` +
    `<td class="id-cell">${escapeHtml(a.rule)}</td>` +
    `<td class="time-cell">${((a.decided_at || a.executed_at || a.created_at) || "").slice(0, 16).replace("T", " ")}</td>` +
    `<td class="action-cell"><button class="btn-action trace" data-trace="${a.id}">trace</button></td>` +
    `</tr>`
  ).join("");
}

async function loadModelDrift() {
  const tbody = document.getElementById("driftBoard");
  let rows = [];
  try {
    const env = await getJSON("/api/v1/model-drift?limit=60");
    rows = env.data || [];
  } catch { /* fall through */ }
  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="6" class="muted-cell">no drift measurements yet — run <code>make monitor</code></td></tr>`;
    return;
  }
  tbody.innerHTML = rows.map((r) =>
    `<tr>` +
    `<td class="id-cell">${escapeHtml(r.model)}</td>` +
    `<td>${escapeHtml(r.feature)}</td>` +
    `<td class="score-cell">${r.psi.toFixed(4)}</td>` +
    `<td>${statusPill(r.status)}</td>` +
    `<td>${escapeHtml(r.kind)}</td>` +
    `<td class="time-cell">${(r.computed_at || "").slice(0, 16).replace("T", " ")}</td>` +
    `</tr>`
  ).join("");
}

document.getElementById("approvalQueue").addEventListener("click", (ev) => {
  const btn = ev.target.closest("button[data-decision]");
  if (!btn) return;
  const tr = ev.target.closest("tr[data-id]");
  const input = tr.querySelector(".reason-input");
  const reason = input.value.trim();
  if (!reason) { input.classList.add("bad-input"); return; }
  decide(Number(tr.dataset.id), btn.dataset.decision === "reject", reason, input);
});

document.getElementById("actionHistory").addEventListener("click", async (ev) => {
  const btn = ev.target.closest("button[data-trace]");
  if (!btn) return;
  const box = document.getElementById("actionTrace");
  box.hidden = false;
  box.innerHTML = `<span class="muted-cell">loading trace…</span>`;
  try {
    const env = await getJSON(`/api/v1/actions/${btn.dataset.trace}/trace`);
    const t = env.data || {};
    const audit = t.audit || [];
    const act = t.action || {};
    box.innerHTML =
      `<div class="trace-head"><h3>Trace · action #${act.id} <span class="hint">(${escapeHtml(act.rule || "")})</span></h3>` +
      `<button class="btn-action close-trace" type="button">close</button></div>` +
      audit.map((e) =>
        `<div class="trace-row">` +
        `<span class="trace-trans">${escapeHtml(e.transition)}</span>` +
        `<span class="trace-actor">${escapeHtml(e.actor)}</span>` +
        `<span class="trace-at">${((e.at || "").slice(0, 19) || "").replace("T", " ")}</span>` +
        (e.reason ? `<span class="trace-reason">“${escapeHtml(e.reason)}”</span>` : "") +
        (e.detail ? `<code class="trace-detail">${escapeHtml(JSON.stringify(e.detail))}</code>` : "") +
        `</div>`
      ).join("");
  } catch {
    box.innerHTML = `<span class="muted-cell">trace unavailable</span>`;
  }
});

document.getElementById("actionTrace").addEventListener("click", (ev) => {
  if (ev.target.closest(".close-trace")) ev.target.closest(".close-trace").parentElement.hidden = true;
});

loadApprovalQueue();
loadActionHistory();
loadModelDrift();
setInterval(() => { loadApprovalQueue(); loadActionHistory(); loadModelDrift(); }, 20000);

load();