/* ABI dashboard — Phase 0 */
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

load();