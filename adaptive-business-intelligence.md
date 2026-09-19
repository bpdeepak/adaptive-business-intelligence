# Adaptive Business Intelligence (ABI)

### A Real-Time, Explainable, Agentic BI Platform for E-Commerce

---

## 1. Why Rebuild This Project

Your original problem statement (data volume/velocity, lack of real-time adaptability, hidden patterns, weak decision support, hard-to-manage AI) was written for the pre-LLM, pre-agent era of BI. Most of it is now table stakes — dashboards with sub-5-minute latency, basic recommendation engines, and rule-based fraud flags are commodity features in 2026. Research into where the field has actually moved shows three things worth building around instead:

1. **BI stopped being a reporting function and became an operational one.** Systems increasingly *act* (pause a campaign, reprice a SKU, trigger a retention email) rather than just report.
2. **AI agents are now a class of site visitor and a class of analyst.** Shoppers use AI agents to browse/compare/buy, and businesses increasingly ask analytics questions in natural language and expect an agent to fetch, join, and answer — not just render a chart.
3. **Cross-platform / cross-channel synthesis is the actual bottleneck, not per-platform analytics.** Native dashboards (Shopify, Meta, Adobe) each answer their own slice; nobody answers "how did last week's ad spend affect margin on Shopify orders."

So the rebuild should keep your five original pillars as the *skeleton* (they're still valid framing) but re-target each one at what's actually hard today.

---

## 2. Updated Problem Statements (2026-relevant)

| **#Old framingRewritten, current framing** |                                                   |                                                                                                                                                                                                                                                                   |
| ------------------------------------------ | ------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1                                          | Data volume/velocity overwhelms traditional tools | **Fragmentation, not just volume, is the bottleneck.** Data lives across storefront, ad platforms, payment processors, logistics, and support tools; no single platform's AI can answer cross-source questions.                                                   |
| 2                                          | BI is historical, not real-time                   | **BI is real-time but still passive.** Sub-minute dashboards exist; what's missing is closed-loop action — insight → decision → automated execution → measured outcome.                                                                                           |
| 3                                          | Complex patterns hide in data                     | **Patterns now include agentic/bot traffic.** AI shopping agents browse, compare, and buy on a customer's behalf, producing behavioral signals legacy analytics stacks were never built to classify (agent vs. human, intent vs. noise).                          |
| 4                                          | Limited decision support                          | **Decision support exists but isn't trusted.** Forecasts/recommendations are often black-box; without explainability and confidence bounds, operators still override with intuition.                                                                              |
| 5                                          | AI integration is operationally hard              | **The hard part shifted from "can we deploy a model" to "can we govern a fleet of models and agents."** Continuous drift monitoring, explainability, human-in-the-loop guardrails, and cost control for LLM-driven analytics are now the real integration burden. |

**Net-new problem statement worth adding explicitly:**

> Decision-makers can't yet ask a single BI system a plain-language, cross-channel business question ("why did margin drop on mobile orders in the Northeast last week?") and get a trustworthy, source-cited, action-ready answer. Closing that gap — not just faster charts — is where 2026-era value lives.

---

## 3. Project Vision

**Adaptive Business Intelligence (ABI)** is a platform that ingests multi-source e-commerce data, keeps a continuously updated analytical layer, and layers three things traditional BI stacks don't combine well:

- **Real-time + predictive + prescriptive** — not just "what happened" but "what's about to happen" and "what should we do about it."
- **An agentic natural-language interface** — ask a business question in plain English, the system plans a multi-step query across sources, executes it, and returns a cited, explainable answer (not a static chart).
- **Governed AI/ML** — every model and agent decision is logged, explainable, monitored for drift, and gated by human approval where the action has financial consequence (e.g., auto-repricing above a threshold).

Think of it as "a junior data analyst + a forecasting team + a fraud analyst, all on call 24/7, who show their work."

---

## 4. Core Features

### A. Data Foundation

- Unified ingestion from transactional (orders, payments), customer (CRM/support), product (catalog, inventory), and behavioral (clickstream) sources.
- Streaming ingestion layer for near-real-time events (order placed, cart abandoned, price changed) alongside batch/historical loads.
- A semantic layer / metrics store so "revenue," "margin," and "active customer" mean the same thing everywhere in the system.

### B. Real-Time Analytics

- Streaming dashboards (sub-minute) for sales, conversion, traffic, and inventory levels.
- Anomaly detection on live metrics (sudden conversion drop, spike in refunds, checkout errors).

### C. Predictive & Prescriptive Layer

- Demand forecasting per SKU/category (with confidence intervals, not point estimates).
- Dynamic pricing recommendations (advisory by default; auto-apply only within guardrails you configure).
- Customer churn / CLV prediction feeding retention triggers.
- Inventory reorder recommendations tied to forecasted demand + lead times.

### D. Pattern & Anomaly Detection

- Fraud/risk scoring on transactions using behavioral + device signals.
- Bot/agent traffic classification (distinguishing AI shopping agents from human sessions, and from malicious bots) — this is the genuinely new pattern-detection problem in 2026.
- Market-basket / association mining for cross-sell and bundling.

### E. Agentic Natural-Language BI

- A conversational interface: "Why did AOV drop in the last 7 days?" → agent plans queries across the semantic layer, pulls the relevant tables/metrics, returns an answer with citations to the underlying numbers and a confidence note.
- Multi-step reasoning: agent can chain "find the anomaly → correlate with campaign data → check inventory stockouts" without a human writing SQL.

### F. Explainability & Governance (this is the feature that differentiates the rebuild)

- Every model prediction and agent action carries a "why" (feature importance / retrieved evidence), not just a number.
- Human-in-the-loop approval queue for any action above a configurable financial/risk threshold.
- Model monitoring: drift detection, accuracy decay alerts, retraining triggers, and a versioned audit log of what model/agent made which decision when.

### G. Action Layer (closes the loop — this is what most "BI" projects skip)

- Configurable playbooks: "if predicted stockout in <5 days, create purchase order draft"; "if fraud score > X, hold order for review"; "if churn risk > Y, enqueue retention email."
- Every automated action is logged and reversible.

---

## 5. Data: What You'll Actually Use

Since this is a rebuild/portfolio-style project, you don't need a live production e-commerce business — combine a real historical dataset with a synthetic real-time generator:

1. **Historical/batch backbone:** the [Olist Brazilian E-Commerce public dataset](https://www.kaggle.com/olistbr/brazilian-ecommerce) — \~100K real, anonymized orders (2016–2018) across customers, products, payments, reviews, and geolocation. It's the de facto standard for this kind of project and is rich enough for demand forecasting, churn, and market-basket work. There's also a companion **Marketing Funnel by Olist** dataset you can join in for campaign-attribution questions.
2. **Synthetic real-time stream:** write a simple event generator (Python) that "replays" Olist-like orders/clickstream events on a clock, publishing to a message queue (Kafka/Redpanda) to simulate live traffic — this is what lets you build and demo the real-time layer without needing a real store.
3. **Optional enrichment sources:** public competitor-pricing scrapes, exchange rates, or holiday/seasonality calendars, to make forecasting and dynamic-pricing features non-trivial.
4. **Synthetic fraud/bot-traffic labels:** since public datasets rarely include labeled fraud or bot-agent traffic, you'll need to synthetically inject a small % of anomalous sessions/orders with known ground truth so the anomaly/fraud/bot-classification models are actually testable.

---

## 6. Constraints That Now Shape the Plan

You've clarified four things that change the design meaningfully:

- **Deep, working system, not an MVP.** Every phase below should ship *fully*, not as a stub — this changes how much time to budget per phase, and means the "explainability/governance" pieces aren't optional polish, they're core deliverables.
- **Portfolio project for an audience.** The system needs a demo that's actually *watchable*: a live-feeling dashboard, a working conversational agent someone can type questions into, and ideally something reachable via a URL rather than "clone the repo and run docker-compose." Budget time for a deploy, not just local dev.
- **Learning goal: new stacks that help your SWE career, on top of existing Go + Python.** So the stack choices below are picked to (a) give you real reps in things that are in-demand right now — streaming systems, a semantic/metrics layer, agentic AI with tool-use, MLOps — and (b) let Go do what Go is good at (concurrent services, APIs, ingestion) instead of using Python everywhere by default.
- **Hardware reality check: RTX 3050 6GB / 16GB RAM.** This is enough for *all* the classical ML in this project (forecasting, fraud scoring, anomaly detection — these are CPU-friendly, not GPU-hungry) and enough to fine-tune/run small embedding models. It is **not** enough to comfortably run a capable local LLM for the agentic NL-BI feature — under 8GB VRAM is the tier where local LLM chat is a poor experience even quantized. So: use a hosted LLM API (e.g. Claude) for the agent's reasoning, and reserve your GPU for smaller, genuinely local-friendly ML work (embeddings, a small sequence model for fraud, experimenting with quantized 3–4B models for learning purposes). This split is also just how real systems are built in 2026 — small local/cheap models for high-volume simple tasks, a frontier API for complex reasoning — so it doubles as a legitimate architectural lesson, not a compromise.

---

## 7. Tech Stack & What Each Piece Teaches You

Split by language, since you already know Go and Python — the goal is to put each language where it's naturally strong and where the *unfamiliar* piece (the surrounding system, not the syntax) is the actual learning target.

### Go — services, concurrency, infra

| **ComponentWhat to buildWhat you learn** |                                                                                                                                      |                                                                                                       |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| Ingestion/ streaming producer & consumer | Go service(s) that replay the synthetic order/clickstream events onto Kafka/Redpanda, and a consumer that writes to the bronze layer | Kafka/Redpanda client patterns, backpressure, concurrency (goroutines/channels) under real throughput |
| Real-time API layer                      | A Go API (gRPC or REST) serving live metrics to the dashboard and the agent's tool-calls                                             | gRPC, API design under latency constraints                                                            |
| Fraud/anomaly scoring service            | A Go microservice that wraps a trained Python model (via ONNX Runtime or ONNX export) for low-latency inference at request time      | Model serving without Python's runtime overhead, ONNX interop                                         |
| Action/playbook engine                   | Go service implementing the rule-based automation ("if forecast < threshold, draft PO") with an approval-queue workflow              | Workflow/state-machine design, idempotency, audit logging                                             |
| Orchestration glue (optional stretch)    | A small Go-based job runner or use of a Go-native tool if you want to avoid Airflow's Python weight                                  | Systems-level orchestration thinking                                                                  |

### Python — data, ML, agent

| **ComponentWhat to buildWhat you learn**   |                                                                                                                                                                                            |                                                                                                                  |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------- |
| Batch ETL / dbt models                     | Bronze → silver → gold transforms, data quality checks                                                                                                                                     | dbt, data modeling, testing pipelines                                                                            |
| Forecasting & churn models                 | XGBoost/LightGBM + Prophet or statsforecast, all CPU-friendly                                                                                                                              | Classical ML you can defend in an interview — this is still what most real BI/forecasting work actually is       |
| Fraud/anomaly + bot-traffic classification | Isolation forest / gradient boosting first; optionally a small sequence model (LSTM/1D-CNN, fits easily on 6GB VRAM) for session-based bot detection                                       | Where GPU actually helps vs. where it's unnecessary                                                              |
| Explainability                             | SHAP wired into every model's prediction output                                                                                                                                            | Model interpretability, a skill many engineers skip                                                              |
| Model monitoring                           | Evidently AI (or a hand-rolled drift job) + a scheduled retraining trigger                                                                                                                 | MLOps fundamentals                                                                                               |
| Agentic NL-BI                              | LLM with tool-use (Claude API) calling into the Go API / semantic layer, multi-step planning, citation of sources                                                                          | This is the big new one: agent design, tool schemas, grounding answers in real data instead of hallucinating     |
| Embeddings / local model experimentation   | A small local embedding model (e.g., via `sentence-transformers`, or a quantized 3–4B model via `llama.cpp`/Ollama) for tasks like semantic search over product catalog or support tickets | Local inference, quantization, GGUF — genuinely useful to understand even though the main agent uses a cloud API |

### Shared / infra layer

- **Storage:** MinIO (S3-compatible, runs fine locally) for raw/bronze; Parquet/Iceberg for processed layers; Postgres for the semantic/gold layer and app state.
- **Streaming:** Redpanda over Kafka for local dev — same protocol, far lighter footprint, which matters on 16GB RAM.
- **Containerization:** Docker Compose for local dev; this alone is a good learning target if you haven't operated a multi-service stack before.
- **Observability:** Prometheus + Grafana for system metrics (separate from the business-metrics dashboard) — genuinely valuable SWE-career skill and cheap to run.
- **CI/CD:** GitHub Actions — build/test both the Go and Python services, lint, and (for the portfolio angle) auto-deploy the demo on merge.

### Deployment (for the portfolio audience)

Your laptop shouldn't be the thing people hit when they view your demo. Plan for:

- A small cloud VM (Hetzner, DigitalOcean, or similar low-cost options) hosting the Go API, the dashboard, and the agent endpoint, running against a trimmed/sampled version of the dataset so it stays cheap.
- Heavy local-only work (training, experimentation, the streaming simulation for development) stays on your laptop; you push trained model artifacts and code to the cloud deployment.
- A recorded demo walkthrough (video) as a fallback/companion to the live link — reviewers of portfolios often won't click through, so don't rely on the live demo alone.

---

## 8. Learning Roadmap (AI/ML side)

Since you're relearning the AI/ML side from scratch, sequence it so each phase's learning directly unlocks that phase's feature — don't front-load months of theory before building anything.

1. **Classical ML refresher (before Phase 2):** regression/classification basics, gradient boosting (XGBoost/LightGBM), time-series forecasting (Prophet/statsforecast), and how to evaluate models properly (backtesting for forecasts, precision/recall tradeoffs for fraud). This is the highest-leverage stretch — most of ABI's "AI" is this, not LLMs.
2. **Explainability (alongside Phase 2):** learn SHAP well enough to explain *why* a model made a prediction — this is a differentiator most portfolio projects skip entirely.
3. **Embeddings & vector search (before Phase 3):** enough to build semantic search over the product catalog — a small, contained way to learn embeddings before touching agents.
4. **LLM agents & tool-use (Phase 3):** this is the newest ground for you — learn how tool/function-calling works, how to design tool schemas the model can use reliably, how to keep an agent grounded (citing real query results, not inventing numbers), and basic prompt/context engineering. Building the NL-BI agent *is* the course here.
5. **MLOps (Phase 4):** drift detection, monitoring, retraining triggers, model versioning — the "keep AI operationally sane" skillset that's explicitly one of your original problem statements.
6. **Local inference & quantization (stretch, anytime):** run a small quantized model locally via Ollama/llama.cpp just to understand GGUF, VRAM budgeting, and CPU/GPU offload — useful knowledge even though your main agent uses a hosted API.

---

## 9. Suggested Architecture

```
                     ┌─────────────────────┐
 Historical CSVs ───▶│   Bronze (raw) layer │  (S3/MinIO, immutable landing)
 Synthetic stream ──▶│                      │
                     └─────────┬────────────┘
                               ▼
                     ┌─────────────────────┐
                     │  Silver (cleaned)    │  (typed, deduped, quality-checked)
                     └─────────┬────────────┘
                               ▼
                     ┌─────────────────────┐
                     │  Gold / semantic     │  (metrics store, dbt models)
                     └───────┬─────┬───────┘
              ┌──────────────┘     └───────────────┐
              ▼                                     ▼
     ┌──────────────────┐                 ┌───────────────────────┐
     │ Real-time serving │                 │ ML/AI layer            │
     │ (dashboards, API) │                 │ - forecasting          │
     └──────────────────┘                 │ - fraud/anomaly         │
              │                            │ - agentic NL-BI (LLM)  │
              ▼                            │ - explainability/monitor│
     ┌──────────────────┐                 └──────────┬─────────────┘
     │ Action layer /    │◀───────────────────────────┘
     │ playbooks         │
     └──────────────────┘

```

**A pragmatic stack** (swap for whatever you already know):

- **Ingestion/streaming:** Kafka or Redpanda (or just a lightweight Python producer if you want to keep it simple) for the simulated real-time feed; batch loads via Airbyte/simple scripts for the historical CSVs.
- **Storage:** S3/MinIO for raw + Parquet/Iceberg for processed layers; Postgres or DuckDB for the serving layer if you want something lightweight.
- **Transformation/orchestration:** dbt for the semantic layer, Airflow or Dagster for orchestration.
- **Processing:** Spark or Polars/DuckDB depending on scale (Olist is small enough that Spark is optional — DuckDB or Polars will comfortably handle it and simplify your stack a lot).
- **ML:** scikit-learn/XGBoost or Prophet/statsforecast for forecasting; an isolation forest / gradient-boosted classifier for fraud/anomaly; SHAP for explainability.
- **Agentic NL-BI layer:** an LLM (e.g., via the Claude API) with tool-use over your semantic layer/metrics store — this is the "ask a question in English" feature, and is the most novel/differentiating piece to build.
- **Serving/dashboards:** a BI tool (Metabase/Superset) or a custom dashboard, plus the conversational agent as a separate interface.
- **Monitoring/governance:** Evidently AI or a custom drift-monitoring job, plus a simple audit log table for every model/agent decision.

---

## 10. Suggested Build Order (Phases)

Given "deep, not MVP," treat each phase as shippable-quality, not a stub — including tests, error handling, and a demoable UI slice, before moving on.

1. **Phase 0 — Foundations (Go + Python + infra reps):** Docker Compose stack (MinIO, Postgres, Redpanda); load Olist into bronze/silver/gold via dbt; a Go API serving the gold-layer metrics; one real dashboard (revenue, orders, top categories) end to end. Prove the full pipeline before adding intelligence.
2. **Phase 1 — Real-time layer:** Go producer/consumer replaying the synthetic event stream through Redpanda into bronze in near-real-time; live-updating dashboard; a basic streaming anomaly alert (e.g., conversion drop). This is your deepest Go/concurrency phase.
3. **Phase 2 — Predictive layer:** demand forecasting per category, churn model, fraud/anomaly scoring, bot/agent traffic classification — every model wired to SHAP explanations and served through the Go inference service (ONNX) from day one, not bolted on later.
4. **Phase 3 — Agentic NL-BI:** design tool schemas over your Go API/semantic layer; build the Claude-powered agent that plans multi-step queries and answers with citations; build the chat UI. This is your flagship demo feature — budget real time here.
5. **Phase 4 — Action layer + governance:** playbooks (forecast → draft PO, fraud score → hold order, churn risk → retention trigger), human-in-the-loop approval queue, model drift monitoring, full audit log. This is what makes it "adaptive" rather than just "predictive," and it's the piece most similar projects skip — a strong differentiator for a portfolio.
6. **Phase 5 — Polish + deploy:** dynamic pricing recommendations, cross-channel attribution (join in the Marketing Funnel dataset), Prometheus/Grafana observability, CI/CD, cloud deploy of the demo, and a recorded walkthrough video.

Each phase is independently demoable — useful both for pacing the project and for having something to show at any point if you want to start writing it up publicly (blog posts, LinkedIn, a GitHub README with GIFs) as you go, which is worth doing for the portfolio angle rather than waiting until the very end.

---

## 11. Immediate Next Step

The highest-leverage next artifact is a **detailed spec for Phase 0**: the exact schema design for bronze/silver/gold, the Docker Compose file, the dbt project structure, and the Go API contract. Once that's solid, Phases 1–5 build on top of it cleanly. Say the word and I'll write that spec next — or, if you'd rather start with the agentic NL-BI tool-schema design (since that's the newest territory for you), I can start there instead.