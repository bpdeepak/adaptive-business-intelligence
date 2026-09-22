# Adaptive Business Intelligence (ABI)

A real-time, explainable, agentic BI platform for e-commerce analytics. Phase 0
is a **deep, working foundations pipeline**: containerized infrastructure, a
medallion data stack on the Olist Brazilian E-Commerce dataset, an idiomatic Go
metrics API, and a browser dashboard. **Phase 1 adds the real-time layer**: the
history is replayed onto Redpanda, Go consumers compute hot 1-minute metrics
with online anomaly detection, and live SSE/gRPC streams feed live dashboard
tiles. **Phase 2 adds the predictive layer**: four gradient-boosted models
(demand forecast, churn, fraud, bot) in Python, each with SHAP explanations
from day one, a versioned Postgres model registry, every prediction persisted
with its explanation to `gold.predictions`, and a lightweight Python scoring
sidecar wired into the Go API — all built, tested, and running end to end.

| Layer | What ships |
|---|---|
| Infrastructure | Docker Compose: Postgres 17, MinIO, Redpanda 26.2 (all healthy-checked) |
| Bronze | Olist (9 CSVs, ~126 MB) → immutable objects in MinIO `bronze` bucket **and** verbatim raw tables in Postgres `bronze.*`; streaming events + bot ground truth in `bronze.stream_events` / `bronze.training_ground_truth` (with lineage) |
| Silver | 8 typed, cleaned staging views (`silver.stg_*`), dedup of duplicate reviews, NULL handling |
| Gold | `gold.fct_orders` + `gold.fct_order_items`, 4 dims, metric tables (`daily_revenue`, `daily_orders`, `daily_aov`, `top_categories`) — plus **hot-path** `gold.realtime_metrics` (1-min buckets, reconciled from bronze), `gold.anomalies`, and `gold.detector_state` (restart-resumable baseline) written by the Go realtime stack |
| Streaming | `cmd/producer` replays history (virtual clock, 3-source merge, infinite loops); `cmd/server` runs the bronze-writer + realtime aggregator (Welford/z-score + sigma floor), a retention janitor (enforces 12 h bronze / 3 h bucket budgets), a bronze→realtime reconcile (self-heals redelivery double-counts), and Prometheus `/metrics` |
| Serving | Go REST API + SSE live stream + gRPC :8090 (live metrics), embedded dashboard with live tiles + anomaly banner; **Phase 2** predict surface (`/api/v1/model-registry`, `/predictions[/latest]`, `/score`, `/models/health`) served from Postgres, with an optional Python scoring sidecar (`127.0.0.1:8093`) |
| Predictive (Phase 2) | `ml/` Python stack (uv `ml` group): LightGBM + XGBoost trained with strict time splits, SHAP `TreeExplainer` explanations on every prediction, `gold.model_registry` (5 active versions), `gold.predictions` (42k+ backtest rows + live scores, each with explanation jsonb), per-category forecast confidence bands, versioned joblib artifacts in `artifacts/`, stdlib HTTP sidecar `ml/serve.py` |
| Dashboard | Vanilla JS + Chart.js: KPI cards, daily charts, top categories, 7D/30D/90D/All windows, **live revenue/orders/sessions tiles, anomaly banner, replay-speed badge** |

## Architecture

```
                    ┌──────────────┐         ┌──────────────────┐
  raw Olist CSVs ──►│  MinIO       │         │  Postgres 17     │
  (data/raw,        │  bucket      │         │  bronze.* (raw)  │
   verified sizes)  │  bronze      │         └────────▲─────────┘
                    └──────┬───────┘                  │ dbt (Python, uv)
                           │ immutable                │
                           │ object store             │
                           │                          │
                    ┌──────▼──────────────────────────▼─────────────────┐
                    │  dbt + Postgres                                   │
  bronze ──────────►│  silver.stg_*  (typed views)                      │
                    │  gold.fct_orders / fct_order_items / dim_*        │
                    │  gold.daily_* metrics          (tables)           │
                    └───────────────┬────────────────┬─────────────────┘
                                    │                │
                    ┌───────────────▼───┐   ┌────────▼──────────────────┐
                    │ cmd/producer      │   │ cmd/server (primary bin)  │
                    │ gold-only reads → │   │ Postgres                    │
                    │ Redpanda replay   │   │ ├─ bronze-writer → bronze.* │
                    │ (virtual clock,   │   │ ├─ aggregator → gold.rt_*  │
                    │  infinite loops)  │   │ │   + online z-score alarms│
                    └───────────┬───────┘   │ ├─ REST + SSE (live)       │
                                │ events    │ └─ gRPC :8090 (live)       │
                          ┌─────▼───────────┴──────────────────────────┐ │
                          │  Redpanda (ecommerce.* topics)            │ │
                          └─────┬───────────┬──────────────────────────┘ │
                                │           │                            │
                    ┌───────────▼───┐   ┌───▼────────────────────────────▼─┐
                    │ Dashboard     │   │ SSE /api/v1/stream/metrics        │
                    │ live tiles +  │   │ gRPC metricsv1.MetricsService     │
                    │ batch charts  │   └───────────────────────────────────┘
                    └───────────────┘
```

## Predictive layer (Phase 2)

```
dbt SQL feature stores            Python feature stores
  gold.feature_forecast_weekly      gold.feature_fraud_orders (99,441)
  gold.feature_customer_churn       gold.session_features    (3.3M)
        └──────────┬─────────────────────┘
                   ▼
        ml/train_all.py  ──►  artifacts/<model>/<version>.joblib + sidecar_models.json
                   │                        │
                   ▼                        ▼
   gold.model_registry  ◄── versioning   ml/serve.py  (127.0.0.1:8093, stdin-free stdlib sidecar)
                   │                        │ POST /score ──► prediction + SHAP
                   ▼                        │
   gold.predictions  (every prediction  ◄──┘ persists each live score)
        ▲
        └── Go API: /api/v1/model-registry · /predictions · /predictions/latest
            /api/v1/score (sidecar optional: 503 unset, 502 unreachable) · /api/v1/models/health
```

Four backtested models — **forecast** (weekly revenue WMAPE 0.31, orders 0.27),
**churn** (AUC 0.65, honest label-availability + no calendar-year proxy),
**fraud** (AUC 0.85, lift@5% 19.9×), **bot** (AUC 1.0 on the deterministic
synthetic corpus — expected, documented). Every scored entity lands in
`gold.predictions` with its SHAP contributions. Full detail, decisions, and
deferrals: `docs/phase2.md`.

## Repository layout

```
├── docker-compose.yml        # Postgres + MinIO + Redpanda, healthchecked
├── pyproject.toml            # uv project: dbt-core, dbt-postgres, boto3, psycopg (+ `ml` group)
├── Makefile                  # infra/test/app/integration-test/smoke + ml-sync/ml-features/train/serve-models
├── .github/workflows/ci.yml  # CI: compose up → load → dbt → vet/unit/integration → smoke
├── scripts/
│   ├── bootstrap.ps1         # infra → data → bronze → dbt → docs → build (Windows)
│   ├── bootstrap.sh          # same pipeline for Linux/macOS (make bootstrap)
│   ├── smoke_test.ps1        # starts server+producer, validates every endpoint (Windows)
│   ├── smoke_check.sh        # portable smoke test (Linux/macOS/CI) incl. Phase 2 predict asserts
│   ├── run_api.ps1           # run cmd/server in the foreground (dev)
│   ├── run_producer.ps1      # run cmd/producer replay in the foreground (dev)
│   └── download_olist.py     # stdlib downloader with byte-size verification
├── loader/loader.py          # CSV → MinIO bronze → Postgres bronze.* (COPY + lineage cols)
├── dbt/                      # dbt project (profile, macros, sources incl. realtime gold tables)
├── ml/                       # Phase 2 Python: feature builders, trainers, SHAP explainer,
│                            #   registry/prediction writers, stdlib serving sidecar (serve.py), tests
├── artifacts/                # derived (gitignored): versioned model joblibs + serving
│                            #   manifest, written by `make train` — regenerate, don't commit
├── api/                      # Go module: cmd/{server,producer,api} + internal/{stream,realtime,
│                            #   predict,kafka,grpcapi,config,http,metrics,model,store,telemetry}
└── docs/
    ├── phase0.md             # Phase 0 decisions, schema dictionary, metric definitions
    ├── phase1.md             # Phase 1 event contract, realtime schema, anomaly detection, ops
    └── phase2.md             # Phase 2 model cards (verified metrics), registry/predictions
                              #   contracts, sidecar API, ops, deferrals
```

## Quickstart (Windows)

Prerequisites: Docker Desktop (WSL2), Go ≥ 1.22, uv, and an internet connection.

```powershell
# 1. One-time: create the uv environment (Python 3.12 + dbt + loaders)
uv sync

# 2. Full pipeline: infra up → download → bronze load → dbt build + docs → build
./scripts/bootstrap.ps1

# 3. Run the platform (two processes, two terminals)
./scripts/run_producer.ps1      # replays history onto Redpanda (2880× ≈ 1 day/30 s)
./scripts/run_api.ps1           # server: dashboard + REST + SSE + gRPC + consumers (http://localhost:8080)

# 4. End-to-end verification (starts/stops its own server + producer on scratch ports)
./scripts/smoke_test.ps1

# 5. Phase 2: predictive layer
# Trained artifacts + the serving manifest are gitignored (derived): a fresh
# clone regenerates them with `make train` before starting the sidecar.
uv sync --group ml                        # ML stack (scikit-learn, lightgbm, xgboost, shap)
uv run --group ml python ml/build_fraud_features.py   # non-SQL feature stores
uv run --group ml python ml/replay_session_corpus.py
uv run dbt build --project-dir dbt --profiles-dir dbt # SQL feature stores
uv run --group ml python ml/train_all.py  # backtest + register 4 models + write manifest
uv run --group ml python ml/serve.py      # sidecar http://127.0.0.1:8093 (phase 2 scoring)
```

Linux/CI equivalents:

```bash
# 1. Full pipeline (infra up → download → bronze load → dbt build + docs → build)
make bootstrap            # or: bash scripts/bootstrap.sh

# 2. Run the platform
make app                  # builds bin/server + bin/producer
./api/bin/server &        # http://localhost:8080
ABI_SPEED_MULTIPLIER=2880 ./api/bin/producer &

# 3. Verify
bash scripts/smoke_check.sh         # portable end-to-end smoke test
make integration-test               # real Postgres + Redpanda pipeline tests

# 4. Phase 2: feature stores → train → sidecar (see docs/phase2.md §7 for ops)
make ml-sync
make dbt ml-features
make train                           # backtest + register models + sidecar_models.json
make serve-models                    # scoring sidecar on 127.0.0.1:8093
```

The Go API treats the sidecar as optional: without it `POST /api/v1/score`
returns 503/502 while every read endpoint (`model-registry`, `predictions`,
`predictions/latest`, `models/health`) keeps serving from Postgres.

Every push/PR to `main` also runs the full pipeline in CI: `docker compose up -d
--wait` → load → dbt → `go vet` + unit + integration → `make app` + smoke
(`.github/workflows/ci.yml`).

## Verified results

- **Bronze:** 9/9 files downloaded and size-verified from a public mirror; row
  counts match the official dataset (`orders` 99,441, `order_items` 112,650,
  `geolocation` 1,000,163, …).
- **dbt:** `dbt build` → **116/116 pass** (20 models — 12 tables incl. the Phase 2
  churn feature store + 8 views — with 96 data tests incl. FK, uniqueness,
  accepted-value, and 5 singular tests), 0 warnings — declaring `fct_order_items`
  (FK-tested to `fct_orders`), the realtime gold tables, and the gold feature
  stores as sources.
- **Go unit tests:** simulator ordering/loop-wrap/ring capacity, Welford/z-score
  anomaly detection, SSE live stream (snapshot + flush + anomaly frames,
  heartbeat, 503-without-live), anomalies + dismiss, realtime metrics endpoint,
  broadcaster fan-out/drop, kafka client, store surface.
- **Realtime integration tests (`-tags=integration`, live Redpanda + Postgres):**
  a controlled 24-bucket baseline + 15× spike verifies bronze lineage columns,
  exact bucket revenue/orders (canceled orders excluded), `gold.anomalies` spike
  at `severe` (z ≥ 5) on the exact spike minute, active-session counts,
  `gold.detector_state` snapshot/restore round-trip, and the bronze→realtime
  reconcile clamping a double-counted bucket — all through the real producer path.
- **API smoke test:** revenue R$15,739,137.01 · orders 98,206 · AOV R$160.27 ·
  top category `bed_bath_table`; daily series return the full observed range
  (`2016-09-04 → 2018-09-03`); live SSE frames + realtime buckets + anomalies
  endpoint all answer.
- **Lineage:** every bronze row carries `_loaded_at`, `_source_file`, `_batch_id`
  (streaming rows add `loop_id`, `_kafka_topic/partition/offset`).
- **Phase 2 models (strict time splits, out-of-sample):** forecast revenue WMAPE
  **0.310**, orders **0.272** — with **per-category confidence bands** (category
  WMAPE spans 0.16 … 2.87 / 0.15 … 4.17, so rare categories report a visibly
  lower confidence than the headline); churn AUC **0.652** (label-availability
  rule, no calendar-year drift proxy); fraud AUC **0.85**, lift@5% **19.9×**;
  bot AUC **1.0** (deterministic synthetic corpus — documented, not
  over-claimed). Every classifier records a `recommended_threshold` (fraud
  **0.795** for the hold-for-review playbook, churn 0.595, bot 0.5) so Phase 4
  never inherits an undocumented 0.5 default. All metrics stored in
  `gold.model_registry.metrics` per version.
- **Phase 2 explainability:** every persisted prediction in `gold.predictions`
  carries a SHAP `explanation` jsonb (per-feature contributions +
  predicted_probability); TreeExplainer is built once per model, so backtest
  persistence and live scoring stay fast (17k-row timeout fixed by design).
- **Phase 2 serving, verified live:** stdlib sidecar `ml/serve.py` scores all
  four families with explanations (forecast confidence is per-category);
  Go `POST /api/v1/score` round-trips and persists a scored row, and the read
  endpoints serve the registry (11 versions, 5 active) and 42k+ predictions;
  502/503 degrade paths and 400 validation verified; predict integration tests
  (fake in-process sidecar) green; the serving DDL is single-sourced from
  `api/internal/predict/schema.sql` (Go `//go:embed` + `ml/common.py`), pinned
  by unit tests.
- **ML tests:** `uv run --group ml python -m pytest ml/tests -q` green
  (backtest metric maths incl. single-class guard).

## Metric definitions

| Metric | Definition |
|---|---|
| Revenue | `SUM(payment_value)` of orders with status **not** in `('canceled','unavailable')` and positive payment, attributed to `order_purchase_date` |
| Orders | Distinct non-lost orders with payment (summary) or all orders (daily) |
| AOV | Revenue ÷ contributing orders |
| Active customers | Distinct `customer_unique_id` with a non-lost order |
| Top category | Category rank by revenue (order-level payment attributed per line-item category) |
| Revenue (realtime) | Same filter per **1-minute bucket** — labelled **live (replay)**, never summed with batch |
| Orders (realtime) | Distinct orders per 1-minute bucket, batch filter |
| Active sessions | Distinct sessions with a click/cart event in the 1-minute bucket |

Every metric carries a **required `source` field** — `batch` vs `live_replay` —
so the split is structural, not a prose caveat; the realtime rows above are
`live_replay` and are **never additive** with the batch totals.

This dictionary is served **programmatically** at `GET /api/v1/metrics`
(`api/internal/metrics/catalog.go`, versioned — currently **v1.3.0**, which adds
the Phase 2 `model` source) — the machine-readable semantic layer that agents
should fetch before answering metric questions.

## Roadmap

- **Phase 1** — live replay + real-time metrics: `cmd/producer`
  replays history onto Redpanda; `cmd/server` runs the bronze-writer and the
  realtime aggregator (Welford/z-score online anomaly detection) into
  `gold.realtime_metrics` / `gold.anomalies`; SSE + gRPC streams feed live
  dashboard tiles with an anomaly banner. **Complete and verified.**
- **Phase 2 (this phase)** — predictive layer: four gradient-boosted models
  (forecast, churn, fraud, bot) with strict time-split backtests, SHAP
  explanations on every prediction, `gold.model_registry` + `gold.predictions`
  contracts, a Python scoring sidecar behind the Go API, CI coverage, and
  `docs/phase2.md`. **Complete and verified.**
- **Phase 3** — agent-driven root-cause explanations (reacting to
  `ecommerce.anomalies` events) plus the semantic layer / natural-language
  queries path. Phase 3 also owns the live-path work deferred from Phase 2
  (recorded in `docs/phase2.md` §9): **stream-driven scoring** (fraud per
  order / bot per session off the Redpanda consumers, reusing the sidecar),
  the **fraud model-score writer into `gold.anomalies`**, and the
  **prediction UI pass** (forecast chart with confidence bands, churn-risk
  leaderboard, fraud-score column on the live order feed) — all data for it
  is already exposed by the Phase 2 API.
- **Phase 4** — conversational agents, subscriptions, alerting; drift monitoring
  and drift-triggered retraining of the Phase 2 models.
- **Phase 5** — Deployment: Docker image (already provided in `api/Dockerfile`),
  observability, horizontal scaling of consumers.

See `docs/phase0.md`, `docs/phase1.md`, and `docs/phase2.md` for full detail.