# Adaptive Business Intelligence (ABI)

A real-time, explainable, agentic BI platform for e-commerce analytics. Phase 0
is a **deep, working foundations pipeline**: containerized infrastructure, a
medallion data stack on the Olist Brazilian E-Commerce dataset, an idiomatic Go
metrics API, and a browser dashboard. **Phase 1 adds the real-time layer**: the
history is replayed onto Redpanda, Go consumers compute hot 1-minute metrics
with online anomaly detection, and live SSE/gRPC streams feed live dashboard
tiles — all built, tested, and running end to end.

| Layer | What ships |
|---|---|
| Infrastructure | Docker Compose: Postgres 17, MinIO, Redpanda 26.2 (all healthy-checked) |
| Bronze | Olist (9 CSVs, ~126 MB) → immutable objects in MinIO `bronze` bucket **and** verbatim raw tables in Postgres `bronze.*`; streaming events + bot ground truth in `bronze.stream_events` / `bronze.training_ground_truth` (with lineage) |
| Silver | 8 typed, cleaned staging views (`silver.stg_*`), dedup of duplicate reviews, NULL handling |
| Gold | `gold.fct_orders` + `gold.fct_order_items`, 4 dims, metric tables (`daily_revenue`, `daily_orders`, `daily_aov`, `top_categories`) — plus **hot-path** `gold.realtime_metrics` (1-min buckets) and `gold.anomalies` written by the Go realtime stack |
| Streaming | `cmd/producer` replays history (virtual clock, 3-source merge, infinite loops); `cmd/server` runs the bronze-writer + realtime aggregator (Welford/z-score anomaly detection) |
| Serving | Go REST API + SSE live stream + gRPC :8090 (live metrics), embedded dashboard with live tiles + anomaly banner |
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

## Repository layout

```
├── docker-compose.yml        # Postgres + MinIO + Redpanda, healthchecked
├── pyproject.toml            # uv project: dbt-core, dbt-postgres, boto3, psycopg
├── Makefile                  # infra/test/app/integration-test/smoke targets
├── .github/workflows/ci.yml  # CI: compose up → load → dbt → vet/unit/integration → smoke
├── scripts/
│   ├── bootstrap.ps1         # infra → data → bronze → dbt → docs → build (Windows)
│   ├── bootstrap.sh          # same pipeline for Linux/macOS (make bootstrap)
│   ├── smoke_test.ps1        # starts server+producer, validates every endpoint (Windows)
│   ├── smoke_check.sh        # portable smoke test (Linux/macOS/CI)
│   ├── run_api.ps1           # run cmd/server in the foreground (dev)
│   ├── run_producer.ps1      # run cmd/producer replay in the foreground (dev)
│   └── download_olist.py     # stdlib downloader with byte-size verification
├── loader/loader.py          # CSV → MinIO bronze → Postgres bronze.* (COPY + lineage cols)
├── dbt/                      # dbt project (profile, macros, sources incl. realtime gold tables)
├── api/                      # Go module: cmd/{server,producer,api} + internal/{stream,realtime,
│                            #   kafka,grpcapi,config,http,metrics,model,store}
└── docs/
    ├── phase0.md             # Phase 0 decisions, schema dictionary, metric definitions
    └── phase1.md             # Phase 1 event contract, realtime schema, anomaly detection, ops
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
```

Every push/PR to `main` also runs the full pipeline in CI: `docker compose up -d
--wait` → load → dbt → `go vet` + unit + integration → `make app` + smoke
(`.github/workflows/ci.yml`).

## Verified results

- **Bronze:** 9/9 files downloaded and size-verified from a public mirror; row
  counts match the official dataset (`orders` 99,441, `order_items` 112,650,
  `geolocation` 1,000,163, …).
- **dbt:** `dbt build` → **95/95 pass** (17 models + 78 data tests incl. FK,
  uniqueness, accepted-value, and 5 singular tests), 0 warnings — now also
  declaring `fct_order_items` (FK-tested to `fct_orders`) and the realtime gold
  tables as sources.
- **Go unit tests:** simulator ordering/loop-wrap/ring capacity, Welford/z-score
  anomaly detection, SSE live stream (snapshot + flush + anomaly frames,
  heartbeat, 503-without-live), anomalies + dismiss, realtime metrics endpoint,
  broadcaster fan-out/drop, kafka client, store surface.
- **Realtime integration tests (`-tags=integration`, live Redpanda + Postgres):**
  a controlled 24-bucket baseline + 15× spike verifies bronze lineage columns,
  exact bucket revenue/orders (canceled orders excluded), `gold.anomalies` spike
  at `severe` (z ≥ 5) on the exact spike minute, and active-session counts — all
  through the real producer path.
- **API smoke test:** revenue R$15,739,137.01 · orders 98,206 · AOV R$160.27 ·
  top category `bed_bath_table`; daily series return the full observed range
  (`2016-09-04 → 2018-09-03`); live SSE frames + realtime buckets + anomalies
  endpoint all answer.
- **Lineage:** every bronze row carries `_loaded_at`, `_source_file`, `_batch_id`
  (streaming rows add `loop_id`, `_kafka_topic/partition/offset`).

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

This dictionary is served **programmatically** at `GET /api/v1/metrics`
(`api/internal/metrics/catalog.go`, versioned — currently **v1.1.0**) — the
machine-readable semantic layer that agents should fetch before answering metric
questions.

## Roadmap

- **Phase 1 (this phase)** — live replay + real-time metrics: `cmd/producer`
  replays history onto Redpanda; `cmd/server` runs the bronze-writer and the
  realtime aggregator (Welford/z-score online anomaly detection) into
  `gold.realtime_metrics` / `gold.anomalies`; SSE + gRPC streams feed live
  dashboard tiles with an anomaly banner. **Complete and verified.**
- **Phase 2** — Bot-detection model trained against
  `bronze.training_ground_truth` (labels already stream under a restricted
  topic), semantic layer + natural-language queries (LLM → metric DSL → SQL).
- **Phase 3** — Forecasting, agent-driven root-cause explanations (reacting to
  `ecommerce.anomalies` events).
- **Phase 4** — Conversational agents, subscriptions, alerting.
- **Phase 5** — Deployment: Docker image (already provided in `api/Dockerfile`),
  observability, horizontal scaling of consumers.

See `docs/phase0.md` and `docs/phase1.md` for full detail.