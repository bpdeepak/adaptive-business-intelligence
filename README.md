# Adaptive Business Intelligence (ABI)

A real-time, explainable, agentic BI platform for e-commerce analytics. Phase 0 is
a **deep, working foundations pipeline**: containerized infrastructure, a
medallion data stack on the Olist Brazilian E-Commerce dataset, an idiomatic Go
metrics API, and a browser dashboard — all built, tested, and running end to end.

| Layer | What ships in Phase 0 |
|---|---|
| Infrastructure | Docker Compose: Postgres 17, MinIO, Redpanda 26.2 (all healthy-checked) |
| Bronze | Olist (9 CSVs, ~126 MB) → immutable objects in MinIO `bronze` bucket **and** verbatim raw tables in Postgres `bronze.*` |
| Silver | 8 typed, cleaned staging views (`silver.stg_*`), dedup of duplicate reviews, NULL handling |
| Gold | `gold.fct_orders`, 4 dims, and metric tables (`daily_revenue`, `daily_orders`, `daily_aov`, `top_categories`) — 95 dbt checks green |
| Serving | Go 1.27 REST API (`/healthz`, `/api/v1/summary`, `/revenue/daily`, `/orders/daily`, `/categories/top`) with embedded dashboard |
| Dashboard | Vanilla JS + Chart.js: KPI cards, daily revenue/orders charts, top categories, 7D/30D/90D/All windows |

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
                    │  gold.fct_orders / dim_* / metrics  (tables)      │
                    └──────────────────────┬────────────────────────────┘
                                           │ SQL (pgx)
                    ┌──────────────────────▼────────────────────────────┐
                    │  Go REST API (abi/api) → JSON envelopes            │
                    │   GET /api/v1/summary | /revenue/daily |            │
                    │   /orders/daily | /categories/top                   │
                    └──────────────────────┬────────────────────────────┘
                                           │ same-origin (embedded assets)
                    ┌──────────────────────▼────────────────────────────┐
                    │  Dashboard (vanilla JS + Chart.js)                │
                    └───────────────────────────────────────────────────┘
  Redpanda (topic-ready, healthy) ─ reserved for Phase 1 streaming
```

## Repository layout

```
├── docker-compose.yml        # Postgres + MinIO + Redpanda, healthchecked
├── pyproject.toml            # uv project: dbt-core, dbt-postgres, boto3, psycopg
├── scripts/
│   ├── bootstrap.ps1         # infra → data → bronze → dbt → docs → API build
│   ├── smoke_test.ps1        # starts API, validates every endpoint, stops it
│   ├── run_api.ps1           # run the API in the foreground (dev)
│   └── download_olist.py     # stdlib downloader with byte-size verification
├── loader/loader.py          # CSV → MinIO bronze → Postgres bronze.* (COPY)
├── dbt/                      # dbt project (profile, macros, sources, models, tests)
├── api/                      # Go module: cmd/api + internal/{config,http,model,store}
└── docs/phase0.md            # decisions, schema dictionary, metric definitions
```

## Quickstart (Windows)

Prerequisites: Docker Desktop (WSL2), Go ≥ 1.22, uv, and an internet connection.

```powershell
# 1. One-time: create the uv environment (Python 3.12 + dbt + loaders)
uv sync

# 2. Full pipeline: infra up → download → bronze load → dbt build + docs → API build
./scripts/bootstrap.ps1

# 3. Serve the API + dashboard
./scripts/run_api.ps1          # http://localhost:8080

# 4. End-to-end verification (starts/stops its own API instance)
./scripts/smoke_test.ps1
```

Linux/CI equivalents live in the `Makefile` (`make infra-up data load dbt api test smoke`).

## Verified Phase 0 results

- **Bronze:** 9/9 files downloaded and size-verified from a public mirror; row
  counts match the official dataset (`orders` 99,441, `order_items` 112,650,
  `geolocation` 1,000,163, …).
- **dbt:** `dbt build` → **95/95 pass** (17 models + 78 data tests incl. FK,
  uniqueness, accepted-value, and 5 singular tests), 0 warnings.
- **API smoke test:** revenue R$15,739,137.01 · orders 98,206 · AOV R$160.27 ·
  top category `bed_bath_table`; daily series return the full observed range
  (`2016-09-04 → 2018-09-03`).

## Metric definitions (Phase 0)

| Metric | Definition |
|---|---|
| Revenue | `SUM(payment_value)` of orders with status **not** in `('canceled','unavailable')` and positive payment, attributed to `order_purchase_date` |
| Orders | Distinct non-lost orders with payment (summary) or all orders (daily) |
| AOV | Revenue ÷ contributing orders |
| Active customers | Distinct `customer_unique_id` with a non-lost order |
| Top category | Category rank by revenue (order-level payment attributed per line-item category) |

## Roadmap

- **Phase 1** — Redpanda topics: order events → Go streaming consumers → real-time
  metrics (the Redpanda node is already running healthy and client-reachable).
- **Phase 2** — Semantic layer + natural-language queries (LLM → metric DSL → SQL).
- **Phase 3** — Anomaly detection, forecasting, root-cause explanations.
- **Phase 4** — Conversational agents, subscriptions, alerting.
- **Phase 5** — Deployment: Docker image (already provided in `api/Dockerfile`),
  observability, CI.

See `docs/phase0.md` for full detail.