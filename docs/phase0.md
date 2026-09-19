# Phase 0 — Foundations: end-to-end pipeline

**Status: complete and verified.** This document records the decisions, data
lineage, metric semantics, API contract, and test evidence for Phase 0.

## 1. Goals

Ship the platform's foundations as a deep, working system:

1. One command brings up the full infrastructure stack.
2. Olist e-commerce data lands in a proper medallion architecture.
3. A real Go API serves gold metrics.
4. A real dashboard (revenue, orders, top categories) renders end to end.
5. Everything is reproducible and verified (row counts, 95 dbt checks, smoke test).

## 2. Environment & key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Runtime machine | Windows 11 host + Docker Desktop (WSL2) | user's environment; containers run Postgres/MinIO/Redpanda |
| Python | uv-managed **3.12** venv at repo root | system Python 3.14.7 is too new for dbt; uv pins and isolates |
| Python deps | dbt-core + dbt-postgres + boto3 + psycopg | single environment drives loader, dbt, and future tooling |
| Data source | public GitHub mirror `ductransponster/olist-brazilian-ecommerce` | no Kaggle credentials needed; sizes match official release |
| Bronze | raw bytes in MinIO (`bronze` bucket) **plus** verbatim TEXT tables in Postgres | MinIO = immutable object landing; Postgres = queryable raw for dbt sources |
| Silver | dbt views `silver.stg_*` (typed, cleaned) | single source of clean staging truth |
| Gold | dbt tables `gold.fct_orders`, `dim_*`, metrics | consumed by the API |
| Geospatial | geolocation (1M rows) lands in bronze but is **not staged** in Phase 0 | documented deferral; keep raw landed and immutable |
| API | Go stdlib router + pgx; REST only | gRPC deferred to Phase 1 where streaming arrives naturally |
| Dashboard | vanilla JS + Chart.js (CDN) embedded in the API binary | zero build chain; `go build` produces one deployable binary |

## 3. Data lineage

### 3.1 Bronze (`bronze` schema / MinIO bucket `bronze`)

Loaded by `loader/loader.py`. Every column is `text` — nothing is cast or cleaned
at this layer, so bronze mirrors the raw source byte-for-byte.

| Table | Source file | Rows (verified) |
|---|---|---|
| bronze.orders | olist_orders_dataset.csv | 99,441 |
| bronze.order_items | olist_order_items_dataset.csv | 112,650 |
| bronze.order_payments | olist_order_payments_dataset.csv | 103,886 |
| bronze.order_reviews | olist_order_reviews_dataset.csv | 99,224 |
| bronze.customers | olist_customers_dataset.csv | 99,441 |
| bronze.sellers | olist_sellers_dataset.csv | 3,095 |
| bronze.products | olist_products_dataset.csv | 32,951 |
| bronze.product_category_name_translation | product_category_name_translation.csv | 71 |
| bronze.geolocation | olist_geolocation_dataset.csv | 1,000,163 |

Source data quirks handled:
- **BOM:** `product_category_name_translation.csv` carries a UTF-8 BOM on its
  first header cell; the loader strips it when deriving column names.
- Loader idempotency: `create_bucket` tolerates an existing bucket; bronze tables
  are dropped `CASCADE` and recreated (dbt rebuilds silver/gold afterwards).

### 3.2 Silver (`silver` schema, dbt views, 8 models)

All date/time columns → `timestamptz` (empty strings → NULL), money → `numeric(10,2)`,
ids kept as text. Notable logic:

- `stg_customers` — flags the 3 mystery customers with empty city/state as
  `is_identified = false`.
- `stg_products` — renames Olist's misspelled `*_lenght` columns and flags
  fully-empty placeholder products.
- `stg_order_reviews` — `SELECT DISTINCT` drops exact duplicates (the dataset is
  known to contain a handful of perfectly duplicated rows).
- `stg_orders` — preserves nullable `order_approved_at`.

### 3.3 Gold (`gold` schema, dbt tables)

- `fct_orders` — one row per order: item/payment/review aggregates plus business
  flags `is_delivered` / `is_lost`. 99,441 rows.
- `dim_customers` (96,096 unique customers), `dim_products`, `dim_sellers`,
  `dim_dates` (day spine, 2016-09-04 → 2018-09-03).
- `daily_revenue` (613 days), `daily_orders` (634 days, includes zero-activity
  days), `daily_aov`, `top_categories` (72 categories).

## 4. Metric definitions

Everything is attributed to **`order_purchase_date`** (the business "when the
customer decided to buy" timestamp).

| Metric | Definition | Where |
|---|---|---|
| Revenue | `SUM(payment_value)` from payments of orders with `order_status NOT IN ('canceled','unavailable')` and `payment_value_total > 0` | `daily_revenue` |
| Order count | `COUNT(DISTINCT order_id)` of the same non-lost, paying population (summary); daily table additionally splits delivered/lost over **all** orders | `daily_orders` |
| AOV | `revenue ÷ order_count`, rounded to 2 dp | `daily_aov` |
| Active customers | distinct `customer_unique_id` among non-lost orders | summary |
| Top categories | order-level payment attributed to **each category** present in the order's line items, then ranked by revenue / order count | `top_categories` |

Note: the summary's revenue/orders pool is the "paying non-lost" population, so
`summary.orders` (98,206) differs slightly from total order count (99,441).

## 5. API contract

Base URL `http://localhost:8080`. Every response uses an envelope:

```json
{
  "data": { ... },
  "meta": { "generated_at": "2026-09-19T09:00:00Z", "took_ms": 1.2 }
}
```

Errors: `{ "error": "...", "meta": {...} }` with 400 (bad `from`/`to`/`limit`/`metric`)
or 500 (store failure).

| Endpoint | Query params | Data shape |
|---|---|---|
| `GET /healthz` | — | `{status:"ok"}` |
| `GET /api/v1/summary` | `from`,`to` (YYYY-MM-DD, optional) | revenue, orders, aov, customers, top_category, data_start, data_end |
| `GET /api/v1/revenue/daily` | `from`,`to` | `[{date, value}]` |
| `GET /api/v1/orders/daily` | `from`,`to` | `[{date, orders, delivered, lost}]` |
| `GET /api/v1/categories/top` | `metric=revenue\|orders` (default revenue), `limit=1..50` (default 10) | `[{category, orders, revenue, revenue_rank, order_rank}]` |

The dashboard calls these same-origin; `//go:embed` bakes HTML/CSS/JS into the
binary (`api/internal/http/static/`).

## 6. Test evidence

- `dbt build` — **PASS=95 WARN=0 ERROR=0**: 17 models (9 tables, 8 views) + 78 data
  tests (uniqueness, not-null, FK `relationships`, `accepted_values`, custom
  singular tests: no future orders, non-negative payments/item counts/revenue,
  review scores 1–5).
- `go build ./...` / `go vet ./...` / `go test ./...` — all green (handler tests
  against an in-memory fake store: health, summary, range validation, series,
  category validation, 405 handling).
- Smoke test (starts a freshly built `api.exe`, polls, checks, stops it):
  - revenue **R$15,739,137.01** · orders **98,206** · AOV **R$160.27** · top **bed_bath_table**
  - revenue/daily 613 points · orders/daily 634 points · top-5 categories correct

## 7. Rebuild / day-2 operations

```powershell
uv sync                      # recreate Python env if needed
./scripts/bootstrap.ps1      # infra + data + bronze + dbt + docs + api build
./scripts/run_api.ps1        # serve
./scripts/smoke_test.ps1     # verify
```

Known operational notes:
- API test runs build a temporary `api/bin/api.exe` so killing the process is
  clean (`go run` would leave an orphaned child binary).
- The summary query runs a few correlated subqueries; if the metric tables get
  large, precompute a `gold.daily_*summary*` rollup. Not needed at this scale.
- Chart.js loads from a CDN; an offline machine needs the module vendored.

## 8. Phase 1 hooks

Redpanda is already healthy and advertised to the host
(`localhost:29092` Kafka API, `localhost:28082` proxy, `localhost:9644` admin;
single node, overprovisioned). Phase 1 will add order-events topics, a Go
producer on the bronze path, streaming consumers feeding the same gold metrics,
and gRPC streaming on the API.