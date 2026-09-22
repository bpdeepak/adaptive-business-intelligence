# Phase 2 — Predictive layer

**Status: complete and verified.** This document records the goals, decisions,
durable contracts, model cards (with real, backtested metrics), serving
surface, ops procedures, and test evidence for Phase 2 of ABI — the predictive
layer that turns the Phase 0/1 batch + streaming foundation into a working,
explainable ML platform.

## 1. Goals

1. Four gradient-boosted models, trained in Python, each with **SHAP-backed
   explanations from the first commit**, not retrofitted:
   - **demand forecasting** — weekly revenue and orders per product category;
   - **customer churn risk** — will a repeat customer keep buying?
   - **transaction fraud risk** — is this order suspicious?
   - **bot-vs-human session classification** — is this session automated?
2. A **model registry + versioning discipline** so every prediction is
   traceable to the exact model version and training window that produced it.
3. **Low-latency serving** through a lightweight Python scoring sidecar that
   the Go API treats as an optional dependency.
4. **Honest backtesting** on a historical, non-stationary dataset — no leakage
   across the train/test time boundary (strict time splits everywhere; the one
   place a calendar feature tried to cheat was found and removed, §4.2).
5. Every model output lands in Postgres (`gold.predictions`) **with an
   explanation payload**, ready for the dashboard and the Phase 3 agent to
   surface "why".

## 2. Environment & key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Framework | scikit-learn (metrics) + **LightGBM** (forecast, bot) + **XGBoost** (churn, fraud) | the mainstream boosting pair; both expose `predict_proba` and native SHAP `TreeExplainer` support |
| Dependency hygiene | ML stack isolated in a `ml` uv dependency group | `uv sync` for dbt/loaders stays light; `uv sync --group ml` adds scikit-learn/lightgbm/xgboost/shap |
| Registry | `gold.model_registry` (Postgres) + versioned artifacts on disk | MLflow was considered and deferred (§9); a thin Postgres mirror keeps the Go API dependency-free. `ml/common.py` and `api/internal/predict/schema.go` hold **identical DDL by contract** |
| Prediction storage | single shared `gold.predictions` table | one query surface for the dashboard/agent across all four models |
| Serving | Python stdlib HTTP sidecar `ml/serve.py` on `127.0.0.1:8093` | ONNX was the original design; dropped for a Python-native sidecar (§9). Go stays the request front-door; the sidecar is optional (503 when disabled, 502 when unreachable) |
| Manifest | `artifacts/sidecar_models.json` written by the trainers | the sidecar's whole serving contract: model name → artifact path, task, grain, feature list |
| Backtesting | strict **time split** (oldest 80% train, latest 20% test) for every model | random splits would leak the future; worst-case the numbers are modest and that is *reported*, not hidden |
| Churn label availability | rows whose 90-day window is not yet in-data are excluded | labeling those rows "churned" would silently assume the future (right-censoring trap), §4.2 |
| Fraud labels | synthetic injection (§4.3) | Olist ships no fraud labels; documented limitation, injected patterns follow the Phase 2 spec |
| Bot labels | offline session corpus mirroring the producer's synthesis (`gold.session_features.is_synthetic_bot`) | the streaming ground-truth topic (`bronze.training_ground_truth`) is a 12 h retention stream, not a durable training set; the mirrored corpus replicates the same generator with documented parameters (§9) |
| Explainability | `shap.TreeExplainer`, computed once per model (not per row) | the first memory/CPU surprise of this phase: per-row explainer construction made persistence O(rows × model); the trainer reuses one explainer |
| Retraining trigger | `make train` + documented cron-shape (§7); drift-triggered automation deferred | Phase 4 owns drift monitoring; automating retrain now would guess thresholds blind |

## 3. Pipelines at a glance

```text
SQL feature stores (dbt):                       Python feature stores:
  gold.feature_forecast_weekly  (7,632 rows)      gold.feature_fraud_orders  (99,441)
  gold.feature_customer_churn   (2,892 rows)      gold.session_features      (3,314,684)

        ┌─────────────── all four feed gold.predictions + gold.model_registry ───────────────┐
        ▼                                                                                    ▼
ml/train_all.py ── artifact + version ──► artifacts/<model>/<version>.joblib ──► artifact
   │                                                      sidecar_models.json │
   │                                                                          ▼
   └─ gold.predictions (prediction + confidence + SHAP explanation)      ml/serve.py :8093
       ▲                                                                        │ POST /score
       │                                                                        ▼
GET /api/v1/model-registry · /predictions · /predictions/latest          Go API (POST /api/v1/score)
GET /api/v1/models/health  · POST /api/v1/score  (Go front door, optional sidecar)
```

## 4. Model cards (verified numbers, `docs/` truth)

All metrics below are **out-of-sample on a strict time split** (oldest 80% →
train, latest 20% → test), stored in `gold.model_registry.metrics`, and
reproducible with `make dbt ml-features train`.

### 4.1 Forecast — `forecast_category_weekly_revenue` / `..._orders`

| | revenue | orders |
|---|---|---|
| Model | LightGBM regression, 67 categories × week | same |
| Train rows | 7,102 category-weeks (2016-08-29 → 2018-09-03) | same |
| **WMAPE** | **0.31** | **0.27** |
| MAPE (non-zero weeks only) | 0.72 | 0.49 |
| MAE / RMSE | 1,196 / 2,421 | 6.4 / 13.3 |
| Top SHAP drivers | `orders_lag1`, `avg_order_value`, `revenue_lag1`, `category_code` | `orders_lag1`, `orders_roll4_mean`, `week_of_year`, `category_code` |

Features: category (revenue-rank code), week-of-year, year, AOV, revenue/orders
lags `1/2/4/8`, rolling-4 mean/std, `has_prior_week`. The week-ahead horizon at
the time of writing shows WMAPE in the 27–31 % band — the dominant signal is
last week's own level (lag-1), which is exactly what a demand forecaster should
learn from this dataset.

### 4.2 Churn — `churn_risk`

| | |
|---|---|
| Model | XGBoost binary classifier, grain `customer` |
| Cohort | repeat customers (≥ 2 orders), as-of = penultimate order, label from the observed last order: **churned = no purchase within 90 days** |
| Train / test | 2,313 / 579 rows (test from 2018-03-02; test churn rate 9.7 % vs train 37.9 %) |
| **AUC** | **0.65** |
| Top-5 % vs regime base | 13.8 % vs 9.7 % (**≈ 1.4×**) |
| Precision / recall @ 0.5 | 0.10 / 0.96 |
| Top SHAP drivers | `days_since_first_order` (recency), `aov_prior`, `total_spend_prior`, `avg_review_prior`, `orders_last_90d_prior` |

**Honesty rules encoded in the feature store** (`dbt/models/marts/predict/feature_customer_churn.sql`):

- **Label availability.** Rows whose as-of falls inside the final 90 days of
  the dataset are excluded — their 90-day window hasn't been observable yet.
- **No `as_of_year`.** The first build of this model silently leaned on the
  year: the marketplace's base churn rate fell steeply (2016 ≈ 67 % → 2017 ≈
  41 % → 2018 ≈ 14 %) and `as_of_year` was a pure proxy for that drift, so the
  time-split AUC collapsed to ≈ 0.53. The fix removed the calendar year (kept
  `as_of_month` as genuine seasonality) and enforced the label-availability
  rule — the honest AUC is 0.65, and the drift finding is documented here
  rather than laundered.
- Churn for **single-purchase customers is explicitly out of scope** (≈ 96 k
  of 99 k customers — a different, not-yet-modeled problem, like the spec).

### 4.3 Fraud — `fraud_risk`

| | |
|---|---|
| Model | XGBoost binary classifier, grain `order`, synthetic labels |
| Train / test | 79,552 / 19,889 orders (test from 2018-05-24; fraud rate ~1 %) |
| **AUC** | **0.85** |
| **Lift at top 5 %** | **19.9×** (top-5 % contains 19.3 % of all fraud vs a 1 % base) |
| Precision / recall @ 0.5 | 0.48 / 0.71; at 0.9 threshold: 0.89 / 0.68 |
| Top SHAP drivers | `velocity_24h`, `price_vs_benchmark`, `order_value`, `order_hour`, `freight_share` |

Labels are injected (`ml/build_fraud_features.py`): velocity bursts (≥ 3
orders/24 h from a new account), price outliers (≥ 8× the category benchmark),
ride-the-installments signals (≥ 12 installments on high-value orders), plus
0.4 % random noise. The model's top SHAP driver being `velocity_24h` confirms
it learned the injected ground truth, not artifacts.

### 4.4 Bot — `bot_score`

| | |
|---|---|
| Model | LightGBM binary classifier, grain `session` |
| Train / test | 2,651,747 / 662,937 sessions (test from 2018-05-24; bot rate 2 %) |
| **AUC** | **1.0** (see the caveat) |
| Lift at top 5 % | 20.2× |
| Top SHAP drivers | `has_search` (negative), `click_interval_cv`, `duration_seconds`, `click_count` |

AUC 1.0 is **expected and honest**: the offline corpus
(`ml/replay_session_corpus.py`) replicates the producer's deterministic bot
synthesis at session level (compressed dwell time, low inter-click variance,
reduced page-type diversity), so the model rediscovered the injection pattern
with near-perfect separation. On real (non-synthetic) clickstreams the
performance would be materially lower; the synthetic corpus is a documented
stand-in until streaming labels are persisted long-term (§9).

## 5. Durable contracts

### 5.1 `gold.model_registry`

| Column | Meaning |
|---|---|
| `model_name`, `model_version` | PK; version = UTC `YYYYMMDD.hhmmss` of training |
| `status` | `active` | `superseded` (retraining supersedes the previous active version) |
| `framework`, `task`, `grain` | e.g. `xgboost` / `binary_classification` / `customer` |
| `artifact_path` | repo-root-relative joblib path (the serving sidecar loads it) |
| `params` / `metrics` / `features` / `trained_on` / `trained_window` | full reproducibility record (metrics = the §4 tables) |

### 5.2 `gold.predictions`

One row per scored entity; the `explanation` jsonb carries the per-feature
SHAP contributions (most influential first) plus `predicted_probability` and,
for backtests, the held-out `label`. Batch-tag `test_split` marks evaluation
rows; live `/score` calls land with `{"source":"live_score"}`.

```jsonc
{
  "model_name": "fraud_risk", "model_version": "20260919.181959",
  "grain": "order", "entity_id": "0e5f...", "predicted_at": "...",
  "prediction": 0.995, "confidence": 0.995,
  "explanation": {
    "shap": { "velocity_24h": 5.905, "price_vs_benchmark": -0.904, "...": 0 },
    "predicted_probability": 0.995
  },
  "metadata": { "label": 1, "model_version": "20260919.181959", "batch": "test_split" }
}
```

Both DDLs live in **two places that must stay identical**: `ml/common.py` and
`api/internal/predict/schema.go` (a `predict.EnsureSchema` bootstrap makes the
Go server self-sufficient against a fresh stack).

### 5.3 Serving manifest — `artifacts/sidecar_models.json`

Written by `ml/train_all.py::write_sidecar_manifest()` from the registry's
`active` rows: name, version, task, grain, artifact path, feature list
(+ `baseline_wmape` for regressions, which drives the regression confidence
`1 − wmape`).

`artifacts/` is **derived, not committed** (items are build products of
training — same policy as `data/raw/`, `api/bin/`, `dbt/target/`, and
enforced by `.gitignore`). A fresh clone must run `make dbt ml-features train`
before `make serve-models`; a missing manifest makes the sidecar boot with an
empty model list and a clear stderr hint rather than crashing.

## 6. API surface

### 6.1 Sidecar (Python, `127.0.0.1:8093`)

| Endpoint | Behaviour |
|---|---|
| `GET /health` | `{"status":"ok","models":[...]}` |
| `GET /models` | descriptors (name, version, task, grain, features) |
| `POST /score` | `{"model":"fraud_risk","features":{...}}` → prediction, confidence, top-25 SHAP contributions |

`ml/serve.py` is stdlib-only at run time (the ML imports happen at first
score), so the sidecar stays a small, restartable process.

### 6.2 Go API (`:8080`, wired in `cmd/server`)

| Endpoint | Behaviour |
|---|---|
| `GET /api/v1/model-registry` | all registered versions (read from Postgres) |
| `GET /api/v1/predictions?model=&grain=&entity=&limit=` | persisted predictions (≤ 1,000) |
| `GET /api/v1/predictions/latest?model=&limit=` | newest first |
| `POST /api/v1/score` | validate → sidecar `/score` → persist row → return it with id; **503** if `ABI_SCORE_URL` unset, **502** if the sidecar is unreachable, **400** on a malformed payload |
| `GET /api/v1/models/health` | `{"sidecar":"up|down","active_models":N}` |

Insight-overload rule: the Go API never *needs* the sidecar to serve the
registry and persisted predictions; scoring is optional by design. Prometheus
gauges (`abi_models_active`, `abi_model_sidecar_up`, `abi_score_latency_ms`,
`abi_predictions_written_total`, `abi_score_errors_total`) make sidecar health
visible on `/metrics`.

## 7. Ops

| Command | Effect |
|---|---|
| `make ml-sync` | `uv sync --group ml` |
| `make dbt ml-features` | SQL + Python feature stores (idempotent; Python builders rebuild their tables) |
| `make train` | backtest + register all four families + write the manifest |
| `make serve-models` | run `ml/serve.py` (requires manifest from `make train`) |
| `make app` + `bash scripts/smoke_check.sh` | build + end-to-end Linux smoke (env: `ABI_SCORE_URL` defaults to the sidecar; server tolerates it being down) |

Recommended cron shape (Phase 4 will automate the trigger): weekly `make dbt
ml-features train` regenerates the two model generations that drift with time
(forecast, churn); fraud/bot retrain on label or data change.

Environment: `ABI_SCORE_URL` (default `http://127.0.0.1:8093`), `ABI_SCORE_ADDR`,
`ABI_ARTIFACTS_DIR` (default `artifacts/`), `ABI_TEST_DATABASE_URL` (integration
tests). See `api/internal/config/config.go`, `ml/serve.py`, `.env.example`.

## 8. Test evidence

- **ML unit tests**: `uv run --group ml python -m pytest ml/tests -q` — green
  (backtest metric maths incl. the single-class guard).
- **dbt**: `uv run dbt build` green in CI (feature-store tests incl. the new
  churn contract, 96 data tests).
- **Go vet + build + unit tests**: `go vet ./...`, `go build ./...`,
  `go test ./...` — green.
- **Integration** (real Postgres; predict tests spin a fake in-process
  sidecar): `go test -tags integration ./internal/store/ ./internal/realtime/
  ./internal/predict/` — green locally and in CI.
- **Live E2E**: sidecar `/health` + `/score` exercised for all four families
  (regression WMAPE-driven confidence; classifier confidence = distance from
  toss-up) with SHAP contributions returned; Go `POST /api/v1/score` round-trip
  persisted a live-scored row and read it back; stale-port and 503/502 paths
  verified. `scripts/smoke_check.sh` asserts the predict endpoints in CI.

## 9. Deferred (explicitly out of Phase 2 scope)

1. **ONNX + Go-native inference.** The design doc's `onnxruntime-go` hot path
   was replaced by the Python sidecar: native full-SHAP at score time and
   zero Go/ONNX toolchain cost, at the price of an extra process. The manifest
   contract is designed so ONNX export could slot in behind the same
   `/score` verb later.
2. **MLflow.** Registry versioning is served by `gold.model_registry` +
   artifacts; an MLflow trace backend (experiment history, metric diffs across
   versions) is a Phase 4 ops addition, not a retraining-blocker.
3. **Streaming bot labels.** `bronze.training_ground_truth` is a 12 h-retention
   stream; validating the live label path and building a durable labeled set
   from it is deferred (the offline corpus covers the model today).
4. **Churn for single-purchase customers** (96 k of 99 k customers) — a
   different cohort question.
5. **Real fraud labels** — synthetic only, by necessity.
6. **Drift-triggered retraining** — Phase 4 (monitoring decides *when*).
7. **Sequence bot model** (1D-CNN/LSTM over raw events) — stretch goal,
   optional in the spec; the tabular session model ships.
8. **Category-level trend lines / holiday flags** in the forecast — the spec's
   Brazilian holiday flag is noted as a future feature; current WMAPE already
   meets the phase bar.