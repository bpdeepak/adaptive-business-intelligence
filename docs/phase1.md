# Phase 1 — Real-time replay + live metrics

**Status: complete and verified.** This document records the decisions, event
contracts, realtime schema, anomaly detection semantics, API surface, ops
procedures, and test evidence for Phase 1 — the streaming layer of the ABI
platform, piggybacking on the Phase 0 batch foundation.

## 1. Goals

1. Replay the Olist history (~774 days, 99,441 orders + ~330k line items) onto
   Redpanda as a realistic event stream, indefinitely, at a configurable pace.
2. Consume that stream in real time: a bronze event recorder plus a hot-path
   1-minute metrics aggregator with **online anomaly detection**.
3. Serve live metrics over SSE and gRPC, with live dashboard tiles.
4. Keep every Phase 0 review guarantee in the streaming path: lineage on every
   row, a machine-readable metric catalog, real-DB integration tests, CI, a
   separated hot-path table, and cross-platform bootstrap/smoke scripts.

## 2. Environment & key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Broker | Redpanda 26.2.3 in Docker Compose, `localhost:29092` | Kafka-compatible, already provisioned in Phase 0 |
| Streaming client | `franz-go` (twmb), `kgo` | idiomatic, production-grade, per-key ordering without a serializer |
| Simulated clock | virtual clock: `simulated = dayStart + elapsed×SPEED` | deterministic pacing; no wall-clock sleeps inside the replay |
| Default pace | `SPEED_MULTIPLIER=2880` (1 historical day ≈ 30 s) | full loop ≈ 6.1–6.5 h; set higher for smoke/fast runs |
| Loop wrap | loops forever with per-loop offset (+±1 h jitter) and `l0`, `l1`, … suffixes on event/session ids | a never-ending stream is the point of Phase 1; suffixes guarantee idempotent re-landing |
| Primary binary | `cmd/server` hosts REST + SSE + gRPC + bronze-writer + aggregator | one always-on process; `cmd/api` retained but retired from scripts |
| Producer | `cmd/producer` loads `gold.fct_orders`/`gold.fct_order_items` (gold-only reads) | clean separation; batch layer stays authoritative for source data |
| Session synthesis | k-way min-heap merge of 3 sources: orders(+funnels), abandoned sessions, price changes | events leave the producer in simulated-time order |
| Ground truth | `ecommerce.internal.training_labels` → `bronze.training_ground_truth` | synthetic bot labels never travel in clickstream envelopes; Phase 2 classifies offline |
| Revenue (live) | identical filter to batch: status **not** in `('canceled','unavailable')` and `payment_value_total > 0` | a realtime **live (replay)** view, never summed with batch numbers |
| Hot path | `gold.realtime_metrics` (1-minute buckets) — a separate table from batch `gold.daily_*` | stream writes never contend with batch reads (review item) |
| Anomaly detection | online Welford mean/variance + z-score in the aggregator process | no ML service dependency; state lives with the buckets it scores |
| Backpressure | producer ring buffer, drop-oldest | the replay clock never blocks on a slow broker |

## 3. Event model

### 3.1 Topics

| Topic | Key | Event types served |
|---|---|---|
| `ecommerce.orders.events` | `customer_id` | `order.placed` |
| `ecommerce.clickstream.events` | `session_id` | `page.view`, `cart.abandoned` |
| `ecommerce.catalog.price_changes` | `product_id` | `price.changed` |
| `ecommerce.internal.training_labels` | `session_id` | `training.session_label` (restricted) |
| `ecommerce.anomalies` | `metric` | `anomaly.detected` (consumed nowhere serving; recorded to bronze) |

### 3.2 Envelope

Every message is one JSON envelope (mirrors the Phase 0 REST envelope shape):

```jsonc
{
  "schema_version": 1,
  "event_id": "l3-9f1c…",          // loop-suffixed unique id
  "event_type": "order.placed",
  "occurred_at": "2016-09-04T02:00:00Z",
  "produced_at": "2026-09-19T14:12:00Z",
  "key": "9b8c…",
  "loop_id": "l3",                  // streaming counterpart of _batch_id
  "payload": { /* typed payload */ }
}
```

### 3.3 Simulator

- **Virtual clock + k-way merge.** Three sources feed one min-heap; the producer
  always pops the oldest pending event and pushes its successor. Events exit in
  simulated-time order, so per-key ordering and per-bucket ordering hold.
- **orderSource** replays `gold.fct_orders` (+ items) with a converting-session
  funnel of `page.view` clicks, the order, and — only for synthetic bots — a
  label. Bot browsing is compressed (<250 ms) so bots are subtle, not obvious.
- **abandonSource** synthesizes per-day abandoned sessions scaled by
  `dayCounts×(1−conversion)/conversion`, lazily, so abandoned traffic matches
  the conversion rate.
- **priceSource** walks price changes (≈1/7 of products/day, ±15%, state
  persists across loops).
- **Loop wrap.** When every source is exhausted the loop index increments, the
  base day shifts `loopIdx×loopDur + jitter(±1 h)`, ids get a `l<k>` suffix, and
  the same data replays — a genuinely never-ending stream with unique events.
  The clock is anchored **once** at the dataset start; only events shift by
  offset, so a wrap can never re-grant a fresh "due" window (this exact bug — a
  runaway that emitted 500k events and 5,495 loops in one advance — was caught
  by the unit tests and is regression-tested).

## 4. Realtime schema

Self-bootstrapped by `realtime.EnsureSchema` (idempotent `CREATE … IF NOT
EXISTS`, schema creation included so a fresh stack needs no dbt run first).

### 4.1 `bronze.stream_events`

Verbatim event plus full lineage — the pairing-level guarantee that "which
pipeline wrote this row" is answerable for streaming rows too:

| Column | Meaning |
|---|---|
| `event_id` | PK, loop-suffixed, unique across replays |
| `event_type`, `occurred_at`, `produced_at`, `key` | envelope fields |
| `loop_id` | replay loop that produced the event |
| `payload` | the full envelope JSON |
| `_source_file` | `stream:<topic>` |
| `_batch_id` | envelope `loop_id`, else the writer run id |
| `_loaded_at` | stamp time |
| `_kafka_topic`, `_kafka_partition`, `_kafka_offset` | broker coordinates |

Writes are batched multi-row `INSERT … ON CONFLICT (event_id) DO NOTHING`, so
at-least-once redelivery is idempotent. Retained 12 h (≈2 loops at default pace).

### 4.2 `bronze.training_ground_truth`

`sessions` from the restricted label topic: `session_id`, `is_synthetic_bot`,
`loop_id`, `is_converting`, `recorded_at`. Nothing serving reads it.

### 4.3 `gold.realtime_metrics`

1-minute buckets, one row per `bucket_start`:

`revenue numeric(12,2)`, `orders int`, `active_sessions int`,
`anomaly_flag bool`, `updated_at`. Upserted every 5 s (`ON CONFLICT (bucket_start)
DO UPDATE`). Retained 3 h for the dashboard look-back.

### 4.4 `gold.anomalies`

| Column | Meaning |
|---|---|
| `id` | bigserial PK (referenced by the dismiss endpoint) |
| `metric` | `revenue` or `orders` |
| `bucket_start`, `observed`, `expected`, `z_score` | what happened vs. baseline |
| `severity` | `warning` (|z|≥3) / `severe` (|z|≥5) |
| `status` | `open` → `dismissed`/`resolved` lifecycle |
| `detected_at`, `resolved_at`, `dismissed_at` | timestamps |

Both realtime gold tables are declared as **dbt sources** (`dbt/models/sources.yml`)
so lineage/tests see them; they are written exclusively by the Go stack.

## 5. Anomaly detection

- Online **Welford** mean/variance per metric (`revenue`, `orders`); no history
  retained beyond the accumulator.
- 20-bucket warm-up before anything can fire; then |z| ≥ 3.0 → `warning`,
  |z| ≥ 5.0 → `severe`.
- z is computed **before** the observation folds into the baseline, so a spike
  scores sharply and then adapts (a sustained shift re-fires until the baseline
  catches up — by construction).
- A constant baseline (zero variance) deviating by any amount yields an
  infinite z; the writer clamps non-finite scores to ±10¹⁵ so they round-trip
  through JSON and SQL while remaining far past the `severe` threshold.
- On trigger the aggregator writes `gold.anomalies`, publishes
  `ecommerce.anomalies`, and pushes the anomaly in the same broadcast tick to
  SSE/gRPC clients.

## 6. API / streaming contract

| Route / surface | Description |
|---|---|
| `GET /api/v1/stream/metrics` | SSE; first frame = snapshot + meta, then a frame per flush and per anomaly; heartbeats every 15 s |
| `GET /api/v1/realtime/metrics?limit=60` | recent buckets, ascending (non-streaming read surface) |
| `GET /api/v1/anomalies` | open anomalies, newest first |
| `POST /api/v1/anomalies/{id}/dismiss` | close an anomaly (dashboard banner clears) |
| gRPC `:8090` `metricsv1.MetricsService` (stream) | same feed over gRPC; reflection enabled |
| `GET /api/v1/metrics` | metric catalog **v1.1.0** — adds `revenue_realtime`, `orders_realtime`, `active_sessions` with replay caveats |

SSE specifics: the response controller clears the write deadline, the status
wrapper implements `http.Flusher` (else the assertion fails), and frames carry
`{snapshot?, current, anomaly?, speed_multiplier, status}`.

## 7. Metric definitions added in v1.1.0

| Metric | Definition | Caveat |
|---|---|---|
| `revenue_realtime` | per 1-minute bucket, batch revenue filter | label **live (replay)**; never additive with batch |
| `orders_realtime` | distinct orders per 1-minute bucket, batch filter | at-least-once consumers dedupe by order id |
| `active_sessions` | distinct sessions with a click/cart event in the bucket | phase-1 clickstream is synthesized |

## 8. Ops

```bash
# one-time: infra + data + dbt (Phase 0 already built serves gold)
make bootstrap                 # or: ./scripts/bootstrap.ps1 (Windows)

# two processes
make app                       # builds bin/server + bin/producer
./bin/server &                 # REST :8080 + SSE + gRPC :8090 + consumers
ABI_SPEED_MULTIPLIER=2880 ./bin/producer &

# verify
bash scripts/smoke_check.sh    # or: ./scripts/smoke_test.ps1 (Windows)
make integration-test          # real Postgres + Redpanda pipeline tests
```

Dashboard: `http://localhost:8080` — live tiles (revenue/orders/sessions per
minute), replay-speed badge, anomaly banner with dismiss, and a 1-minute
revenue sparkline, all fed by SSE.

## 9. Test evidence

- **Unit:** simulator ordering/payloads/loop-wrap/ring drop-oldest; anomaly
  pre-add z scoring and severity escalation; broadcaster fan-out/drop; SSE live
  stream, 503 without live, anomalies + dismiss, realtime metrics endpoint.
- **Integration (`-tags=integration`, real Redpanda + Postgres):** a controlled
  series is produced (24 baseline minute-buckets + a 15× spike bucket, canceled
  orders interleaved) and the pipeline asserts: all events land in
  `bronze.stream_events` with `loop_id`/`_source_file`/`_batch_id` lineage,
  exactly 25 buckets with only valid orders counted (revenue exact to the cent),
  the spike trip `gold.anomalies` at `severe` with z ≥ 5 pointing at the exact
  spike minute, and `active_sessions` matches the published clickstream. Rows
  are cleaned up after the run.
- **CI:** one compose-driven job — `docker compose up -d --wait` → load → dbt →
  `go vet` + unit + integration → `make app` + `smoke_check.sh`.

## 10. Deferred to Phase 2+

- Training the bot classifier against `bronze.training_ground_truth`.
- Reacting to anomaly events (`ecommerce.anomalies`) with agents/alerting.
- `gold.anomalies` `resolved` lifecycle driven by downstream confirmation.
- Horizontal scaling of consumers (groups already partition by key, but the
  aggregator state is single-node today).