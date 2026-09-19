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
| Detector robustness | sigma floor inside the z-score (abs 1e-2, rel 5% of mean) | zero-variance baselines yield finite z; tiny deviations no longer fire spurious `severe` |
| Detector persistence | Welford accumulators snapshotted to `gold.detector_state` every flush | a restarted server resumes the baseline instead of re-warming (and false-alarming) |
| Retention | server-side janitor (`retentionLoop`, 5-min ticker, env-tuned budgets) | the 12 h / 3 h budgets are enforced, not aspirational |
| Self-healing | `reconcileLoop` recomputes realtime buckets from bronze (`ABI_RECONCILE_EVERY`) | redelivery double-counts self-correct within one interval |
| Observability | Prometheus text `/metrics` on server and producer | consumer lag, ring shedding, baseline progress are visible before Phase 2 adds consumers |

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
at-least-once redelivery is idempotent. Retention is **enforced, not just
documented**: a server-side janitor (`retentionLoop`, every 5 min) deletes rows
older than the window (`ABI_RETENTION_STREAM_EVENTS`, default **12 h** ≈ 2
loops at default pace), backed by an index on `_loaded_at`.

### 4.2 `bronze.training_ground_truth`

`sessions` from the restricted label topic: `session_id`, `is_synthetic_bot`,
`loop_id`, `is_converting`, `recorded_at`. Nothing serving reads it.

### 4.3 `gold.realtime_metrics`

1-minute buckets, one row per `bucket_start`:

`revenue numeric(12,2)`, `orders int`, `active_sessions int`,
`anomaly_flag bool`, `updated_at`. Upserted every 5 s (`ON CONFLICT (bucket_start)
DO UPDATE`). Retained 3 h for the dashboard look-back
(`ABI_RETENTION_METRICS_BUCKETS`, same janitor).

**Self-healing against redelivery.** The aggregator dedupes by order id in
memory, but under at-least-once delivery a consumer restart before an offset
commit still re-folds a batch into the running sums. A `reconcileLoop` (every
`ABI_RECONCILE_EVERY`, default 60 s) recomputes the visible window directly
from the idempotent `bronze.stream_events` landing and overwrites the bucket
sums — so any double-counted revenue/orders self-corrects within one interval.
Detection results survive the pass: `anomaly_flag` is preserved on conflict,
because the anomaly lifecycle lives in `gold.anomalies`.

### 4.4 `gold.anomalies`

| Column | Meaning |
|---|---|
| `id` | bigserial PK (referenced by the dismiss endpoint) |
| `metric` | `revenue` or `orders` |
| `bucket_start`, `observed`, `expected`, `z_score` | what happened vs. baseline |
| `severity` | `warning` (|z|≥3) / `severe` (|z|≥5) |
| `status` | `open` → `dismissed`/`resolved` lifecycle |
| `detected_at`, `resolved_at`, `dismissed_at` | timestamps |

### 4.5 `gold.detector_state`

The online Welford accumulators (`metric` PK, plus `n`, `mean`, `m2`) are
snapshotted by the aggregator after every flush. On start, `cmd/server`
restores the baseline from this table, so a deploy or crash recovery **resumes
detection instead of re-warming 20 buckets** — which would otherwise ride a
false-alarm burst right after a restart, exactly when someone is watching.

The Go-written realtime gold tables (`realtime_metrics`, `anomalies`,
`detector_state`) are declared as **dbt sources** (`dbt/models/sources.yml`) so
lineage/tests see them; they are written exclusively by the Go stack.

## 5. Anomaly detection

- Online **Welford** mean/variance per metric (`revenue`, `orders`); no history
  retained beyond the accumulator.
- 20-bucket warm-up before anything can fire; then |z| ≥ 3.0 → `warning`,
  |z| ≥ 5.0 → `severe`.
- z is computed **before** the observation folds into the baseline, so a spike
  scores sharply and then adapts (a sustained shift re-fires until the baseline
  catches up — by construction).
- A constant baseline (zero variance) is handled by a **sigma floor inside the
  z-score computation** — never a post-hoc clamp. The sample sigma is floored
  to the larger of 1e-2 (absolute, in the metric's own units) and 5% of the
  baseline magnitude, so a genuinely quiet stretch yields large-but-finite
  z-scores for real divergence, while a merely tiny deviation scores
  sub-threshold. This replaced the old behavior where *any* deviation on a
  flat baseline produced an infinite z (clamped to ±10¹⁵ and therefore always
  `severe`) — a false-alarm generator on every low-traffic bucket. The ±10¹⁵
  clamp survives only as a NaN/Inf backstop in the JSON/SQL round-trip.
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
| `GET /api/v1/metrics` | metric catalog **v1.2.0** — adds `revenue_realtime`, `orders_realtime`, `active_sessions`, and every metric now carries a required `source` field |

SSE specifics: the response controller clears the write deadline, the status
wrapper implements `http.Flusher` (else the assertion fails), and frames carry
`{snapshot?, current, anomaly?, speed_multiplier, status, source}` — `source`
is always `live_replay`, so an agent tool layer can never mistake the stream's
numbers for batch truth.

## 7. Metric definitions added in v1.2.0

Every metric in the catalog carries a **required `source` field** — `batch` or
`live_replay` — so the batch/realtime split is structural, not prose. The
realtime metrics below are `source: "live_replay"`; batch metrics are
`source: "batch"`. Never add or compare figures across the two sources.

| Metric | Definition | Caveat |
|---|---|---|
| `revenue_realtime` | per 1-minute bucket, batch revenue filter | `source: live_replay`; label **live (replay)**; never additive with batch |
| `orders_realtime` | distinct orders per 1-minute bucket, batch filter | `source: live_replay`; in-memory order-id dedup plus periodic bronze reconcile |
| `active_sessions` | distinct sessions with a click/cart event in the bucket | `source: live_replay`; phase-1 clickstream is synthesized |

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

**Operational metrics** (Prometheus text format, no extra exporters needed):
- server `GET /metrics` (on :8080): `abi_consumer_lag` per
  group/topic/partition, `abi_detector_baseline_buckets`, `abi_sse_subscribers`,
  `abi_broadcast_updates_total` / `abi_broadcast_dropped_updates_total`,
  `abi_aggregator_flush_errors_total`, `abi_anomalies_detected_total`.
- producer `:8092/metrics` (`ABI_METRICS_ADDR`): `abi_producer_events_total`,
  `abi_producer_dropped_records_total` (the ring's drop-oldest shedding),
  `abi_producer_buffered_records`, `abi_replay_speed_multiplier`,
  `abi_replay_loop`.

Retention & self-healing knobs: `ABI_RETENTION_STREAM_EVENTS` (12 h),
`ABI_RETENTION_METRICS_BUCKETS` (3 h), `ABI_RETENTION_ANOMALIES` (30 d),
`ABI_RECONCILE_EVERY` (60 s).

Dashboard: `http://localhost:8080` — live tiles (revenue/orders/sessions per
minute), replay-speed badge, anomaly banner with dismiss, and a 1-minute
revenue sparkline, all fed by SSE.

## 9. Test evidence

- **Unit:** simulator ordering/payloads/loop-wrap/ring drop-oldest; anomaly
  pre-add z scoring and severity escalation plus the **sigma floor**
  (zero-variance baseline → finite, sub-threshold z for tiny deviations,
  escalation for real divergence; absolute floor on zero-mean baselines);
  broadcaster fan-out/drop + push/drop counters; telemetry registry render
  format, series identity, nil-safety; SSE live stream, 503 without live,
  anomalies + dismiss, realtime metrics endpoint.
- **Integration (`-tags=integration`, real Redpanda + Postgres):** a controlled
  series is produced (24 baseline minute-buckets + a 15× spike bucket, canceled
  orders interleaved) and the pipeline asserts: all events land in
  `bronze.stream_events` with `loop_id`/`_source_file`/`_batch_id` lineage,
  exactly 25 buckets with only valid orders counted (revenue exact to the cent),
  the spike trip `gold.anomalies` at `severe` with z ≥ 5 pointing at the exact
  spike minute, and `active_sessions` matches the published clickstream. The run
  also verifies the Welford baseline is snapshotted into `gold.detector_state`
  (round-trips losslessly through `Snapshot`/`Restore`) and that `Reconcile`
  clamps an injected double-counted bucket back to bronze's deduplicated truth
  while preserving `anomaly_flag`. Rows are cleaned up after the run.
- **CI:** one compose-driven job — `docker compose up -d --wait` → load → dbt →
  `go vet` + unit + integration → `make app` + `smoke_check.sh`.

## 10. Deferred to Phase 2+

- Training the bot classifier against `bronze.training_ground_truth`.
- Reacting to anomaly events (`ecommerce.anomalies`) with agents/alerting.
- `gold.anomalies` `resolved` lifecycle driven by downstream confirmation.
- Horizontal scaling of consumers (groups already partition by key, but the
  aggregator state is single-node today).