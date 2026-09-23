# Phase 3 — Stream scoring, model-driven anomalies & an agentic NL-BI layer

**Status: complete and verified.** This document records the goals, decisions,
durable contracts, implementation notes, test evidence, and ops procedures for
Phase 3 of ABI, which layers real-time intelligence on top of the Phase 0/1/2
foundation:

- **3A — stream-driven scoring**: the `score-writer` consumes
  `order.placed` + `session.end` from the replay, assembles batch-exact feature
  vectors in Go, scores them through the Python sidecar, persists
  `source="stream_score"` predictions, and fires **model-driven rate anomalies**
  (`detector='model'`) on the same SSE banner as the Phase 1 statistical
  detector.
- **3B — dashboard**: forecast confidence bands, a churn-risk leaderboard, a
  live stream-scored fraud feed, and model-anomaly awareness in the live banner.
- **3C — agentic NL-BI**: a free, open-source-LLM question-answering layer with
  **grounding v2**, provenance-labeled tools, an honest-refusal contract and a
  deterministic regression eval. A deliberate deployment constraint: **no vendor
  LLM APIs** (Anthropic/Claude is out by stakeholder decision) — the agent runs
  against local **Ollama Qwen2.5-7B-Instruct** (free, no per-token cost, no
  data leaves the box), with a deterministic mock harness for evals.

## 1. Goals

1. **Stream scoring**: score every order and session as it replays, with the
   exact feature vector the batch trainers used — no train/serve skew (§3).
2. **Model-driven rate anomalies**: flag *rate* movements (fraud flag rate,
   bot rate) over a trailing 5-minute window at **>3× baseline** — a
   windowed-proportion signal, never a single-prediction trigger (§4).
3. **Honest live view**: persisted stream scores carry
   `metadata->>'source' = 'stream_score'` so the API/dashboard/agent can always
   separate *replay-time* from *batch-time* numbers.
4. **One-producer rule**: the producer remains the single source of sessions;
   Go performs **zero** bot math (the SessionEnd event carries the full bot
   feature vector) (§3.2).
5. **Grounding v2 agent**: answers are numeric-only-if-observed-or-derived,
   provenance is never mixed (batch + live are never summed), unanswerable
   questions are **refused honestly** after one bounded revision, and every
   answer cites its sources. A 20-question eval set is a regression gate.
6. Everything ships green: Go `build`/`vet`/`test`, `pytest ml/tests`, the
   fake-DB eval (20/20) **and** the real-DB eval (19/19 + 1 skip).

## 2. Environment & key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Anthropic/Claude | **Out of scope — deliberate constraint** (stakeholder decision) | the agent must run on a free, open-source LLM; this trade-off's risk is absorbed by the verification layer (grounding v2 + eval gate, §9) |
| LLM backend | **Ollama + Qwen2.5-7B-Instruct (Q4_K_M)** via an OpenAI-compatible client | local, free, no telemetry; `ABI_LLM_BASE_URL` / `ABI_LLM_MODEL` override the defaults |
| Agent latency | Qwen2.5-7B live on CPU: **2.0–5.0 s / grounded question** after model warmup, 0 revisions on hard cases | measured in §5; the same loop runs against the mock harness (`ABI_AGENT_MODE=mock`) for deterministic, no-model CI evals |
| Agent serving | Python stdlib sidecar `ml/agent/server.py` (port **8094**, `ABI_AGENT_URL`), mirroring the Phase 2 `ml/serve.py` pattern | no framework dependency; optional for the Go API (503 when disabled) |
| Tool surface | **No free-form SQL tool** — 9 fixed, parameterized, provenance-labeled tools | injection and write access become structurally impossible; every number the model can cite carries `batch` / `live_replay` / `registry` / `predictions` |
| Grounding | **grounding v2** (R1–R4, §6): literal match, within-label derivations (sum / mean / **difference** — q21), batch/live mix rejection, score-entity tracing, hex-id-safe number extraction | falsifiability beats recall; the percentage-of-anything rule was removed because it accepts every number |
| Refusal | one bounded **revision pass**, then `grounded:false, refused:true` | honest refusal is a feature: the model never bullshits when the tools can't support a claim |
| Eval | `ml/agent/eval_questions.json` — 20 questions (3 unanswerable + a batch/live-sum trap + a derived-difference question) against **fake DB** (deterministic) and **real DB** (dev Postgres) | the fake is the CI regression gate; the real run validates every tool's SQL |
| Score persistence | all stream scores share one `gold.predictions` table, tagged by metadata source | one query surface for dashboard + agent; the `source` filter (`/api/v1/predictions?source=stream_score`) is the live-view discriminator |
| Model anomalies | broadcast on the shared SSE/gRPC banner and **persisted** to `gold.anomalies` with `detector='model'`, `z_score NULL` | same banner as statistical anomalies; the detector column is the separation contract (§5) |

## 3. 3A — stream score-writer (`api/internal/scorewriter`)

### 3.1 Flow

```text
producer ──► order.placed / session.end (Redpanda, same topics as Phase 1)
                │
                ▼
score-writer (consumer group ABI_CONSUMER_GROUP_SCORE_WRITER="score-writer")
   │ 1. ring-buffer per order + rolling features per session
   │ 2. feature assembler ──► fraud/session feature vectors
   │ 3. batches scored via the SAME predict.ScoreAndPersist path as the
   │    REST endpoint  (metadata source="stream_score")
   │ 4. gold.predictions     (persisted, source-tagged rows)
   │ 5. rate tracker         (5-min window rate > 3× baseline)
   └─► gold.anomalies (detector='model', z_score NULL) + bc.Publish
        (same SSE/gRPC banner as statistical anomalies)
```

Design notes:

- **Feature parity, not approximation**: the fraud vector is rebuilt from the
  19-feature spec JSON (`api/internal/scorewriter/fraud_feature_spec.json`) — a
  cross-language contract pinned by a Go text test and Python
  `test_fraud_feature_spec.py`; the bot session vector is assembled from the
  `SessionEnd` event's 15 parsed fields, mapping 1:1 to the 13 exact names the
  trainer's `feature_fraud_orders`-style construction expects. The producer
  emits those fields server-side; **Go concatenates, never computes** bot math.
- **Bounded disorder**: events are reordered by event-time with a slack window
  (`ABI_SCOREWRITER_SLACK`, default 120 s) so cross-partition interleaves don't
  scramble features.
- **Ring capacity** (`ABI_SCOREWRITER_BUFFER`) bounds the scoring job queue;
  overflow drops the oldest job (counted, advisory).
- **Restartable**: features/rings/rate windows snapshot to
  `gold.scorewriter_state` every `ABI_SCOREWRITER_STATE_EVERY` (default 30 s)
  so a restarted server resumes detection rather than resetting the baseline.

### 3.2 Just-in-time decisions

| Decision | Choice | Rationale |
|---|---|---|
| Session features | `session.end` carries the **full bot feature vector** (producer side, single source) | the producer is the simulation's ground truth; Go must not re-derive bot math |
| Fraud features | 19-feature spec JSON is the single source; single copy (Go embeds it, Python imports it) | two copies would drift; the cross-language spec test pins equality |
| Score persistence | `metadata.source="stream_score"` (Go writes the metadata on insert) | the REST live-score path already tags `"live_score"`; the source filter keeps the surfaces honest |
| z-score on model anomalies | always `NULL` | rate anomalies have no z-score by construction; the Go insert hardcodes the `detector='model'` + `NULL z_score` shape (pinned by `schema_test.go`) |
| DDL ownership | `gold.anomalies.detector` column lives in the realtime schema migrations; the score-writer only *writes* `'model'` | single schema file (`schema.go`), pinned by the Go text test |

## 4. 3A — rate anomaly trigger (`ratetrack.go`)

The statistical detector flags *levels* (a spiky bucket vs. an online EWMA/z
baseline). The **model detector** flags *rates*: within any rolling 5-minute
window (`RateWindowSecs`), if `p(at-risk | scored) > 3 × baseline rate`
(recommended_threshold rate from the registry during the run), and the window's
bucket wasn't already flagged, the score-writer persists a model anomaly:

- `metric = "<model>_rate"` (e.g. `fraud_risk_rate`, `bot_score_rate`);
- `observed = windowed positive rate`, `expected = model baseline rate`;
- `severity = "elevated" | "severe"` — severe when `> SevereRateMultiplier × baseline`;
- `z_score` stays `NULL`; one anomaly per bucket (`RateWindowSecs`) — no
  duplicate storms.

It can **never** fire from a single prediction: `Observe` needs ≥
`MinRateSamples` scored events in the window and a window rate above the
multiplier. Unit coverage: windowed-rate math, one-anomaly-per-bucket, severity
escalation, and the `detector='model'` field shape (`ratetrack_test.go`,
`schema_test.go`).

## 5. 3B — dashboard surface

- **Forecast confidence band**: `GET /api/v1/forecast/{category}` returns the
  weekly forecast series + backtest actuals for orders and revenue, and the
  latest-week forecast row with a **±MAE band** (floored at 0) carved from the
  model's own registry validation metrics. The dashboard draws the band strip +
  forecast-vs-actual series chart, switchable per category.
- **Churn leaderboard**: top customers by pending churn score
  (`/api/v1/predictions?model=churn_risk&limit=50`, sorted desc, top 12).
- **Fraud-score column on the live feed**: recent *stream-scored* fraud risk
  (`?model=fraud_risk&source=stream_score`, fallback to latest unfiltered),
  refreshed every 15 s next to the live tiles.
- **Model anomalies on the live banner**: score-writer anomalies publish on the
  same SSE `metrics` frames; the banner renders `detector=model` rates as
  percentages with ">3× baseline" instead of a z-score.

## 6. 3C — agentic NL-BI (`ml/agent`)

### 6.1 Serving

```text
dashboard / API client
        │ POST /api/v1/agent/query {question}
        ▼
Go server (http) ──► ml/agent/server.py :8094 (ABI_AGENT_URL)
                        │ loop: system prompt + tools + ≤1 revision
                        ▼
              grounding v2 (R1–R4) → answer + sources + grounded
```

The **Go API owns the HTTP contract** (503 when the sidecar is missing, 502 on
sidecar failure) and the operational metrics (`abi_agent_up`,
`abi_agent_query_total`, `abi_agent_latency_ms`, `abi_agent_turns_total`,
`abi_agent_grounding_refused_total`, `abi_agent_errors_total`). All *decisions*
live in Python: tool selection is prompt-driven but bounded by the 9 curated
tools, and every decision is verifiable through the response contract.

### 6.2 Tool layer (`tools.py`)

9 tools, all SELECT-only, row-capped, parameterized (`psycopg3` positional
params), each carrying a provenance label:

| Tool | Label | Backing query |
|---|---|---|
| `batch_overview` | batch | gold semantic layer totals (orders, valid revenue, AOV, distinct customers) |
| `batch_top_categories` | batch | top categories by revenue |
| `batch_revenue_by_week` | batch | weekly revenue (anchored to `MAX(order_purchase_timestamp)`) |
| `batch_distinct_customers` | batch | distinct customer count |
| `forecast` | batch | persisted forecast rows for a category (`entity_id LIKE '{cat}@%'`) |
| `model_scores` | predictions | recent persisted scores for `fraud_risk` / `bot_score` / `churn_risk` |
| `model_rate` | predictions | at-risk rate vs the registry's own `recommended_threshold` |
| `registry_metric` | registry | active registry metrics (flattened to first-class row keys) |
| `live_realtime` | live_replay | recent replay buckets + anomalies, anchored to the **latest replay bucket** (never wall-clock) |

Design details worth keeping:

- **Schema defensive**: the `detector` column is omitted from live-anomalies
  SQL because the dev DB's `gold.anomalies` only gains it when the Go server
  migration runs — the agent works before and after.
- **Registry metrics flatten**: thresholds/baselines become first-class row
  keys so the model can cite them directly.
- **No `*` ever reaches the query text** in the forecast tool: patterns are
  composed in Python (`{cat}@%` as a bound parameter) to stay psycopg3-safe.

### 6.3 Grounding v2 (`grounding.py`)

Applied to the final answer text against the tool observations of that
conversation:

- **R1 — literal**: every extracted number must be *observed* in a tool row
  (including prose constants: 0.795 threshold, 0.5, 0.02, 0.01, 3.0, 0.03,
  2880× speed).
- **R2 — derived (within one label only)**: the exact sum, arithmetic mean or
  absolute difference of **two observed values from the same label** — a
  comparison answer ("how much more / how much did it change") cites a gap the
  tools never return directly, and the difference branch grounds it (eval q21
  is the regression pin). Percentages-of-anything are **not** derived (they
  accept every number). Crucially, derivation can never mix `batch` +
  `live_replay`.
- **R3 — provenance**: bidirectional regex rejects batch+live additive
  phrasings *before* the numeric check; the eval's q17 is the regression trap.
- **R4 — entity scores**: any 32-hex entity token quoted in the answer must
  exist among the observed `predictions` rows, and a decimal cited next to an
  observed entity must equal the persisted prediction (within tolerance).
- **Number extraction** skips hex-flanked digits (ids like `a1b2c3…`), ISO
  date/time separators, trailing `Z`, and standalone years 1900–2100; a
  sentence-final period is punctuation, not part of a number.

### 6.4 Response contract

```json
{
  "question": "…", "answer": "…",
  "grounded": true|false, "refused": false|true,
  "revisions": 0..1, "turns": 2..4,
  "tool_uses": [{"tool": "…", "params": {}, "label": "batch", "note": ""}],
  "sources": ["batch"]
}
```

A refused answer is never grounded. `sources` is the provenance union — the
dashboard/agent can see at a glance which surfaces a claim rests on.

### 6.5 Eval set (`eval_questions.json`) — the regression gate

20 questions: 16 answerable (grounded facts + derivations + tool labels) and
**4 unanswerable** (q14 gross-margin-invention, q15 defect-return-rate
invention, q16 product-percentage invention, **q17 batch+live sum trap**) —
plus q19 (anomaly citation) which runs in the deterministic fake suite only,
because "was an anomaly fired?" legitimately depends on what the latest replay
epoch did (in this dev DB it produced none). q21 (revenue gap between the top
two categories) pins the R2 **difference** branch: the gap is never an observed
value, so the grounder must derive it.

Gates:
- `uv run --group ml python -m ml.agent.eval_agent --db fake --llm mock` →
  **20 passed, 0 failed** (CI-deterministic).
- `… --db real --llm mock` against the dev DB → **19 passed, 0 failed, 1 skip**
  (validates every tool against real SQL and Decimal columns).

## 7. Verification

### 7.1 What this phase pinned

| Contract | Pinned by |
|---|---|
| `gold.anomalies.detector` column + statistical-insert shape | `realtime/schema_test.go`, `schema.go` DDL text test |
| Score-writer `detector='model'` + `NULL z_score` insert | `scorewriter/schema_test.go` |
| Fraud feature vector (19 names, one-hot order) | `fraud_feature_spec.json` + `api` text test + `ml/tests/test_fraud_feature_spec.py` |
| Agent grounding R1–R4 + hex/thousand-separator/prose-date extraction | `ml/tests/test_agent_grounding.py` |
| Agent eval end-to-end (refusal, revision cap, batch/live rejection, contract) | `ml/tests/test_agent_eval.py` |
| Go agent client/service contract + metrics | `api/internal/agent/agent_test.go` |
| Tool-layer SQL against a live Postgres | `eval_agent --db real` (19/19 + 1 skip) |

### 7.2 Evidence (this run)

- Real-DB facts used by the eval (definitions pinned here so the numbers are
  reproducible — the revenue figure had a mislabeled-definition paper cut under
  review, fixed this round):

  | Figure | Value | Definition |
  |---|---|---|
  | total orders | **99,441** | `COUNT(*)` over `gold.fct_orders` |
  | valid orders | **98,207** | the `NOT is_lost` subset |
  | valid revenue | **15,739,137.01** | `SUM(payment_value_total)` restricted to `NOT is_lost` — the tool's `valid_revenue` (a previous doc said "sum over non-lost" while the SQL summed *all* orders: 16,008,872.12) |
  | AOV | **160.26** | valid revenue ÷ valid orders, same `NOT is_lost` predicate on numerator and denominator (previously the numerator summed all orders → 163.01) |
  | distinct customers | **96,096** | `COUNT(DISTINCT customer_unique_id)` |
  | fraud at-risk | **221 / 19,889 (1.11%)** | at the registry's 0.795 threshold |
  | top category | `bed_bath_table`, **1,711,258.08** | `batch_top_categories` revenue |

  Live buckets are the latest replay epoch; `gold.anomalies` pre-migration rows
  are Phase-1 statistical (no detector column).
- A psycopg3 subtlety the real run caught: NUMERIC columns arrive as `Decimal`,
  which the observed-values walker must cast like floats — the fake (floats)
  had masked it.
- A wall-clock subtlety: the live window is anchored to the **latest replay
  bucket**, not `now()` (a replay that stopped yesterday must still be the
  live view; `now()`-based windows went empty).
- **Live LLM smoke (Ollama Qwen2.5-7B-Instruct, Q4_K_M)**: the sidecar in
  `--mode live` grounded `q01` (99,441 orders), `q04` (`bed_bath_table`,
  1,711,258.08), the weekly series and the batch/live-sum trap — 2.0–5.0 s per
  question, 0 revisions, honest "not available" for the unanswerable gross
  margin. The llama runner holds ~5.6 GB private / 1.2 GB resident.
- The live run exposed two extraction warts the mock scripts had masked (both
  pinned by regression tests in `test_agent_grounding.py`): (a) a real LLM
  formats numbers with thousands separators (`98,207`, `1,711,258.08`) — the
  claim regex now consumes `,\d{3}` groups instead of splitting the number
  into unobserved claims; (b) prose dates ("September 17, 2018") leaked the
  day-of-month `17` as a claim — a 1–31 directly after a month name is now
  dropped before extraction.
- **Model anomalies fired and persisted on the live run**: after ~90 min of
  2880× replay the score-writer wrote **309 `detector='model'` rows** — **268
  severe** (>5× baseline) and **41 elevated** (3–5×), *not* all elevated (the
  earlier draft understated them: `rate > SevereRateMultiplier * baseRate` was
  routinely true) — all `bot_score_rate`, observed 0.071–0.40 vs the 0.02
  baseline, one per 5-minute bucket. Reconciliation: 268 + 41 = 309 ≈ 0.6%
  of the ~51,840 five-minute bucket slots in the replayed epoch (90 wall-min ×
  2880× ≈ 180 simulated days). The *global* stream-score positive rate stayed
  on-target throughout (bot 1.89% vs 2% baseline, fraud 0.36% vs 0.97%), so the
  trigger verifies the full 3A write path (windowed-rate math, rate-track
  persistence, `detector='model'` insert, banner metrics). Review teardown: in
  quiet replay the sparse early windows held only 1–10 observations, so small
  counts crossed the 3× line — a small-sample lull, not a real breach. Fix
  applied: `MinRateSamples` 5 → **20** (§4) so a busy-window breach still
  fires while lull noise is silenced; the lull-window story stays a Phase 4
  drift-monitoring item, not a code defect.

## 8. Ops

### 8.1 Run it (Windows dev / Linux CI)

```bash
# infra + gold layer + training (already done for this dataset)
docker compose up -d --wait
uv run dbt build --project-dir dbt --profiles-dir dbt
make train                 # or use the registered versions already in the DB

# 1) model sidecar    2) agent sidecar  3) server (score-writer inside)
uv run --group ml python ml/serve.py
uv run --group ml python ml/agent/server.py --mode mock|live
cd api && go run ./cmd/server

# 4) replay producer (drives orders/sessions through the score-writer)
cd api && go run ./cmd/producer
```

Ollama (agent live mode): `winget install Ollama.Ollama`, `ollama pull
qwen2.5:7b-instruct` (or set `ABI_LLM_BASE_URL`/`ABI_LLM_MODEL` to another
OpenAI-compatible endpoint), then run the agent sidecar with
`ABI_AGENT_MODE=live`.

### 8.2 Environment

`.env.example` additions: `ABI_AGENT_URL` (default
`http://127.0.0.1:8094`), `ABI_AGENT_ADDR`, `ABI_AGENT_MODE=mock|live`,
`ABI_LLM_BASE_URL`, `ABI_LLM_MODEL`. The Go server additionally honors
`ABI_CONSUMER_GROUP_SCORE_WRITER`, `ABI_SCOREWRITER_SLACK`,
`ABI_SCOREWRITER_BUFFER`, `ABI_SCOREWRITER_STATE_EVERY`.

### 8.3 Make targets

`make agent-mock`, `make agent-eval` (`--db fake`), `make agent-eval-real`
(`--db real`), `make agent-test` (ml tests + fake eval). CI runs
`make agent-test` in the ML step (deterministic — no Ollama, no network).

## 9. Known limitations & deferred

- **LLM latency**: a Qwen2.5-7B (Q4_K_M) answer on CPU takes **2.0–5.0 s**
  grounded questions after warmup (measured §5, ~1.2 GB resident / 5.6 GB
  private for the llama runner); the Go client allows 60 s. The mock harness
  keeps evals fast and CI-safe; a live demo needs Ollama installed with
  `qwen2.5:7b-instruct` pulled (§8.1).
- **R4 is recall-limited**: it detects fabricated scores for observed entities
  and refuses unknown-entity citations, but does not positive-verify prose
  claims that carry no number.
- **Stream-stat restart safety is verified, not changed**: the realtime
  aggregator's Welford baseline survives restarts — `SaveDetectorState` runs
  after every flush and `LoadDetectorState`/`restoreBaseline` replay the last
  snapshot at boot ("aggregator: detector baseline restored, metrics:2", with
  a round-trip test at `integration_test.go`) — so a consumer restart never
  zeroes the mean/variance history or re-deduces drift. No code change was
  needed on review.
- **The local-LLM choice is deliberate, and safe because of the verification
  layer**: a less capable base model is a conscious trade-off (self-hosted,
  free, no per-token cost, no vendor API) whose risk is absorbed by the
  grounder + the eval gate — the acceptance test is on the agent's tool chain
  and provenance invariants, not on the model's prose.
- **Agent window anchoring**: `live_realtime` describes the *latest replay
  epoch*, which is the truthful view for a replay system; a real-time (not
  replay) deployment would re-anchor to wall-clock.
- **Autonomous retraining**: still manual (`make train`); drift-triggered
  retraining remains Phase 4 work.