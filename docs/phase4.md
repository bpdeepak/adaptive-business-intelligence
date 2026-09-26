# Phase 4 — Governance: events, playbooks, an approval queue with an immutable audit log, and drift-driven retraining

**Status: complete and verified.** This document records the goals, decisions,
durable contracts, implementation notes, test evidence, and ops procedures for
Phase 4 of ABI: the layer that stops the machine from acting silently. Every
business-AI effect (hold an order, retrain a model, draft a purchase order,
propose a retention offer) now flows through one declarative path — a domain
event on an in-process bus, a playbook rule that *proposes*, and a human (or the
explicit allow-list) that *decides* — with every transition written to an
append-only audit log.

- **4A — domain event bus + playbook engine** (`api/internal/events`,
  `api/internal/playbook`): typed events published *after* DB writes; rules in
  `config/playbooks.yml` (validated at boot) match on a small pure expression
  language and propose governed actions. No rule can act; it can only propose.
- **4B — approval queue + immutable audit log** (`api/internal/actions`): the
  queue is authoritative state, the audit log is append-only history. Every
  transition (`proposed → approved/rejected → executed → outcome`) records an
  actor, a reason (mandatory for human decisions), and the triggering payload.
- **4C — simulated action registry**: real database effects on fake tables
  (`hold_order_flags`, `purchase_orders`, `retention_actions`), idempotence
  guards, and a strict dependency-injected executor contract so no executor can
  run without a prior `approved`/`auto_approved` audit row.
- **4D — drift/decay monitoring + retrain-as-governed-action**: a Python
  pipeline (`ml/monitor`) writes plain rows to `gold.model_drift` (the DB table
  *is* the Go↔Python contract, F5); a Go poller turns each (model, computed_at)
  run into a `drift_computed` event; critical findings *propose* `retrain_model`,
  which after approval enqueues a `gold.retrain_requests` row that a worker
  consumes to train a **candidate** version (never auto-promoted).
- **4E — a graceful degradation rule**: the queue carries clear `risk_tier` on
  every row; consumers surface it ("human in the loop") instead of pretending
  the platform acts autonomously.

Carry-forwards from the Phase 4 review (F1–F8) are all in place: features jsonb,
stream/batch PSI split, dormant events, hysteresis, the `DriftComputed` bridge,
`retrain_requests` + worker, `dedup_key UNIQUE`, plus the Phase 3 invariants
(observed/derived numbers only, batch+live never summed, ≤1 bounded revision,
derivations within one provenance label, grounding v2, "propose, don't silently
act").

## 1. Goals

1. **No silent action**: every producer publishes a typed event *after* its DB
   write; the only path from event to effect is a validated playbook rule →
   proposal → (human approval | allow-list auto tier) → audited execution.
2. **Audit as a first-class truth**: `gold.action_audit_log` is append-only
   (no UPDATE/DELETE in the API), foreign-keyed to `gold.action_queue`, and
   replayed by the `/api/v1/actions/{id}/trace` surface so a reviewer sees the
   whole decision trail with reasons and payloads.
3. **Declarative policy**: rules live in `config/playbooks.yml`, not code;
   changing policy is a diff + restart, and a malformed rule aborts boot.
4. **Drift that drives action (after a human)**: model input drift and forecast
   drift is measured honestly (PSI bands ok <0.10 / warning 0.10–0.20 /
   critical >0.20; the forecast *backtest-reproduction* check is warning-capped
   and is not decay, §11.10), and a critical finding *proposes*
   a retrain. The retrain trains a **candidate** and never promotes itself.
5. **Deterministic verification**: Go `build`/`vet`/`test` (+ `-tags
   integration`), all Python unit tests, and the fake-DB agent eval
   (25/25, incl. the new governance questions q22–q26) stay green; the real-DB
   eval runs with `--db real --llm mock`.

## 2. Environment & key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Event bus | in-process `events.Bus`, fan-out with drop-slow-consumer (mirrors the Phase 1 SSE broadcaster contract) | the scoring/anomaly hot path pays zero latency for governance; the authoritative record is always the DB row already written |
| Policy representation | YAML in `config/playbooks.yml`, compiled and validated at boot | policy is config, not code; a typo aborts startup instead of silently misbehaving |
| Condition language | tiny pure expression language over the event payload (`prediction.score >= 0.81`), no function calls, unknown fields fail closed | matches are mechanically checkable and testable |
| Human-in-the-loop default | `approval_required` is the default posture; `auto` is restricted to an explicit **code** allow-list, `actions.AutoAllowed` (`log_event_note`, `log_retention_email` — informational log writes with no revenue exposure; see §11.2) | "propose, don't silently act" is the invariant; the allow-list is the audited exception, and a rule in the YAML cannot grant itself membership |
| Deduplication | `dedup_key = rule|scope` UNIQUE on the queue; a replayed event (same rule + trigger instance) can never flood the queue | idempotent replay survival across restarts |
| Go↔Python contract | `gold.model_drift` is the contract (F5) — Python writes rows; Go polls them; no bespoke HTTP bridge | one durable table both sides can reason about; the same parity pattern as realtime/predict |
| Drift measurement | 10-bin PSI per feature vs the training-matrix histogram stored in the registry `drift_baseline` (over the model's own feature columns) | honest comparison: recent inputs vs what the model was trained on |
| Drift exclusion scope | features that cannot be re-measured in a comparable window are excluded (`category_code` is derived at predict time, not a mart column; `year` is constant within any rolling window) | measuring what can't be measured comparably produces pure noise — same honesty rule as everything else in this repo |
| Governance posture for retrain | approved `retrain_model` → `gold.retrain_requests` row → `ml/monitor/retrain_worker.py` trains with `--version <now_tag>.retrain<action_id>` **as a candidate** | candidates never auto-promote; promotion to `active` is a deliberate registry flip by a human |

## 3. 4A — event bus & playbook engine (`api/internal/events`, `api/internal/playbook`)

### 3.1 Events

Only facts the playbook understands are typed. Producers publish **after** their
DB write:

| Type | Producer | Published when |
|---|---|---|
| `order_scored` | score-writer | an order's fraud prediction is persisted (`gold.predictions`, source=`stream_score`) |
| `session_scored` | score-writer | a session's bot prediction is persisted |
| `anomaly_detected` | realtime aggregator | any anomaly (statistical or model) row lands in `gold.anomalies` |
| `drift_computed` | monitor poller | a new (model, computed_at) run appears in `gold.model_drift` |
| `forecast_updated` | *(none yet — Phase 5 scheduled forecast worker)* | dormant rules kept parseable |
| `churn_scored` | *(none yet — Phase 5 scheduled churn job)* | dormant retention rules kept parseable |

### 3.2 Rules

```yaml
version: 1
rules:
  - name: hold-high-fraud-order      # order_scored  → approval_required hold
  - name: log-midband-bot-session    # session_scored → auto informational note
  - name: retrain-on-critical-drift  # drift_computed → approval_required retrain
  # dormant (enabled: false, kept parseable + validated):
  - name: propose-retention-offer-high-churn
  - name: nudge-low-risk-churn
  - name: draft-po-on-weak-forecast
```

A rule reads: *when `trigger` fires and `condition` holds, propose `action` on
the queue with `risk_tier`.* The engine dedupes (`rule|scope`), refuses to
propose twice for the same trigger instance, and compiles all conditions at boot
(a malformed expression aborts the server).

## 4. 4B — approval queue & immutable audit log (`api/internal/actions`)

### 4.1 Table contracts

```sql
gold.action_queue (id, action, entity, risk_tier, status, params jsonb,
                   trigger jsonb, rule, dedup_key UNIQUE, created_at,
                   decided_at, executed_at, outcome jsonb)
gold.action_audit_log (id, action_id FK, transition, actor, reason, detail jsonb, at)
gold.retrain_requests (id, action_id UNIQUE, model, status, requested_at,
                       finished_at, new_version)
```

- `action_queue` is **authoritative state**: `pending → approved|rejected →
  executed`; the executor writes `outcome` synchronously.
- `action_audit_log` is **append-only history**: every transition inserts one
  row (`proposed` by `playbook`, `approved`/`rejected` by a human actor with a
  mandatory reason, `executed` with outcome, `outcome` by the retrain worker).
  Nothing in the API UPDATEs or DELETEs audit rows, and since the 2026-09-24 audit
  the **database refuses** any UPDATE, DELETE or TRUNCATE on the table (§11.13).

### 4.2 The execution guard (the invariant that makes "propose, don't silently act" real)

`actions.New(pool, log)` builds the executor registry. `execute` is unexported and
reachable only through `Approve`, `Retry` and `AutoApproveAndExecute`; it refuses to
run unless the queue row has an `approved`/`auto_approved` audit row. The integration
test (`api/internal/actions/integration_test.go`) pins this — a caller cannot invoke an
action's effect without the prior audited decision. Two further paths exist and are
guarded separately: the `auto` tier is limited by `actions.AutoAllowed` (§11.2), and the
Python retrain worker, which launches trainers, verifies the approval itself (§11.1).
Even so, these are checks in *our* code: anything with direct write access to
`gold.action_queue` / `gold.retrain_requests` is outside them (the audit log itself is
protected by a trigger, §11.13).

**Identity scope (stated explicitly, audit K3).** There is no authentication. The
`actor` on an approval or rejection is caller-supplied text (default
`analyst@abi.demo`), so the audit log records the *claimed* decider and the mandatory
reason, not a verified identity. The governance guarantees here are about *process*
(no execution without a recorded decision; nothing recorded can be silently edited),
not about who is at the keyboard. Real authN/authZ is out of scope for this portfolio
project.

### 4.3 API surface

| Endpoint | Shape |
|---|---|
| `GET  /api/v1/actions?status=&limit=` | `{data:[ActionRow…]}` — queue state + history |
| `POST /api/v1/actions/{id}/approve` | `{reason, actor}` → status `approved`, then auto-executes |
| `POST /api/v1/actions/{id}/reject` | `{reason, actor}` → status `rejected`, never executes |
| `POST /api/v1/actions/{id}/retry` | re-runs a `failed` execution under its original approval (§10.4) |
| `GET  /api/v1/actions/{id}/trace` | `{data:{action, audit:[…]}}` — full decision trail |

Reasons are mandatory on human decisions; an approval without a reason is not an
auditable decision.

## 5. 4C — simulated action registry (`api/internal/actions/actions.go`)

The registry maps action names to executor functions; effects land on dedicated
**fake** tables so they cannot degrade the production gold surfaces:

| Action | Effect (fake table) | Notes |
|---|---|---|
| `hold_order_for_review` | `gold.order_flags` (order_id PK, held_for_review, reason, released_at) | upsert (idempotent); immutable `gold.fct_orders` never touched; every transition audited |
| `release_order` | `gold.order_flags` (clears the hold) | always human-gated; never allow-listed |
| `log_event_note` | `gold.event_log` | the **auto-tier allow-list** action (informational) |
| `log_retention_email` | `gold.retention_actions` (`status='logged'` — nothing is sent) | allow-listed; 24h idempotence guard; dormant rule |
| `propose_retention_offer` | `gold.retention_actions` (`status='proposed'`) | approval required; dormant rule |
| `retrain_model` | `gold.retrain_requests` (action_id UNIQUE) | consumed by the Python worker, which re-verifies the approval |
| `draft_purchase_order` | `gold.purchase_orders` (`ON CONFLICT (category, forecast_week) DO NOTHING`) | dormant rule; idempotent |

Every table is created idempotently at boot from the single shared
`api/internal/actions/schema.sql` (also loaded by the Python side); drift baselines were computed over the Phase 2 training matrices.

## 6. 4D — drift/decay monitoring & retrain-as-governed-action

### 6.1 Pipeline

```text
ml/monitor/drift_check.py              Go (api/internal/monitor)
  ├─ per active model:                   │
  │   stream PSI  ← gold.predictions     │  poller (ABI_DRIFT_POLL_EVERY, default 60s)
  │   batch PSI   ← feature marts        │    reads rows id > watermark
  │   backtest repro ← held-out rows vs  │    merges one event per (model, computed_at)
  │                       trained WMAPE  │    worst status wins … DriftComputed
  │        └──► writes gold.model_drift  └──► playbook rule retrain-on-critical-drift
                                                   │ (drift.status == 'critical')
                                                   ▼
              gold.action_queue (pending retrain_model) ──► human approve/reject
                                                   │ approved
                                                   ▼
              gold.retrain_requests (pending) ──► ml/monitor/retrain_worker.py
                                                   │ trains <now_tag>.retrain<action_id>
                                                   ▼
              gold.model_registry (status='candidate')  +  audit 'outcome'
```

### 6.2 Measurement semantics

- **PSI bands**: ok < 0.10, warning 0.10–0.20, critical > 0.20; NaN → ok, inf →
  critical (`ml/monitor/psi.py`).
- **Stream vs batch split** (F2): stream models (`fraud_risk`, `bot_score`)
  sample the exact feature vectors the Go score-writer persisted in
  `gold.predictions.features` (metadata source=`stream_score`); batch models
  (`churn_risk`, forecasts) sample their feature marts. The two are never
  mixed.
- **Run merging** (F4; unrelated to the anomaly-banner hysteresis) lives in the Go rank/merge and the durable watermark: a
  run is emitted once, worst status wins, and restarts resume where the
  watermark left off — no row is replayed twice, none is lost.
- **Forecast backtest reproduction** (`kind='backtest_repro'`; formerly, wrongly,
  "forecast decay"): WAPE recomputed from the *active* version's persisted
  held-out predictions vs the `wmape` the registry recorded for it; warning above
  1.25×, never critical. Not a decay measurement — see §11.10.

### 6.3 Retrain worker contract

`uv run --group ml python -m ml.monitor.retrain_worker` consumes pending
`retrain_requests`, re-runs the model's trainer (script mode so `import common`
resolves) pinned to `--version <now_tag>.retrain<action_id> --candidate`,
registers the result with `status='candidate'`, writes the audit `outcome`
transition (`{status: done, new_version, trained_candidate: true}`), and marks
the request done. **Candidates never auto-promote.**

## 7. 4E — API, dashboard & agent surfaces

### 7.1 Metrics catalog

`GET /api/v1/metrics` (catalog version **1.4.0**) adds two provenance sources and
four metrics:

- `governance_pending_actions`, `governance_decided_actions` — source
  `governance` (counts of proposals/decisions, never additive with business
  figures).
- `model_drift_psi`, `model_backtest_reproduction` (catalog **1.5.0**; was
  `model_forecast_decay` in 1.4.0) — source `monitoring` (per-feature PSI and the
  backtest-reproduction check, both advisory).

### 7.2 Dashboard

The dashboard gains an **Approval queue** (approve/reject with a mandatory
reason input, refreshing every 20s), an **Action history + trace** panel (trace
opens the full audit trail), and a **Model health** board rendering
`gold.model_drift` (PSI + backtest-reproduction with status pills).

### 7.3 Agent tools

The NL-BI agent gains three provenance-labeled tools (`ACTIONS="actions"`,
`MODEL_HEALTH="model_health"` labels): `get_pending_actions`,
`get_action_history`, `get_model_drift` — plus fake-DB routes and eval questions
q22–q26 (governance rationale, action count, model health; q26 is deliberately
unanswerable so the grounder drives a refusal). The fake eval is the regression
gate; the real-DB run validates the live SQL.

## 8. Test evidence

- Go: `go build ./...`, `go vet ./...`, `go test ./...` green.
- Go integration: `go test -tags integration ./...` covers the execution guard
  (no executor without a prior audited decision), dedup, and the drift poller
  (bootstrap, worst-status merge, watermark advance, restart resume —
  `api/internal/monitor/poller_integration_test.go`).
- Python: 64 unit tests green (incl. `ml/tests/test_monitor_psi.py`).
- Agent eval: fake DB 25/25 (incl. q22–q26); real DB with `--llm mock` green.
- Live end-to-end (this session): injected critical PSI (churn_risk) → poller
  → `retrain-on-critical-drift` proposal → human approve → executor enqueued
  `retrain_requests` → worker trained candidate
  `20260924.050835.retrain15` → audit `outcome` written; a second proposal
  (bot_score bootstrap-window critical) was rejected with a recorded reason and
  never executed. Audit trail verified via `/api/v1/actions/15/trace`.

## 9. Ops

- **Run the monitor**: `uv run --group ml python -m ml.monitor.drift_check`
  (or `make monitor`); watch: `make monitor-worker` (long-polling worker loop).
- **Restart safety**: current watermark + dedup keys persist in Postgres;
  replaying a drift run never re-proposes.
- **Promote a candidate**: flip `status='candidate' → 'active'` in
  `gold.model_registry` deliberately; the retrain worker never does it.
- **Tune the poller**: `ABI_DRIFT_POLL_EVERY` (seconds, default 60).
- **Tune the reconciliation backstop**: the governance reconciler (§10.2) runs
  on the same `ABI_RECONCILE_EVERY` period as the realtime reconcile (default
  60s); its restart-safe cursor lives in `gold.governance_state`.
- **Live demo scenario**: `docs/phase4-demo.sql` (if present) documents the
  injected critical-drift row used to exercise the loop; rows carry
  `detail.injected=true` so the board stays honest.

## 10. Post-review hardening pass (carry-forward status + H1–H4)

This section records the second review cycle on top of the completed phase: the
carry-forward status for both earlier review rounds (with commit anchors), and
the four hardening items the follow-up review produced (drop-proof proposal
delivery, dedup scope, failed→retry, boot-time field checks). All landed in
commit `35db6c2` on top of the Phase 4 base.

### 10.1 Carry-forward status — two review cycles, all landed

**Cycle 1 — Phase 3 review carry-forwards** (delivered in `f0b5ac8`, a separate
pre-Phase-4 pass; each verified with code evidence before Phase 4 started):

| # | Item | Where it lives | Verification |
|---|---|---|---|
| 1 | Welford restart-safety | `api/internal/realtime/aggregator.go` (`restoreBaseline`/`saveBaseline`, `detector_state.go`) | roundtrip test: a restarted aggregator resumes same detectors |
| 2 | Model-rate banner quiet-period | Two separate mechanisms: `MinRateSamples` 5 → **20** (small-sample lull noise) and, added in Phase 4, `ConsecutiveWindowsRequired = 2` banner hysteresis persisted as `gold.anomalies.surfaced` (phase3 §4, §11.9). Neither is the drift-run merge (F4). | `ratetrack_test.go` (lull regression, hysteresis, streak restore); `store/anomalies_integration_test.go` (REST path honours it) |
| 3 | Grounding R2 differences (abs) | `ml/agent/grounding.py` — `_match_derived` uses `abs(a−b)`; MockLLM emits `{diff_…}/{last_…}` | eval q21 (`derived` kind) green in both fake + real runs |
| 4 | Revenue definitions | `ml/agent/tools.py` — AOV = `SUM(…)/COUNT(…)` over the **catalog's** population (`NOT is_lost AND payment_value_total > 0`, aligned in §11.11); truth `avg_valid_order_value = 160.27` (was 160.26 over a different population) | real eval q03 green; `docs/phase3.md` §7.2 |

**Cycle 2 — Phase 4 spec review findings (F1–F8)** (all in the 4A–4E commits
`de99a89`, `93a17c2`, `820de3c`; enumerated in the phase header above):

| F | Item | Where it lives | Verification |
|---|---|---|---|
| F1 | `features` jsonb on `gold.model_drift` | `api/internal/predict/schema.sql` | `ml/tests/test_monitor_psi.py` |
| F2 | Stream/batch PSI split | monitor merge + rank logic (`api/internal/monitor`, `ml/monitor`) | PSI semantics tests |
| F3 | Dormant events declared as engine contract | `api/internal/events/bus.go` (`forecast_updated`, `churn_scored`) | dormant rules pass boot, never arm |
| F4 | Drift-run merge (worst status per run) + durable watermark — *not* the banner hysteresis | `api/internal/monitor/poller.go` | `poller_integration_test.go` (bootstrap, merge, watermark, restart) |
| F5 | `gold.model_drift` table is the Go↔Python contract | `api/internal/monitor` package doc | monitor + integrate tests |
| F6 | `retrain_requests` + candidate-only worker | `gold.retrain_requests` + `ml/monitor/retrain_worker.py` | live loop demo (trained `20260924.050835.retrain15`, never auto-promoted) |
| F7 | `dedup_key UNIQUE` | `gold.action_queue` `uq_action_queue_dedup` | `TestDedupKeyPreventsQueueFlood` |

Standing invariants carried across both cycles (regression-pinned): observed /
derived numbers only, batch+live never summed, ≤1 bounded revision, derivations
within one provenance label, "propose, don't silently act", candidates never
auto-promote, `skip_when: "real"` eval guard, dedup UNIQUE, schema/config
fail-closed at boot.

### 10.2 H1 — at-least-once proposal delivery: the reconciliation backstop (`api/internal/govern`)

**Why it exists.** The in-process domain bus is fan-out / drop-slow-consumer by
design (the same contract as the Phase 1 SSE broadcaster). For the dashboard
that is cosmetic; for governance it is not. This was demonstrated live during
the Phase 4 demo: the replay producer replayed ~11,000 scored sessions while the
engine's propose path (one DB round-trip per event) could not keep up with the
producer rate, and the 64-slot buffer overflowed — **zero** scoring-triggered
proposals survived *and nothing visibly broke*: every prediction row was safe in
Postgres. A dropped `order_scored`/`session_scored`/`drift_computed` event costs
nothing visible and silently costs the proposal itself.

**Design.** `api/internal/govern/reconcile.go` — a period job that treats the DB
as the source of truth and *re-derives* proposals from persisted rows:

- Scans `gold.predictions` (`metadata->>'source' = 'stream_score'`) and
  `gold.model_drift` above a persisted cursor and reconstructs the exact event
  each row should have produced (prediction + registry threshold resolved per
  exact model version from `gold.model_registry.metrics` — the same column the
  live rate trackers read; drift merged per (model, computed_at), worst status
  wins — identical shape to the poller, so dedup keys align).
- Feeds every reconstructed event through the **same decision path as the live
  bus** — exported `playbook.Engine.Handle` (one propose code path; conditions
  still decide; dedup keys make re-proposal a no-op).
- Recovery is visible: a proposal created by reconciliation carries
  `payload.reconciled=true` inside its persisted trigger evidence, so the audit
  trail shows it was backfilled, not delivered live.
- Restart-safe: cursors live in `gold.governance_state` (key `'reconcile'`,
  one per scan source), written only after a fully successful pass — a pass
  with proposal-storage failures refuses to advance its cursor and retries next
  tick (`Engine.Handle` returns the failure count for exactly this).
- Only triggers with an enabled rule are scanned (armed-rule set from the same
  rules passed to the engine).
- Runs on `ABI_RECONCILE_EVERY` (default `60s`), the same period as the Phase 1
  realtime reconcile loop — both are "re-derive truth from the DB" backstops.

**Honest boundary.** The reconciler catches drop-slow-consumer and restart gaps.
It does not defend against Postgres being unavailable during its own pass — at
that point the whole layer is degraded, and the engine logs every storage
failure loudly.

**What the reconciler cannot recover (added by the 2026-09-24 audit).** It rebuilds
proposals from *persisted* prediction and drift rows, so anything that never became a
row is out of its reach: (a) an order/session whose score job was **dropped by the
score-writer's bounded queue** (drop-oldest, counted in `abi_score_writer_drops_total`)
or whose **sidecar call failed** (`abi_score_writer_errors_total`) was never persisted,
hence never proposed; (b) `anomaly_detected` is not scanned (no live rule uses it);
(c) the dormant `churn_scored`/`forecast_updated` types have no producer. Those losses
are visible only as counters, not recovered. Scans are capped per pass
(20,000 rows, §11.15) and continue on the next tick.

**Test** (`govern/reconcile_integration_test.go`): rows inserted straight into
`gold.predictions`/`gold.model_drift` with **no bus event ever published** are
proposed by one `Once` pass (hold pending, auto mid-band note executed end to
end, critical-drift retrain pending), below-threshold rows propose nothing, a
second pass adds nothing (idempotency), a fresh row after the first pass is
picked up on the next (cursor advance), and the hold row's trigger evidence
carries `reconciled=true`.

### 10.3 H2 — dedup scope is per trigger instance

`dedupKey` for scored events is now `rule | prediction.entity_id |
prediction.id` (the persisted prediction row id, published by the score-writer
on every scored event). Entity-only dedup had a silent-vaccination bug: an
entity flagged, reviewed and released could never be flagged again, even when a
*later* scoring crossed the threshold once more. Instance-scoping means one
prediction row can never propose twice, while each new scoring of the entity is
its own trigger occurrence. Drift (`rule|model|feature|computed_at`) and
anomaly (`rule|metric|bucket_start`) were already instance-scoped and are
unchanged. Tested in `playbook/engine_test.go`
(`TestDedupKeyScopedToTriggerInstance`) and the govern integration test's dedup
assertion.

### 10.4 H3 — failed → retry (a failed execution is not terminal)

`status='failed'` is an outcome, not a grave: `POST /api/v1/actions/{id}/retry`
re-runs a failed execution **under its original approval**. No new human
decision is needed — the `approved`/`auto_approved` audit row remains the
authority — and the executor guard in `execute()` still applies on every
attempt. The retry appends a `retrying` transition, so the trail reads
`proposed → approved → failed → retrying → executed` (or `… → retrying →
failed` with the fresh outcome when it fails again; the row stays visible and
retryable). The dashboard renders a **retry** button on failed history rows.
State machine via `actions.Service.Retry`; tested in
`actions/integration_test.go` with a transiently-failing executor (recovers on
retry) and a permanently-failing one (stays `failed`).

### 10.5 H4 — boot-time condition field validation (typos abort startup)

The expression evaluator has always failed closed at *evaluation* time on a
field the event does not carry. That is a good second line but not enough: a
parse-valid typo (`prediction.scrore`…) compiles, boots, and silently never
fires — forever. The playbook compiler now cross-checks every path a condition
references against a per-event-type payload schema
(`events.DeclaredFields`, `api/internal/events/payloads.go`; §11.3), so a wrong field name aborts startup the
way a malformed expression does. Dormant rules are parsed and field-validated
too (finally matching `config/playbooks.yml`'s "kept parseable + validated on
purpose" claim), without being armed or action-checked. Runtime fail-closed for
optional-but-absent fields stays the second line. Tests:
`TestBootRejectsUnknownConditionField` (typo fails `NewEngine`),
`TestRuntimeFailClosedWhenSchemaFieldAbsentFromPayload`.

### 10.6 Updated gates (post-hardening)

- `go build ./...`, `go vet ./...`, `go test ./...`, `go test -tags integration ./...` — green (incl. the new govern reconciler + retry integration suites).
- Python: `uv run --group ml python -m pytest ml/tests -q` — **64 passed**.
- Agent eval: `--db fake --llm mock` — **25/25**; `--db real --llm mock` — **19 passed / 0 failed / 6 skipped**, re-run green. (q12 is *data-racy*
  against a live, actively-scoring DB: `model_scores` orders by
  `predicted_at DESC`, so a score landing between the answer and the check can
  flip the expected head row; observed once, passed on immediate re-run,
  unrelated to post-phase changes. Candidates for a future `limit N > 1`
  R1-golden pin.)
## 11. Audit remediation (2026-09-24)

`docs/audit-2026-09-24.md` checked this phase's claims against the code. This
section records each fix as it landed (finding ids refer to that report); the
sections above were corrected in place where they had drifted.

### 11.1 C1 — the retrain worker enforces the governance invariant itself

The worker is a second action-executing path (it launches a trainer), so the
"no execution without an approved audit row" guard in `actions.Service.execute`
did not cover it: it trained for any pending `gold.retrain_requests` row.
`ml/monitor/retrain_worker.py::authorise` now refuses a request unless its
`action_id` is a `retrain_model` action with an `approved`/`auto_approved` audit
transition **and** the request's model is the model that approved action targets
(same precedence as the Go executor: `drift.model`, `model`, `params.model`).
A refused request is marked `failed` (`detail.refused=true`) and an audit
`outcome` is written when the action exists. Tests:
`ml/tests/test_retrain_worker.py` (DB-backed; no approval, missing action,
wrong action type, tampered model, both authority transitions).

The governance DDL is now single-sourced like the serving DDL:
`api/internal/actions/schema.sql` is embedded by Go and read by
`ml/common.py` (`GOVERNANCE_SQL`, `ensure_governance_tables`).

### 11.2 C2 — `auto` is a real allow-list

Before: `risk_tier: auto` was whatever the rule claimed, so a rule such as
`{action: retrain_model, risk_tier: auto}` booted and executed with no human.
Now `actions.AutoAllowed` (`autoAllowList` in `actions.go`) is the single source:

* `playbook.LoadRules` and `NewEngine` reject an `auto` rule whose action is not
  listed — dormant rules included, so flipping `enabled` can never be the
  moment a non-allow-listed action arms;
* `Service.Propose` refuses `auto` for a non-listed action (`ErrAutoNotAllowed`);
* `AutoApproveAndExecute` re-checks the action against the list even when the
  row's tier says `auto`, and checks `status = pending` *before* it writes the
  `auto_approved` audit row (previously calling it on an executed or rejected
  row left a misleading approval transition behind).

Tests: `playbook/engine_test.go` (`TestBootRejectsAutoTierOnNonAllowListedAction`,
`TestAutoAllowListIsOnlyInformationalActions`), `playbook_test.go`
(`TestLoadRulesRejectsAutoOnNonAllowListedAction`, and
`TestShippedPlaybooksBootTheEngine`, which runs the real `config/playbooks.yml`
through `NewEngine`), `actions/integration_test.go`
(`TestProposeRefusesAutoForNonAllowListedAction`,
`TestAutoPathRefusesNonAllowListedActionEvenWhenTierSaysAuto`,
`TestAutoPathDoesNotStampAuthorityOnDecidedRows`).

### 11.3 C4 + C5 + T5 — one payload builder, fail-closed thresholds, schema pinned to producers

Three defects shared a root cause: live events and reconciled events were built
by separate hand-written code, and the boot-time field schema was a third
hand-written copy.

* **Shared constructors** (`api/internal/events/payloads.go`): `NewScored`,
  `NewAnomalyDetected` and `DriftRun` are the only places these payloads are
  built. The score-writer, the aggregator, the drift poller **and** the
  reconciler all call them, so a live and a reconciled event cannot differ in
  shape (previously `registry.positive_rate` existed only on the live path).
  The duplicated drift-run merge in the poller and reconciler is now one type.
* **Fail closed on a missing threshold (C4).** The reconciler read
  `COALESCE(metrics->>'recommended_threshold', 0)`, and the live path took a
  zero-valued struct field, so a model version with no threshold made
  `score >= registry.recommended_threshold` true for every order. Now a missing
  (or non-positive) threshold makes the constructor **omit** the threshold
  fields — the rule fails closed and never fires — and the score-writer's rate
  tracker stays silent (`RateTracker.Configured`) and logs a warning at boot.
* **Schema pinned to producers (C5/T5).** The declared field set moved next to
  the constructors (`events.DeclaredFields`; the playbook compiler reads it).
  `TestDeclaredFieldsMatchWhatProducersEmit` checks both directions: a
  declared field nobody emits (this is how `anomaly.z_score` slipped through)
  and an emitted field the schema omits both fail. `anomaly.z_score` is now
  emitted for the statistical detector and deliberately absent for rate-based
  model anomalies (their DB value is NULL), so a rule on it fires for
  statistical anomalies only. `forecast_updated` (no producer yet) is the one
  documented exemption.
* `prediction.id` — the dedup scope — is always emitted by the constructor
  (`TestProducerEventsCarryThePredictionIdThatScopesDedup`).

Tests: `events/payloads_test.go`; `govern/reconcile_integration_test.go`
(`TestReconcileFailsClosedWhenRegistryHasNoThreshold`, plus a shape assertion on
the reconciled evidence); `scorewriter/ratetrack_test.go`
(`TestUnconfiguredThresholdNeverFiresAndIsReported`).

### 11.4 C3 — one open proposal per rule + entity

H2 scoped the dedup key to `prediction.id` so a re-flagged entity could propose
again — but a *redelivered* or *re-scored* order also gets a new prediction id,
so one order could sit in the queue twice (the live dev queue had 8 orders with
two pending holds each, and the key-format change itself had produced the first
duplicates when the reconciler backfilled). `Service.Propose` now also suppresses
a proposal while a **pending** one exists for the same `rule` + `entity`; once a
human decides the open one, a later genuinely new trigger proposes again, so the
"first incident vaccinates the entity forever" bug H2 fixed stays fixed. The
check and insert run in one transaction under a per-(rule, entity) advisory lock
(the live bus and the reconciler can propose concurrently), and the queue row and
its `proposed` audit row now commit together. Suppression is logged at info.
This also stops a persistently-critical model from stacking a fresh pending
retrain proposal on every drift run.

Existing duplicate pending rows are left for a human to decide — resolving them
automatically would be a governance decision without a human. Tests:
`TestOpenProposalSuppressesRepeatUntilDecided`,
`TestConcurrentProposalsForOneEntityCreateOne`.

### 11.5 C9 — strict sidecar features (train/serve skew cannot be silent)

Not a Phase 4 component, recorded here with the rest of the audit remediation.
`ml/serve.py` no longer defaults a missing feature to `0.0`: an incomplete or
non-finite vector is refused with a **400** naming the features, and the Go
client surfaces that as `predict.RejectedError` → `POST /api/v1/score` answers
400 (previously any sidecar error was a 502). The bot model's 13 feature names,
hand-mirrored between Go and Python, are now pinned to
`api/internal/scorewriter/bot_feature_spec.json` (Go: `TestBotFeaturesMatchSharedSpec`;
Python: `test_bot_feature_spec.py`), the same pattern as the fraud spec.
Tests: `ml/tests/test_serve_strict_features.py`, `predict/client_test.go`,
`TestPredictScoreIncompleteFeaturesIs400`. Value-level train/serve parity is a
separate item (audit T4).

### 11.6 T4 — train/serve parity is now a test, and it found real skew

Phase 3 claimed the stream assembler produces "the exact feature vector the batch
trainers used". Only the feature *names* were pinned. `scorewriter/parity_integration_test.go`
now replays the dataset's first 25,000 orders — loaded by the same
`store.LoadReplayOrders` and turned into events by the same `stream.NewOrderPlaced`
as the live producer (one shared loader replaced the producer's private copy and an
unused, payment-less duplicate) — through the real `FraudAssembler` + rings and
compares every feature to `gold.feature_fraud_orders`. The earliest orders are used
because the rings start empty exactly like the batch windows do. CI rebuilds the
full feature table and sets `ABI_REQUIRE_PARITY=1`, so the test cannot silently skip.
It found:

| # | Skew | Where fixed |
|---|---|---|
| 1 | **Batch windows in ~1000-second units** (pandas 3): velocity = lifetime prior orders, benchmark = lifetime category mean, account age always 0 | `ml/build_fraud_features.py` (`epoch_seconds`); see phase2 §4.3 |
| 2 | `order_hour` / `is_weekend` read in the event's zone (pgx returns the machine's local zone; wrong on any non-UTC host, e.g. this IST dev box) | assembler reads `PurchaseTime.UTC()` |
| 3 | Velocity / account age keyed by `customer_id`, which Olist mints **per order** — a repeat buyer was never seen twice; batch groups by `customer_unique_id` | `References.CustomerKey` (from `gold.fct_orders`), no event-schema change |
| 4 | Orders with no item rows: stream gave category `""` / `categories_count` 0; batch fills `'unknown'` / 1 | assembler |
| 5 | Same-timestamp ties made the batch benchmark/velocity depend on an unstable sort | batch now orders ties by `(timestamp, order_id)`, the stream's order |
| 6 | Primary category of an order with two equally priced items in different categories was arbitrary on both sides (batch `ORDER BY price DESC`, stream first-loaded item); it agreed only by coincidence of physical row order and broke after a `dbt build` reshuffled `fct_order_items` (caught by the final verification run) | batch tie-breaks by `order_item_id`; the replay loader returns items in `(order_id, order_item_id)` order |

Live tie order is arrival order and can differ from `order_id` order; that residual
is inherent to any stream and is not asserted away.

**Operational consequence (needs a decision, not done):** the *registered* fraud
model was trained on the pre-fix table, and its stream-scored predictions used the
old assembler. Re-run `make dbt ml-features train` to regenerate the feature table,
the fraud model and its `recommended_threshold` (this changes the active registry
version, artifacts, and therefore which orders the `hold-high-fraud-order` rule
proposes); model-card figures in phase2 §4.3 should then be refreshed.

### 11.7 C7 + C8 — realtime time semantics (retention, reconcile, bucket closure)

Recorded here with the rest of the audit remediation; the full explanation is in
phase1 §4.3. Replay events carry *simulated* timestamps, so wall-clock predicates on
event time are wrong: `gold.realtime_metrics` retention and the reconcile window now
use ingestion time (`updated_at`, `_loaded_at`); bucket closure/eviction use an
event-time watermark with an idle fallback; the bucket upsert is chunked. This is
why the real-DB agent eval's q13 (`live_realtime`) was failing on a dev database
whose producer wasn't running — the bucket table had been purged. Design decision
D3: the replay clock itself is **not** re-anchored (the compressed-historical-replay
design and event-time features such as velocity stay honest); only the code that
meant "how long ago did we persist this?" was fixed.

### 11.8 C10 + T6 — restart safety, for real

The "restart-safe" claims of phases 1 and 3 were partly true: state was persisted,
but not consistently with the stream position. Aggregator: the analysis cursor is
now persisted atomically with the Welford accumulators (`gold.aggregator_state`).
Score-writer: the snapshot carries per-partition processed offsets (advanced only as
events leave the reorder gate, so the position never passes a record still
buffered), offsets are committed only after the snapshot is durable
(auto-commit disabled for this consumer), redeliveries below the restored position
are skipped, the final shutdown snapshot is taken *after* the gate drains, and
`RestoreState` no longer dereferences a nil `Rings`. The wiring to a live broker
(manual commit) is exercised by the running stack rather than by an automated
broker test; the pure pieces — offset skipping, snapshot/restore round trip,
position-vs-buffered-record semantics, nil-safety — are unit-tested
(`scorewriter/restart_test.go`), and the aggregator cursor is DB-tested.

### 11.9 C6 — the banner hysteresis reaches the REST path (and a read bug it exposed)

`Surfaced` was computed in memory and only gated the SSE frame; the dashboard
calls `GET /api/v1/anomalies` on every page load and showed the newest *open* row
regardless. `gold.anomalies.surfaced` is now persisted (additive column; rows that
predate it stay visible) by both writers, and `OpenAnomalies` returns surfaced rows
only. The same test also caught a **separate defect**: `OpenAnomalies` scanned
`z_score` into a `float64`, but rate-based model anomalies store NULL there, so the
endpoint failed whenever *any* open model anomaly existed (309 on the dev DB) — the
dashboard swallows that error, so the banner just never loaded from REST. The query
now `COALESCE`s it. Tests: `store/anomalies_integration_test.go`, the two schema pin
tests. API note: `GET /api/v1/anomalies` now excludes unsurfaced model anomalies
(decision D5) and every returned row is surfaced.

### 11.10 C11 — "forecast decay" renamed: it never measured decay

The check compared the registry's WMAPE with a WAPE recomputed from the trainer's
*own persisted held-out predictions* (rows carrying an `actual`, written at training
time) — pooled across **all** model versions (on the dev DB `n=1072` = 4 versions ×
268 rows). No live ground truth ever entered it, so it could only return ≈1.0, and its
documented "1.5× bound" was never the code's (1.25× warn / 2.0× critical).
Decision D6: rename it honestly and rebuild real decay separately. Changes:

* `kind='forecast_decay'` → **`backtest_repro`**; catalog metric
  `model_forecast_decay` → **`model_backtest_reproduction`** (catalog **1.5.0**);
  dashboard/agent/doc labels updated. Rows written earlier keep the old label as
  history of the same check.
* The check now reads only the **active version's** held-out rows.
* It is **warning-capped** (`psi.reproduction_status`): it cannot establish decay, so it
  can never be `critical` and never proposes a retrain. The old test that pinned
  `>2× → critical` is replaced deliberately, not loosened to pass.
* `churn_decay` (mentioned in the schema comment and docstring) never existed and is
  no longer referred to.

**Follow-up, not done:** a genuine decay monitor needs realised outcomes for served
forecasts — e.g. join each `stream`/served weekly forecast to the actual category-week
from `gold.fct_orders` once the week closes, then compare rolling WAPE with the
trained WMAPE — and should get its own kind and thresholds.

### 11.11 Item 4 — one "valid revenue" definition (D1)

The agent's `batch_overview` defined "valid" as `NOT is_lost` (98,207 orders, AOV
160.26) while the metrics catalog, `/api/v1/summary` and the dashboard define
revenue/orders/AOV over the paying, non-lost population (98,206 orders, AOV 160.27).
Revenue was identical (15,739,137.01); one zero-payment, non-lost order made the
two AOVs disagree, so the agent could state a different AOV from the product's own
dashboard. Phase 0's catalog is canonical, so the agent was aligned to it
(`valid_orders`, `valid_revenue`, `avg_valid_order_value`, and the top-category
revenue now all use `NOT is_lost AND payment_value_total > 0`); the catalog gained
one clarifying note on `orders` and no new metric (no API shape change).
`ml/tests/test_agent_catalog_alignment.py` pins the agent's figures to the Phase 0
`gold.daily_revenue` semantic layer so they cannot drift apart again.

### 11.12 C12 + T10 — grounding that actually falsifies, and the eval's q12 flake

* **R2 restricted to the same column, made direction-aware; stale constants removed**
  (phase3 §6.3). Measured before the fix: ~7 % of arbitrary integers in [1, 3000]
  grounded on a handful of observed values; a "fell by 30" claim about a rising
  series grounded; `0.795` (the pre-retrain fraud threshold) grounded with no tool
  evidence. A second loophole surfaced while testing: *every* integer 1900–2100 was
  exempted as a "year", so `2,000 orders` was never checked; years are now exempt only
  in a year-like context. The measured false-accept rate on the same fixtures is now
  <3 % (asserted). Numbers the agent passes as tool parameters count as observed (an
  echo of the request), which is what legitimately grounds "the last 4 weeks".
* **Deliberately updated tests.** The R2 unit tests that used two *different* columns
  of one row (`{"a":10,"b":20}`, `{"top_revenue":…,"second_revenue":…}`) pinned the
  permissive behaviour and now use two rows of one column; the cross-column cases are
  asserted *not* grounded instead. This tightens the checker (it is not a loosening to
  make something pass); the eval's q21 is unchanged in meaning.
* **T10:** eval **q27** — "by how much did weekly revenue change between the two most
  recent weeks?" (a period change; q21 is between categories, q24 between models). The
  fake weekly fixture now has non-zero revenue so the derived gap is not the
  degenerate 0 − 0. Fake eval is **26/26**.
* **Root cause of the eval's "data-racy" q12 (phase4 §10.6):** `model_scores` selected
  `to_char(predicted_at …) AS predicted_at` and ordered by `predicted_at`; Postgres
  resolves that to the second-resolution *output* column, so rows within one second
  came back in arbitrary order and "the latest score" wasn't the latest. The query now
  orders by the qualified column with an `id` tie-break (same fix for the drift tool);
  `test_agent_tools_db.py` reproduces it (fails on the old query). The real-DB eval is
  stable across repeated runs.

### 11.13 D7 — the audit log is append-only in the database, not by convention

The docs called the log "immutable", but the guarantee was only that the API never
issued an UPDATE/DELETE: the application role held UPDATE/DELETE/TRUNCATE on it, and the
test suites really did `DELETE` audit rows. `api/internal/actions/schema.sql` (shared by
Go and Python) now installs a trigger that raises on any UPDATE, DELETE or TRUNCATE of
`gold.action_audit_log` unless the session has explicitly opted in with
`SET LOCAL abi.audit_maintenance = 'on'` inside a transaction. INSERT — all the service
ever does — is untouched. **Honest scope:** this stops accidental and application-level
mutation and makes deliberate mutation an explicit, logged act; it is not a security
boundary against a privileged database user (who can drop the trigger or set the flag).
A stronger posture would give the application a role with INSERT/SELECT only and keep
the owner role out of the app. The test suites use the maintenance switch for cleanup
(`auditMaintenance` in Go, a `set local` in the Python fixture). Test:
`TestAuditLogIsAppendOnlyAtTheDatabase`.

### 11.14 H2 — governance writes are transactional and no longer swallowed

`Service.execute` discarded the errors of its own status/audit writes (`_, _ =`),
`Approve`/`Reject` set the status and then inserted the audit row separately (a failed
insert left an `approved` row with no approval on the log — unexecutable and
un-re-decidable), and `Retry` read the status then wrote unconditionally, so two
concurrent retries could both run the executor. Now: `decide` (approve/reject) writes
status + audit atomically; `recordOutcome` writes the terminal status + audit row in one
transaction and its failure is logged at error and returned to the caller ("effect
applied but outcome not recorded" is reported, not hidden); `Retry`'s re-arm is
`UPDATE … WHERE status='failed'` in a transaction with its `retrying` audit row, so the
loser of a race gets `ErrInvalidState`. Test: `TestConcurrentRetriesRunTheExecutorOnce`.

### 11.15 H1 + H4 + H6 — drops, backlog and failures are now numbers

* **H1.** `events.Bus` counts what it publishes and what it drops for a full
  subscriber buffer; the server exports `abi_domain_events_published_total` and
  `abi_domain_events_dropped_total`. A drop is still harmless to the dashboard and the
  reconciler still recovers the proposal — but it is no longer silent.
* **H6.** `actions.Service.Stats` feeds gauges `abi_governance_pending_actions`,
  `abi_governance_failed_actions`, `abi_governance_stuck_approved_actions` and
  `abi_governance_reconciled_proposals` (proposals created by the backstop rather than
  the live bus), refreshed every 10 s. A failed execution is now an alertable number,
  not only a row in the history panel.
* **H4.** Reconciler passes read at most 20,000 predictions / drift rows and advance the
  cursor to the last row handled, so the first pass after deploy (cursor 0) no longer
  loads every stream-scored prediction into memory. `gold.predictions` itself still has
  **no retention policy** (131k rows on the dev DB and growing); deleting model
  evidence is a product decision and is left open — see the audit summary.
Tests: `TestDroppedDeliveriesAreCounted`, `TestStatsCountsFailedAndPendingRows`,
`TestReconcileDrainsABacklogInBoundedBatches`.

### 11.16 H3 + H5 — the monitor's edge cases

* **H3.** The retrain worker marked a request `running` before launching the trainer; a
  crash mid-training left it `running` forever with no audit outcome. The shared schema
  gained `retrain_requests.started_at` (additive); each worker cycle first fails any
  request running for more than 2 h (`reap_stale_running`) and writes an audit
  `outcome` (`interrupted: true`) so a human sees it. It does **not** re-queue: another
  training run is a governed effect, and the approval behind the first should be looked
  at first. Tests: `test_a_crashed_run_left_running_is_failed_not_forgotten`,
  `test_a_recent_running_request_is_left_alone`.
* **H5.** Stream drift sampling (`fraud_risk`, `bot_score`) took the newest N persisted
  vectors regardless of model version, so after a promotion it compared the *old*
  version's inputs with the *new* version's baseline. It is now restricted to the active
  version (`test_stream_sampling_is_restricted_to_the_active_version`).

### 11.17 T8 + T9 — hermetic real-DB eval, and a smoke test that covers Phases 3–4

* **T8.** q13 ("the most recent live replay bucket") needs replay data that only exists
  while a producer has run recently. It now declares `requires_rows` and the evaluator
  **skips it visibly** when the tool returns nothing, so the real-DB run
  (`19 passed / 0 failed / 7 skipped` on a database with no recent replay) no longer
  depends on the producer having been started in the last few hours.
* **T9.** `scripts/smoke_check.sh` asserted nothing about the governance or agent
  surfaces. It now checks `GET /api/v1/actions`, `GET /api/v1/model-drift`, that an
  approval without a reason is refused (400) and that deciding a missing action is a
  409, and that `/api/v1/agent/health` degrades cleanly without the sidecar. The script
  itself had a portability bug (`curl … | head -c 400` SIGPIPEs curl and, under
  `set -o pipefail`, aborted the run whenever SSE frames arrived quickly); fixed.
  The whole script was run end-to-end against an isolated scratch stack for this pass.

### 11.18 H7 + T7 — handler tests, and a destructive test that no longer guesses

* **H7.** The approve/reject/retry handlers had no HTTP-level tests. `httpapi` now
  depends on a small `ActionsService` interface (satisfied by `*actions.Service`), and
  `http/actions_test.go` pins the mandatory reason (400 on missing/empty, service never
  called), actor defaulting, and the error mapping (`ErrInvalidState`→409,
  `ErrUnauthorized`→403, anything else→500), bad ids/bodies→400, missing trace→404, and
  503 when unattached — all without a database.
* **T7.** `TestRealtimePipelineEndToEnd` truncates `gold.detector_state` and trims the
  shared Kafka topics. It used to fall back to the dev defaults, so a bare
  `make integration-test` would wipe a running stack's anomaly baseline; it now skips
  unless `ABI_TEST_DATABASE_URL` **and** `ABI_TEST_KAFKA` are set (CI sets both). The
  monitor suite's leaked rows and rewound watermark were fixed earlier (§11's T1 commit).

### 11.19 Bronze landing no longer loses a batch (found by the full smoke run)

The final verification run logged `bronze-writer flush failed … extended protocol
limited to 65535 parameters`: `BronzeWriter.flush` took the whole pending batch off its
queue, wrote it as one 13-parameter-per-row statement (≈5,000 rows is the ceiling; a
fetch after a restart or under backlog exceeds it) and, on **any** error, only logged —
the batch was silently discarded. Bronze is what the realtime reconcile heals from, so
that was silent data loss (the same 65,535 limit as the aggregator upsert fixed in
§11.7, in code the audit had not reached). Flush now writes chunks of ≤1,000 rows and
re-queues everything not written, in order, for the next tick. Tests:
`bronze_writer_integration_test.go` (a 6,000-row batch lands; a failed flush keeps all
rows and a retry lands them exactly once).

### 11.20 Fraud model retrained on the corrected features (2026-09-24)

Only the fraud family was retrained: churn, bot and forecast features contain no
epoch-second windows, so superseding them would have churned three unaffected versions.
Steps on the dev database: `ml/build_fraud_features.py` (full table, corrected windows),
then `ml/train_fraud.py`, which registered **`20260924.174626`** as `active` and marked
`20260924.045434` `superseded`. The sidecar manifest was regenerated. Results are in
phase2 §4.3: AUC 0.851 → 0.817, lift@5 % 19.96× → 17.14×, threshold 0.81 → **0.835**.
Parity against dev passes.

**Still to do when the stack restarts:**
* **Restart the sidecar.** It loads the manifest at boot.
* **Restart the server.** The score-writer loads thresholds at boot; until then it
  compares against 0.81.
* **Review existing pending hold proposals.** They were created by the pre-fix model,
  so the reviewer should know that. They are left for a human decision, not
  auto-rejected, because rejecting them is itself a governance decision.
* **Scope of past impact on dev:** one executed hold and 17 pending hold proposals, 8 of
  them duplicates (§11.4).

### 11.21 Policy: `gold.predictions` retention (implemented in Phase 5, docs/phase5.md §5A.4)

`gold.predictions` has no retention: 131k+ rows and growing with every stream-scored
order and session. It is model *evidence*: the Go API, the agent, the drift monitor and
the governance reconciler all read it. So the policy follows the same principle as the
audit log: **archive, never silently delete**.

| Rows | Hot retention in `gold.predictions` | Then |
|---|---|---|
| Held-out test predictions (`metadata ? 'label'`) of a registered version | while the version exists in `gold.model_registry` | archived with the version if it is ever removed |
| `metadata.source = 'stream_score'` | **30 days** by `created_at` | moved to `gold.predictions_archive` (same shape) |
| `metadata.source = 'live_score'` (REST) | **90 days** | archived |
| Any row whose `id` is cited in a still-`pending` or `failed` action's trigger evidence | kept hot until that action is decided or succeeds | archived |

Rules the job must follow when built:
1. **Move, don't delete.** Use one transaction per chunk: insert into the archive, then
   delete from hot. Chunk with the same ≤1,000-row discipline as the realtime writers.
2. **Never archive above the reconciler's cursor.** Only move rows with
   `id <= gold.governance_state['reconcile'].predictions`. Otherwise a row could vanish
   before it was ever considered for a proposal.
3. **Account for every run.** Each run writes one `gold.event_log` row (`kind='retention'`)
   with the source, id range and row count. The archive can then be reconciled against the
   hot table.
4. **Keep the drift monitor's sampling window hot.** 30 days is far more than its newest
   5,000 rows.
5. **Tell agent readers.** The agent's `model_scores` / `model_rate` tools read hot rows
   only, and their descriptions must say so once the job exists.

Implemented in Phase 5 exactly as specified (`api/internal/predict/retention.go`); see
docs/phase5.md §5A.4 for the tests.
