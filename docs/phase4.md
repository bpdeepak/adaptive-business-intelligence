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
   decay are measured honestly (PSI bands ok <0.10 / warning 0.10–0.20 /
   critical >0.20; decay 1.5× trained WMAPE), and a critical finding *proposes*
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
| Human-in-the-loop default | `approval_required` is the default posture; `auto` is restricted to reversible/informational rows on an explicit allow-list (`is_converting`-adjacent notes, no revenue exposure) | "propose, don't silently act" is the invariant; the allow-list is the audited exception |
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
  Nothing in the API UPDATEs or DELETEs audit rows.

### 4.2 The execution guard (the invariant that makes "propose, don't silently act" real)

The executor contract is dependency-injected: `New(pool, bus)` builds the
registry, and `Execute` refuses to run unless the queue row has an
`approved`/`auto_approved` audit row. The integration test
(`api/internal/actions/integration_test.go`) pins this — a caller cannot invoke
an action's effect without the prior audited decision, even by calling the
registry directly.

### 4.3 API surface

| Endpoint | Shape |
|---|---|
| `GET  /api/v1/actions?status=&limit=` | `{data:[ActionRow…]}` — queue state + history |
| `POST /api/v1/actions/{id}/approve` | `{reason, actor}` → status `approved`, then auto-executes |
| `POST /api/v1/actions/{id}/reject` | `{reason, actor}` → status `rejected`, never executes |
| `GET  /api/v1/actions/{id}/trace` | `{data:{action, audit:[…]}}` — full decision trail |

Reasons are mandatory on human decisions; an approval without a reason is not an
auditable decision.

## 5. 4C — simulated action registry (`api/internal/actions/actions.go`)

The registry maps action names to executor functions; effects land on dedicated
**fake** tables so they cannot degrade the production gold surfaces:

| Action | Effect (fake table) | Notes |
|---|---|---|
| `hold_order` | `gold.hold_order_flags` (order_id UNIQUE, reason, status) | immutable `gold.fct_orders` never touched; every transition audited |
| `log_midband_bot_session` | `gold.bot_session_log` | auto tier; idempotence guard per session |
| `retrain_model` | `gold.retrain_requests` (action_id UNIQUE) | consumed by the Python worker |
| `draft_purchase_order` | `gold.purchase_orders` (`ON CONFLICT (category, forecast_week) DO NOTHING`) | dormant rule; idempotent |
| `log_retention_email` / `propose_retention_offer` | `gold.retention_actions` | 24h idempotence guard; dormant rules |

Every table is `EnsureSchema`'d at boot alongside the rest of the governance
schema; drift baselines were computed over the Phase 2 training matrices.

## 6. 4D — drift/decay monitoring & retrain-as-governed-action

### 6.1 Pipeline

```text
ml/monitor/drift_check.py              Go (api/internal/monitor)
  ├─ per active model:                   │
  │   stream PSI  ← gold.predictions     │  poller (ABI_DRIFT_POLL_EVERY, default 60s)
  │   batch PSI   ← feature marts        │    reads rows id > watermark
  │   forecast decay ← backtest vs       │    merges one event per (model, computed_at)
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
- **Hysteresis** (F4) lives in the Go rank/merge and the durable watermark: a
  run is emitted once, worst status wins, and restarts resume where the
  watermark left off — no row is replayed twice, none is lost.
- **Forecast decay**: current WAPE over the newest rolling-origin backtest
  window vs the trained `wmape` in the registry metrics; crosses the 1.5×
  boundary.

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
- `model_drift_psi`, `model_forecast_decay` — source `monitoring` (per-feature
  PSI and decay findings with their advisory posture).

### 7.2 Dashboard

The dashboard gains an **Approval queue** (approve/reject with a mandatory
reason input, refreshing every 20s), an **Action history + trace** panel (trace
opens the full audit trail), and a **Model health** board rendering
`gold.model_drift` (PSI + decay with status pills).

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
- **Live demo scenario**: `docs/phase4-demo.sql` (if present) documents the
  injected critical-drift row used to exercise the loop; rows carry
  `detail.injected=true` so the board stays honest.