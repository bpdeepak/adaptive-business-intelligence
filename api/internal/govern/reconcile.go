// Package govern is the Phase 4 governance reconciliation backstop: the job
// that re-derives playbook proposals directly from the persisted gold tables,
// so a dropped domain-bus event can never silently void a proposal.
//
// Why this exists: the in-process bus deliberately drops events for slow
// consumers (drop-slow-consumer, the same contract as the Phase 1 SSE
// broadcaster). For the dashboard that is cosmetic; for governance it is
// not — a dropped order_scored / session_scored / drift_computed event costs
// nothing visible (the prediction/drift row is fine in Postgres) and silently
// costs the proposal itself. The reconciler makes proposal delivery
// at-least-once with idempotent-on-instance effects:
//
//   - Source of truth is the DB, not the bus: scans gold.predictions
//     (source='stream_score') and gold.model_drift for rows above a persisted
//     cursor and reconstructs the exact event each row should have produced.
//   - The reconstructed event goes through the SAME decision path the live bus
//     uses (playbook.Engine.Handle) — one propose code path, conditions still
//     decide, the dedup key still makes re-proposal a no-op.
//   - Recovery is visible: a proposal created by reconciliation carries
//     payload.reconciled=true inside its persisted trigger evidence, so the
//     audit trail shows it was backfilled, not delivered live.
//   - Restart-safety: cursors live in gold.governance_state, one row per scan
//     source, written only after a fully successful pass (a pass with proposal
//     storage failures refuses to advance its cursor and retries next tick).
//
// Boundary (documented honestly): reconciliation catches drop-slow-consumer
// and restart gaps. It does not defend against Postgres being unavailable
// during its own pass — at that point the whole layer is degraded, and the
// engine logs every storage failure loudly.
package govern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"abi/internal/batchscore"
	"abi/internal/events"
	"abi/internal/playbook"
)

// EnsureSchema idempotently creates the reconciliation state table (the same
// parity pattern as monitor_state / scorewriter_state).
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS gold.governance_state (
			key        text PRIMARY KEY,
			value      jsonb NOT NULL DEFAULT '{}'::jsonb,
			updated_at timestamptz NOT NULL DEFAULT now()
		)`,
	}
	for _, q := range ddl {
		if _, err := pool.Exec(ctx, q); err != nil {
			return fmt.Errorf("ensure govern schema: %w", err)
		}
	}
	return nil
}

// Reconciler replays the persisted gold tables through the playbook engine on
// a schedule, proposing whatever a live event delivery would have proposed.
type Reconciler struct {
	pool   *pgxpool.Pool
	engine *playbook.Engine
	armed  map[events.Type]bool // triggers that have an enabled playbook rule
	log    *slog.Logger
}

// NewReconciler builds the backstop for one engine. `rules` is the same rule
// set handed to the engine; only triggers with an enabled rule are scanned.
func NewReconciler(pool *pgxpool.Pool, engine *playbook.Engine, rules []playbook.Rule, log *slog.Logger) *Reconciler {
	armed := map[events.Type]bool{}
	for _, r := range rules {
		if r.Enabled {
			armed[events.Type(r.Trigger)] = true
		}
	}
	return &Reconciler{pool: pool, engine: engine, armed: armed, log: log}
}

// Run ticks until ctx is cancelled, reconciling once per period (and once
// immediately so a fresh boot backfills anything a restart would have missed).
func (r *Reconciler) Run(ctx context.Context, every time.Duration) error {
	if err := r.Once(ctx); err != nil && r.log != nil {
		r.log.Warn("govern: initial reconcile pass failed", "error", err)
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.Once(ctx); err != nil && r.log != nil {
				r.log.Warn("govern: reconcile pass failed", "error", err)
			}
		}
	}
}

// Per-pass row caps. Without them the first pass after deploying the backstop
// (cursor 0) loaded every stream-scored prediction ever persisted into memory at
// once. A capped pass advances the cursor to the last row it processed and the
// next tick continues, so a large backlog drains over several passes instead of
// one unbounded one. Variables so a test can force tiny batches.
var (
	predictionBatch = 20000
	driftBatch      = 20000
)

// watermark is one cursor per scanned source: the max row id already fully
// processed for that source.
type watermark struct {
	Predictions int64
	Drift       int64
}

// Once runs one reconciliation pass over every source that has an armed rule.
func (r *Reconciler) Once(ctx context.Context) error {
	wm, err := r.readWatermark(ctx)
	if err != nil {
		return err
	}
	if len(r.scoredSources()) > 0 {
		evs, maxID, err := r.scanPredictions(ctx, wm.Predictions)
		if err != nil {
			return err
		}
		failures := 0
		for _, ev := range evs {
			failures += r.engine.Handle(ctx, ev)
		}
		if failures > 0 {
			// A storage failure inside Handle: keep the old cursor so the
			// next pass re-attempts the same rows (dedup keys make that a
			// no-op for the ones that did land).
			r.log.Error("govern: prediction pass had propose failures; cursor held", "failures", failures)
		} else {
			wm.Predictions = maxID
		}
	}
	if r.armed[events.TypeDriftComputed] {
		evs, maxID, err := r.scanDrift(ctx, wm.Drift)
		if err != nil {
			return err
		}
		failures := 0
		for _, ev := range evs {
			failures += r.engine.Handle(ctx, ev)
		}
		if failures > 0 {
			r.log.Error("govern: drift pass had propose failures; cursor held", "failures", failures)
		} else {
			wm.Drift = maxID
		}
	}
	return r.writeWatermark(ctx, wm)
}

// scoredSources lists the gold.predictions sources whose scored events have an
// armed rule: 'stream_score' (order/session, the Phase 3 score-writer) and
// 'churn_score' (the Phase 5 churn scorer). Only armed sources are scanned.
func (r *Reconciler) scoredSources() []string {
	var out []string
	if r.armed[events.TypeOrderScored] || r.armed[events.TypeSessionScored] {
		out = append(out, "stream_score")
	}
	if r.armed[events.TypeChurnScored] {
		out = append(out, "churn_score")
	}
	if r.armed[events.TypeForecastUpdated] {
		out = append(out, "forecast_score")
	}
	return out
}

// scoredType maps a persisted prediction back to the event its producer published.
func scoredType(source, grain string) events.Type {
	switch {
	case source == "churn_score":
		return events.TypeChurnScored
	case grain == "order":
		return events.TypeOrderScored
	default:
		return events.TypeSessionScored
	}
}

// scanPredictions reconstructs order/session scored events from stream_score
// prediction rows above the cursor — the same payload shape the score-writer
// publishes (plus the reconciled marker), with the registry's recommended
// threshold resolved per exact model version.
func (r *Reconciler) scanPredictions(ctx context.Context, cursor int64) ([]events.Event, int64, error) {
	// Preload registry thresholds per (model, version) so the reconstructed
	// event carries the same recommended_threshold the rule compares against.
	// The threshold lives in model_registry.metrics (written by the training
	// pipeline as backtest.recommended_threshold), NOT params — the same
	// column the live score-writer's rate trackers and the agent SQL read.
	//
	// A version with no (or a non-positive) threshold gets NO threshold fields:
	// the rule then fails closed and never fires. It must never default to 0,
	// which would make `score >= registry.recommended_threshold` true for every
	// prediction and flood the queue with holds.
	type threshKey struct{ model, version string }
	type registryCfg struct{ threshold, positiveRate *float64 }
	cfgs := map[threshKey]registryCfg{}
	tr, err := r.pool.Query(ctx, `
SELECT model_name, model_version,
       (metrics->>'recommended_threshold')::float8,
       (metrics->>'positive_rate')::float8
FROM gold.model_registry`)
	if err != nil {
		return nil, cursor, fmt.Errorf("query model_registry: %w", err)
	}
	for tr.Next() {
		var m, v string
		var thr, pos *float64
		if err := tr.Scan(&m, &v, &thr, &pos); err != nil {
			tr.Close()
			return nil, cursor, fmt.Errorf("scan model_registry: %w", err)
		}
		cfg := registryCfg{}
		if thr != nil && *thr > 0 {
			cfg.threshold = thr
			// Same floored baseline the live score-writer publishes.
			base := events.BaselineRate(0)
			if pos != nil {
				base = events.BaselineRate(*pos)
			}
			cfg.positiveRate = &base
		}
		cfgs[threshKey{m, v}] = cfg
	}
	tr.Close()
	if err := tr.Err(); err != nil {
		return nil, cursor, err
	}

	pr, err := r.pool.Query(ctx, `
SELECT id, model_name, model_version, grain, entity_id, prediction, confidence, predicted_at,
       metadata->>'source',
       COALESCE(metadata->>'category', ''), COALESCE(metadata->>'week', ''),
       COALESCE((metadata->>'recent_avg')::float8, 0)
FROM gold.predictions
WHERE id > $1 AND metadata->>'source' = ANY($3)
ORDER BY id
LIMIT $2`, cursor, predictionBatch, r.scoredSources())
	if err != nil {
		return nil, cursor, fmt.Errorf("query predictions: %w", err)
	}
	defer pr.Close()

	var out []events.Event
	churnByVersion := map[string][]int64{}
	maxID := cursor
	for pr.Next() {
		var id int64
		var model, version, grain, entity, source, fcCategory, fcWeek string
		var score, conf, fcRecent float64
		var at time.Time
		if err := pr.Scan(&id, &model, &version, &grain, &entity, &score, &conf, &at, &source,
			&fcCategory, &fcWeek, &fcRecent); err != nil {
			return nil, cursor, fmt.Errorf("scan prediction: %w", err)
		}
		if id > maxID {
			maxID = id
		}
		if source == "forecast_score" {
			// The forecast worker's event, rebuilt from what it persisted.
			out = append(out, events.NewForecastUpdated(events.Forecast{
				Category: fcCategory, Week: fcWeek, PointEstimate: score, RecentAvg: fcRecent,
				Reconciled: true,
			}, at.UTC()))
			continue
		}
		evType := scoredType(source, grain)
		if evType == events.TypeChurnScored {
			churnByVersion[version] = append(churnByVersion[version], id)
		}
		cfg := cfgs[threshKey{model, version}]
		// Built by the same constructor the live score-writer uses, so the two
		// paths cannot differ in shape. Reconciled=true is the recovery marker: it
		// rides inside the trigger evidence the queue row persists, so the trace
		// shows the proposal was backfilled, not delivered live.
		out = append(out, events.NewScored(evType, events.Scored{
			Model: model, Version: version, Entity: entity,
			Score: score, Confidence: conf, PredictionID: id,
			Threshold: cfg.threshold, PositiveRate: cfg.positiveRate,
			Reconciled: true,
		}, at.UTC()))
	}
	if err := pr.Err(); err != nil {
		return nil, cursor, err
	}
	pr.Close()
	// Churn events carry the batch rank; rebuild it with the scorer's own
	// definition so a reconciled event matches the dropped live one.
	if len(churnByVersion) > 0 {
		rank := map[int64]int{}
		for version, ids := range churnByVersion {
			ranked, err := batchscore.RankedChurnScores(ctx, r.pool, version, ids)
			if err != nil {
				return nil, cursor, err
			}
			for _, rs := range ranked {
				rank[rs.ID] = rs.Rank
			}
		}
		for i := range out {
			if out[i].Type != events.TypeChurnScored {
				continue
			}
			pred, _ := out[i].Payload["prediction"].(map[string]any)
			if id, ok := pred["id"].(int64); ok && rank[id] > 0 {
				pred["rank"] = rank[id]
			}
		}
	}
	return out, maxID, nil
}

// scanDrift reconstructs drift_computed events from gold.model_drift rows
// above the cursor, merged per (model, computed_at) by the same events.DriftRun
// the drift poller uses, so the reconstructed event and its dedup key match
// exactly what the live path produced.
func (r *Reconciler) scanDrift(ctx context.Context, cursor int64) ([]events.Event, int64, error) {
	rows, err := r.pool.Query(ctx, `
SELECT id, model_name, model_version, feature, psi, status, kind, computed_at
FROM gold.model_drift WHERE id > $1 ORDER BY id LIMIT $2`, cursor, driftBatch)
	if err != nil {
		return nil, cursor, fmt.Errorf("query model_drift: %w", err)
	}
	defer rows.Close()

	runs := map[string]*events.DriftRun{}
	maxID := cursor
	for rows.Next() {
		var id int64
		var model, version, feature, status, kind string
		var psi float64
		var at time.Time
		if err := rows.Scan(&id, &model, &version, &feature, &psi, &status, &kind, &at); err != nil {
			return nil, cursor, fmt.Errorf("scan model_drift: %w", err)
		}
		if id > maxID {
			maxID = id
		}
		computedAt := at.UTC().Format("2006-01-02T15:04:05Z")
		key := model + "|" + computedAt
		run, ok := runs[key]
		if !ok {
			run = &events.DriftRun{Model: model, Version: version, ComputedAt: computedAt}
			runs[key] = run
		}
		run.Add(feature, psi, status, kind)
	}
	if err := rows.Err(); err != nil {
		return nil, cursor, err
	}

	var out []events.Event
	for _, run := range runs {
		out = append(out, run.Event(true, time.Now().UTC()))
		if r.log != nil {
			r.log.Info("govern: drift run reconstructed", "model", run.Model, "computed_at", run.ComputedAt)
		}
	}
	return out, maxID, nil
}

func (r *Reconciler) readWatermark(ctx context.Context) (watermark, error) {
	var wm watermark
	err := r.pool.QueryRow(ctx, `
SELECT COALESCE((value->>'predictions')::bigint, 0), COALESCE((value->>'drift')::bigint, 0)
FROM gold.governance_state WHERE key = 'reconcile'`).Scan(&wm.Predictions, &wm.Drift)
	if errors.Is(err, pgx.ErrNoRows) {
		// Fresh state: never reconciled before — start from the beginning
		// (the first pass is the honest backfill).
		return wm, nil
	}
	if err != nil {
		return wm, fmt.Errorf("read govern watermark: %w", err)
	}
	return wm, nil
}

func (r *Reconciler) writeWatermark(ctx context.Context, wm watermark) error {
	_, err := r.pool.Exec(ctx, `
INSERT INTO gold.governance_state (key, value, updated_at)
VALUES ('reconcile', $1::jsonb, now())
ON CONFLICT (key) DO UPDATE SET value = $1::jsonb, updated_at = now()`,
		fmt.Sprintf(`{"predictions": %d, "drift": %d}`, wm.Predictions, wm.Drift))
	if err != nil {
		return fmt.Errorf("write govern watermark: %w", err)
	}
	return nil
}
