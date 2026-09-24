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
	if r.armed[events.TypeOrderScored] || r.armed[events.TypeSessionScored] {
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

// scanPredictions reconstructs order/session scored events from stream_score
// prediction rows above the cursor — the same payload shape the score-writer
// publishes (plus the reconciled marker), with the registry's recommended
// threshold resolved per exact model version.
func (r *Reconciler) scanPredictions(ctx context.Context, cursor int64) ([]events.Event, int64, error) {
	// Preload registry thresholds per (model, version) so the reconstructed
	// event carries the same recommended_threshold the rule compares against.
	type threshKey struct{ model, version string }
	thresh := map[threshKey]float64{}
	tr, err := r.pool.Query(ctx, `
SELECT model_name, model_version, COALESCE((params->>'recommended_threshold')::float8, 0)
FROM gold.model_registry`)
	if err != nil {
		return nil, cursor, fmt.Errorf("query model_registry: %w", err)
	}
	for tr.Next() {
		var m, v string
		var t float64
		if err := tr.Scan(&m, &v, &t); err != nil {
			tr.Close()
			return nil, cursor, fmt.Errorf("scan model_registry: %w", err)
		}
		thresh[threshKey{m, v}] = t
	}
	tr.Close()
	if err := tr.Err(); err != nil {
		return nil, cursor, err
	}

	pr, err := r.pool.Query(ctx, `
SELECT id, model_name, model_version, grain, entity_id, prediction, confidence, predicted_at
FROM gold.predictions
WHERE id > $1 AND metadata->>'source' = 'stream_score'
ORDER BY id`, cursor)
	if err != nil {
		return nil, cursor, fmt.Errorf("query predictions: %w", err)
	}
	defer pr.Close()

	var out []events.Event
	maxID := cursor
	for pr.Next() {
		var id int64
		var model, version, grain, entity string
		var score, conf float64
		var at time.Time
		if err := pr.Scan(&id, &model, &version, &grain, &entity, &score, &conf, &at); err != nil {
			return nil, cursor, fmt.Errorf("scan prediction: %w", err)
		}
		if id > maxID {
			maxID = id
		}
		evType := events.TypeSessionScored
		if grain == "order" {
			evType = events.TypeOrderScored
		}
		payload := map[string]any{
			"prediction": map[string]any{
				"model":         model,
				"model_version": version,
				"entity_id":     entity,
				"score":         score,
				"confidence":    conf,
				"id":            id,
			},
			// Recovery marker: this proposal was backfilled from the persisted
			// row, not delivered on the live bus. It rides inside the trigger
			// evidence the queue row persists, so the trace shows it.
			"reconciled": true,
		}
		if t, ok := thresh[threshKey{model, version}]; ok {
			// The registry carries the threshold; otherwise the field is
			// omitted and the rule fails closed on it (never fires without a
			// threshold to compare against).
			payload["prediction"].(map[string]any)["threshold"] = t
			payload["registry"] = map[string]any{"recommended_threshold": t}
		}
		out = append(out, events.Event{Type: evType, At: at.UTC(), Payload: payload})
	}
	if err := pr.Err(); err != nil {
		return nil, cursor, err
	}
	return out, maxID, nil
}

// driftRun is one (model, computed_at) measurement run, worst status wins —
// the same merge the drift poller performs, so the reconstructed event and the
// dedup keys match exactly what the live path produced.
type driftRun struct {
	model       string
	version     string
	computedAt  string
	worst       int
	worstStatus string
	maxPSI      float64
	features    []map[string]any
}

// scanDrift reconstructs drift_computed events from gold.model_drift rows
// above the cursor, merged per (model, computed_at) exactly like the poller.
func (r *Reconciler) scanDrift(ctx context.Context, cursor int64) ([]events.Event, int64, error) {
	rows, err := r.pool.Query(ctx, `
SELECT id, model_name, model_version, feature, psi, status, kind, computed_at
FROM gold.model_drift WHERE id > $1 ORDER BY id`, cursor)
	if err != nil {
		return nil, cursor, fmt.Errorf("query model_drift: %w", err)
	}
	defer rows.Close()

	runs := map[string]*driftRun{}
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
			run = &driftRun{model: model, version: version, computedAt: computedAt, worstStatus: "ok"}
			runs[key] = run
		}
		run.features = append(run.features, map[string]any{
			"feature": feature, "psi": psi, "status": status, "kind": kind,
		})
		if s := driftRank(status); s > run.worst {
			run.worst, run.worstStatus = s, status
		}
		if psi > run.maxPSI {
			run.maxPSI = psi
		}
	}
	if err := rows.Err(); err != nil {
		return nil, cursor, err
	}

	var out []events.Event
	for _, run := range runs {
		out = append(out, events.Event{
			Type: events.TypeDriftComputed,
			At:   time.Now().UTC(),
			Payload: map[string]any{
				"drift": map[string]any{
					"model":         run.model,
					"model_version": run.version,
					"status":        run.worstStatus,
					"psi":           run.maxPSI,
					"feature_count": len(run.features),
					"features":      run.features,
					"computed_at":   run.computedAt,
				},
				"reconciled": true,
			},
		})
		if r.log != nil {
			r.log.Info("govern: drift run reconstructed",
				"model", run.model, "status", run.worstStatus, "computed_at", run.computedAt)
		}
	}
	return out, maxID, nil
}

// driftRank mirrors the poller's worst-takes-all status ordering.
func driftRank(status string) int {
	switch status {
	case "critical":
		return 2
	case "warning":
		return 1
	default:
		return 0
	}
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
