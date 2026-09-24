package realtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultCursorKey is the gold.aggregator_state row the server's aggregator uses.
const DefaultCursorKey = "cursor"

// SaveDetectorStateAt persists the accumulators AND the analysis cursor in one
// transaction. The two must move together: the cursor says which minutes are
// already inside the accumulators, so saving one without the other lets a
// restart either re-fold a minute (double count) or skip one (lost data).
// A zero analyzedThrough (nothing analysed yet) stores no cursor.
func SaveDetectorStateAt(ctx context.Context, pool *pgxpool.Pool, states []MetricState,
	cursorKey string, analyzedThrough time.Time) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin detector state: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, st := range states {
		if _, err := tx.Exec(ctx, `
			INSERT INTO gold.detector_state (metric, n, mean, m2, updated_at)
			VALUES ($1, $2, $3, $4, now())
			ON CONFLICT (metric) DO UPDATE SET
				n = EXCLUDED.n, mean = EXCLUDED.mean, m2 = EXCLUDED.m2, updated_at = now()`,
			st.Metric, st.N, st.Mean, st.M2); err != nil {
			return fmt.Errorf("save detector state: %w", err)
		}
	}
	if !analyzedThrough.IsZero() {
		if _, err := tx.Exec(ctx, `
			INSERT INTO gold.aggregator_state (key, value, updated_at)
			VALUES ($1, jsonb_build_object('analyzed_through', $2::timestamptz), now())
			ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`,
			cursorKey, analyzedThrough.UTC()); err != nil {
			return fmt.Errorf("save analysis cursor: %w", err)
		}
	}
	return tx.Commit(ctx)
}

// LoadAnalyzedThrough reads the analysis cursor (zero when none was saved).
func LoadAnalyzedThrough(ctx context.Context, pool *pgxpool.Pool, cursorKey string) (time.Time, error) {
	var t *time.Time
	err := pool.QueryRow(ctx, `
		SELECT (value->>'analyzed_through')::timestamptz
		FROM gold.aggregator_state WHERE key = $1`, cursorKey).Scan(&t)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && t == nil) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("load analysis cursor: %w", err)
	}
	return t.UTC(), nil
}

// SaveDetectorState upserts the online detector's accumulators so a restarted
// cmd/server can resume the anomaly baseline instead of re-warming. Called on
// every flush (a handful of rows) and implicitly on shutdown via the final
// flush.
func SaveDetectorState(ctx context.Context, pool *pgxpool.Pool, states []MetricState) error {
	if len(states) == 0 {
		return nil
	}
	for _, st := range states {
		if _, err := pool.Exec(ctx, `
			INSERT INTO gold.detector_state (metric, n, mean, m2, updated_at)
			VALUES ($1, $2, $3, $4, now())
			ON CONFLICT (metric) DO UPDATE SET
				n          = EXCLUDED.n,
				mean       = EXCLUDED.mean,
				m2         = EXCLUDED.m2,
				updated_at = now()`,
			st.Metric, st.N, st.Mean, st.M2); err != nil {
			return fmt.Errorf("save detector state: %w", err)
		}
	}
	return nil
}

// LoadDetectorState reads the last snapshot per metric (empty when none has
// been persisted yet, which is the first-start case).
func LoadDetectorState(ctx context.Context, pool *pgxpool.Pool) ([]MetricState, error) {
	rows, err := pool.Query(ctx, `
		SELECT metric, n, mean, m2
		FROM gold.detector_state
		ORDER BY metric`)
	if err != nil {
		return nil, fmt.Errorf("load detector state: %w", err)
	}
	defer rows.Close()
	var out []MetricState
	for rows.Next() {
		var st MetricState
		if err := rows.Scan(&st.Metric, &st.N, &st.Mean, &st.M2); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}
