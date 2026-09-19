package realtime

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
