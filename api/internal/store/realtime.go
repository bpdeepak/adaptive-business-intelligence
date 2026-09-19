package store

import (
	"context"
	"errors"
	"fmt"

	"abi/internal/model"
)

// RecentRealtimeMetrics returns the most recent `n` one-minute buckets in
// ascending time order — the snapshot a live subscriber starts from.
func (s *PostgresStore) RecentRealtimeMetrics(ctx context.Context, n int) ([]model.RealtimeBucket, error) {
	if n < 1 {
		n = 1
	}
	rows, err := s.pool.Query(ctx, `
SELECT to_char(bucket_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
       revenue::float8, orders::int8, active_sessions::int8, anomaly_flag,
       to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
FROM gold.realtime_metrics
ORDER BY bucket_start DESC
LIMIT $1`, n)
	if err != nil {
		return nil, fmt.Errorf("recent realtime metrics: %w", err)
	}
	defer rows.Close()

	var desc []model.RealtimeBucket
	for rows.Next() {
		var b model.RealtimeBucket
		if err := rows.Scan(&b.BucketStart, &b.Revenue, &b.Orders, &b.ActiveSessions, &b.AnomalyFlag, &b.UpdatedAt); err != nil {
			return nil, fmt.Errorf("recent realtime metrics scan: %w", err)
		}
		desc = append(desc, b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Return ascending so the dashboard can render left→right.
	for i, j := 0, len(desc)-1; i < j; i, j = i+1, j-1 {
		desc[i], desc[j] = desc[j], desc[i]
	}
	return desc, nil
}

// OpenAnomalies returns undismissed anomaly rows, newest first.
func (s *PostgresStore) OpenAnomalies(ctx context.Context) ([]model.Anomaly, error) {
	rows, err := s.pool.Query(ctx, `
SELECT id, metric,
       to_char(bucket_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
       observed, expected, z_score, severity, status,
       to_char(detected_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
FROM gold.anomalies
WHERE status = 'open'
ORDER BY detected_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("open anomalies: %w", err)
	}
	defer rows.Close()

	var out []model.Anomaly
	for rows.Next() {
		var a model.Anomaly
		if err := rows.Scan(&a.ID, &a.Metric, &a.BucketStart, &a.Observed, &a.Expected,
			&a.ZScore, &a.Severity, &a.Status, &a.DetectedAt); err != nil {
			return nil, fmt.Errorf("open anomalies scan: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// DismissAnomaly marks an open anomaly as dismissed (the dashboard banner
// "undismissed" row disappears). It errors if the row is missing or no longer
// open.
func (s *PostgresStore) DismissAnomaly(ctx context.Context, id int64) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE gold.anomalies SET status = 'dismissed', dismissed_at = now() WHERE id = $1 AND status = 'open'`, id)
	if err != nil {
		return fmt.Errorf("dismiss anomaly: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return errors.New("anomaly not found or not open")
	}
	return nil
}
