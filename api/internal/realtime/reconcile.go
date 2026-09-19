package realtime

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Reconcile recomputes the gold.realtime_metrics buckets in `window` directly
// from bronze.stream_events — the idempotent, event_id-deduped landing layer —
// and overwrites the running sums.
//
// Why this exists: the aggregator's in-memory buckets are exactly-once per
// process (each order id is tracked in a set, so broker redelivery within one
// lifetime is ignored), but under at-least-once delivery a consumer restart
// before its offset commits re-reads a batch and folds those events into the
// bucket sums *again*. The bronze layer stays correct (ON CONFLICT (event_id)
// DO NOTHING dedupes redelivery); the running aggregate does not. This periodic
// pass clamps the served table back to bronze's deduplicated truth, so a
// restart that double-counted revenue/orders self-heals within one reconcile
// interval.
//
// The recomputation mirrors the aggregator's exact semantics — minute buckets
// truncated in UTC, the Phase-0 revenue filter (is_lost = false and
// payment_value_total > 0), and distinct orders/sessions per bucket — so the
// corrected rows are interchangeable with what the hot path would have
// produced. anomaly_flag is preserved on conflict: detection results are not
// silently unmarked by a correction pass (anomaly lifecycle lives in
// gold.anomalies).
func Reconcile(ctx context.Context, pool *pgxpool.Pool, window time.Duration) error {
	sql := `
WITH buckets AS (
	SELECT DISTINCT to_timestamp(floor(extract(epoch FROM occurred_at) / 60) * 60) AS bucket_start
	FROM bronze.stream_events
	WHERE occurred_at >= now() - make_interval(secs => $1)
),
orders AS (
	SELECT to_timestamp(floor(extract(epoch FROM occurred_at) / 60) * 60) AS bucket_start,
	       COUNT(DISTINCT payload->>'order_id') FILTER (
	           WHERE NOT coalesce((payload->>'is_lost')::boolean, true)
	             AND coalesce((payload->>'payment_value_total')::numeric, 0) > 0) AS n,
	       COALESCE(SUM((payload->>'payment_value_total')::numeric) FILTER (
	           WHERE NOT coalesce((payload->>'is_lost')::boolean, true)
	             AND coalesce((payload->>'payment_value_total')::numeric, 0) > 0), 0) AS revenue
	FROM bronze.stream_events
	WHERE event_type = 'order.placed'
	  AND occurred_at >= now() - make_interval(secs => $1)
	GROUP BY 1
),
sessions AS (
	SELECT to_timestamp(floor(extract(epoch FROM occurred_at) / 60) * 60) AS bucket_start,
	       COUNT(DISTINCT payload->>'session_id') AS sessions
	FROM bronze.stream_events
	WHERE event_type IN ('page.view','cart.abandoned')
	  AND payload->>'session_id' IS NOT NULL
	  AND payload->>'session_id' <> ''
	  AND occurred_at >= now() - make_interval(secs => $1)
	GROUP BY 1
),
combined AS (
	SELECT b.bucket_start,
	       COALESCE(o.revenue, 0)::numeric(12,2) AS revenue,
	       COALESCE(o.n, 0)::int                 AS orders,
	       COALESCE(s.sessions, 0)::int          AS active_sessions
	FROM buckets b
	LEFT JOIN orders   o USING (bucket_start)
	LEFT JOIN sessions s USING (bucket_start)
)
INSERT INTO gold.realtime_metrics
	(bucket_start, revenue, orders, active_sessions, anomaly_flag, updated_at)
SELECT c.bucket_start, c.revenue, c.orders, c.active_sessions,
       COALESCE(rt.anomaly_flag, false),
       now()
FROM combined c
LEFT JOIN gold.realtime_metrics rt ON rt.bucket_start = c.bucket_start
ON CONFLICT (bucket_start) DO UPDATE
	SET revenue        = EXCLUDED.revenue,
	    orders         = EXCLUDED.orders,
	    active_sessions = EXCLUDED.active_sessions,
	    updated_at     = now()`
	if _, err := pool.Exec(ctx, sql, window.Seconds()); err != nil {
		return fmt.Errorf("reconcile realtime metrics: %w", err)
	}
	return nil
}
