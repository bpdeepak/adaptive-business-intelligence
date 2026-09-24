//go:build integration

// Retention and reconciliation are keyed on when a row was PERSISTED, not on the
// event's own timestamp (audit C7). The replay stamps events with simulated
// historical time, so an event-time predicate purged every replayed bucket on the
// first janitor tick and left reconcile with nothing to heal. These tests use
// 2015-dated buckets - as far from the wall clock as any replayed data.
//
// Postgres only (no Kafka), and they touch nothing but rows they insert.
package realtime

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func retentionPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("ABI_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://abi:abi@localhost:5432/abi?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("postgres unreachable (%v)", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("postgres unreachable (%v)", err)
	}
	if err := EnsureSchema(ctx, pool); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	return pool
}

func TestRetentionKeepsFreshlyPersistedReplayBuckets(t *testing.T) {
	pool := retentionPool(t)
	ctx := context.Background()
	fresh, stale := "2015-01-01 10:00:00+00", "2015-01-01 10:01:00+00"
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM gold.realtime_metrics WHERE bucket_start IN ($1, $2)`, fresh, stale)
	})
	if _, err := pool.Exec(ctx, `
INSERT INTO gold.realtime_metrics (bucket_start, revenue, orders, active_sessions, updated_at)
VALUES ($1, 1, 1, 1, now()), ($2, 1, 1, 1, now() - interval '5 hours')
ON CONFLICT (bucket_start) DO UPDATE SET updated_at = EXCLUDED.updated_at`, fresh, stale); err != nil {
		t.Fatalf("seed buckets: %v", err)
	}
	if err := Retention(ctx, pool, 12*time.Hour, 3*time.Hour, 720*time.Hour); err != nil {
		t.Fatalf("Retention: %v", err)
	}
	var freshN, staleN int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM gold.realtime_metrics WHERE bucket_start = $1`, fresh).Scan(&freshN)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM gold.realtime_metrics WHERE bucket_start = $1`, stale).Scan(&staleN)
	if freshN != 1 {
		t.Error("a replayed bucket persisted moments ago was purged (event-time retention)")
	}
	if staleN != 0 {
		t.Error("a bucket not written for longer than the retention window must be purged")
	}
}

func TestReconcileHealsBucketsOfHistoricalReplayEvents(t *testing.T) {
	pool := retentionPool(t)
	ctx := context.Background()
	marker := fmt.Sprintf("it-rec-%d", time.Now().UnixNano())
	recent := "2015-06-01 10:00:00+00" // ingested now
	old := "2015-06-01 11:00:00+00"    // ingested 5h ago: outside the window
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM bronze.stream_events WHERE loop_id = $1`, marker)
		_, _ = pool.Exec(ctx, `DELETE FROM gold.realtime_metrics WHERE bucket_start IN ($1, $2)`, recent, old)
	})
	insertOrder := func(id string, occurred string, value float64, hoursAgoLoaded int) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
INSERT INTO bronze.stream_events
 (event_id, schema_version, event_type, occurred_at, produced_at, key, loop_id, payload,
  _source_file, _batch_id, _loaded_at, _kafka_topic, _kafka_partition, _kafka_offset)
VALUES ($1, 1, 'order.placed', $2::timestamptz, now(), 'k', $3,
        jsonb_build_object('order_id', $1::text, 'is_lost', false, 'payment_value_total', $4::numeric),
        'stream:test', $3, now() - make_interval(hours => $5), 't', 0, 0)`,
			marker+id, occurred, marker, value, hoursAgoLoaded); err != nil {
			t.Fatalf("insert event %s: %v", id, err)
		}
	}
	insertOrder("-a", "2015-06-01 10:00:10+00", 10, 0)
	insertOrder("-b", "2015-06-01 10:00:40+00", 32.5, 0)
	insertOrder("-c", "2015-06-01 11:00:10+00", 99, 5)
	// Double-counted / stale aggregates for both buckets.
	if _, err := pool.Exec(ctx, `
INSERT INTO gold.realtime_metrics (bucket_start, revenue, orders, active_sessions, anomaly_flag)
VALUES ($1, 999, 9, 0, true), ($2, 999, 9, 0, false)
ON CONFLICT (bucket_start) DO UPDATE SET revenue = EXCLUDED.revenue, orders = EXCLUDED.orders,
    anomaly_flag = EXCLUDED.anomaly_flag`, recent, old); err != nil {
		t.Fatalf("seed metrics: %v", err)
	}

	if err := Reconcile(ctx, pool, time.Hour); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var rev float64
	var orders int
	var flag bool
	if err := pool.QueryRow(ctx, `SELECT revenue::float8, orders, anomaly_flag FROM gold.realtime_metrics WHERE bucket_start = $1`, recent).
		Scan(&rev, &orders, &flag); err != nil {
		t.Fatalf("read healed bucket: %v", err)
	}
	if rev != 42.5 || orders != 2 {
		t.Errorf("healed bucket = revenue %v orders %d, want 42.5 / 2 (bronze truth for a 2015-dated replay bucket)", rev, orders)
	}
	if !flag {
		t.Error("reconcile must preserve anomaly_flag")
	}
	if err := pool.QueryRow(ctx, `SELECT revenue::float8 FROM gold.realtime_metrics WHERE bucket_start = $1`, old).Scan(&rev); err != nil {
		t.Fatalf("read untouched bucket: %v", err)
	}
	if rev != 999 {
		t.Errorf("a bucket with no recently ingested events was rewritten (revenue %v)", rev)
	}
}
