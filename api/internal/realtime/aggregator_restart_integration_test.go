//go:build integration

// Aggregator restart-safety (audit C10a). The Welford accumulators were saved
// after every flush, but the analysis cursor (which minutes are already folded
// into them) lived only in memory. After a crash the broker redelivers up to a
// commit interval of records - at 2880x that is hours of simulated minutes - and
// each one was analysed again and folded into the restored baseline a second
// time. The cursor is now persisted in the same transaction as the accumulators.
//
// Uses a unique metric name and cursor key so it can never touch a live
// baseline, and 2014-dated buckets so it can never touch live rows.
package realtime

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"
)

func TestRestartRestoresBaselineAndAnalysisCursorTogether(t *testing.T) {
	pool := retentionPool(t)
	ctx := context.Background()
	uniq := time.Now().UnixNano()
	metric := fmt.Sprintf("it_metric_%d", uniq)
	key := fmt.Sprintf("it_cursor_%d", uniq)
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM gold.detector_state WHERE metric = $1`, metric)
		_, _ = pool.Exec(ctx, `DELETE FROM gold.aggregator_state WHERE key = $1`, key)
	})

	through := time.Date(2014, 3, 1, 12, 30, 0, 0, time.UTC)
	a1 := &Aggregator{pool: pool, log: slog.Default(), detector: NewDetector(),
		flushEvery: 5 * time.Second, buckets: map[time.Time]*bucket{}, cursorKey: key}
	for i := 0; i < 30; i++ {
		a1.detector.Evaluate(metric, float64(10+i%3))
	}
	a1.lastAnalyzed = through
	a1.saveBaseline(ctx)

	// "Restart": a brand-new process restores from the database.
	a2 := &Aggregator{pool: pool, log: slog.Default(), detector: NewDetector(),
		flushEvery: 5 * time.Second, buckets: map[time.Time]*bucket{}, cursorKey: key}
	if err := a2.restoreBaseline(ctx); err != nil {
		t.Fatalf("restoreBaseline: %v", err)
	}
	if !a2.lastAnalyzed.Equal(through) || !a2.restoredThrough.Equal(through) {
		t.Errorf("cursor = %v / %v, want %v: without it every redelivered minute is folded in again",
			a2.lastAnalyzed, a2.restoredThrough, through)
	}
	var n int64
	for _, st := range a2.detector.Snapshot() {
		if st.Metric == metric {
			n = st.N
		}
	}
	if n != 30 {
		t.Errorf("restored baseline n = %d, want 30", n)
	}
	// The restored cursor makes the redelivered, already-folded minutes ineligible.
	a2.watermark = through.Add(5 * time.Minute)
	a2.lastIngest = time.Now()
	if a2.closedLocked(through.Add(-time.Minute), time.Now()) && through.Add(-time.Minute).After(a2.lastAnalyzed) {
		t.Error("a minute before the cursor must never be selected for analysis again")
	}
}

func TestRedeliveredPartialBucketDoesNotOverwriteAFinishedRow(t *testing.T) {
	pool := retentionPool(t)
	ctx := context.Background()
	finished := time.Date(2014, 3, 1, 12, 0, 0, 0, time.UTC)
	fresh := finished.Add(10 * time.Minute)
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM gold.realtime_metrics WHERE bucket_start IN ($1, $2)`, finished, fresh)
	})
	if _, err := pool.Exec(ctx, `
INSERT INTO gold.realtime_metrics (bucket_start, revenue, orders, active_sessions)
VALUES ($1, 500, 20, 4) ON CONFLICT (bucket_start) DO UPDATE SET revenue = 500, orders = 20`, finished); err != nil {
		t.Fatalf("seed finished bucket: %v", err)
	}

	a := &Aggregator{pool: pool, log: slog.Default(), detector: NewDetector(), flushEvery: 5 * time.Second,
		buckets: map[time.Time]*bucket{}, restoredThrough: finished.Add(time.Minute)}
	a.observe(finished, "o1", 10)      // the redelivered fraction of an already-finished minute
	a.observe(fresh, "o2", 7)          // genuinely new data after the cursor
	a.advanceLocked(fresh)             // watermark
	if err := a.persistBuckets(ctx, []time.Time{finished, fresh}); err != nil {
		t.Fatalf("persistBuckets: %v", err)
	}

	var rev float64
	if err := pool.QueryRow(ctx, `SELECT revenue::float8 FROM gold.realtime_metrics WHERE bucket_start = $1`, finished).Scan(&rev); err != nil {
		t.Fatal(err)
	}
	if rev != 500 {
		t.Errorf("finished bucket revenue = %v, want 500: a partial redelivery overwrote it", rev)
	}
	if err := pool.QueryRow(ctx, `SELECT revenue::float8 FROM gold.realtime_metrics WHERE bucket_start = $1`, fresh).Scan(&rev); err != nil || rev != 7 {
		t.Errorf("post-cursor bucket = %v (err %v), want 7", rev, err)
	}
}
