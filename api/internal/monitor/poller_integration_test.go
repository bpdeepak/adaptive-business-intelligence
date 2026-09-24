//go:build integration

// Integration tests for the Phase 4 drift poller against the live Postgres
// stack. They pin the behavior the governance loop relies on:
//
//  1. Bootstrap: with no watermark row yet (fresh DB), polling works — every
//     later event stream depends on the first poll succeeding.
//  2. One event per (model, computed_at) run, worst status wins.
//  3. The watermark advances so a row is never emitted twice and restarts
//     resume where they left off.
//
// Skipped when Postgres is unreachable, like the other suites.
package monitor

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"abi/internal/events"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("ABI_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://abi:abi@localhost:5432/abi?sslmode=disable"
	}
	probeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(probeCtx, dsn)
	if err != nil {
		t.Skipf("postgres unreachable (%v) — skipping monitor integration tests", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(probeCtx); err != nil {
		t.Skipf("postgres unreachable (%v) — skipping monitor integration tests", err)
	}
	if err := EnsureSchema(probeCtx, pool); err != nil {
		t.Fatalf("ensure monitor schema: %v", err)
	}
	// model_drift lives in the predict schema; ensure it here for isolation.
	if _, err := pool.Exec(probeCtx, `
CREATE TABLE IF NOT EXISTS gold.model_drift (
	id bigserial PRIMARY KEY,
	model_name text NOT NULL,
	model_version text NOT NULL,
	computed_at timestamptz NOT NULL DEFAULT now(),
	feature text NOT NULL,
	psi double precision NOT NULL,
	status text NOT NULL,
	kind text NOT NULL DEFAULT 'psi',
	detail jsonb NOT NULL DEFAULT '{}'::jsonb
)`); err != nil {
		t.Fatalf("ensure model_drift: %v", err)
	}
	return pool
}

// uniqueModel derives a model name for this test run so live traffic can never
// collide with the test rows.
func uniqueModel(t *testing.T) string {
	return "it_monitor_" + t.Name()
}

func resetWatermark(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `DELETE FROM gold.monitor_state WHERE key = 'drift'`); err != nil {
		t.Fatalf("reset watermark: %v", err)
	}
}

func insertDrift(t *testing.T, pool *pgxpool.Pool, model, feature, status string, computedAt time.Time) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
INSERT INTO gold.model_drift (model_name, model_version, computed_at, feature, psi, status, kind)
VALUES ($1, 'v-test', $2, $3, 0.2, $4, 'psi')`, model, computedAt, feature, status)
	if err != nil {
		t.Fatalf("insert drift: %v", err)
	}
}

func pollEvents(t *testing.T, pool *pgxpool.Pool) []events.Event {
	t.Helper()
	bus := events.New()
	// Subscribe BEFORE polling: the bus does not replay events published
	// before a subscriber attaches.
	ch, unsub := bus.Subscribe()
	defer unsub()
	p := NewPoller(pool, bus, func(string, ...any) {})
	if err := p.poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	var got []events.Event
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return got
			}
			got = append(got, ev)
		case <-time.After(500 * time.Millisecond):
			return got
		}
	}
}

func TestPollerBootstrapsAndMergesRuns(t *testing.T) {
	pool := testPool(t)
	resetWatermark(t, pool)
	model := uniqueModel(t)

	// Bootstrap: drain whatever un-polled rows exist (on a fresh DB this is
	// nothing; against live data the poller must happily consume them too),
	// then the table is quiet and the watermark row exists.
	var got []events.Event
	for {
		if got = pollEvents(t, pool); len(got) == 0 {
			break
		}
	}

	// One run (same computed_at) with ok + warning + critical rows -> ONE
	// event, worst status wins, all four features reported.
	run := time.Now().UTC().Add(-time.Minute)
	insertDrift(t, pool, model, "feat_a", "ok", run)
	insertDrift(t, pool, model, "feat_b", "warning", run)
	insertDrift(t, pool, model, "feat_c", "critical", run)
	insertDrift(t, pool, model, "feat_d", "ok", run)
	got = pollEvents(t, pool)
	if len(got) != 1 {
		t.Fatalf("expected 1 merged event, got %d", len(got))
	}
	drift, _ := got[0].Payload["drift"].(map[string]any)
	if drift["status"] != "critical" {
		t.Fatalf("worst-status merge: got %v, want critical", drift["status"])
	}
	if drift["feature_count"].(int) != 4 {
		t.Fatalf("feature_count = %v, want 4", drift["feature_count"])
	}
	if drift["model"] != model {
		t.Fatalf("model = %v, want %s", drift["model"], model)
	}

	// A later different run -> a second event; then nothing more (watermark).
	insertDrift(t, pool, model, "feat_x", "ok", time.Now().UTC().Add(time.Minute))
	got = pollEvents(t, pool)
	if len(got) != 1 || got[0].Payload["drift"].(map[string]any)["status"] != "ok" {
		t.Fatalf("expected 1 ok event for the new run, got %d", len(got))
	}
	got = pollEvents(t, pool)
	if len(got) != 0 {
		t.Fatalf("expected no re-emitted events after watermark advance, got %d", len(got))
	}
}

func TestPollerResumesAfterRestart(t *testing.T) {
	pool := testPool(t)
	resetWatermark(t, pool)
	model := uniqueModel(t)

	insertDrift(t, pool, model, "feat_a", "ok", time.Now().UTC().Add(-time.Minute))
	bus := events.New()
	p := NewPoller(pool, bus, func(string, ...any) {})
	if err := p.poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}

	// Second poller (simulated restart) must not re-emit the consumed row.
	got := pollEvents(t, pool)
	if len(got) != 0 {
		t.Fatalf("restart re-emitted %d consumed events", len(got))
	}
}