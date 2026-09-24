//go:build integration

// Integration tests for the Phase 4 governance reconciliation backstop
// against the live Postgres stack. They pin the property the reviewer flagged:
// a proposal must be derived from the persisted gold rows, so a dropped
// domain-bus event can never silently void it.
//
//  1. Backfill: prediction + drift rows inserted directly into the DB with NO
//     bus event published are proposed by One One pass, with the recovery
//     marker (payload.reconciled=true) on the persisted trigger evidence.
//  2. Conditions still decide: below-threshold rows produce nothing.
//  3. Idempotent: a second pass proposes nothing new (dedup keys + cursor).
//  4. Cursor advance: a fresh row after the first pass is proposed on the next.
//  5. The auto-tier path completes end-to-end from a reconstructed event.
//
// Skipped when Postgres is unreachable, like the other suites.
package govern

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"abi/internal/actions"
	"abi/internal/events"
	"abi/internal/playbook"
	"abi/internal/predict"
)

func governTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("ABI_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://abi:abi@localhost:5432/abi?sslmode=disable"
	}
	probeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(probeCtx, dsn)
	if err != nil {
		t.Skipf("postgres unreachable (%v) — skipping govern integration tests", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(probeCtx); err != nil {
		t.Skipf("postgres unreachable (%v) — skipping govern integration tests", err)
	}
	for name, fn := range map[string]func(context.Context, *pgxpool.Pool) error{
		"govern":  EnsureSchema,
		"predict": predict.EnsureSchema,
		"actions": actions.EnsureSchema,
	} {
		if err := fn(probeCtx, pool); err != nil {
			t.Fatalf("ensure %s schema: %v", name, err)
		}
	}
	return pool
}

// governRules mirrors the three armed rules of config/playbooks.yml so the
// test exercises the shipped policy, not a bespoke copy.
func governRules() []playbook.Rule {
	return []playbook.Rule{
		{Name: "hold-high-fraud-order", Trigger: "order_scored",
			Condition: "prediction.model == 'fraud_risk' && prediction.score >= registry.recommended_threshold",
			Action:    "hold_order_for_review", RiskTier: "approval_required", Enabled: true},
		{Name: "log-midband-bot-session", Trigger: "session_scored",
			Condition: "prediction.model == 'bot_score' && prediction.score >= 0.5 && prediction.score < 0.7",
			Action:    "log_event_note", RiskTier: "auto", Enabled: true,
			Params: map[string]any{"kind": "bot_midband"}},
		{Name: "retrain-on-critical-drift", Trigger: "drift_computed",
			Condition: "drift.status == 'critical'", Action: "retrain_model",
			RiskTier: "approval_required", Enabled: true},
	}
}

func TestReconcileBackfillsProposalsFromPersistedRows(t *testing.T) {
	pool := governTestPool(t)
	ctx := context.Background()

	uniq := fmt.Sprintf("%d", time.Now().UnixNano())
	tg := "tg" + uniq              // entity prefix for every row this test owns
	driftModel := "it_gov_" + uniq // unique drift model name (no rule pins the model)
	ver := "it_gov_v1"

	// The fraud/bot rules pin the model name, so predictions must use the real
	// model names — but a unique version keeps this run isolated from live
	// registry rows. Registry threshold: 0.80.
	saveWatermark(t, pool)
	if _, err := pool.Exec(ctx, `
INSERT INTO gold.model_registry
  (model_name, model_version, status, framework, task, grain, artifact_path, params)
VALUES
  ('fraud_risk', $1, 'active', 'fake', 'classification', 'order', '/tmp/fake', $2::jsonb),
  ('bot_score',  $1, 'active', 'fake', 'classification', 'session', '/tmp/fake', $2::jsonb)`,
		ver, `{"recommended_threshold": 0.80}`); err != nil {
		t.Fatalf("insert registry rows: %v", err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(ctx, `
DELETE FROM gold.model_registry WHERE model_name IN ('fraud_risk','bot_score') AND model_version = $1`, ver); err != nil {
			t.Logf("cleanup registry rows: %v", err)
		}
	})

	insertPred := func(grain, entity string, score float64) int64 {
		t.Helper()
		model := "fraud_risk"
		if grain == "session" {
			model = "bot_score"
		}
		var id int64
		if err := pool.QueryRow(ctx, `
INSERT INTO gold.predictions (model_name, model_version, grain, entity_id, prediction, confidence, metadata)
VALUES ($1, $2, $3, $4, $5, 0.9, $6::jsonb)
RETURNING id`,
			model, ver, grain, entity, score,
			fmt.Sprintf(`{"source": "stream_score", "grain": %q}`, grain)).Scan(&id); err != nil {
			t.Fatalf("insert prediction: %v", err)
		}
		return id
	}
	cleanupAll := func() {
		if _, err := pool.Exec(ctx, `DELETE FROM gold.predictions WHERE entity_id LIKE $1`, tg+"-%"); err != nil {
			t.Logf("cleanup predictions: %v", err)
		}
		if _, err := pool.Exec(ctx, `DELETE FROM gold.model_drift WHERE model_name = $1`, driftModel); err != nil {
			t.Logf("cleanup drift rows: %v", err)
		}
		if _, err := pool.Exec(ctx, `
DELETE FROM gold.action_audit_log WHERE action_id IN (
    SELECT id FROM gold.action_queue
    WHERE dedup_key LIKE $1 OR dedup_key LIKE $2 OR dedup_key LIKE $3)`,
			"hold-high-fraud-order|"+tg+"-%", "log-midband-bot-session|"+tg+"-%",
			"retrain-on-critical-drift|"+driftModel+"%"); err != nil {
			t.Logf("cleanup audit rows: %v", err)
		}
		if _, err := pool.Exec(ctx, `
DELETE FROM gold.action_queue
WHERE dedup_key LIKE $1 OR dedup_key LIKE $2 OR dedup_key LIKE $3`,
			"hold-high-fraud-order|"+tg+"-%", "log-midband-bot-session|"+tg+"-%",
			"retrain-on-critical-drift|"+driftModel+"%"); err != nil {
			t.Logf("cleanup queue rows: %v", err)
		}
		if _, err := pool.Exec(ctx, `DELETE FROM gold.event_log WHERE entity LIKE $1`, tg+"-%"); err != nil {
			t.Logf("cleanup event_log rows: %v", err)
		}
	}
	t.Cleanup(cleanupAll)

	// --- Seed this run's cursor at the current max rows, so the reconciler's
	// first pass sees exactly the rows this test inserts (and nothing live).
	startCursor(t, pool)

	// Rows with NO bus event ever published: the score-writer never scored
	// these; they only exist in gold.predictions.
	hfOrder := insertPred("order", tg+"-ord-1", 0.91) // ≥ 0.80 → hold
	insertPred("order", tg+"-ord-2", 0.30)            // < 0.80 → nothing
	insertPred("session", tg+"-ses-1", 0.6)           // mid-band → auto note
	insertPred("session", tg+"-ses-2", 0.9)           // ≥ 0.7 → nothing

	// Drift: one (model, computed_at) run whose worst finding is critical,
	// plus an ok-only run that must propose nothing.
	now := time.Now().UTC().Truncate(time.Second)
	for i, row := range []struct {
		feature string
		psi     float64
		status  string
	}{
		{"f1", 0.41, "critical"},
		{"f2", 0.13, "warning"},
	} {
		if _, err := pool.Exec(ctx, `
INSERT INTO gold.model_drift (model_name, model_version, computed_at, feature, psi, status, kind)
VALUES ($1, $2, $3, $4, $5, $6, 'psi')`,
			driftModel, ver, now, row.feature, row.psi, row.status); err != nil {
			t.Fatalf("insert drift row %d: %v", i, err)
		}
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO gold.model_drift (model_name, model_version, computed_at, feature, psi, status, kind)
VALUES ($1, $2, $3, 'f9', 0.02, 'ok', 'psi')`,
		driftModel, ver, now.Add(-time.Hour)); err != nil {
		t.Fatalf("insert ok drift run: %v", err)
	}

	// --- The backstop pass. No bus, no events: rows in the DB become
	// proposals through the exact engine decision path.
	logger := slog.New(slog.DiscardHandler)
	svc := actions.New(pool, logger)
	engine, err := playbook.NewEngine(events.New(), svc, governRules(), logger)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	reconciler := NewReconciler(pool, engine, governRules(), logger)
	if err := reconciler.Once(ctx); err != nil {
		t.Fatalf("reconcile pass: %v", err)
	}

	// --- Assertions.
	var holdStatus, holdDedup string
	var holdRecovered bool
	if err := pool.QueryRow(ctx, `
SELECT a.status, a.dedup_key, (a.trigger->'payload'->'reconciled')::bool
FROM gold.action_queue a
WHERE a.dedup_key LIKE $1`, "hold-high-fraud-order|"+tg+"-ord-1|%").
		Scan(&holdStatus, &holdDedup, &holdRecovered); err != nil {
		t.Fatalf("hold proposal missing after backfill (%v)", err)
	}
	if holdStatus != "pending" {
		t.Errorf("hold status = %q, want pending", holdStatus)
	}
	if want := fmt.Sprintf("hold-high-fraud-order|%s-ord-1|%d", tg, hfOrder); holdDedup != want {
		t.Errorf("hold dedup = %q, want %q (instance-scoped on prediction id)", holdDedup, want)
	}
	if !holdRecovered {
		t.Error("hold trigger evidence must carry payload.reconciled=true (recovered, not delivered live)")
	}

	var noteStatus string
	if err := pool.QueryRow(ctx, `
SELECT status FROM gold.action_queue
WHERE dedup_key LIKE $1`, "log-midband-bot-session|"+tg+"-ses-1|%").
		Scan(&noteStatus); err != nil {
		t.Fatalf("auto mid-band note missing after backfill (%v)", err)
	}
	if noteStatus != "executed" {
		t.Errorf("mid-band note status = %q, want executed (auto tier runs end-to-end from a reconstructed event)", noteStatus)
	}

	var below int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM gold.action_queue
WHERE dedup_key LIKE $1`, "hold-high-fraud-order|"+tg+"-ord-2|%").Scan(&below); err != nil {
		t.Fatalf("count below-threshold holds: %v", err)
	}
	if below != 0 {
		t.Errorf("below-threshold order produced %d hold proposals, want 0", below)
	}

	var driftStatus, driftDedup string
	if err := pool.QueryRow(ctx, `
SELECT status, dedup_key FROM gold.action_queue
WHERE dedup_key LIKE $1`, "retrain-on-critical-drift|"+driftModel+"%").Scan(&driftStatus, &driftDedup); err != nil {
		t.Fatalf("critical drift retrain proposal missing after backfill (%v)", err)
	}
	if driftStatus != "pending" {
		t.Errorf("drift retrain status = %q, want pending", driftStatus)
	}

	// --- Idempotency: a second pass must propose nothing new.
	countQueue := func() int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM gold.action_queue
WHERE dedup_key LIKE $1 OR dedup_key LIKE $2 OR dedup_key LIKE $3`,
			"hold-high-fraud-order|"+tg+"-%", "log-midband-bot-session|"+tg+"-%",
			"retrain-on-critical-drift|"+driftModel+"%").Scan(&n); err != nil {
			t.Fatalf("count queue rows: %v", err)
		}
		return n
	}
	before := countQueue()
	if err := reconciler.Once(ctx); err != nil {
		t.Fatalf("second reconcile pass: %v", err)
	}
	if after := countQueue(); after != before {
		t.Errorf("second pass created %d new proposals (before=%d after=%d); reconciliation is not idempotent", after-before, before, after)
	}

	// --- Cursor advanced: a NEW row after the first pass is picked up next.
	insertPred("order", tg+"-ord-3", 0.95)
	if err := reconciler.Once(ctx); err != nil {
		t.Fatalf("third reconcile pass: %v", err)
	}
	if after := countQueue(); after != before+1 {
		t.Errorf("cursor did not advance: got %d rows after a new incident, want %d", after, before+1)
	}
}

// saveWatermark captures the shared 'reconcile' watermark so the test can put
// it back afterwards (tests and the live server share gold.governance_state).
func saveWatermark(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	var val string
	_ = pool.QueryRow(ctx, `SELECT COALESCE(value::text, '{}') FROM gold.governance_state WHERE key = 'reconcile'`).Scan(&val)
	t.Cleanup(func() {
		if _, err := pool.Exec(ctx, `
INSERT INTO gold.governance_state (key, value, updated_at)
VALUES ('reconcile', $1::jsonb, now())
ON CONFLICT (key) DO UPDATE SET value = $1::jsonb, updated_at = now()`, val); err != nil {
			t.Logf("restore watermark: %v", err)
		}
	})
}

// startCursor advances the shared watermark to the current max rows so the
// test's pass only processes the rows it inserts.
func startCursor(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	var p, d int64
	if err := pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(id), 0) FROM gold.predictions`).Scan(&p); err != nil {
		t.Fatalf("max prediction id: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(id), 0) FROM gold.model_drift`).Scan(&d); err != nil {
		t.Fatalf("max drift id: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO gold.governance_state (key, value, updated_at)
VALUES ('reconcile', $1::jsonb, now())
ON CONFLICT (key) DO UPDATE SET value = $1::jsonb, updated_at = now()`,
		fmt.Sprintf(`{"predictions": %d, "drift": %d}`, p, d)); err != nil {
		t.Fatalf("set start cursor: %v", err)
	}
}
