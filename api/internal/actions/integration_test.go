//go:build integration

// Integration tests for the Phase 4 governance core against the live Postgres
// stack. They pin the two invariants that make "propose, don't silently act"
// real:
//
//  1. The execution guard: no executor runs without a prior 'approved' or
//     'auto_approved' audit row — the allow-list is an explicit, audited
//     path, everything else waits for a human.
//  2. Dedup: a replayed event (same playbook rule + trigger scope) can never
//     flood the queue.
//
// Skipped when Postgres is unreachable, like the realtime/predict suites.
package actions

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"abi/internal/events"
)

func testActionsPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("ABI_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://abi:abi@localhost:5432/abi?sslmode=disable"
	}
	probeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(probeCtx, dsn)
	if err != nil {
		t.Skipf("postgres unreachable (%v) — skipping actions integration tests", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(probeCtx); err != nil {
		t.Skipf("postgres unreachable (%v) — skipping actions integration tests", err)
	}
	if err := EnsureSchema(probeCtx, pool); err != nil {
		t.Fatalf("ensure actions schema: %v", err)
	}
	return pool
}

func testActionsService(t *testing.T) (*Service, *pgxpool.Pool) {
	pool := testActionsPool(t)
	return New(pool, slog.New(slog.DiscardHandler)), pool
}

// uniqueDedup scopes a dedup key to this test invocation so concurrent/live
// traffic can never collide with the test's rows.
func uniqueDedup(t *testing.T) string {
	return fmt.Sprintf("test.%s.%d", t.Name(), time.Now().UnixNano())
}

// auditMaintenance runs one statement with the audit log's append-only trigger
// explicitly switched off for this transaction only (SET LOCAL). Tests are the
// only code that ever needs it: the service never updates or deletes audit rows.
func auditMaintenance(ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL abi.audit_maintenance = 'on'`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, sql, args...); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// cleanupAction removes the queue + audit rows for one test action (they are
// append-only by design; the test just doesn't want to accumulate).
func cleanupAction(t *testing.T, pool *pgxpool.Pool, id int64) {
	t.Helper()
	ctx := context.Background()
	if err := auditMaintenance(ctx, pool, `DELETE FROM gold.action_audit_log WHERE action_id = $1`, id); err != nil {
		t.Logf("cleanup audit rows: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM gold.action_queue WHERE id = $1`, id); err != nil {
		t.Logf("cleanup queue row: %v", err)
	}
	// Executors write gold.event_log rows (kinds 'test' and the executor
	// default 'info' — only ever produced by this test suite), so remove them.
	if _, err := pool.Exec(ctx, `DELETE FROM gold.event_log WHERE kind IN ('test','info')`); err != nil {
		t.Logf("cleanup event_log rows: %v", err)
	}
}

func auditTransitions(t *testing.T, pool *pgxpool.Pool, id int64) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT transition FROM gold.action_audit_log WHERE action_id = $1 ORDER BY id`, id)
	if err != nil {
		t.Fatalf("query audit log: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var tr string
		if err := rows.Scan(&tr); err != nil {
			t.Fatalf("scan audit row: %v", err)
		}
		out = append(out, tr)
	}
	return out
}

// TestExecutionGuardRefusesUnauthorized is the governance invariant: a pending
// approval-required action, even one whose executor is available, must refuse
// to run until an approved/auto_approved row exists.
func TestExecutionGuardRefusesUnauthorized(t *testing.T) {
	svc, pool := testActionsService(t)

	id, err := svc.Propose(context.Background(), Proposal{
		Action: "log_event_note", Entity: "e-1", RiskTier: RiskApprovalRequired,
		Rule: "test", DedupKey: uniqueDedup(t),
		Params:  map[string]any{"kind": "test"},
		Trigger: triggerEvidenceForTest(t.Name()),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	t.Cleanup(func() { cleanupAction(t, pool, id) })

	err = svc.execute(context.Background(), id, "tester")
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("execute without approval: err = %v, want ErrUnauthorized", err)
	}
	// Only the proposal transition exists; nothing else ran or was recorded.
	if got := auditTransitions(t, pool, id); len(got) != 1 || got[0] != "proposed" {
		t.Fatalf("audit after refused execution = %v, want [proposed]", got)
	}
	row, _, err := svc.Trace(context.Background(), id)
	if err != nil {
		t.Fatalf("Trace: %v", err)
	}
	if row.Status != StatusPending {
		t.Fatalf("status = %s, want pending (refused execution must not touch the queue)", row.Status)
	}
}

// TestApproveAuthorizesExactlyOneExecution walks the happy path: approve →
// execute once, with the immutable chain proposed → approved → executed.
func TestApproveAuthorizesExactlyOneExecution(t *testing.T) {
	svc, pool := testActionsService(t)

	id, err := svc.Propose(context.Background(), Proposal{
		Action: "log_event_note", Entity: "e-2", RiskTier: RiskApprovalRequired,
		Rule: "test", DedupKey: uniqueDedup(t),
		Params:  map[string]any{"kind": "test"},
		Trigger: triggerEvidenceForTest(t.Name()),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	t.Cleanup(func() { cleanupAction(t, pool, id) })

	if err := svc.Approve(context.Background(), id, "ship it: demo", "alice@abi"); err != nil {
		t.Fatalf("Approve: %v", err)
	}
	if got := auditTransitions(t, pool, id); fmt.Sprint(got) != "[proposed approved executed]" {
		t.Fatalf("audit = %v, want [proposed approved executed]", got)
	}

	// A second execution attempt must refuse: the action is already executed.
	err = svc.execute(context.Background(), id, "tester")
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("second execute: err = %v, want ErrInvalidState", err)
	}

	row, _, err := svc.Trace(context.Background(), id)
	if err != nil {
		t.Fatalf("Trace: %v", err)
	}
	if row.Status != StatusExecuted || row.ExecutedAt == nil {
		t.Fatalf("row after approve = status %s executed_at %v", row.Status, row.ExecutedAt)
	}
	// The executor actually ran: an event_log row for the trigger's entity
	// (the executor acts on event evidence, not queue metadata).
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM gold.event_log WHERE entity = 'o-42' AND kind = 'test'`).Scan(&n); err != nil {
		t.Fatalf("count event_log: %v", err)
	}
	if n != 1 {
		t.Fatalf("executor effect count = %d, want exactly 1", n)
	}
}

// TestRetryReRunsAFailedExecution: a failed execution is not terminal — Retry
// re-runs it under the original approval (no new human decision), recording a
// 'retrying' audit row, and the executor is still gated by the same invariant.
func TestRetryReRunsAFailedExecution(t *testing.T) {
	pool := testActionsPool(t)
	svc := New(pool, slog.New(slog.DiscardHandler))

	failOnce := true
	svc.Register("flaky", func(_ context.Context, _ Context) (Result, error) {
		if failOnce {
			failOnce = false
			return Result{}, fmt.Errorf("simulated transient executor failure")
		}
		return Result{OK: true, Detail: map[string]any{"attempt": "second"}}, nil
	})

	id, err := svc.Propose(context.Background(), Proposal{
		Action: "flaky", Entity: "e-retry", RiskTier: RiskApprovalRequired,
		Rule: "test", DedupKey: uniqueDedup(t),
		Params:  map[string]any{},
		Trigger: triggerEvidenceForTest(t.Name()),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	t.Cleanup(func() { cleanupAction(t, pool, id) })

	if err := svc.Approve(context.Background(), id, "demo: test retry", "alice@abi"); err == nil {
		t.Fatal("Approve must surface the executor failure (approval auto-executes synchronously)")
	}
	// First execution failed: status='failed', the error is in the outcome,
	// and the human decision (approved) is already on the trail.
	if row, _, _ := svc.Trace(context.Background(), id); row.Status != StatusFailed {
		t.Fatalf("after failing execute: status = %s, want failed", row.Status)
	}

	if err := svc.Retry(context.Background(), id, "tester"); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	row, _, err := svc.Trace(context.Background(), id)
	if err != nil {
		t.Fatalf("Trace: %v", err)
	}
	if row.Status != StatusExecuted {
		t.Fatalf("after retry: status = %s, want executed", row.Status)
	}
	if got := auditTransitions(t, pool, id); fmt.Sprint(got) != "[proposed approved failed retrying executed]" {
		t.Fatalf("audit = %v, want [proposed approved failed retrying executed]", got)
	}
	// The retry is not a blank check: a retry on an already-executed action is
	// refused, and the fresh attempt was still governed.
	if err := svc.Retry(context.Background(), id, "tester"); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("retry on executed: err = %v, want ErrInvalidState", err)
	}
}

// TestRetryFailsClosedWhenStillFailing: a retry that fails again lands back on
// 'failed' with the fresh error — the row stays visible, the human can retry
// again, and every attempt is on the trail.
func TestRetryFailsClosedWhenStillFailing(t *testing.T) {
	pool := testActionsPool(t)
	svc := New(pool, slog.New(slog.DiscardHandler))
	svc.Register("always-broken", func(_ context.Context, _ Context) (Result, error) {
		return Result{}, fmt.Errorf("permanent executor failure")
	})

	id, err := svc.Propose(context.Background(), Proposal{
		Action: "always-broken", Entity: "e-retry2", RiskTier: RiskApprovalRequired,
		Rule: "test", DedupKey: uniqueDedup(t),
		Params:  map[string]any{},
		Trigger: triggerEvidenceForTest(t.Name()),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	t.Cleanup(func() { cleanupAction(t, pool, id) })

	if err := svc.Approve(context.Background(), id, "demo", "alice@abi"); err == nil {
		t.Fatal("Approve must surface the executor failure")
	}
	if err := svc.Retry(context.Background(), id, "tester"); err == nil {
		t.Fatal("Retry on a still-failing executor must return the new error")
	}
	if row, _, _ := svc.Trace(context.Background(), id); row.Status != StatusFailed {
		t.Fatalf("after failed retry: status = %s, want failed", row.Status)
	}
}

// TestAutoTierIsAnExplicitAllowListPath: the 'auto' tier gets an explicit
// auto_approved audit row before executing — the identical governance shape to
// a human approval, but recorded with the allow-list as the authority.
func TestAutoTierIsAnExplicitAllowListPath(t *testing.T) {
	svc, pool := testActionsService(t)

	id, err := svc.Propose(context.Background(), Proposal{
		Action: "log_event_note", Entity: "e-3", RiskTier: RiskAuto,
		Rule: "test", DedupKey: uniqueDedup(t),
		Params:  map[string]any{"kind": "test"},
		Trigger: triggerEvidenceForTest(t.Name()),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	t.Cleanup(func() { cleanupAction(t, pool, id) })

	if err := svc.AutoApproveAndExecute(context.Background(), id); err != nil {
		t.Fatalf("AutoApproveAndExecute: %v", err)
	}
	if got := auditTransitions(t, pool, id); fmt.Sprint(got) != "[proposed auto_approved executed]" {
		t.Fatalf("audit = %v, want [proposed auto_approved executed]", got)
	}
}

// TestAutoPathRefusesApprovalRequired: the allow-list cannot fast-path an
// approval-required action — a misconfigured playbook is caught here, not at
// the moment the effect lands.
func TestAutoPathRefusesApprovalRequired(t *testing.T) {
	svc, pool := testActionsService(t)

	id, err := svc.Propose(context.Background(), Proposal{
		Action: "log_event_note", Entity: "e-4", RiskTier: RiskApprovalRequired,
		Rule: "test", DedupKey: uniqueDedup(t),
		Params:  map[string]any{"kind": "test"},
		Trigger: triggerEvidenceForTest(t.Name()),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	t.Cleanup(func() { cleanupAction(t, pool, id) })

	if err := svc.AutoApproveAndExecute(context.Background(), id); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("auto path on approval_required: err = %v, want ErrUnauthorized", err)
	}
	if got := auditTransitions(t, pool, id); len(got) != 1 || got[0] != "proposed" {
		t.Fatalf("audit = %v, want just [proposed]", got)
	}
}

// TestRejectRecordsReasonAndNeverExecutes: a rejected proposal keeps its
// reason on the immutable log and cannot be approved afterwards.
func TestRejectRecordsReasonAndNeverExecutes(t *testing.T) {
	svc, pool := testActionsService(t)

	id, err := svc.Propose(context.Background(), Proposal{
		Action: "log_event_note", Entity: "e-5", RiskTier: RiskApprovalRequired,
		Rule: "test", DedupKey: uniqueDedup(t),
		Params:  map[string]any{"kind": "test"},
		Trigger: triggerEvidenceForTest(t.Name()),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	t.Cleanup(func() { cleanupAction(t, pool, id) })

	if err := svc.Reject(context.Background(), id, "business decision: outside policy", "bob@abi"); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	if got := auditTransitions(t, pool, id); fmt.Sprint(got) != "[proposed rejected]" {
		t.Fatalf("audit = %v, want [proposed rejected]", got)
	}
	// The rejection reason is at rest on the log.
	var reason string
	if err := pool.QueryRow(context.Background(),
		`SELECT COALESCE(reason,'') FROM gold.action_audit_log WHERE action_id = $1 AND transition = 'rejected'`, id).
		Scan(&reason); err != nil {
		t.Fatalf("read rejection reason: %v", err)
	}
	if reason == "" {
		t.Fatal("rejection reason must be persisted (mandatory-reason contract)")
	}
	// Approving a rejected action is a state conflict.
	if err := svc.Approve(context.Background(), id, "on second thought", "bob@abi"); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("approve after reject: err = %v, want ErrInvalidState", err)
	}
}

// TestDedupKeyPreventsQueueFlood: the same rule + trigger scope (replayed
// events, at-least-once redelivery, a retried proposal) inserts once.
func TestDedupKeyPreventsQueueFlood(t *testing.T) {
	svc, pool := testActionsService(t)
	key := uniqueDedup(t)

	first, err := svc.Propose(context.Background(), Proposal{
		Action: "log_event_note", Entity: "e-6", RiskTier: RiskApprovalRequired,
		Rule: "test", DedupKey: key,
		Params:  map[string]any{"kind": "test"},
		Trigger: triggerEvidenceForTest(t.Name()),
	})
	if err != nil {
		t.Fatalf("Propose (first): %v", err)
	}
	t.Cleanup(func() { cleanupAction(t, pool, first) })
	if first == 0 {
		t.Fatal("first proposal returned no id")
	}

	dup, err := svc.Propose(context.Background(), Proposal{
		Action: "log_event_note", Entity: "e-6", RiskTier: RiskApprovalRequired,
		Rule: "test", DedupKey: key,
		Params:  map[string]any{"kind": "test"},
		Trigger: triggerEvidenceForTest(t.Name()),
	})
	if err != nil {
		t.Fatalf("Propose (duplicate): %v", err)
	}
	if dup != 0 {
		t.Fatalf("duplicate proposal returned id %d, want 0 (no-op)", dup)
	}

	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM gold.action_queue WHERE dedup_key = $1`, key).Scan(&n); err != nil {
		t.Fatalf("count queue: %v", err)
	}
	if n != 1 {
		t.Fatalf("queue rows for dedup key = %d, want exactly 1", n)
	}
}

// triggerEvidenceForTest builds the persisted evidence trail (event_type,
// event_at, payload, rule, condition) that EventFromTrigger reconstructs the
// triggering event from — same shape the playbook engine writes.
func triggerEvidenceForTest(name string) map[string]any {
	payload := map[string]any{
		"prediction": map[string]any{"model": "fraud_risk", "entity_id": "o-42", "score": 0.91},
		"registry":   map[string]any{"recommended_threshold": 0.795},
	}
	return map[string]any{
		"event_type": string(events.TypeOrderScored),
		"event_at":   "2026-09-23T10:00:00Z",
		"payload":    payload,
		"rule":       "hold-high-fraud-order",
		"condition":  "prediction.score >= registry.recommended_threshold",
	}
}

// TestProposeRefusesAutoForNonAllowListedAction: the auto tier is an
// allow-list, so a hold/retrain proposed as risk_tier "auto" never enters the
// queue as something the engine could fast-path.
func TestProposeRefusesAutoForNonAllowListedAction(t *testing.T) {
	svc, pool := testActionsService(t)
	key := uniqueDedup(t)
	for _, action := range []string{"hold_order_for_review", "retrain_model"} {
		_, err := svc.Propose(context.Background(), Proposal{
			Action: action, Entity: "e-auto", RiskTier: RiskAuto,
			Rule: "test", DedupKey: key + action,
			Trigger: triggerEvidenceForTest(t.Name()),
		})
		if !errors.Is(err, ErrAutoNotAllowed) {
			t.Fatalf("Propose(%s, auto): err = %v, want ErrAutoNotAllowed", action, err)
		}
	}
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM gold.action_queue WHERE dedup_key LIKE $1`, key+"%").Scan(&n); err != nil || n != 0 {
		t.Fatalf("refused proposals left %d queue rows (err %v)", n, err)
	}
}

// TestAutoPathRefusesNonAllowListedActionEvenWhenTierSaysAuto: a row that
// claims tier 'auto' for a non-allow-listed action (inserted behind Propose's
// back) is still refused, and leaves no auto_approved authority row.
func TestAutoPathRefusesNonAllowListedActionEvenWhenTierSaysAuto(t *testing.T) {
	svc, pool := testActionsService(t)
	ctx := context.Background()
	var id int64
	if err := pool.QueryRow(ctx, `
INSERT INTO gold.action_queue (action, entity, risk_tier, rule, dedup_key, trigger)
VALUES ('hold_order_for_review', 'e-forged', 'auto', 'test', $1, $2::jsonb) RETURNING id`,
		uniqueDedup(t), `{"event_type":"order_scored","payload":{"prediction":{"entity_id":"o-forged"}}}`).Scan(&id); err != nil {
		t.Fatalf("insert forged row: %v", err)
	}
	t.Cleanup(func() { cleanupAction(t, pool, id) })

	if err := svc.AutoApproveAndExecute(ctx, id); !errors.Is(err, ErrAutoNotAllowed) {
		t.Fatalf("AutoApproveAndExecute: err = %v, want ErrAutoNotAllowed", err)
	}
	if got := auditTransitions(t, pool, id); len(got) != 0 {
		t.Fatalf("audit after refused auto path = %v, want none", got)
	}
	var flags int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM gold.order_flags WHERE order_id = 'o-forged'`).Scan(&flags)
	if flags != 0 {
		t.Fatal("the held-order effect ran without a human approval")
	}
}

// TestAutoPathDoesNotStampAuthorityOnDecidedRows: calling the auto path on an
// already-executed (or rejected) row must not append a misleading
// auto_approved transition.
func TestAutoPathDoesNotStampAuthorityOnDecidedRows(t *testing.T) {
	svc, pool := testActionsService(t)
	ctx := context.Background()
	id, err := svc.Propose(ctx, Proposal{
		Action: "log_event_note", Entity: "e-once", RiskTier: RiskAuto,
		Rule: "test", DedupKey: uniqueDedup(t),
		Params: map[string]any{"kind": "test"}, Trigger: triggerEvidenceForTest(t.Name()),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	t.Cleanup(func() { cleanupAction(t, pool, id) })
	if err := svc.AutoApproveAndExecute(ctx, id); err != nil {
		t.Fatalf("first auto execute: %v", err)
	}
	if err := svc.AutoApproveAndExecute(ctx, id); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("second auto execute: err = %v, want ErrInvalidState", err)
	}
	if got := auditTransitions(t, pool, id); fmt.Sprint(got) != "[proposed auto_approved executed]" {
		t.Fatalf("audit = %v, want exactly [proposed auto_approved executed]", got)
	}
}

// TestOpenProposalSuppressesRepeatUntilDecided: the audit found one order with
// two pending holds (a re-scored entity got a fresh prediction id, hence a fresh
// dedup key). While a proposal for the same rule+entity is open, a new trigger
// is suppressed; once a human decides it, a later trigger proposes again.
func TestOpenProposalSuppressesRepeatUntilDecided(t *testing.T) {
	svc, pool := testActionsService(t)
	ctx := context.Background()
	rule := fmt.Sprintf("test-open-%d", time.Now().UnixNano())
	propose := func(entity, key string) int64 {
		t.Helper()
		id, err := svc.Propose(ctx, Proposal{
			Action: "log_event_note", Entity: entity, RiskTier: RiskApprovalRequired,
			Rule: rule, DedupKey: rule + "|" + key,
			Params: map[string]any{"kind": "test"}, Trigger: triggerEvidenceForTest(t.Name()),
		})
		if err != nil {
			t.Fatalf("Propose(%s,%s): %v", entity, key, err)
		}
		return id
	}
	first := propose("order-A", "pred-1")
	if first == 0 {
		t.Fatal("first proposal was not created")
	}
	t.Cleanup(func() { cleanupAction(t, pool, first) })

	if dup := propose("order-A", "pred-2"); dup != 0 {
		t.Fatalf("second trigger on the same entity created action %d while #%d is still pending", dup, first)
	}
	other := propose("order-B", "pred-3")
	if other == 0 {
		t.Fatal("a different entity must still propose")
	}
	t.Cleanup(func() { cleanupAction(t, pool, other) })

	if err := svc.Reject(ctx, first, "reviewed: legitimate", "bob@abi"); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	again := propose("order-A", "pred-4")
	if again == 0 {
		t.Fatal("after the earlier proposal was decided, a new trigger on the entity must propose again")
	}
	t.Cleanup(func() { cleanupAction(t, pool, again) })

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM gold.action_queue WHERE rule = $1 AND entity = 'order-A'`, rule).Scan(&n); err != nil || n != 2 {
		t.Fatalf("order-A rows = %d (err %v), want 2 (the decided one + the new one)", n, err)
	}
}

// TestConcurrentProposalsForOneEntityCreateOne: the live bus and the
// reconciler can propose for the same entity at the same instant.
func TestConcurrentProposalsForOneEntityCreateOne(t *testing.T) {
	svc, pool := testActionsService(t)
	ctx := context.Background()
	rule := fmt.Sprintf("test-race-%d", time.Now().UnixNano())
	ids := make(chan int64, 8)
	for i := 0; i < 8; i++ {
		go func(i int) {
			id, err := svc.Propose(ctx, Proposal{
				Action: "log_event_note", Entity: "order-R", RiskTier: RiskApprovalRequired,
				Rule: rule, DedupKey: fmt.Sprintf("%s|%d", rule, i),
				Trigger: triggerEvidenceForTest(t.Name()),
			})
			if err != nil {
				t.Errorf("Propose: %v", err)
			}
			ids <- id
		}(i)
	}
	created := 0
	for i := 0; i < 8; i++ {
		if id := <-ids; id != 0 {
			created++
			t.Cleanup(func() { cleanupAction(t, pool, id) })
		}
	}
	if created != 1 {
		t.Fatalf("8 concurrent proposals created %d actions, want exactly 1", created)
	}
}

// TestAuditLogIsAppendOnlyAtTheDatabase: "immutable" was a convention (the API
// never issued UPDATE/DELETE) while the application role could do anything. The
// trigger makes UPDATE, DELETE and TRUNCATE fail unless the session explicitly
// opts in; INSERT (the only thing the service does) is unaffected.
func TestAuditLogIsAppendOnlyAtTheDatabase(t *testing.T) {
	svc, pool := testActionsService(t)
	ctx := context.Background()
	id, err := svc.Propose(ctx, Proposal{
		Action: "log_event_note", Entity: "e-audit", RiskTier: RiskApprovalRequired,
		Rule: "test", DedupKey: uniqueDedup(t),
		Params: map[string]any{"kind": "test"}, Trigger: triggerEvidenceForTest(t.Name()),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	t.Cleanup(func() { cleanupAction(t, pool, id) })

	for name, stmt := range map[string]string{
		"UPDATE":   `UPDATE gold.action_audit_log SET reason = 'edited' WHERE action_id = $1`,
		"DELETE":   `DELETE FROM gold.action_audit_log WHERE action_id = $1`,
		"TRUNCATE": `TRUNCATE gold.action_audit_log`,
	} {
		var err error
		if name == "TRUNCATE" {
			_, err = pool.Exec(ctx, stmt)
		} else {
			_, err = pool.Exec(ctx, stmt, id)
		}
		if err == nil {
			t.Errorf("%s on gold.action_audit_log succeeded; the log must be append-only", name)
		}
	}
	if got := auditTransitions(t, pool, id); fmt.Sprint(got) != "[proposed]" {
		t.Fatalf("audit rows changed: %v", got)
	}
	// Appending is still fine, and the explicit maintenance switch works.
	if _, err := pool.Exec(ctx, `INSERT INTO gold.action_audit_log (action_id, transition, actor) VALUES ($1, 'outcome', 'test')`, id); err != nil {
		t.Fatalf("INSERT must remain allowed: %v", err)
	}
	if err := auditMaintenance(ctx, pool, `DELETE FROM gold.action_audit_log WHERE action_id = $1 AND transition = 'outcome'`, id); err != nil {
		t.Fatalf("explicit maintenance mode must permit the delete: %v", err)
	}
}

// TestConcurrentRetriesRunTheExecutorOnce: Retry's re-arm is conditional on
// status = 'failed', so of N concurrent retries exactly one executes.
func TestConcurrentRetriesRunTheExecutorOnce(t *testing.T) {
	pool := testActionsPool(t)
	svc := New(pool, slog.New(slog.DiscardHandler))
	var mu sync.Mutex
	calls := 0
	svc.Register("flaky-once", func(_ context.Context, _ Context) (Result, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return Result{}, fmt.Errorf("first attempt fails")
		}
		time.Sleep(100 * time.Millisecond) // widen the window a racing retry would need
		return Result{OK: true}, nil
	})
	id, err := svc.Propose(context.Background(), Proposal{
		Action: "flaky-once", Entity: "e-race", RiskTier: RiskApprovalRequired,
		Rule: "test", DedupKey: uniqueDedup(t), Trigger: triggerEvidenceForTest(t.Name()),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	t.Cleanup(func() { cleanupAction(t, pool, id) })
	if err := svc.Approve(context.Background(), id, "demo", "alice@abi"); err == nil {
		t.Fatal("first execution must fail")
	}

	const racers = 6
	results := make(chan error, racers)
	for i := 0; i < racers; i++ {
		go func() { results <- svc.Retry(context.Background(), id, "tester") }()
	}
	won := 0
	for i := 0; i < racers; i++ {
		err := <-results
		switch {
		case err == nil:
			won++
		case errors.Is(err, ErrInvalidState):
		default:
			t.Errorf("unexpected retry error: %v", err)
		}
	}
	if won != 1 {
		t.Fatalf("%d concurrent retries executed, want exactly 1", won)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Fatalf("executor ran %d times, want 2 (the original failure + one retry)", calls)
	}
	if got := auditTransitions(t, pool, id); fmt.Sprint(got) != "[proposed approved failed retrying executed]" {
		t.Fatalf("audit = %v", got)
	}
}

// TestStatsCountsFailedAndPendingRows: a failed execution must be visible as a
// number an operator can alert on, not only as a row in the history panel.
func TestStatsCountsFailedAndPendingRows(t *testing.T) {
	pool := testActionsPool(t)
	svc := New(pool, slog.New(slog.DiscardHandler))
	svc.Register("always-broken-stats", func(_ context.Context, _ Context) (Result, error) {
		return Result{}, fmt.Errorf("permanent failure")
	})
	before, err := svc.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	mk := func(action string) int64 {
		id, err := svc.Propose(context.Background(), Proposal{
			Action: action, Entity: "e-stats-" + action, RiskTier: RiskApprovalRequired,
			Rule: "test-" + action, DedupKey: uniqueDedup(t) + action, Trigger: triggerEvidenceForTest(t.Name()),
		})
		if err != nil {
			t.Fatalf("Propose: %v", err)
		}
		t.Cleanup(func() { cleanupAction(t, pool, id) })
		return id
	}
	failing := mk("always-broken-stats")
	mk("log_event_note") // stays pending
	if err := svc.Approve(context.Background(), failing, "demo", "alice@abi"); err == nil {
		t.Fatal("approve must surface the executor failure")
	}
	after, err := svc.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if after.Failed != before.Failed+1 {
		t.Errorf("failed = %d, want %d", after.Failed, before.Failed+1)
	}
	if after.Pending != before.Pending+1 {
		t.Errorf("pending = %d, want %d", after.Pending, before.Pending+1)
	}
}
