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

// cleanupAction removes the queue + audit rows for one test action (they are
// append-only by design; the test just doesn't want to accumulate).
func cleanupAction(t *testing.T, pool *pgxpool.Pool, id int64) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `DELETE FROM gold.action_audit_log WHERE action_id = $1`, id); err != nil {
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