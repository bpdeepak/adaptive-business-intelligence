// Package actions is the Phase 4 action registry + approval store. Actions are
// the only way a playbook proposal becomes an executed effect: every execution
// requires a durable authority record (a prior human 'approved' transition or
// the explicit 'auto_approved' transition of an allow-listed auto-tier rule).
// The two tables below are the "current state + append-only history" split
// this project uses for gold.model_registry: gold.action_queue is the
// mutable working view, gold.action_audit_log is one immutable row per
// transition (proposed → approved/rejected → executed/failed → outcome).
package actions

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaDDL is the single source of truth for every governance table,
// embedded from schema.sql in this package. ml/common.py loads the SAME file
// (GOVERNANCE_SQL), so the Go service and the Python retrain worker can never
// drift apart; edit schema.sql only. Statements are idempotent (IF NOT EXISTS)
// and evaluated on every server start.
//
//go:embed schema.sql
var schemaDDL string

// EnsureSchema creates all action/governance tables. Safe on every start.
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, schemaDDL); err != nil {
		return fmt.Errorf("ensure actions schema: %w", err)
	}
	return nil
}

// now is injectable for tests.
var now = func() time.Time { return time.Now().UTC() }