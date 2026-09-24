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
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaDDL is evaluated idempotently at server startup (CREATE IF NOT EXISTS),
// and declared as dbt sources so lineage/tests can see it (the same pattern as
// the realtime tables).
var schemaDDL = []string{
	`CREATE TABLE IF NOT EXISTS gold.action_queue (
		id           bigserial PRIMARY KEY,
		action       text NOT NULL,
		entity       text,
		risk_tier    text NOT NULL,           -- 'auto' | 'approval_required'
		status       text NOT NULL DEFAULT 'pending', -- pending|approved|rejected|executed|failed
		params       jsonb NOT NULL DEFAULT '{}'::jsonb,
		trigger      jsonb NOT NULL DEFAULT '{}'::jsonb, -- evidence trail (event + rule + condition)
		rule         text NOT NULL,
		dedup_key    text NOT NULL,           -- playbook rule + trigger scope; UNIQUE below
		created_at   timestamptz NOT NULL DEFAULT now(),
		decided_at   timestamptz,
		executed_at  timestamptz,
		outcome      jsonb
	)`,
	`ALTER TABLE gold.action_queue ADD COLUMN IF NOT EXISTS dedup_key text`,
	`CREATE UNIQUE INDEX IF NOT EXISTS uq_action_queue_dedup ON gold.action_queue (dedup_key)`,
	`CREATE INDEX IF NOT EXISTS idx_action_queue_status ON gold.action_queue (status, created_at)`,

	`CREATE TABLE IF NOT EXISTS gold.action_audit_log (
		id          bigserial PRIMARY KEY,
		action_id   bigint NOT NULL REFERENCES gold.action_queue (id),
		transition  text NOT NULL,   -- proposed|auto_approved|approved|rejected|executed|failed|outcome
		actor       text NOT NULL DEFAULT 'system',
		reason      text,
		detail      jsonb NOT NULL DEFAULT '{}'::jsonb,
		at          timestamptz NOT NULL DEFAULT now()
	)`,
	`CREATE INDEX IF NOT EXISTS idx_action_audit_action ON gold.action_audit_log (action_id, at)`,

	// Simulated effects — all Phase 4 actions mutate ABI's own tables and are
	// logged as if executed; there is no real store behind them (scope
	// honesty: ABI is a portfolio demo, not a live commerce system).
	`CREATE TABLE IF NOT EXISTS gold.order_flags (
		order_id       text PRIMARY KEY,
		held_for_review boolean NOT NULL DEFAULT true,
		reason         text,
		set_at         timestamptz NOT NULL DEFAULT now(),
		released_at    timestamptz
	)`,
	`CREATE TABLE IF NOT EXISTS gold.purchase_orders (
		id            bigserial PRIMARY KEY,
		category      text NOT NULL,
		forecast_week text NOT NULL,
		quantity      int,
		status        text NOT NULL DEFAULT 'draft',
		reference     jsonb NOT NULL DEFAULT '{}'::jsonb,
		created_at    timestamptz NOT NULL DEFAULT now(),
		UNIQUE (category, forecast_week)   -- idempotency key: one draft PO per forecast week+category
	)`,
	`CREATE TABLE IF NOT EXISTS gold.retention_actions (
		id            bigserial PRIMARY KEY,
		customer_id   text,
		churn_score   double precision,
		kind          text NOT NULL,         -- 'email' | 'offer'
		status        text NOT NULL DEFAULT 'sent', -- sent|proposed|rejected
		message       text,
		created_at    timestamptz NOT NULL DEFAULT now()
	)`,
	`CREATE TABLE IF NOT EXISTS gold.retrain_requests (
		id           bigserial PRIMARY KEY,
		action_id    bigint NOT NULL UNIQUE,  -- one request per governance action
		model        text NOT NULL,
		status       text NOT NULL DEFAULT 'pending', -- pending|running|done|failed
		requested_at timestamptz NOT NULL DEFAULT now(),
		finished_at  timestamptz,
		new_version  text,
		detail       jsonb NOT NULL DEFAULT '{}'::jsonb
	)`,
	`CREATE TABLE IF NOT EXISTS gold.event_log (
		id         bigserial PRIMARY KEY,
		kind       text NOT NULL,
		entity     text,
		detail     text,
		created_at timestamptz NOT NULL DEFAULT now()
	)`,
}

// EnsureSchema creates all action/governance tables. Safe on every start.
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	for _, ddl := range schemaDDL {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("ensure actions schema: %w", err)
		}
	}
	return nil
}

// now is injectable for tests.
var now = func() time.Time { return time.Now().UTC() }