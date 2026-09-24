package actions

import (
	"strings"
	"testing"
)

// The governance DDL is single-sourced in schema.sql (embedded here, read by
// ml/common.py). These pin the constraints the governance guarantees rest on, so a
// refactor cannot silently drop one without a test failing (audit T3).
func TestGovernanceSchemaPinsTheConstraintsTheGuaranteesRestOn(t *testing.T) {
	sql := schemaDDL
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS gold.action_queue",
		"CREATE TABLE IF NOT EXISTS gold.action_audit_log",
		"CREATE TABLE IF NOT EXISTS gold.retrain_requests",
		"CREATE TABLE IF NOT EXISTS gold.order_flags",
		"CREATE TABLE IF NOT EXISTS gold.purchase_orders",
		"CREATE TABLE IF NOT EXISTS gold.retention_actions",
		"CREATE TABLE IF NOT EXISTS gold.event_log",
		"CREATE UNIQUE INDEX IF NOT EXISTS uq_action_queue_dedup ON gold.action_queue (dedup_key)",
		"REFERENCES gold.action_queue (id)",
		"action_id    bigint NOT NULL UNIQUE",
		"UNIQUE (category, forecast_week)",
		"trg_action_audit_log_append_only",
		"BEFORE UPDATE OR DELETE ON gold.action_audit_log",
		"BEFORE TRUNCATE ON gold.action_audit_log",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("actions/schema.sql lost %q", want)
		}
	}
}

// Every DDL statement must be safe to run on every boot (idempotent).
func TestGovernanceSchemaIsIdempotentByConstruction(t *testing.T) {
	for _, line := range strings.Split(schemaDDL, "\n") {
		l := strings.TrimSpace(line)
		if strings.HasPrefix(l, "CREATE TABLE ") || strings.HasPrefix(l, "CREATE INDEX ") ||
			strings.HasPrefix(l, "CREATE UNIQUE INDEX ") || strings.HasPrefix(l, "CREATE SCHEMA ") {
			if !strings.Contains(l, "IF NOT EXISTS") {
				t.Errorf("non-idempotent DDL on a boot path: %q", l)
			}
		}
	}
}
