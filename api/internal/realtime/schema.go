// Package realtime implements the Phase 1 streaming consumers: the bronze
// event recorder, the realtime metrics aggregator (with online anomaly
// detection) and the fan-out broadcaster that feeds SSE and gRPC.
package realtime

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaDDL is evaluated idempotently at server startup (CREATE IF NOT EXISTS),
// mirroring the batch loader's self-bootstrapping style. The gold.* tables
// written here are serving-layer tables governed by the Go realtime stack;
// they are declared as dbt sources so dbt's lineage/tests can see them.
var schemaDDL = []string{
	// Schemas are created here so a fresh stack (empty Postgres, no dbt run
	// yet) can still bootstrap the realtime layer — loader.py creates `bronze`
	// and dbt creates `gold`/`silver`, but the integration test and server
	// startup must not depend on either having run first.
	`CREATE SCHEMA IF NOT EXISTS bronze`,
	`CREATE SCHEMA IF NOT EXISTS gold`,

	// Raw streaming landing record: every event verbatim + lineage + Kafka
	// coordinates so "which pipeline wrote this row" has a precise answer.
	`CREATE TABLE IF NOT EXISTS bronze.stream_events (
		event_id        text PRIMARY KEY,
		schema_version  int NOT NULL,
		event_type      text NOT NULL,
		occurred_at     timestamptz NOT NULL,
		produced_at     timestamptz NOT NULL,
		key             text NOT NULL,
		loop_id         text,
		payload         jsonb NOT NULL,
		_source_file    text NOT NULL,
		_batch_id       text NOT NULL,
		_loaded_at      timestamptz NOT NULL DEFAULT now(),
		_kafka_topic    text NOT NULL,
		_kafka_partition int NOT NULL,
		_kafka_offset   bigint NOT NULL
	)`,

	// Restricted ground-truth landing for the synthetic bot label. Consumed by
	// nothing serving — Phase 2 trains against it offline.
	`CREATE TABLE IF NOT EXISTS bronze.training_ground_truth (
		session_id      text PRIMARY KEY,
		is_synthetic_bot boolean NOT NULL,
		loop_id         text NOT NULL,
		is_converting    boolean NOT NULL,
		recorded_at     timestamptz NOT NULL DEFAULT now()
	)`,

	// 1-minute hot metrics buckets the dashboard/agents poll. Separate from the
	// batch gold.daily_* tables so streaming writes never contend with batch
	// reads (Phase 0 review item #5).
	`CREATE TABLE IF NOT EXISTS gold.realtime_metrics (
		bucket_start    timestamptz PRIMARY KEY,
		revenue         numeric(12,2) NOT NULL DEFAULT 0,
		orders          int NOT NULL DEFAULT 0,
		active_sessions int NOT NULL DEFAULT 0,
		anomaly_flag    boolean NOT NULL DEFAULT false,
		updated_at      timestamptz NOT NULL DEFAULT now()
	)`,

	// Detected anomalies (online EWMA/z-score). `status` lifecycle supports the
	// dashboard's open/dismissed banner.
	`CREATE TABLE IF NOT EXISTS gold.anomalies (
		id          bigserial PRIMARY KEY,
		metric      text NOT NULL,
		bucket_start timestamptz NOT NULL,
		observed    double precision NOT NULL,
		expected    double precision NOT NULL,
		z_score     double precision NOT NULL,
		severity    text NOT NULL,
		status      text NOT NULL DEFAULT 'open',
		detected_at timestamptz NOT NULL DEFAULT now(),
		resolved_at timestamptz,
		dismissed_at timestamptz
	)`,

	// Online detector accumulators, snapshotted so a server restart resumes the
	// Welford/anomaly baseline instead of re-warming (a fresh 20-bucket warm-up
	// would otherwise ride a false-positive burst right after a deploy).
	`CREATE TABLE IF NOT EXISTS gold.detector_state (
		metric      text PRIMARY KEY,
		n           bigint NOT NULL,
		mean        double precision NOT NULL,
		m2          double precision NOT NULL,
		updated_at  timestamptz NOT NULL DEFAULT now()
	)`,

	// Supporting indexes for queries and retention purges.
	`CREATE INDEX IF NOT EXISTS idx_stream_events_loaded ON bronze.stream_events (_loaded_at)`,
	`CREATE INDEX IF NOT EXISTS idx_stream_events_occurred ON bronze.stream_events (occurred_at)`,
	`CREATE INDEX IF NOT EXISTS idx_rt_metrics_bucket ON gold.realtime_metrics (bucket_start)`,
	`CREATE INDEX IF NOT EXISTS idx_anomalies_status ON gold.anomalies (status)`,
	`CREATE INDEX IF NOT EXISTS idx_anomalies_detected ON gold.anomalies (detected_at)`,
}

// EnsureSchema creates all realtime tables and indexes. Safe to call on every
// start.
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	for _, ddl := range schemaDDL {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("ensure schema: %w", err)
		}
	}
	return nil
}

// Retention keeps the streaming tables bounded: raw events for `keep` duration,
// one-minute buckets for `bucketKeep`, and the anomaly log for `anomalyKeep`.
// The replay loop is ~6h at default speed, so 12h of raw events preserves ~2
// loops and 3h of buckets gives the dashboard a comfortable look-back window.
// bronze.training_ground_truth is deliberately NOT pruned: it is the Phase 2
// training corpus and grows only with the number of synthetic sessions per
// loop (hundreds), not with event volume.
func Retention(ctx context.Context, pool *pgxpool.Pool, keep, bucketKeep, anomalyKeep time.Duration) error {
	if _, err := pool.Exec(ctx,
		`DELETE FROM bronze.stream_events WHERE _loaded_at < now() - $1::interval`, keep.String()); err != nil {
		return fmt.Errorf("purge stream_events: %w", err)
	}
	if _, err := pool.Exec(ctx,
		`DELETE FROM gold.realtime_metrics WHERE bucket_start < now() - $1::interval`, bucketKeep.String()); err != nil {
		return fmt.Errorf("purge realtime_metrics: %w", err)
	}
	if _, err := pool.Exec(ctx,
		`DELETE FROM gold.anomalies WHERE detected_at < now() - $1::interval`, anomalyKeep.String()); err != nil {
		return fmt.Errorf("purge anomalies: %w", err)
	}
	return nil
}
