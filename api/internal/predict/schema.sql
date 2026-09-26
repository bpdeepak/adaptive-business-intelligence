-- Phase 2 serving contracts: gold.model_registry + gold.predictions.
--
-- SINGLE SOURCE OF TRUTH. Both sides load this exact file, so the two
-- implementations can never drift apart:
--   * Go     — api/internal/predict/schema.go embeds it (//go:embed schema.sql)
--   * Python — ml/common.py reads it (SCHEMA_SQL) and the trainers use it
--              to bootstrap the tables for `make train`.
--
-- Editing policy: change a column, index, or constraint HERE ONLY. Nothing
-- else in the repo may re-declare these two tables. Both CREATE statements are
-- idempotent (IF NOT EXISTS), so whichever side runs first (Go server bootstrap
-- or the trainers) creates the tables and the other sees them already present.

CREATE TABLE IF NOT EXISTS gold.model_registry (
    model_name text NOT NULL,
    model_version text NOT NULL,
    status text NOT NULL DEFAULT 'active',
    framework text NOT NULL,
    task text NOT NULL,
    grain text NOT NULL,
    artifact_path text NOT NULL,
    params jsonb NOT NULL DEFAULT '{}'::jsonb,
    metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    features jsonb NOT NULL DEFAULT '[]'::jsonb,
    drift_baseline jsonb NOT NULL DEFAULT '{}'::jsonb,
    trained_on jsonb NOT NULL DEFAULT '{}'::jsonb,
    trained_window jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (model_name, model_version)
);

CREATE TABLE IF NOT EXISTS gold.predictions (
    id bigserial PRIMARY KEY,
    model_name text NOT NULL,
    model_version text NOT NULL,
    grain text NOT NULL,
    entity_id text NOT NULL,
    predicted_at timestamptz NOT NULL DEFAULT now(),
    prediction double precision NOT NULL,
    confidence double precision NOT NULL DEFAULT 0,
    lower_bound double precision,
    upper_bound double precision,
    explanation jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    features jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Phase 4 migration for databases where the tables already exist: the two new
-- columns above are additive, so ALTER ... ADD COLUMN IF NOT EXISTS keeps both
-- the fresh-create and the upgrade path idempotent (same statement set every
-- boot, on either side).
ALTER TABLE gold.model_registry
    ADD COLUMN IF NOT EXISTS drift_baseline jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE gold.predictions
    ADD COLUMN IF NOT EXISTS features jsonb NOT NULL DEFAULT '[]'::jsonb;

CREATE INDEX IF NOT EXISTS idx_predictions_model_entity
    ON gold.predictions (model_name, entity_id, predicted_at DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_created
    ON gold.predictions (predicted_at DESC);

-- Phase 4 model monitoring: per-feature drift (PSI) and honest performance
-- one consistency check. `kind` is 'psi' (distribution drift) or 'backtest_repro'
-- (the forecast's registry backtest recomputed from its persisted held-out
-- predictions: warning-capped, NOT live decay; rows written before 2026-09-24
-- carry the old label 'forecast_decay'). A genuine decay kind needs realised
-- outcomes for served forecasts and does not exist yet.
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
);
CREATE INDEX IF NOT EXISTS idx_model_drift_model_computed
    ON gold.model_drift (model_name, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_model_drift_status
    ON gold.model_drift (status, computed_at DESC);
-- Phase 5 retention (policy: docs/phase4.md §11.21). Aged live-scored rows are
-- MOVED here, never deleted: model evidence is archived, the same principle as
-- the append-only audit log. Same columns as gold.predictions plus archived_at.
-- The move names its columns explicitly (predict/retention.go), and a test
-- compares both tables' column lists, so a column added to gold.predictions
-- cannot be silently dropped on archive.
CREATE TABLE IF NOT EXISTS gold.predictions_archive (LIKE gold.predictions);
ALTER TABLE gold.predictions_archive
    ADD COLUMN IF NOT EXISTS archived_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS idx_predictions_archive_model_entity
    ON gold.predictions_archive (model_name, entity_id, predicted_at DESC);
