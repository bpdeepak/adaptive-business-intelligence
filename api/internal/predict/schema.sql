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
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_predictions_model_entity
    ON gold.predictions (model_name, entity_id, predicted_at DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_created
    ON gold.predictions (predicted_at DESC);