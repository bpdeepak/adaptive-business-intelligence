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
-- decay findings. Every row is one (model, feature, computed_at) measurement;
-- status follows the conventional PSI bands: ok (<0.1), warning (0.1–0.2),
-- critical (>0.2). `kind` distinguishes distribution drift ('psi') from the
-- two real-decay findings ('forecast_decay', 'churn_decay') so the Go ticker
-- and dashboard can tell "data drifted" from "ground truth arrived".
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