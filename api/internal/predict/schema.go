// Package predict is the Phase 2 serving layer: it owns the gold.model_registry
// and gold.predictions contracts, queries them for the HTTP API, and brokers
// live scoring through the Python model sidecar (ml/serve.py) so the Go API
// stays stateless about how a model is implemented.
package predict

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaDDL mirrors ml/common.py (MODEL_REGISTRY_DDL / PREDICTIONS_DDL). Either
// side may create the tables first; both statements are idempotent and the
// column sets are kept identical.
const schemaDDL = `
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
`

// EnsureSchema creates the Phase 2 serving tables if they do not exist.
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, schemaDDL); err != nil {
		return fmt.Errorf("ensure predict schema: %w", err)
	}
	return nil
}