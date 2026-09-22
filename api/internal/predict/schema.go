// Package predict is the Phase 2 serving layer: it owns the gold.model_registry
// and gold.predictions contracts, queries them for the HTTP API, and brokers
// live scoring through the Python model sidecar (ml/serve.py) so the Go API
// stays stateless about how a model is implemented.
package predict

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaDDL is the single source of truth for the Phase 2 serving tables
// (gold.model_registry, gold.predictions and their indexes), embedded from
// schema.sql in this package. ml/common.py loads the SAME file, so the Go API
// and the Python trainers can never drift apart; edit schema.sql only.
// Both CREATE statements are idempotent (IF NOT EXISTS), so whichever side
// runs first creates the tables and the other sees them present.
//
//go:embed schema.sql
var schemaDDL string

// EnsureSchema creates the Phase 2 serving tables if they do not exist.
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, schemaDDL); err != nil {
		return fmt.Errorf("ensure predict schema: %w", err)
	}
	return nil
}