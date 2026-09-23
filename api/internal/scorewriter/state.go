package scorewriter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ScoreWriterKeys identify the state rows in gold.scorewriter_state.
const (
	StateKeyRings       = "rings"
	StateKeyRateFraud   = "ratetrack.fraud_risk"
	StateKeyRateBot     = "ratetrack.bot_score"
)

// scorewriterStateDDL is the restart-state table for the stream score-writer.
// Declared as a constant so a text test can pin it (the failure mode it guards
// against is dropping the Phase 3 snapshot contract while refactoring).
const scorewriterStateDDL = `
CREATE TABLE IF NOT EXISTS gold.scorewriter_state (
	key        text PRIMARY KEY,
	payload    jsonb NOT NULL,
	updated_at timestamptz NOT NULL DEFAULT now()
)`

// EnsureSchema creates the score-writer's own restart-state table. Like
// gold.detector_state (Phase 1), it lets a server restart resume the velocity /
// benchmark rings and rate windows instead of re-warming from an empty history
// — which would make the first hours of scoring garbage next to the batch
// references the parity test compares against.
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, scorewriterStateDDL)
	if err != nil {
		return fmt.Errorf("ensure scorewriter_state: %w", err)
	}
	return nil
}

// stateStore wraps the persistence of one state key.
type stateStore struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

// Save upserts one key's JSON payload.
func (s *stateStore) Save(ctx context.Context, key string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s state: %w", key, err)
	}
	if _, err := s.pool.Exec(ctx, `
INSERT INTO gold.scorewriter_state (key, payload, updated_at)
VALUES ($1, $2::jsonb, now())
ON CONFLICT (key) DO UPDATE SET payload = EXCLUDED.payload, updated_at = now()`,
		key, raw); err != nil {
		return fmt.Errorf("save %s state: %w", key, err)
	}
	return nil
}

// Load reads one key's raw payload; not-found returns nil bytes.
func (s *stateStore) Load(ctx context.Context, key string) ([]byte, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `
SELECT payload::text FROM gold.scorewriter_state WHERE key = $1`, key).Scan(&raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("load %s state: %w", key, err)
	}
	return raw, nil
}

// State is the composite snapshot: rings + per-model rate traces.
type State struct {
	Rings *RingsSnapshot           `json:"rings"`
	Rates map[string]*RateTrace    `json:"rates"`
}

// SnapshotState collects everything the score-writer wants to resume from.
func (s *Service) SnapshotState(ctx context.Context) {
	rings := s.rings.Snapshot()
	rates := map[string]*RateTrace{}
	for name, rt := range s.rates {
		rates[name] = rt.Snapshot()
	}
	state := &State{Rings: rings, Rates: rates}
	if err := s.state.Save(ctx, StateKeyRings, state); err != nil {
		s.log.Debug("scorewriter: state snapshot failed", "error", err)
	}
}

// RestoreState loads a previous snapshot into the rings and rate trackers.
func (s *Service) RestoreState(ctx context.Context) {
	raw, err := s.state.Load(ctx, StateKeyRings)
	if err != nil {
		s.log.Warn("scorewriter: state restore failed", "error", err)
		return
	}
	if len(raw) == 0 {
		return
	}
	var st State
	if err := json.Unmarshal(raw, &st); err != nil {
		s.log.Warn("scorewriter: state restore parse failed", "error", err)
		return
	}
	if st.Rings != nil {
		s.rings.Restore(st.Rings)
	}
	for name, trace := range st.Rates {
		if rt, ok := s.rates[name]; ok {
			rt.Restore(trace)
		}
	}
	s.log.Info("scorewriter state restored",
		"customers", len(st.Rings.Customers),
		"categories", len(st.Rings.Categories))
}