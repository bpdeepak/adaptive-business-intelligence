package scorewriter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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

// State is the composite snapshot: rings + per-model rate traces + the consumer
// position the rings/rates correspond to. Offsets make the snapshot
// self-consistent with the stream: on restart the consumer resumes exactly where
// the state left off, and anything at or below these offsets that is delivered
// again (a commit that lagged the snapshot) is skipped rather than folded twice.
type State struct {
	Rings *RingsSnapshot        `json:"rings"`
	Rates map[string]*RateTrace `json:"rates"`
	// Offsets is the next offset to process per "topic|partition".
	Offsets map[string]int64 `json:"offsets,omitempty"`
}

// tp identifies one topic partition.
type tp struct {
	topic     string
	partition int32
}

func (p tp) key() string { return fmt.Sprintf("%s|%d", p.topic, p.partition) }

func parseTP(k string) (tp, bool) {
	i := strings.LastIndex(k, "|")
	if i < 0 {
		return tp{}, false
	}
	n, err := strconv.ParseInt(k[i+1:], 10, 32)
	if err != nil {
		return tp{}, false
	}
	return tp{topic: k[:i], partition: int32(n)}, true
}

// snapshot assembles the state under the position lock.
func (s *Service) snapshot() *State {
	rates := map[string]*RateTrace{}
	for name, rt := range s.rates {
		rates[name] = rt.Snapshot()
	}
	s.posMu.Lock()
	offs := make(map[string]int64, len(s.processed))
	for p, o := range s.processed {
		offs[p.key()] = o
	}
	s.posMu.Unlock()
	return &State{Rings: s.rings.Snapshot(), Rates: rates, Offsets: offs}
}

// restoreFrom applies a snapshot. Nil-safe on every part: a snapshot written by
// an older version (no offsets) or with no rings must still restore what it has.
func (s *Service) restoreFrom(st *State) {
	if st == nil {
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
	s.posMu.Lock()
	for k, o := range st.Offsets {
		if p, ok := parseTP(k); ok {
			s.resume[p] = o
			s.processed[p] = o
		}
	}
	s.posMu.Unlock()
}

// SnapshotState persists everything the score-writer resumes from and, only once
// that write succeeded, commits the matching consumer offsets - so the committed
// position never runs ahead of the durable state (auto-commit did, by up to a
// commit interval, silently dropping those events from the rings).
func (s *Service) SnapshotState(ctx context.Context) {
	st := s.snapshot()
	if err := s.state.Save(ctx, StateKeyRings, st); err != nil {
		s.log.Warn("scorewriter: state snapshot failed; offsets not committed", "error", err)
		return
	}
	s.commitOffsets(ctx, st.Offsets)
}

// commitOffsets commits the snapshot positions to the consumer group.
func (s *Service) commitOffsets(ctx context.Context, offs map[string]int64) {
	if s.cl == nil || len(offs) == 0 {
		return
	}
	commit := map[string]map[int32]kgo.EpochOffset{}
	for k, o := range offs {
		p, ok := parseTP(k)
		if !ok {
			continue
		}
		if commit[p.topic] == nil {
			commit[p.topic] = map[int32]kgo.EpochOffset{}
		}
		commit[p.topic][p.partition] = kgo.EpochOffset{Epoch: -1, Offset: o}
	}
	s.cl.CommitOffsetsSync(ctx, commit, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest,
		_ *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			s.log.Warn("scorewriter: offset commit failed", "error", err)
		}
	})
}

// RestoreState loads a previous snapshot into the rings, rate trackers and
// resume positions.
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
	s.restoreFrom(&st)
	customers, categories := 0, 0
	if st.Rings != nil {
		customers, categories = len(st.Rings.Customers), len(st.Rings.Categories)
	}
	s.log.Info("scorewriter state restored",
		"customers", customers, "categories", categories, "partitions_resumed", len(st.Offsets))
}
