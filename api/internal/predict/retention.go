package predict

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Retention moves aged live-scored predictions from gold.predictions to
// gold.predictions_archive (policy: docs/phase4.md §11.21). It archives, never
// deletes, and it never moves a row that anything could still need:
//
//   - only sources with a hot window are candidates: 'stream_score' (the Phase 3
//     score-writer) and 'live_score' (POST /api/v1/score). Trainer held-out rows,
//     backtests and the Phase 5 batch sources (churn_score / forecast_score, a
//     fixed small population per model version) stay hot;
//   - a row must be at or below the governance reconciler's prediction cursor, so
//     it cannot vanish before the backstop ever considered it for a proposal
//     (no cursor yet = nothing is eligible);
//   - a row cited as the trigger of a still-pending or failed action stays hot
//     until that action is decided or succeeds.
//
// "Aged" is measured on created_at (when the row was PERSISTED), never on event
// time: the replay's event times are simulated (see docs/phase1.md §4.3).
type Retention struct {
	pool *pgxpool.Pool
	log  *slog.Logger
	// Hot windows per source.
	StreamScoreKeep time.Duration
	LiveScoreKeep   time.Duration
}

// retentionChunk bounds one move statement (the same discipline as the realtime
// writers); a variable so tests can force several chunks.
var retentionChunk = 1000

// archiveColumns is the explicit column list of the move: every gold.predictions
// column. retention_integration_test.go asserts it equals the live table's
// columns, so a column added to gold.predictions cannot be silently dropped.
const archiveColumns = `id, model_name, model_version, grain, entity_id, predicted_at, prediction,
       confidence, lower_bound, upper_bound, explanation, metadata, features, created_at`

// NewRetention builds the job with the policy's default windows (30 / 90 days).
func NewRetention(pool *pgxpool.Pool, log *slog.Logger) *Retention {
	return &Retention{pool: pool, log: log,
		StreamScoreKeep: 30 * 24 * time.Hour, LiveScoreKeep: 90 * 24 * time.Hour}
}

// RetentionResult summarises one run.
type RetentionResult struct {
	Moved  int64
	MinID  int64
	MaxID  int64
	Chunks int
}

// Once archives every eligible row, chunk by chunk (each chunk is one atomic
// statement: delete-returning feeding the archive insert), then writes one
// gold.event_log summary row. A failed chunk leaves the rows where they were.
func (r *Retention) Once(ctx context.Context) (RetentionResult, error) {
	var res RetentionResult
	for {
		var n, lo, hi int64
		err := r.pool.QueryRow(ctx, `
WITH cursor_row AS (
    SELECT COALESCE((value->>'predictions')::bigint, 0) AS max_id
    FROM gold.governance_state WHERE key = 'reconcile'
),
cited AS (
    SELECT DISTINCT (q.trigger #>> '{payload,prediction,id}')::bigint AS id
    FROM gold.action_queue q
    WHERE q.status IN ('pending', 'failed')
      AND q.trigger #>> '{payload,prediction,id}' ~ '^[0-9]+$'
),
victims AS (
    SELECT p.id FROM gold.predictions p, cursor_row c
    WHERE p.id <= c.max_id
      AND (   (p.metadata->>'source' = 'stream_score' AND p.created_at < now() - make_interval(secs => $1))
           OR (p.metadata->>'source' = 'live_score'   AND p.created_at < now() - make_interval(secs => $2)))
      AND NOT EXISTS (SELECT 1 FROM cited WHERE cited.id = p.id)
    ORDER BY p.id
    LIMIT $3
    FOR UPDATE SKIP LOCKED
),
moved AS (
    DELETE FROM gold.predictions p USING victims v WHERE p.id = v.id
    RETURNING p.`+archiveColumns+`
),
archived AS (
    INSERT INTO gold.predictions_archive (`+archiveColumns+`)
    SELECT `+archiveColumns+` FROM moved
    RETURNING id
)
SELECT count(*), COALESCE(min(id), 0), COALESCE(max(id), 0) FROM archived`,
			r.StreamScoreKeep.Seconds(), r.LiveScoreKeep.Seconds(), retentionChunk).Scan(&n, &lo, &hi)
		if err != nil {
			return res, fmt.Errorf("archive predictions chunk: %w", err)
		}
		if n == 0 {
			break
		}
		res.Moved += n
		res.Chunks++
		if res.MinID == 0 || lo < res.MinID {
			res.MinID = lo
		}
		if hi > res.MaxID {
			res.MaxID = hi
		}
	}
	// One accounting row per run (also for a run that moved nothing), so the
	// archive can be reconciled against the hot table.
	detail, _ := json.Marshal(map[string]any{
		"table": "gold.predictions", "archive": "gold.predictions_archive",
		"moved": res.Moved, "chunks": res.Chunks, "min_id": res.MinID, "max_id": res.MaxID,
		"stream_score_keep": r.StreamScoreKeep.String(), "live_score_keep": r.LiveScoreKeep.String(),
	})
	if _, err := r.pool.Exec(ctx, `
INSERT INTO gold.event_log (kind, entity, detail) VALUES ('retention', 'gold.predictions', $1)`,
		string(detail)); err != nil {
		return res, fmt.Errorf("log retention run: %w", err)
	}
	return res, nil
}

// Run archives once at start and then every `every` until ctx is cancelled.
func (r *Retention) Run(ctx context.Context, every time.Duration) {
	tick := func() {
		res, err := r.Once(ctx)
		if err != nil {
			r.log.Warn("predictions retention failed; rows left in place", "error", err)
			return
		}
		if res.Moved > 0 {
			r.log.Info("predictions archived", "moved", res.Moved, "chunks", res.Chunks,
				"min_id", res.MinID, "max_id", res.MaxID)
		}
	}
	tick()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tick()
		}
	}
}
