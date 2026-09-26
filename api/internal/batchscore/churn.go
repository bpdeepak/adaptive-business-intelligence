// Package batchscore hosts the Phase 5 batch producers: the churn scorer and the
// weekly demand-forecast worker. They are the producers the dormant Phase 4
// retention / purchase-order rules were waiting for.
//
// Delivery contract (the same one as the stream score-writer):
//   - persist to gold.predictions first (ScoreAndPersist), then publish on the
//     governance bus through the shared events builders;
//   - the bus is only the low-latency path; the governance reconciler
//     (api/internal/govern) rebuilds a dropped event from the persisted row.
//
// Honest time semantics: the dataset ends in 2018 and never moves, so the
// producers do not score "as of now()" on a timer (every recency feature would be
// years stale, and every tick would re-propose the same actions). They score a
// fixed population once per ACTIVE model version and resume where they stopped
// if interrupted; a retrained, promoted version triggers a fresh pass.
package batchscore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"abi/internal/events"
	"abi/internal/playbook"
	"abi/internal/predict"
)

// publishWait bounds how long a batch producer waits for a slow subscriber per
// event before counting a drop (the reconciler still recovers it).
const publishWait = 5 * time.Second

// ChurnModel is the registry/sidecar model the churn scorer drives.
const ChurnModel = "churn_risk"

// ChurnSource tags churn-scorer rows in gold.predictions.metadata.source.
const ChurnSource = "churn_score"

// Scorer is the slice of predict.Service the batch producers use (an interface
// so tests can fake the sidecar round trip).
type Scorer interface {
	Enabled() bool
	ScoreAndPersist(ctx context.Context, model, entity string, features map[string]float64,
		metadata json.RawMessage) (*predict.ScoreResponse, int64, error)
}

// ChurnScorer scores the churn population of gold.feature_customer_churn_current
// (repeat customers whose latest order is too recent for the outcome to be known)
// and publishes churn_scored events for the retention playbooks.
type ChurnScorer struct {
	pool    *pgxpool.Pool
	scorer  Scorer
	bus     *events.Bus
	log     *slog.Logger
	enabled bool
}

// NewChurnScorer builds the scorer. It only runs when an enabled playbook rule
// triggers on churn_scored: producer and policy arm together.
func NewChurnScorer(pool *pgxpool.Pool, scorer Scorer, bus *events.Bus, rules []playbook.Rule, log *slog.Logger) *ChurnScorer {
	return &ChurnScorer{pool: pool, scorer: scorer, bus: bus, log: log,
		enabled: Armed(rules, events.TypeChurnScored)}
}

// Armed reports whether any enabled rule triggers on t.
func Armed(rules []playbook.Rule, t events.Type) bool {
	for _, r := range rules {
		if r.Enabled && r.Trigger == string(t) {
			return true
		}
	}
	return false
}

// Run checks for work at boot and then every `every` (a poll for a newly
// promoted model version, not a re-scoring cadence).
func (c *ChurnScorer) Run(ctx context.Context, every time.Duration) error {
	if !c.enabled {
		c.log.Info("churn scorer idle: no enabled churn_scored playbook rule")
		return nil
	}
	if !c.scorer.Enabled() {
		c.log.Warn("churn scorer idle: model sidecar disabled (empty ABI_SCORE_URL)")
		return nil
	}
	pollLoop(ctx, every, RetryAfterFailure, func() error {
		n, err := c.Once(ctx)
		if err != nil {
			c.log.Warn("churn scorer pass failed; retrying soon", "error", err, "retry_in", RetryAfterFailure)
			return err
		}
		if n > 0 {
			c.log.Info("churn scorer pass complete", "scored", n)
		}
		return nil
	})
	return nil
}

// RetryAfterFailure is how soon a batch producer retries a failed pass (e.g. the
// model sidecar was still starting at boot), instead of waiting a full poll
// interval. A variable so tests can shorten it.
var RetryAfterFailure = time.Minute

// pollLoop runs tick now and then again after `every` when it succeeded, or after
// `retry` when it failed, until ctx is cancelled.
func pollLoop(ctx context.Context, every, retry time.Duration, tick func() error) {
	for {
		next := every
		if err := tick(); err != nil && retry < every {
			next = retry
		}
		t := time.NewTimer(next)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// Once scores every customer of the current population not yet scored by the
// active churn version, returning how many it scored. It stops at the first
// scoring error (a flapping sidecar must not half-run silently); already
// persisted rows are skipped on the next pass, so it resumes rather than
// re-scores.
func (c *ChurnScorer) Once(ctx context.Context) (int, error) {
	version, thr, pos, err := ActiveModel(ctx, c.pool, ChurnModel)
	if err != nil {
		return 0, err
	}
	rows, err := c.pool.Query(ctx, `
SELECT f.*
FROM gold.feature_customer_churn_current f
WHERE NOT EXISTS (
    SELECT 1 FROM gold.predictions p
    WHERE p.model_name = $1 AND p.model_version = $2
      AND p.metadata->>'source' = $3 AND p.entity_id = f.customer_unique_id)
ORDER BY f.customer_unique_id`, ChurnModel, version, ChurnSource)
	if err != nil {
		return 0, fmt.Errorf("query churn scoring population: %w", err)
	}
	type row struct {
		customer string
		asOf     time.Time
		features map[string]float64
	}
	var todo []row
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			rows.Close()
			return 0, fmt.Errorf("read churn features: %w", err)
		}
		r := row{features: map[string]float64{}}
		for i, fd := range rows.FieldDescriptions() {
			switch fd.Name {
			case "customer_unique_id":
				r.customer, _ = vals[i].(string)
			case "as_of_date":
				r.asOf, _ = vals[i].(time.Time)
			default:
				// Every other mart column is a model feature: the mart (one dbt
				// macro shared with training) is the contract, and the strict
				// sidecar refuses a vector missing any trained feature.
				f, err := toFloat(vals[i])
				if err != nil {
					rows.Close()
					return 0, fmt.Errorf("feature %s for %s: %w", fd.Name, r.customer, err)
				}
				r.features[fd.Name] = f
			}
		}
		todo = append(todo, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	scored := 0
	var fresh []int64
	for _, r := range todo {
		md, _ := json.Marshal(map[string]any{
			"source": ChurnSource, "grain": "customer",
			"as_of_date": r.asOf.Format("2006-01-02"),
		})
		_, predID, err := c.scorer.ScoreAndPersist(ctx, ChurnModel, r.customer, r.features, md)
		if err != nil {
			return scored, fmt.Errorf("score customer %s: %w", r.customer, err)
		}
		fresh = append(fresh, predID)
		scored++
	}
	// Events go out AFTER the pass: the rank (1 = highest risk) is only defined
	// once the whole population is scored. The persisted rows are durable
	// already; a crash here loses nothing (the reconciler rebuilds the events,
	// with the same rank, from the rows).
	if c.bus != nil && len(fresh) > 0 {
		ranked, err := RankedChurnScores(ctx, c.pool, version, fresh)
		if err != nil {
			return scored, err
		}
		for _, rs := range ranked {
			_ = c.bus.PublishWait(ctx, events.NewScored(events.TypeChurnScored, events.Scored{
				Model: ChurnModel, Version: version, Entity: rs.Entity,
				Score: rs.Score, Confidence: rs.Confidence, PredictionID: rs.ID,
				Threshold: thr, PositiveRate: pos, Rank: rs.Rank,
			}, time.Now().UTC()), publishWait)
		}
	}
	return scored, nil
}

// RankedScore is one persisted churn score with its rank in the version's
// scored population.
type RankedScore struct {
	ID         int64
	Entity     string
	Score      float64
	Confidence float64
	Rank       int
}

// churnRankSQL is THE rank definition, shared by the scorer (live events) and
// the governance reconciler (rebuilt events), so a reconciled event carries the
// same rank as the one the bus dropped: highest score first, ties by customer id.
const churnRankSQL = `
SELECT id, entity_id, prediction, confidence, rnk::int FROM (
    SELECT id, entity_id, prediction, confidence,
           row_number() OVER (ORDER BY prediction DESC, entity_id) AS rnk
    FROM gold.predictions
    WHERE model_name = 'churn_risk' AND model_version = $1
      AND metadata->>'source' = 'churn_score'
) ranked
WHERE id = ANY($2)
ORDER BY rnk`

// RankedChurnScores returns the given churn prediction rows with their rank
// within all churn_score rows of the same model version.
func RankedChurnScores(ctx context.Context, pool *pgxpool.Pool, version string, ids []int64) ([]RankedScore, error) {
	rows, err := pool.Query(ctx, churnRankSQL, version, ids)
	if err != nil {
		return nil, fmt.Errorf("rank churn scores: %w", err)
	}
	defer rows.Close()
	var out []RankedScore
	for rows.Next() {
		var r RankedScore
		if err := rows.Scan(&r.ID, &r.Entity, &r.Score, &r.Confidence, &r.Rank); err != nil {
			return nil, fmt.Errorf("scan ranked churn score: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ActiveModel returns the active registry version of a model and its threshold
// and floored positive-rate baseline. Both are nil when the registry does not
// carry a usable threshold, so the published event omits them and rules that
// compare against them fail closed (the same rule as the stream score-writer).
func ActiveModel(ctx context.Context, pool *pgxpool.Pool, model string) (string, *float64, *float64, error) {
	var version string
	var thr, pos *float64
	err := pool.QueryRow(ctx, `
SELECT model_version, (metrics->>'recommended_threshold')::float8, (metrics->>'positive_rate')::float8
FROM gold.model_registry
WHERE model_name = $1 AND status = 'active'
ORDER BY created_at DESC LIMIT 1`, model).Scan(&version, &thr, &pos)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil, nil, fmt.Errorf("no active %s model in gold.model_registry", model)
	}
	if err != nil {
		return "", nil, nil, fmt.Errorf("read active %s model: %w", model, err)
	}
	if thr == nil || *thr <= 0 {
		return version, nil, nil, nil
	}
	base := events.BaselineRate(0)
	if pos != nil {
		base = events.BaselineRate(*pos)
	}
	return version, thr, &base, nil
}

func toFloat(v any) (float64, error) {
	switch t := v.(type) {
	case nil:
		return 0, fmt.Errorf("NULL feature")
	case float64:
		return t, nil
	case float32:
		return float64(t), nil
	case int64:
		return float64(t), nil
	case int32:
		return float64(t), nil
	case int16:
		return float64(t), nil
	case pgtype.Numeric:
		f, err := t.Float64Value()
		if err != nil || !f.Valid {
			return 0, fmt.Errorf("numeric not representable")
		}
		return f.Float64, nil
	}
	return 0, fmt.Errorf("unsupported feature type %T", v)
}
