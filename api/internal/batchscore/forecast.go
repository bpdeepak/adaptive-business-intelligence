package batchscore

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"abi/internal/events"
	"abi/internal/playbook"
)

// ForecastModel is the model the forecast worker drives: weekly ORDERS per
// category. A purchase-order decision is about demand in units, so the orders
// model (not the revenue model) is the relevant forecast.
const ForecastModel = "forecast_category_weekly_orders"

// ForecastSource tags forecast-worker rows in gold.predictions.metadata.source.
const ForecastSource = "forecast_score"

// ForecastWorker produces the next-week demand forecast per category from
// gold.feature_forecast_next_week (the week after the data's last complete
// week) and publishes forecast_updated for the purchase-order playbook.
type ForecastWorker struct {
	pool    *pgxpool.Pool
	scorer  Scorer
	bus     *events.Bus
	log     *slog.Logger
	enabled bool
}

// NewForecastWorker builds the worker; it runs only when an enabled rule
// triggers on forecast_updated.
func NewForecastWorker(pool *pgxpool.Pool, scorer Scorer, bus *events.Bus, rules []playbook.Rule, log *slog.Logger) *ForecastWorker {
	return &ForecastWorker{pool: pool, scorer: scorer, bus: bus, log: log,
		enabled: Armed(rules, events.TypeForecastUpdated)}
}

// Run checks for work at boot and then every `every` (a poll for a newly
// promoted forecast version; the forecast week is fixed by the dataset).
func (w *ForecastWorker) Run(ctx context.Context, every time.Duration) error {
	if !w.enabled {
		w.log.Info("forecast worker idle: no enabled forecast_updated playbook rule")
		return nil
	}
	if !w.scorer.Enabled() {
		w.log.Warn("forecast worker idle: model sidecar disabled (empty ABI_SCORE_URL)")
		return nil
	}
	pollLoop(ctx, every, RetryAfterFailure, func() error {
		n, skipped, err := w.Once(ctx)
		if err != nil {
			w.log.Warn("forecast worker pass failed; retrying soon", "error", err, "retry_in", RetryAfterFailure)
			return err
		}
		if n > 0 || skipped > 0 {
			w.log.Info("forecast worker pass complete", "forecast", n, "skipped_unranked_categories", skipped)
		}
		return nil
	})
	return nil
}

// Once forecasts every category of the next-week mart not yet forecast by the
// active version. Categories the model never trained on (not in its
// category_rank: too little history) are skipped and counted, never guessed.
func (w *ForecastWorker) Once(ctx context.Context) (forecast, skipped int, err error) {
	version, _, _, err := ActiveModel(ctx, w.pool, ForecastModel)
	if err != nil {
		return 0, 0, err
	}
	var featJSON, metricsJSON []byte
	if err := w.pool.QueryRow(ctx, `
SELECT features, metrics FROM gold.model_registry WHERE model_name = $1 AND model_version = $2`,
		ForecastModel, version).Scan(&featJSON, &metricsJSON); err != nil {
		return 0, 0, fmt.Errorf("read forecast registry row: %w", err)
	}
	var featureNames []string
	if err := json.Unmarshal(featJSON, &featureNames); err != nil || len(featureNames) == 0 {
		return 0, 0, fmt.Errorf("forecast registry row has no feature list")
	}
	var metrics struct {
		CategoryRank map[string]int `json:"category_rank"`
	}
	_ = json.Unmarshal(metricsJSON, &metrics)

	rows, err := w.pool.Query(ctx, `
SELECT n.*
FROM gold.feature_forecast_next_week n
WHERE NOT EXISTS (
    SELECT 1 FROM gold.predictions p
    WHERE p.model_name = $1 AND p.model_version = $2 AND p.metadata->>'source' = $3
      AND p.entity_id = n.category || '@' || to_char(n.week_start, 'YYYY-MM-DD'))
ORDER BY n.category`, ForecastModel, version, ForecastSource)
	if err != nil {
		return 0, 0, fmt.Errorf("query next-week forecast rows: %w", err)
	}
	type row struct {
		category, week string
		recentAvg      float64
		cols           map[string]any
	}
	var todo []row
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			rows.Close()
			return 0, 0, err
		}
		r := row{cols: map[string]any{}}
		for i, fd := range rows.FieldDescriptions() {
			r.cols[fd.Name] = vals[i]
		}
		r.category, _ = r.cols["category"].(string)
		if ws, ok := r.cols["week_start"].(time.Time); ok {
			r.week = ws.Format("2006-01-02")
		}
		if v := r.cols["recent_orders_avg"]; v != nil {
			r.recentAvg, _ = toFloat(v)
		}
		todo = append(todo, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, 0, err
	}

	for _, r := range todo {
		code, ok := metrics.CategoryRank[r.category]
		if !ok {
			skipped++
			continue
		}
		// The registry's feature list is the contract: every trained feature must
		// come from the mart (plus category_code, derived from the training-time
		// rank). A NULL lag (a series younger than the lag) is 0, exactly as the
		// trainer's fillna(0) treated it.
		features := make(map[string]float64, len(featureNames))
		for _, name := range featureNames {
			if name == "category_code" {
				features[name] = float64(code)
				continue
			}
			v, present := r.cols[name]
			if !present {
				return forecast, skipped, fmt.Errorf("feature %q is not in gold.feature_forecast_next_week", name)
			}
			if v == nil {
				features[name] = 0
				continue
			}
			if b, isBool := v.(bool); isBool {
				features[name] = map[bool]float64{true: 1, false: 0}[b]
				continue
			}
			f, err := toFloat(v)
			if err != nil {
				return forecast, skipped, fmt.Errorf("feature %s for %s: %w", name, r.category, err)
			}
			features[name] = f
		}
		entity := r.category + "@" + r.week
		md, _ := json.Marshal(map[string]any{
			"source": ForecastSource, "grain": "category_week",
			"category": r.category, "week": r.week,
			"recent_avg": r.recentAvg, "horizon_weeks": 1,
		})
		resp, _, err := w.scorer.ScoreAndPersist(ctx, ForecastModel, entity, features, md)
		if err != nil {
			return forecast, skipped, fmt.Errorf("forecast %s: %w", entity, err)
		}
		if w.bus != nil {
			_ = w.bus.PublishWait(ctx, events.NewForecastUpdated(events.Forecast{
				Category: r.category, Week: r.week,
				PointEstimate: resp.Prediction, RecentAvg: r.recentAvg,
			}, time.Now().UTC()), publishWait)
		}
		forecast++
	}
	return forecast, skipped, nil
}

