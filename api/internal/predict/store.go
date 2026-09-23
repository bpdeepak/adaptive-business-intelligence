package predict

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"abi/internal/model"
)

// PredictionsFilter narrows a predictions listing. Empty fields are ignored.
type PredictionsFilter struct {
	Model  string
	Grain  string
	Entity string
	Source string // matches metadata->>'source' (e.g. "stream_score", "live_score")
	Limit  int
}

// InsertPredictionParams carries one explainable prediction to persist.
type InsertPredictionParams struct {
	ModelName    string
	ModelVersion string
	Grain        string
	EntityID     string
	Prediction   float64
	Confidence   float64
	LowerBound   *float64
	UpperBound   *float64
	Explanation  json.RawMessage
	Metadata     json.RawMessage
}

const predictionCols = `
    id, model_name, model_version, grain, entity_id, predicted_at,
    prediction, confidence, lower_bound, upper_bound, explanation, metadata`

// ListRegistry returns every registered model version, newest last.
func (s *Service) ListRegistry(ctx context.Context) ([]model.ModelRegistryEntry, error) {
	const q = `
SELECT model_name, model_version, status, framework, task, grain,
       artifact_path, features, metrics, trained_window, created_at
FROM gold.model_registry
ORDER BY model_name, created_at`

	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("list registry: %w", err)
	}
	defer rows.Close()

	out := make([]model.ModelRegistryEntry, 0, 8)
	for rows.Next() {
		var e model.ModelRegistryEntry
		var features, metrics, trainedWindow []byte
		var createdAt time.Time
		if err := rows.Scan(&e.ModelName, &e.ModelVersion, &e.Status, &e.Framework,
			&e.Task, &e.Grain, &e.ArtifactPath, &features, &metrics,
			&trainedWindow, &createdAt); err != nil {
			return nil, fmt.Errorf("scan registry: %w", err)
		}
		if len(features) > 0 {
			_ = json.Unmarshal(features, &e.Features)
		}
		e.Metrics = json.RawMessage(metrics)
		e.TrainedWindow = json.RawMessage(trainedWindow)
		e.CreatedAt = createdAt.UTC().Format(time.RFC3339)
		out = append(out, e)
	}
	return out, rows.Err()
}

// ListPredictions returns recent predictions matching the filter.
func (s *Service) ListPredictions(ctx context.Context, f PredictionsFilter) ([]model.Prediction, error) {
	if f.Limit <= 0 || f.Limit > 1000 {
		f.Limit = 200
	}
	const q = `
SELECT` + predictionCols + `
FROM gold.predictions
WHERE ($1 = '' OR model_name = $1)
  AND ($2 = '' OR grain = $2)
  AND ($3 = '' OR entity_id = $3)
  AND ($4 = '' OR metadata->>'source' = $4)
ORDER BY predicted_at DESC, id DESC
LIMIT $5`

	rows, err := s.pool.Query(ctx, q, f.Model, f.Grain, f.Entity, f.Source, f.Limit)
	if err != nil {
		return nil, fmt.Errorf("list predictions: %w", err)
	}
	defer rows.Close()
	return scanPredictions(rows)
}

// LatestPredictions returns the newest prediction per (model, entity), so the
// dashboard shows a current risk board rather than a history.
func (s *Service) LatestPredictions(ctx context.Context, modelName string, limit int) ([]model.Prediction, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	const q = `
SELECT` + predictionCols + `
FROM (
    SELECT DISTINCT ON (model_name, entity_id)` + predictionCols + `
    FROM gold.predictions
    WHERE ($1 = '' OR model_name = $1)
    ORDER BY model_name, entity_id, predicted_at DESC
) latest
ORDER BY predicted_at DESC
LIMIT $2`

	rows, err := s.pool.Query(ctx, q, modelName, limit)
	if err != nil {
		return nil, fmt.Errorf("latest predictions: %w", err)
	}
	defer rows.Close()
	return scanPredictions(rows)
}

// InsertPrediction persists one scored prediction and returns its id.
func (s *Service) InsertPrediction(ctx context.Context, p InsertPredictionParams) (int64, error) {
	explanation := p.Explanation
	if len(explanation) == 0 {
		explanation = json.RawMessage(`{}`)
	}
	metadata := p.Metadata
	if len(metadata) == 0 {
		metadata = json.RawMessage(`{}`)
	}
	const q = `
INSERT INTO gold.predictions
    (model_name, model_version, grain, entity_id, prediction, confidence,
     lower_bound, upper_bound, explanation, metadata)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb)
RETURNING id`

	var id int64
	err := s.pool.QueryRow(ctx, q, p.ModelName, p.ModelVersion, p.Grain, p.EntityID,
		p.Prediction, p.Confidence, p.LowerBound, p.UpperBound,
		string(explanation), string(metadata)).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert prediction: %w", err)
	}
	return id, nil
}

// ForecastPoint is one forecast-week row for the dashboard's confidence band:
// the weekly orders/revenue forecast for the category plus the observed actual
// when the row came from a backtest (metadata.batch = 'backtest').
type ForecastPoint struct {
	Week          string   `json:"week_start"`
	Orders        *float64 `json:"orders,omitempty"`
	OrdersActual  *float64 `json:"orders_actual,omitempty"`
	Revenue       *float64 `json:"revenue,omitempty"`
	RevenueActual *float64 `json:"revenue_actual,omitempty"`
}

// ForecastRow is the "current" forecast for one model family (latest week),
// with a ±MAE band carved from the model's own validation metrics (the band
// the dashboard draws; the models do not persist per-week quantile intervals).
type ForecastRow struct {
	ModelVersion string  `json:"model_version"`
	Week         string  `json:"week_start"`
	Prediction   float64 `json:"prediction"`
	LowerBound   float64 `json:"lower_bound"`
	UpperBound   float64 `json:"upper_bound"`
	Mae          float64 `json:"mae"`
	Wmape        float64 `json:"wmape"`
}

// ForecastSummary merges both forecast model families for one category.
type ForecastSummary struct {
	Category string           `json:"category"`
	Orders   []ForecastRow    `json:"orders"`   // latest week per forecast family
	Revenue  []ForecastRow    `json:"revenue"`
	Series   []ForecastPoint  `json:"series"`   // week-by-week backtest history
}

// Groups of the two forecast model names by output role.
const (
	modelForecastOrders  = "forecast_category_weekly_orders"
	modelForecastRevenue = "forecast_category_weekly_revenue"
)

// ForecastByCategory returns the merged forecast series + latest-point bands
// for one category (entity_id is "{category}@{week_start}").
func (s *Service) ForecastByCategory(ctx context.Context, category string) (*ForecastSummary, error) {
	rows, err := s.pool.Query(ctx, `
SELECT model_name, entity_id, prediction::float8, metadata
FROM gold.predictions
WHERE model_name IN ('forecast_category_weekly_orders', 'forecast_category_weekly_revenue')
  AND entity_id LIKE $1 || '@%'
ORDER BY entity_id`, category)
	if err != nil {
		return nil, fmt.Errorf("forecast rows: %w", err)
	}
	defer rows.Close()

	// family errors surfaced uniformly; registry metrics supply the band.
	maes, err := s.forecastMetrics(ctx)
	if err != nil {
		return nil, err
	}

	out := &ForecastSummary{Category: category}
	byWeek := map[string]*ForecastPoint{}
	latestOrders, latestRevenue := "", ""
	for rows.Next() {
		var modelName, entityID string
		var prediction float64
		var md []byte
		if err := rows.Scan(&modelName, &entityID, &prediction, &md); err != nil {
			return nil, fmt.Errorf("scan forecast row: %w", err)
		}
		week := categoryWeek(entityID)
		if week == "" {
			continue
		}
		pt := byWeek[week]
		if pt == nil {
			pt = &ForecastPoint{Week: week}
			byWeek[week] = pt
		}
		var m struct {
			Actual *float64 `json:"actual"`
		}
		_ = json.Unmarshal(md, &m)
		switch modelName {
		case modelForecastOrders:
			v := prediction
			pt.Orders = &v
			pt.OrdersActual = m.Actual
			if week > latestOrders {
				latestOrders = week
			}
		case modelForecastRevenue:
			v := prediction
			pt.Revenue = &v
			pt.RevenueActual = m.Actual
			if week > latestRevenue {
				latestRevenue = week
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, pt := range byWeek {
		out.Series = append(out.Series, *pt)
	}
	sort.Slice(out.Series, func(i, j int) bool { return out.Series[i].Week < out.Series[j].Week })
	out.Orders = buildForecastRows(category, byWeek, latestOrders, maes[modelForecastOrders])
	out.Revenue = buildForecastRows(category, byWeek, latestRevenue, maes[modelForecastRevenue])
	return out, nil
}

// forecastMetrics loads mae + per-category wmape for both forecast families
// from the ACTIVE registry rows (the validation artifacts shipped with them).
func (s *Service) forecastMetrics(ctx context.Context) (map[string]forecastMae, error) {
	rows, err := s.pool.Query(ctx, `
SELECT model_name, metrics FROM gold.model_registry
WHERE model_name IN ('forecast_category_weekly_orders', 'forecast_category_weekly_revenue')
  AND status = 'active'`)
	if err != nil {
		return nil, fmt.Errorf("forecast metrics: %w", err)
	}
	defer rows.Close()
	out := map[string]forecastMae{}
	for rows.Next() {
		var name string
		var raw []byte
		if err := rows.Scan(&name, &raw); err != nil {
			return nil, fmt.Errorf("scan forecast metrics: %w", err)
		}
		var m struct {
			Mae   float64            `json:"mae"`
			Wmape map[string]float64 `json:"wmape_by_category"`
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			continue
		}
		out[name] = forecastMae{Mae: m.Mae, WmapeByCategory: m.Wmape}
	}
	return out, rows.Err()
}

type forecastMae struct {
	Mae            float64
	WmapeByCategory map[string]float64
}

// buildForecastRows yields the latest-week forecast for one family with a ±MAE
// band (floored at 0, the band the dashboard draws) and the model's
// per-category wmape as the uncertainty label.
func buildForecastRows(category string, byWeek map[string]*ForecastPoint, latest string, m forecastMae) []ForecastRow {
	if latest == "" {
		return nil
	}
	pt := byWeek[latest]
	var value float64
	hasValue := false
	if pt.Orders != nil {
		value, hasValue = *pt.Orders, true
	}
	if pt.Revenue != nil {
		value, hasValue = *pt.Revenue, true
	}
	if !hasValue {
		return nil
	}
	lo, hi := value-m.Mae, value+m.Mae
	if lo < 0 {
		lo = 0
	}
	return []ForecastRow{{
		Week: latest, Prediction: round2(value),
		LowerBound: round2(lo), UpperBound: round2(hi),
		Mae: m.Mae, Wmape: m.WmapeByCategory[category],
	}}
}

// categoryWeek extracts the week_start suffix from "{category}@{YYYY-MM-DD}".
func categoryWeek(entityID string) string {
	i := strings.LastIndex(entityID, "@")
	if i < 0 || i == len(entityID)-1 {
		return ""
	}
	return entityID[i+1:]
}

// round2 rounds to 2 decimals for JSON display (forecast bands).
func round2(v float64) float64 {
	if math.IsInf(v, 0) || math.IsNaN(v) {
		return 0
	}
	return math.Round(v*100) / 100
}

// ActiveModelCount counts models with at least one active version (for gauges).
func (s *Service) ActiveModelCount(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `
SELECT COUNT(DISTINCT model_name)::int
FROM gold.model_registry
WHERE status = 'active'`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("active model count: %w", err)
	}
	return n, nil
}

type rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func scanPredictions(rows rows) ([]model.Prediction, error) {
	out := make([]model.Prediction, 0, 64)
	for rows.Next() {
		var p model.Prediction
		var predictedAt time.Time
		var lower, upper *float64
		var explanation, metadata []byte
		if err := rows.Scan(&p.ID, &p.ModelName, &p.ModelVersion, &p.Grain, &p.EntityID,
			&predictedAt, &p.Prediction, &p.Confidence, &lower, &upper,
			&explanation, &metadata); err != nil {
			return nil, fmt.Errorf("scan prediction: %w", err)
		}
		p.PredictedAt = predictedAt.UTC().Format(time.RFC3339)
		p.LowerBound, p.UpperBound = lower, upper
		p.Explanation = json.RawMessage(explanation)
		p.Metadata = json.RawMessage(metadata)
		out = append(out, p)
	}
	return out, rows.Err()
}