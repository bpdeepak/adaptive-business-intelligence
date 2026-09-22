package predict

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"abi/internal/model"
)

// PredictionsFilter narrows a predictions listing. Empty fields are ignored.
type PredictionsFilter struct {
	Model  string
	Grain  string
	Entity string
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
ORDER BY predicted_at DESC, id DESC
LIMIT $4`

	rows, err := s.pool.Query(ctx, q, f.Model, f.Grain, f.Entity, f.Limit)
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