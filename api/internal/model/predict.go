package model

import "encoding/json"

// ModelRegistryEntry is one versioned model in gold.model_registry — the
// durable contract between the Python training pipeline and the Go serving API.
type ModelRegistryEntry struct {
	ModelName     string          `json:"model_name"`
	ModelVersion  string          `json:"model_version"`
	Status        string          `json:"status"`
	Framework     string          `json:"framework"`
	Task          string          `json:"task"`
	Grain         string          `json:"grain"`
	ArtifactPath  string          `json:"artifact_path"`
	Features      []string        `json:"features"`
	Metrics       json.RawMessage `json:"metrics,omitempty"`
	TrainedWindow json.RawMessage `json:"trained_window,omitempty"`
	CreatedAt     string          `json:"created_at"`
}

// Prediction is one explainable model output from gold.predictions. The
// explanation payload always carries the SHAP feature contributions for
// classifiers (and the global ranking for forecasts).
type Prediction struct {
	ID           int64           `json:"id"`
	ModelName    string          `json:"model_name"`
	ModelVersion string          `json:"model_version"`
	Grain        string          `json:"grain"`
	EntityID     string          `json:"entity_id"`
	PredictedAt  string          `json:"predicted_at"`
	Prediction   float64         `json:"prediction"`
	Confidence   float64         `json:"confidence"`
	LowerBound   *float64        `json:"lower_bound,omitempty"`
	UpperBound   *float64        `json:"upper_bound,omitempty"`
	Explanation  json.RawMessage `json:"explanation,omitempty"`
	Metadata     json.RawMessage `json:"metadata,omitempty"`
}

// ScoreRequest is the POST /api/v1/score body: score one entity through the
// model sidecar and persist the explainable output to gold.predictions.
type ScoreRequest struct {
	Model    string             `json:"model"`
	EntityID string             `json:"entity_id"`
	Features map[string]float64 `json:"features"`
}