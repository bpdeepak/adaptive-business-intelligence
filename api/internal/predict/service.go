package predict

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"abi/internal/model"
	"abi/internal/telemetry"
)

// Service serves the Phase 2 prediction API: the model registry, persisted
// explainable predictions, and live scoring through the sidecar.
type Service struct {
	pool   *pgxpool.Pool
	client *ScoreClient
	log    *slog.Logger
	reg    *telemetry.Registry
	now    func() time.Time
}

// NewService builds the serving layer. `client` may be nil to disable scoring.
func NewService(pool *pgxpool.Pool, client *ScoreClient, log *slog.Logger) *Service {
	return &Service{pool: pool, client: client, log: log, now: time.Now}
}

// Instrument attaches the operational registry (nil-safe; optional).
func (s *Service) Instrument(reg *telemetry.Registry) { s.reg = reg }

// Register wires the Phase 2 endpoints into an existing mux.
func (s *Service) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/model-registry", s.handleRegistry)
	mux.HandleFunc("GET /api/v1/predictions", s.handlePredictions)
	mux.HandleFunc("GET /api/v1/predictions/latest", s.handleLatestPredictions)
	mux.HandleFunc("POST /api/v1/score", s.handleScore)
	mux.HandleFunc("GET /api/v1/models/health", s.handleModelsHealth)
}

// RunGaugeLoop refreshes model-serving gauges on a schedule.
func (s *Service) RunGaugeLoop(ctx context.Context, every time.Duration) {
	if every <= 0 {
		every = 30 * time.Second
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.RefreshGauges(ctx)
		}
	}
}

// RefreshGauges updates abi_models_active and abi_model_sidecar_up.
func (s *Service) RefreshGauges(ctx context.Context) {
	if n, err := s.ActiveModelCount(ctx); err == nil {
		s.reg.Gauge("abi_models_active", "Registered model families with an active version.").Set(float64(n))
	}
	up := 0.0
	if s.client.Health(ctx) == nil {
		up = 1
	}
	s.reg.Gauge("abi_model_sidecar_up", "1 when the Python model sidecar answers /health.").Set(up)
}

func (s *Service) handleRegistry(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	entries, err := s.ListRegistry(r.Context())
	if err != nil {
		s.log.Error("model_registry", "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	s.writeJSON(w, http.StatusOK, entries, start)
}

func (s *Service) handlePredictions(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	q := r.URL.Query()
	limit := 200
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			s.writeError(w, http.StatusBadRequest, "limit must be an integer between 1 and 1000")
			return
		}
		limit = n
	}
	rows, err := s.ListPredictions(r.Context(), PredictionsFilter{
		Model: q.Get("model"), Grain: q.Get("grain"), Entity: q.Get("entity"), Limit: limit,
	})
	if err != nil {
		s.log.Error("predictions", "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	s.writeJSON(w, http.StatusOK, rows, start)
}

func (s *Service) handleLatestPredictions(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	q := r.URL.Query()
	limit := 100
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			s.writeError(w, http.StatusBadRequest, "limit must be an integer between 1 and 1000")
			return
		}
		limit = n
	}
	rows, err := s.LatestPredictions(r.Context(), q.Get("model"), limit)
	if err != nil {
		s.log.Error("latest_predictions", "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	s.writeJSON(w, http.StatusOK, rows, start)
}

func (s *Service) handleScore(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if s.client == nil {
		s.reg.Counter("abi_score_errors_total", "Live scoring failures.").Inc()
		s.writeError(w, http.StatusServiceUnavailable,
			"live scoring disabled: set ABI_SCORE_URL and run ml/serve.py")
		return
	}
	var req model.ScoreRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Model == "" || len(req.Features) == 0 {
		s.writeError(w, http.StatusBadRequest, "payload needs model and a non-empty features object")
		return
	}
	entityID := req.EntityID
	if entityID == "" {
		entityID = "adhoc"
	}

	resp, err := s.client.Score(r.Context(), req.Model, req.Features)
	if err != nil {
		s.reg.Counter("abi_score_errors_total", "Live scoring failures.").Inc()
		s.log.Warn("score", "model", req.Model, "error", err)
		s.writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	s.reg.Gauge("abi_score_latency_ms", "Wall-clock latency of the last sidecar scoring call.").
		Set(float64(time.Since(start).Milliseconds()))

	id, err := s.InsertPrediction(r.Context(), InsertPredictionParams{
		ModelName:    resp.Model,
		ModelVersion: resp.Version,
		Grain:        resp.Grain,
		EntityID:     entityID,
		Prediction:   resp.Prediction,
		Confidence:   resp.Confidence,
		Explanation:  resp.Explanation,
		Metadata:     json.RawMessage(`{"source":"live_score"}`),
	})
	if err != nil {
		s.reg.Counter("abi_score_errors_total", "Live scoring failures.").Inc()
		s.log.Error("persist prediction", "error", err)
		s.writeError(w, http.StatusInternalServerError, "scored but failed to persist prediction")
		return
	}
	s.reg.Counter("abi_predictions_written_total", "Predictions persisted to gold.predictions.").Inc()

	s.writeJSON(w, http.StatusOK, map[string]any{
		"id": id, "model": resp.Model, "model_version": resp.Version,
		"task": resp.Task, "grain": resp.Grain, "entity_id": entityID,
		"prediction": resp.Prediction, "confidence": resp.Confidence,
		"explanation": resp.Explanation,
	}, start)
}

func (s *Service) handleModelsHealth(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	sidecar := "down"
	if err := s.client.Health(r.Context()); err == nil {
		sidecar = "up"
	}
	models, _ := s.ActiveModelCount(r.Context())
	s.writeJSON(w, http.StatusOK, map[string]any{
		"sidecar": sidecar, "active_models": models,
	}, start)
}

func (s *Service) writeJSON(w http.ResponseWriter, status int, data any, start time.Time) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(model.Envelope{
		Data: data,
		Meta: model.Meta{
			GeneratedAt: s.now().UTC().Format(time.RFC3339),
			TookMS:      float64(time.Since(start).Microseconds()) / 1000,
		},
	})
}

func (s *Service) writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(model.Envelope{
		Error: msg,
		Meta:  model.Meta{GeneratedAt: s.now().UTC().Format(time.RFC3339)},
	})
}