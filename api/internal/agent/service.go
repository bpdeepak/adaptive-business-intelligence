package agent

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"abi/internal/model"
	"abi/internal/telemetry"
)

// Service brokers the Phase 3 NL→BI agent sidecar to the HTTP API. All agent
// decisions (grounding, provenance labels, bounded revisions, honest refusal)
// happen in the Python sidecar; this layer only proxies the contract and keeps
// the operational metrics the dashboard/infra needs.
type Service struct {
	client *AgentClient
	log    *slog.Logger
	reg    *telemetry.Registry
	now    func() time.Time
}

// NewService builds the serving layer. `client` may be nil to disable the agent.
func NewService(client *AgentClient, log *slog.Logger) *Service {
	return &Service{client: client, log: log, now: time.Now}
}

// Instrument attaches the operational registry (nil-safe; optional).
func (s *Service) Instrument(reg *telemetry.Registry) { s.reg = reg }

// Enabled reports whether the agent sidecar is configured (ABI_AGENT_URL set).
func (s *Service) Enabled() bool { return s.client != nil }

// Register wires the Phase 3 agent endpoints into an existing mux.
func (s *Service) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/agent/health", s.handleHealth)
	mux.HandleFunc("POST /api/v1/agent/query", s.handleQuery)
}

// RunGaugeLoop refreshes the sidecar-up gauge on a schedule.
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

// RefreshGauges updates abi_agent_up (1 when the sidecar answers /health).
func (s *Service) RefreshGauges(ctx context.Context) {
	up := 0.0
	if s.client.Health(ctx) == nil {
		up = 1
	}
	s.reg.Gauge("abi_agent_up", "1 when the Python agent sidecar answers /health.").Set(up)
}

func (s *Service) handleHealth(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	state := "down"
	if s.client.Health(r.Context()) == nil {
		state = "up"
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"agent": state}, start)
}

func (s *Service) handleQuery(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if s.client == nil {
		s.reg.Counter("abi_agent_errors_total", "Agent query failures.").Inc()
		s.writeError(w, http.StatusServiceUnavailable,
			"agent disabled: set ABI_AGENT_URL and run ml/agent/server.py")
		return
	}
	var req struct {
		Question string `json:"question"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		s.reg.Counter("abi_agent_errors_total", "Agent query failures.").Inc()
		s.writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if strings.TrimSpace(req.Question) == "" {
		s.reg.Counter("abi_agent_errors_total", "Agent query failures.").Inc()
		s.writeError(w, http.StatusBadRequest, "payload needs a non-empty question")
		return
	}

	resp, err := s.client.Query(r.Context(), strings.TrimSpace(req.Question))
	if err != nil {
		s.reg.Counter("abi_agent_errors_total", "Agent query failures.").Inc()
		s.log.Warn("agent query", "question", req.Question, "error", err)
		s.writeError(w, http.StatusBadGateway, "agent sidecar error: "+err.Error())
		return
	}

	s.reg.Counter("abi_agent_query_total", "Natural-language queries answered by the agent sidecar.").Inc()
	s.reg.Gauge("abi_agent_latency_ms", "Wall-clock latency of the last agent query.").
		Set(float64(time.Since(start).Milliseconds()))
	s.reg.Counter("abi_agent_turns_total", "Agent tool+answer turns accumulated across queries.").
		Add(float64(resp.Turns))
	if resp.Refused {
		s.reg.Counter("abi_agent_grounding_refused_total", "Answers refused because claims could not be grounded.").Inc()
	}
	s.writeJSON(w, http.StatusOK, resp, start)
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
