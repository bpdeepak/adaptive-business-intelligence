// Package httpapi wires the HTTP route table and serves the embedded dashboard.
package httpapi

import (
	"embed"
	"io/fs"
	"log/slog"
	"net/http"
	"time"

	"abi/internal/actions"
	"abi/internal/predict"
	"abi/internal/realtime"
	"abi/internal/store"
)

//go:embed all:static
var staticFS embed.FS

// Server owns routes, dependencies, and the request logger.
type Server struct {
	store store.Store
	log   *slog.Logger
	now   func() time.Time
	mux   *http.ServeMux
	// live is optional: when attached, SSE + realtime endpoints activate.
	live *realtime.LiveFeed
	// actions + predict are Phase 4 surfaces (approval queue + model health);
	// while nil their endpoints 503 (see AttachActions / AttachPredict).
	actions *actions.Service
	predict *predict.Service
}

// New builds a Server around a Store. Realtime endpoints register but return
// 503 until AttachLive is called (kept nil in tests).
func New(store store.Store, log *slog.Logger) *Server {
	s := &Server{store: store, log: log, now: time.Now}
	s.mux = s.routes()
	return s
}

// AttachLive activates the SSE / anomalies endpoints for a running realtime
// stack. Safe to call once, before serving.
func (s *Server) AttachLive(live *realtime.LiveFeed) {
	s.live = live
}

func (s *Server) routes() *http.ServeMux {
	mux := http.NewServeMux()

	static, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err)
	}
	mux.Handle("GET /", http.FileServer(http.FS(static)))
	mux.Handle("GET /healthz", s.middleware(s.handleHealth))
	mux.Handle("GET /api/v1/summary", s.middleware(s.handleSummary))
	mux.Handle("GET /api/v1/metrics", s.middleware(s.handleMetrics))
	mux.Handle("GET /api/v1/revenue/daily", s.middleware(s.handleRevenueDaily))
	mux.Handle("GET /api/v1/orders/daily", s.middleware(s.handleOrdersDaily))
	mux.Handle("GET /api/v1/categories/top", s.middleware(s.handleTopCategories))

	// Phase 1 realtime surface.
	mux.Handle("GET /api/v1/stream/metrics", s.middleware(s.handleMetricsStream))
	mux.Handle("GET /api/v1/realtime/metrics", s.middleware(s.handleRealtimeMetrics))
	mux.Handle("GET /api/v1/anomalies", s.middleware(s.handleAnomalies))
	mux.Handle("POST /api/v1/anomalies/{id}/dismiss", s.middleware(s.handleDismissAnomaly))

	// Phase 4 governance + model-health surfaces.
	mux.Handle("GET /api/v1/actions", s.middleware(s.handleListActions))
	mux.Handle("GET /api/v1/actions/{id}/trace", s.middleware(s.handleTraceAction))
	mux.Handle("POST /api/v1/actions/{id}/approve", s.middleware(s.handleApproveAction))
	mux.Handle("POST /api/v1/actions/{id}/reject", s.middleware(s.handleRejectAction))
	mux.Handle("POST /api/v1/actions/{id}/retry", s.middleware(s.handleRetryAction))
	mux.Handle("GET /api/v1/model-drift", s.middleware(s.handleModelDrift))
	return mux
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}
