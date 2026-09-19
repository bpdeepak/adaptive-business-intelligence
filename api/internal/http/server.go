// Package httpapi wires the HTTP route table and serves the embedded dashboard.
package httpapi

import (
	"embed"
	"io/fs"
	"log/slog"
	"net/http"
	"time"

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
}

// New builds a Server around a Store.
func New(store store.Store, log *slog.Logger) *Server {
	s := &Server{store: store, log: log, now: time.Now}
	s.mux = s.routes()
	return s
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
	mux.Handle("GET /api/v1/revenue/daily", s.middleware(s.handleRevenueDaily))
	mux.Handle("GET /api/v1/orders/daily", s.middleware(s.handleOrdersDaily))
	mux.Handle("GET /api/v1/categories/top", s.middleware(s.handleTopCategories))
	return mux
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}