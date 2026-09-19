package httpapi

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"abi/internal/metrics"
	"abi/internal/model"
)

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, model.Envelope{Data: model.Health{Status: "ok"}, Meta: newMeta(s.now)})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	writeJSON(w, http.StatusOK, envelope(metrics.Default(), start, s.now))
}

func (s *Server) handleSummary(w http.ResponseWriter, r *http.Request) {
	from, to, err := parseRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), s.now)
		return
	}
	start := time.Now()
	sum, err := s.store.Summary(r.Context(), from, to)
	if err != nil {
		s.log.Error("summary", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error", s.now)
		return
	}
	writeJSON(w, http.StatusOK, envelope(sum, start, s.now))
}

func (s *Server) handleRevenueDaily(w http.ResponseWriter, r *http.Request) {
	from, to, err := parseRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), s.now)
		return
	}
	start := time.Now()
	points, err := s.store.RevenueDaily(r.Context(), from, to)
	if err != nil {
		s.log.Error("revenue_daily", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error", s.now)
		return
	}
	writeJSON(w, http.StatusOK, envelope(points, start, s.now))
}

func (s *Server) handleOrdersDaily(w http.ResponseWriter, r *http.Request) {
	from, to, err := parseRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), s.now)
		return
	}
	start := time.Now()
	points, err := s.store.OrdersDaily(r.Context(), from, to)
	if err != nil {
		s.log.Error("orders_daily", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error", s.now)
		return
	}
	writeJSON(w, http.StatusOK, envelope(points, start, s.now))
}

func (s *Server) handleTopCategories(w http.ResponseWriter, r *http.Request) {
	metric := r.URL.Query().Get("metric")
	if metric == "" {
		metric = "revenue"
	}
	if metric != "revenue" && metric != "orders" {
		writeError(w, http.StatusBadRequest, `metric must be "revenue" or "orders"`, s.now)
		return
	}

	limit := 10
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 50 {
			writeError(w, http.StatusBadRequest, "limit must be an integer between 1 and 50", s.now)
			return
		}
		limit = n
	}

	start := time.Now()
	cats, err := s.store.TopCategories(r.Context(), metric, limit)
	if err != nil {
		s.log.Error("top_categories", "metric", metric, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error", s.now)
		return
	}
	writeJSON(w, http.StatusOK, envelope(cats, start, s.now))
}

// parseRange reads optional `from` and `to` query params (YYYY-MM-DD).
func parseRange(r *http.Request) (*time.Time, *time.Time, error) {
	from, err := parseDate(r, "from")
	if err != nil {
		return nil, nil, err
	}
	to, err := parseDate(r, "to")
	if err != nil {
		return nil, nil, err
	}
	if from != nil && to != nil && from.After(*to) {
		return nil, nil, errors.New("query param `from` must be <= `to`")
	}
	return from, to, nil
}

func parseDate(r *http.Request, name string) (*time.Time, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return nil, nil
	}
	t, err := time.Parse("2006-01-02", v)
	if err != nil {
		return nil, fmt.Errorf("query param %q must be YYYY-MM-DD", name)
	}
	return &t, nil
}

// newMeta is on model package; keep a tiny alias here for handler brevity.
func newMeta(now func() time.Time) model.Meta {
	return model.Meta{GeneratedAt: now().UTC().Format(time.RFC3339)}
}
