package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"abi/internal/model"
	"abi/internal/store"
)

func testServer(t *testing.T) *Server {
	t.Helper()
	fake := &store.FakeStore{
		SummaryFn: func(context.Context, *time.Time, *time.Time) (model.Summary, error) {
			return model.Summary{
				Revenue: 1000.5, Orders: 10, AOV: 100.05, Customers: 5,
				TopCategory: "beauty_health", DataStart: "2017-01-01", DataEnd: "2018-12-31",
			}, nil
		},
		RevenueDailyFn: func(context.Context, *time.Time, *time.Time) ([]model.SeriesPoint, error) {
			return []model.SeriesPoint{
				{Date: "2017-01-01", Value: 100}, {Date: "2017-01-02", Value: 200},
			}, nil
		},
		OrdersDailyFn: func(context.Context, *time.Time, *time.Time) ([]model.OrderSeriesPoint, error) {
			return []model.OrderSeriesPoint{{Date: "2017-01-01", Orders: 5, Delivered: 4, Lost: 1}}, nil
		},
		TopCategoriesFn: func(context.Context, string, int) ([]model.Category, error) {
			return []model.Category{
				{Category: "beauty_health", Orders: 3, Revenue: 600, RevenueRank: 1, OrderRank: 2},
				{Category: "toys", Orders: 4, Revenue: 400, RevenueRank: 2, OrderRank: 1},
			}, nil
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(fake, logger)
}

func doReq(t *testing.T, srv http.Handler, method, target string) (*httptest.ResponseRecorder, model.Envelope) {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	var env model.Envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("invalid JSON response: %v (body: %s)", err, rec.Body.String())
	}
	return rec, env
}

func TestHealthz(t *testing.T) {
	srv := testServer(t)
	rec, env := doReq(t, srv, http.MethodGet, "/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	health := env.Data.(map[string]any)
	if health["status"] != "ok" {
		t.Fatalf("health status = %v, want ok", health["status"])
	}
	if env.Meta.GeneratedAt == "" {
		t.Fatal("meta.generated_at missing")
	}
}

func TestSummary(t *testing.T) {
	srv := testServer(t)
	rec, env := doReq(t, srv, http.MethodGet, "/api/v1/summary")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	data := env.Data.(map[string]any)
	if data["revenue"] != 1000.5 {
		t.Errorf("revenue = %v, want 1000.5", data["revenue"])
	}
	if data["orders"] != float64(10) {
		t.Errorf("orders = %v, want 10", data["orders"])
	}
}

func TestSummaryBadRange(t *testing.T) {
	srv := testServer(t)
	rec, _ := doReq(t, srv, http.MethodGet, "/api/v1/summary?from=not-a-date")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	rec, _ = doReq(t, srv, http.MethodGet, "/api/v1/summary?from=2018-01-01&to=2017-01-01")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for from>to", rec.Code)
	}
}

func TestRevenueDaily(t *testing.T) {
	srv := testServer(t)
	rec, env := doReq(t, srv, http.MethodGet, "/api/v1/revenue/daily")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	points := env.Data.([]any)
	if len(points) != 2 {
		t.Fatalf("points = %d, want 2", len(points))
	}
	first := points[0].(map[string]any)
	if first["date"] != "2017-01-01" || first["value"] != float64(100) {
		t.Errorf("unexpected first point: %v", first)
	}
}

func TestOrdersDaily(t *testing.T) {
	srv := testServer(t)
	rec, env := doReq(t, srv, http.MethodGet, "/api/v1/orders/daily")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	points := env.Data.([]any)
	if len(points) != 1 {
		t.Fatalf("points = %d, want 1", len(points))
	}
	p := points[0].(map[string]any)
	if p["orders"] != float64(5) || p["lost"] != float64(1) {
		t.Errorf("unexpected order point: %v", p)
	}
}

func TestTopCategories(t *testing.T) {
	srv := testServer(t)
	rec, env := doReq(t, srv, http.MethodGet, "/api/v1/categories/top?metric=revenue&limit=10")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	cats := env.Data.([]any)
	if len(cats) != 2 {
		t.Fatalf("categories = %d, want 2", len(cats))
	}
	first := cats[0].(map[string]any)
	if first["category"] != "beauty_health" {
		t.Errorf("first category = %v, want beauty_health", first["category"])
	}
}

func TestTopCategoriesValidation(t *testing.T) {
	srv := testServer(t)
	for _, target := range []string{
		"/api/v1/categories/top?metric=bogus",
		"/api/v1/categories/top?limit=0",
		"/api/v1/categories/top?limit=abc",
		"/api/v1/categories/top?limit=999",
	} {
		rec, _ := doReq(t, srv, http.MethodGet, target)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("target %q: status = %d, want 400", target, rec.Code)
		}
	}
}

func TestMetrics(t *testing.T) {
	srv := testServer(t)
	rec, env := doReq(t, srv, http.MethodGet, "/api/v1/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	data, ok := env.Data.(map[string]any)
	if !ok {
		t.Fatalf("metrics data is not an object: %T", env.Data)
	}
	cats, ok := data["metrics"].([]any)
	if !ok {
		t.Fatalf("metrics list missing: %v", data)
	}
	if len(cats) < 8 {
		t.Errorf("expected at least 8 metrics, got %d", len(cats))
	}
	byName := map[string]map[string]any{}
	for _, c := range cats {
		m := c.(map[string]any)
		byName[m["name"].(string)] = m
	}
	for _, name := range []string{"revenue", "orders", "aov", "active_customers"} {
		if _, ok := byName[name]; !ok {
			t.Errorf("metric %q missing", name)
		}
	}
	rev := byName["revenue"]
	if !strings.Contains(rev["population"].(string), "canceled") {
		t.Errorf("revenue population should mention excluded statuses, got %q", rev["population"])
	}
	if data["version"] == "" {
		t.Error("catalog version missing")
	}
}

func TestUnknownMethod(t *testing.T) {
	srv := testServer(t)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/summary", strings.NewReader(""))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}
