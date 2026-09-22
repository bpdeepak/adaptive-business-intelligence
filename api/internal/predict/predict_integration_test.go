//go:build integration

// Integration tests for the Phase 2 predictive API (gold.model_registry +
// gold.predictions + the sidecar scoring flow), running against a live
// Postgres stack.
//
// Skipped automatically when Postgres is unreachable or the gold serving
// tables have no data yet, so plain `go test ./...` stays green everywhere:
//
//   - Local dev: docker compose up -d --wait, run all trainers, then run as-is
//     (the default DSN matches the stack: postgres://abi:abi@localhost:5432/abi).
//   - CI: the workflow builds the gold layer + runs the Phase 2 trainers before
//     the test step and overrides the DSN with ABI_TEST_DATABASE_URL.
//
// Live scoring is exercised against a fake in-process sidecar (httptest) that
// mimics ml/serve.py's /health + /score contract, so the test never depends on
// a running Python process.
package predict

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"abi/internal/model"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("ABI_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://abi:abi@localhost:5432/abi?sslmode=disable"
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("postgres unreachable (%v) - skipping predict integration tests", err)
	}
	if err := EnsureSchema(ctx, pool); err != nil {
		t.Fatalf("ensure predict schema: %v", err)
	}
	var n int
	if err := pool.QueryRow(ctx, "select count(*) from gold.model_registry").Scan(&n); err != nil || n == 0 {
		t.Skipf("gold.model_registry empty (run the Phase 2 trainers first): %v", err)
	}
	return pool
}

func newServiceMux(t *testing.T, client *ScoreClient) (*pgxpool.Pool, *http.ServeMux) {
	t.Helper()
	pool := testPool(t)
	svc := NewService(pool, client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mux := http.NewServeMux()
	svc.Register(mux)
	return pool, mux
}

// fakeSidecar mimics ml/serve.py: /health returns ok and /score reflects the
// fraud_risk feature vector back with a canned SHAP explanation.
func fakeSidecar() *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/health":
			_, _ = io.WriteString(w, `{"status":"ok"}`)
		case "/score":
			var req map[string]any
			_ = json.NewDecoder(r.Body).Decode(&req)
			feats, _ := req["features"].(map[string]any)
			pred := 0.02
			if v, ok := feats["velocity_24h"].(float64); ok && v >= 3 {
				pred = 0.9
			}
			_, _ = fmt.Fprintf(w,
				`{"status":"ok","model":"%v","version":"20260919.181959","task":"binary_classification","grain":"order","prediction":%v,"confidence":0.85,"explanation":{"shap":{"velocity_24h":0.42,"order_value":0.10}}}`,
				req["model"], pred)
		default:
			http.NotFound(w, r)
		}
	}))
	return srv
}

func doReq(t *testing.T, mux http.Handler, method, target string, body any) (*httptest.ResponseRecorder, model.Envelope) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		rdr = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, target, rdr)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	var env model.Envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("invalid JSON response: %v (body: %s)", err, rec.Body.String())
	}
	return rec, env
}

func fraudFeatureVector() map[string]float64 {
	return map[string]float64{
		"order_value": 500, "item_count": 2, "freight_share": 0.1, "payment_count": 1,
		"installments_max": 3, "categories_count": 1, "price_vs_benchmark": 1.0,
		"velocity_24h": 0, "account_age_days": 200, "order_hour": 14,
		"is_weekend": 0, "is_lost": 0, "pay_boleto": 0, "pay_credit_card": 1,
		"pay_debit_card": 0, "pay_not_defined": 0, "pay_unknown": 0,
		"pay_voucher": 0, "category_code": 5,
	}
}

func TestPredictRegistryListsActiveModels(t *testing.T) {
	_, mux := newServiceMux(t, nil)
	rec, env := doReq(t, mux, http.MethodGet, "/api/v1/model-registry", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	entries, ok := env.Data.([]any)
	if !ok || len(entries) == 0 {
		t.Fatalf("data = %#v, want a non-empty registry listing", env.Data)
	}
	first := entries[0].(map[string]any)
	for _, key := range []string{"model_name", "model_version", "status", "task", "grain", "artifact_path"} {
		if _, ok := first[key]; !ok {
			t.Fatalf("registry entry missing %q: %v", key, first)
		}
	}
	if first["status"] != "active" {
		t.Fatalf("expected only active models listed, got %v", first["status"])
	}
}

func TestPredictListAndLatest(t *testing.T) {
	_, mux := newServiceMux(t, nil)
	rec, env := doReq(t, mux, http.MethodGet, "/api/v1/predictions?limit=5", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	rows, ok := env.Data.([]any)
	if !ok || len(rows) == 0 {
		t.Fatalf("data = %#v, want persisted predictions", env.Data)
	}
	row := rows[0].(map[string]any)
	for _, key := range []string{"id", "model_name", "model_version", "entity_id", "predicted_at", "prediction"} {
		if _, ok := row[key]; !ok {
			t.Fatalf("prediction row missing %q: %v", key, row)
		}
	}

	rec2, env2 := doReq(t, mux, http.MethodGet, "/api/v1/predictions/latest?limit=2", nil)
	if rec2.Code != http.StatusOK {
		t.Fatalf("latest status = %d, want 200", rec2.Code)
	}
	latest, ok := env2.Data.([]any)
	if !ok || len(latest) != 2 {
		t.Fatalf("latest data = %#v, want exactly 2 rows", env2.Data)
	}
}

func TestPredictScoreRoundTrip(t *testing.T) {
	srv := fakeSidecar()
	defer srv.Close()
	pool, mux := newServiceMux(t, NewScoreClient(srv.URL))

	entity := fmt.Sprintf("test-order-%d", os.Getpid())
	rec, env := doReq(t, mux, http.MethodPost, "/api/v1/score", model.ScoreRequest{
		Model: "fraud_risk", EntityID: entity, Features: fraudFeatureVector(),
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("score status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	scored := env.Data.(map[string]any)
	if scored["prediction"].(float64) == 0 {
		t.Fatal("expected a prediction from the fake sidecar")
	}
	if _, ok := scored["explanation"]; !ok {
		t.Fatal("score response missing explanation payload")
	}

	// The scored row must be queryable back through the read API.
	rec2, env2 := doReq(t, mux, http.MethodGet, "/api/v1/predictions?entity="+entity, nil)
	if rec2.Code != http.StatusOK {
		t.Fatalf("read-back status = %d, want 200", rec2.Code)
	}
	rows, ok := env2.Data.([]any)
	if !ok || len(rows) == 0 {
		t.Fatalf("data = %#v, want the scored prediction row back", env2.Data)
	}
	if rows[0].(map[string]any)["entity_id"] != entity {
		t.Fatalf("wrong entity back: %v", rows[0])
	}

	// Clean up the test row so the test is re-runnable without polluting the feed.
	if _, err := pool.Exec(context.Background(),
		"delete from gold.predictions where entity_id = $1 and metadata->>'source' = 'live_score'", entity); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
}

func TestPredictScoreValidation(t *testing.T) {
	srv := fakeSidecar()
	defer srv.Close()
	_, mux := newServiceMux(t, NewScoreClient(srv.URL))

	rec, env := doReq(t, mux, http.MethodPost, "/api/v1/score", map[string]any{"model": "fraud_risk"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for missing features (body: %s)", rec.Code, rec.Body.String())
	}
	if env.Error == "" {
		t.Fatal("expected an error message")
	}
}

func TestPredictScoreDisabledWhenSidecarNil(t *testing.T) {
	_, mux := newServiceMux(t, nil)
	rec, env := doReq(t, mux, http.MethodPost, "/api/v1/score", model.ScoreRequest{
		Model: "fraud_risk", EntityID: "x", Features: fraudFeatureVector(),
	})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 when no sidecar is configured", rec.Code)
	}
	if env.Error == "" {
		t.Fatal("expected an error message explaining scoring is disabled")
	}
}

func TestPredictScoreBadGatewayWhenSidecarDown(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	url := dead.URL
	dead.Close() // sidecar is down

	_, mux := newServiceMux(t, NewScoreClient(url))
	rec, _ := doReq(t, mux, http.MethodPost, "/api/v1/score", model.ScoreRequest{
		Model: "fraud_risk", EntityID: "x", Features: fraudFeatureVector(),
	})
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 when the sidecar is unreachable", rec.Code)
	}
}

func TestPredictModelsHealth(t *testing.T) {
	srv := fakeSidecar()
	defer srv.Close()
	_, mux := newServiceMux(t, NewScoreClient(srv.URL))

	rec, env := doReq(t, mux, http.MethodGet, "/api/v1/models/health", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	health := env.Data.(map[string]any)
	if health["sidecar"] != "up" {
		t.Fatalf("sidecar = %v, want up", health["sidecar"])
	}
	if n, _ := health["active_models"].(float64); n < 1 {
		t.Fatalf("active_models = %v, want >= 1", health["active_models"])
	}
}