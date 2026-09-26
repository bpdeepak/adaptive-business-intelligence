//go:build integration

package batchscore

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"abi/internal/events"
	"abi/internal/playbook"
	"abi/internal/predict"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("ABI_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://abi:abi@localhost:5432/abi?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("postgres unreachable (%v)", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("postgres unreachable (%v)", err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM gold.feature_customer_churn_current`).Scan(&n); err != nil || n == 0 {
		t.Skipf("gold.feature_customer_churn_current unavailable (run dbt): %v", err)
	}
	return pool
}

// strictSidecar mimics ml/serve.py's contract for one model: the feature keys
// must be EXACTLY the list the active registry version was trained on (the
// real sidecar refuses anything missing with a 400).
func strictSidecar(t *testing.T, version string, want []string) (*httptest.Server, *[]map[string]float64) {
	return strictSidecarFn(t, version, want, func(map[string]float64) float64 { return 0.8 })
}

// strictSidecarFn is strictSidecar with a caller-chosen prediction per request.
func strictSidecarFn(t *testing.T, version string, want []string, predict func(map[string]float64) float64) (*httptest.Server, *[]map[string]float64) {
	t.Helper()
	var mu sync.Mutex
	var seen []map[string]float64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model    string             `json:"model"`
			Features map[string]float64 `json:"features"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		got := make([]string, 0, len(req.Features))
		for k := range req.Features {
			got = append(got, k)
		}
		sort.Strings(got)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, `{"error":"feature mismatch: got %v want %v"}`, got, want)
			return
		}
		mu.Lock()
		seen = append(seen, req.Features)
		mu.Unlock()
		_, _ = fmt.Fprintf(w, `{"status":"ok","model":%q,"version":%q,"task":"x","grain":"x","prediction":%v,"confidence":0.8,"explanation":{}}`,
			req.Model, version, predict(req.Features))
	}))
	t.Cleanup(srv.Close)
	return srv, &seen
}

func TestChurnScorerScoresThePopulationOncePerVersionAndResumes(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	version, thr, _, err := ActiveModel(ctx, pool, ChurnModel)
	if err != nil {
		t.Skipf("no active churn model: %v", err)
	}
	var featuresJSON []byte
	if err := pool.QueryRow(ctx, `SELECT features FROM gold.model_registry WHERE model_name=$1 AND model_version=$2`,
		ChurnModel, version).Scan(&featuresJSON); err != nil {
		t.Fatalf("registry features: %v", err)
	}
	var want []string
	_ = json.Unmarshal(featuresJSON, &want)
	sort.Strings(want)

	cleanup := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM gold.predictions WHERE model_name=$1 AND model_version=$2 AND metadata->>'source'=$3`,
			ChurnModel, version, ChurnSource)
	}
	cleanup()
	t.Cleanup(cleanup)

	srv, seen := strictSidecar(t, version, want)
	logger := slog.New(slog.DiscardHandler)
	svc := predict.NewService(pool, predict.NewScoreClient(srv.URL), logger)
	bus := events.New()
	ch, unsub := bus.Subscribe()
	defer unsub()
	rules := []playbook.Rule{{Name: "r", Trigger: string(events.TypeChurnScored), Enabled: true}}
	cs := NewChurnScorer(pool, svc, bus, rules, logger)

	var population int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM gold.feature_customer_churn_current`).Scan(&population)

	// Drain the bus concurrently (its buffer is 64; the population is larger).
	var evMu sync.Mutex
	var evs []events.Event
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case e := <-ch:
				evMu.Lock()
				evs = append(evs, e)
				evMu.Unlock()
			case <-time.After(2 * time.Second):
				return
			}
		}
	}()

	n, err := cs.Once(ctx)
	if err != nil {
		t.Fatalf("first pass: %v (the sidecar rejects any feature-name drift)", err)
	}
	if n != population || len(*seen) != population {
		t.Fatalf("scored %d (sidecar saw %d), want the whole population %d", n, len(*seen), population)
	}
	<-done
	evMu.Lock()
	first := evs[0]
	evMu.Unlock()
	pred, _ := first.Payload["prediction"].(map[string]any)
	if first.Type != events.TypeChurnScored || pred["id"] == nil || pred["model_version"] != version {
		t.Fatalf("event = %+v", first)
	}
	if thr != nil && pred["threshold"] != *thr {
		t.Errorf("event threshold = %v, want the registry's %v", pred["threshold"], *thr)
	}

	// Once per version: a second pass scores nothing.
	if n, err := cs.Once(ctx); err != nil || n != 0 {
		t.Fatalf("second pass scored %d (err %v); the population is fixed, so it must be 0", n, err)
	}

	// Resume: an interrupted pass (rows missing) is completed, not restarted.
	if _, err := pool.Exec(ctx, `
DELETE FROM gold.predictions WHERE id IN (
  SELECT id FROM gold.predictions WHERE model_name=$1 AND model_version=$2 AND metadata->>'source'=$3
  ORDER BY id DESC LIMIT 5)`, ChurnModel, version, ChurnSource); err != nil {
		t.Fatal(err)
	}
	if n, err := cs.Once(ctx); err != nil || n != 5 {
		t.Fatalf("resume pass scored %d (err %v), want exactly the 5 missing customers", n, err)
	}
	var persisted, distinct int
	_ = pool.QueryRow(ctx, `SELECT count(*), count(DISTINCT entity_id) FROM gold.predictions
WHERE model_name=$1 AND model_version=$2 AND metadata->>'source'=$3`, ChurnModel, version, ChurnSource).Scan(&persisted, &distinct)
	if persisted != population || distinct != population {
		t.Fatalf("persisted %d rows / %d customers, want %d each", persisted, distinct, population)
	}
}

func TestActiveModelFailsClosedWithoutAThreshold(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	name := fmt.Sprintf("it_batch_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, `
INSERT INTO gold.model_registry (model_name, model_version, status, framework, task, grain, artifact_path, metrics)
VALUES ($1, 'v1', 'active', 'fake', 'binary_classification', 'customer', '/tmp/x', '{}'::jsonb)`, name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `DELETE FROM gold.model_registry WHERE model_name=$1`, name) })
	v, thr, pos, err := ActiveModel(ctx, pool, name)
	if err != nil || v != "v1" {
		t.Fatalf("ActiveModel: %q %v", v, err)
	}
	if thr != nil || pos != nil {
		t.Fatal("a registry row without recommended_threshold must yield nil (fail closed), never 0")
	}
	if _, _, _, err := ActiveModel(ctx, pool, name+"-missing"); err == nil {
		t.Fatal("a model with no active version must be an error, not an empty version")
	}
}

func TestChurnScorerIsIdleUnlessARuleIsArmed(t *testing.T) {
	cs := NewChurnScorer(nil, nil, nil, []playbook.Rule{
		{Trigger: string(events.TypeChurnScored), Enabled: false},
		{Trigger: string(events.TypeOrderScored), Enabled: true},
	}, slog.New(slog.DiscardHandler))
	if cs.enabled {
		t.Fatal("scorer armed with no enabled churn_scored rule")
	}
	if err := cs.Run(context.Background(), time.Hour); err != nil {
		t.Fatal(err)
	}
}
