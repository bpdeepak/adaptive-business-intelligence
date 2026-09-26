//go:build integration

// End-to-end tests of the Phase 5 batch producers under the SHIPPED policy, through
// the real delivery path: producer -> lossy in-process bus -> playbook engine, plus
// one reconciliation pass that rebuilds whatever the bus dropped. The bus drops
// events for a slow subscriber by design (it never blocks producers), so the
// guarantee under test is "live delivery + backstop yields exactly the expected
// proposals", never "every event arrives live".
package govern

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"abi/internal/actions"
	"abi/internal/batchscore"
	"abi/internal/events"
	"abi/internal/playbook"
	"abi/internal/predict"
)

// registryContract returns the active version, its sorted feature list and its
// training-time category rank.
func registryContract(t *testing.T, pool *pgxpool.Pool, model string) (string, []string, map[string]int) {
	t.Helper()
	ctx := context.Background()
	v, _, _, err := batchscore.ActiveModel(ctx, pool, model)
	if err != nil {
		t.Skipf("no active %s model: %v", model, err)
	}
	var fj, mj []byte
	_ = pool.QueryRow(ctx, `SELECT features, metrics FROM gold.model_registry WHERE model_name=$1 AND model_version=$2`,
		model, v).Scan(&fj, &mj)
	var feats []string
	_ = json.Unmarshal(fj, &feats)
	sort.Strings(feats)
	var m struct {
		CategoryRank map[string]int `json:"category_rank"`
	}
	_ = json.Unmarshal(mj, &m)
	return v, feats, m.CategoryRank
}

// strictSidecar refuses any vector that is not exactly `want` (ml/serve.py's
// contract) and returns predict(features).
func strictSidecar(t *testing.T, version string, want []string, predictFn func(map[string]float64) float64) string {
	t.Helper()
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
			_, _ = fmt.Fprintf(w, `{"error":"feature mismatch"}`)
			return
		}
		_, _ = fmt.Fprintf(w, `{"status":"ok","model":%q,"version":%q,"task":"x","grain":"x","prediction":%v,"confidence":0.8,"explanation":{}}`,
			req.Model, version, predictFn(req.Features))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// shippedRule loads one rule from the real policy file.
func shippedRule(t *testing.T, name string) playbook.Rule {
	t.Helper()
	rules, err := playbook.LoadRules("../../../config/playbooks.yml")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rules {
		if r.Name == name {
			if !r.Enabled {
				t.Fatalf("%s must ship enabled", name)
			}
			return r
		}
	}
	t.Fatalf("%s missing from config/playbooks.yml", name)
	return playbook.Rule{}
}

// liveThenBackstop runs produce() with the engine consuming the live bus, then
// one reconciliation pass over everything produced (cursor seeded before the run).
func liveThenBackstop(t *testing.T, pool *pgxpool.Pool, rules []playbook.Rule, produce func(bus *events.Bus)) {
	t.Helper()
	ctx := context.Background()
	saveWatermark(t, pool)
	startCursor(t, pool)
	logger := slog.New(slog.DiscardHandler)
	bus := events.New()
	eng, err := playbook.NewEngine(bus, actions.New(pool, logger), rules, logger)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = eng.Run(runCtx) }()
	time.Sleep(50 * time.Millisecond) // let the engine subscribe
	produce(bus)
	time.Sleep(500 * time.Millisecond) // let the engine drain what it received
	cancel()
	<-done
	t.Logf("bus: published %d, dropped %d (recovered by the reconciler)", bus.Published(), bus.Dropped())
	if err := NewReconciler(pool, eng, rules, logger).Once(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

func cleanupRule(t *testing.T, pool *pgxpool.Pool, rule string) {
	ctx := context.Background()
	_ = auditMaintenance(ctx, pool, `DELETE FROM gold.action_audit_log WHERE action_id IN (SELECT id FROM gold.action_queue WHERE rule = $1)`, rule)
	_, _ = pool.Exec(ctx, `DELETE FROM gold.action_queue WHERE rule = $1`, rule)
}

// The shipped retention rule (score >= 0.70 AND rank <= 20) turns a population
// that all clears 0.70 into exactly 20 proposals, whatever the bus dropped.
func TestShippedRetentionRuleProposesExactlyTheTop20(t *testing.T) {
	pool := governTestPool(t)
	ctx := context.Background()
	var population int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM gold.feature_customer_churn_current`).Scan(&population); err != nil || population < 20 {
		t.Skipf("churn scoring mart unavailable or too small (%d): %v", population, err)
	}
	version, feats, _ := registryContract(t, pool, batchscore.ChurnModel)
	rule := shippedRule(t, "propose-retention-offer-high-churn")
	cleanup := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM gold.predictions WHERE model_name=$1 AND model_version=$2 AND metadata->>'source'=$3`,
			batchscore.ChurnModel, version, batchscore.ChurnSource)
		cleanupRule(t, pool, rule.Name)
	}
	cleanup()
	t.Cleanup(cleanup)

	logger := slog.New(slog.DiscardHandler)
	svc := predict.NewService(pool, predict.NewScoreClient(strictSidecar(t, version, feats,
		func(map[string]float64) float64 { return 0.8 })), logger)
	rules := []playbook.Rule{rule}
	liveThenBackstop(t, pool, rules, func(bus *events.Bus) {
		if n, err := batchscore.NewChurnScorer(pool, svc, bus, rules, logger).Once(ctx); err != nil || n != population {
			t.Fatalf("scored %d of %d: %v", n, population, err)
		}
	})

	var proposals, maxRank int
	if err := pool.QueryRow(ctx, `
SELECT count(*), COALESCE(max((trigger->'payload'->'prediction'->>'rank')::int), 0)
FROM gold.action_queue WHERE rule = $1`, rule.Name).Scan(&proposals, &maxRank); err != nil {
		t.Fatal(err)
	}
	if proposals != 20 || maxRank != 20 {
		t.Fatalf("%d proposals (max rank %d) from %d qualifying customers; want exactly the top 20", proposals, maxRank, population)
	}
}

// The shipped demand-surge rule proposes a PO only for the category whose
// forecast clears both the 1.2x and the +5 orders bars, and approving it drafts
// a PO for ceil(forecast - trailing 4-week average).
func TestShippedDemandSurgeRuleDraftsAPurchaseOrderForTheExcess(t *testing.T) {
	pool := governTestPool(t)
	ctx := context.Background()
	var cats int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM gold.feature_forecast_next_week`).Scan(&cats); err != nil || cats == 0 {
		t.Skipf("forecast scoring mart unavailable: %v", err)
	}
	version, feats, rank := registryContract(t, pool, batchscore.ForecastModel)
	surge := ""
	for c, code := range rank {
		if code == 0 {
			surge = c
		}
	}
	if surge == "" {
		t.Skip("no rank-0 category")
	}
	rule := shippedRule(t, "draft-po-on-demand-surge")
	cleanup := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM gold.predictions WHERE model_name=$1 AND model_version=$2 AND metadata->>'source'=$3`,
			batchscore.ForecastModel, version, batchscore.ForecastSource)
		cleanupRule(t, pool, rule.Name)
		_, _ = pool.Exec(ctx, `DELETE FROM gold.purchase_orders
WHERE forecast_week IN (SELECT to_char(week_start, 'YYYY-MM-DD') FROM gold.feature_forecast_next_week)`)
	}
	cleanup()
	t.Cleanup(cleanup)

	logger := slog.New(slog.DiscardHandler)
	// Surge (recent average x 1.5 + 10) for the rank-0 category, flat elsewhere.
	svc := predict.NewService(pool, predict.NewScoreClient(strictSidecar(t, version, feats,
		func(f map[string]float64) float64 {
			if f["category_code"] == 0 {
				return f["orders_roll4_mean"]*1.5 + 10
			}
			return f["orders_roll4_mean"]
		})), logger)
	rules := []playbook.Rule{rule}
	liveThenBackstop(t, pool, rules, func(bus *events.Bus) {
		if _, _, err := batchscore.NewForecastWorker(pool, svc, bus, rules, logger).Once(ctx); err != nil {
			t.Fatal(err)
		}
	})

	var id int64
	var entity string
	if err := pool.QueryRow(ctx, `SELECT id, entity FROM gold.action_queue WHERE rule = $1`, rule.Name).Scan(&id, &entity); err != nil {
		t.Fatalf("want exactly one PO proposal (the surging category): %v", err)
	}
	if entity != surge {
		t.Fatalf("proposal for %q, want %q", entity, surge)
	}
	if err := actions.New(pool, logger).Approve(ctx, id, "surge confirmed", "tester"); err != nil {
		t.Fatalf("approve: %v", err)
	}
	var recent float64
	_ = pool.QueryRow(ctx, `SELECT recent_orders_avg::float8 FROM gold.feature_forecast_next_week WHERE category=$1`, surge).Scan(&recent)
	var qty int
	if err := pool.QueryRow(ctx, `SELECT quantity FROM gold.purchase_orders WHERE category=$1`, surge).Scan(&qty); err != nil {
		t.Fatalf("no draft purchase order after approval: %v", err)
	}
	if want := int(math.Ceil(recent*0.5 + 10)); qty != want {
		t.Fatalf("PO quantity = %d, want ceil(forecast - recent average) = %d", qty, want)
	}
}
