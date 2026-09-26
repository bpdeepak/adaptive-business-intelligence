//go:build integration

package batchscore

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"testing"

	"abi/internal/events"
	"abi/internal/playbook"
	"abi/internal/predict"
)

func forecastContract(t *testing.T) (version string, want []string, rank map[string]int) {
	t.Helper()
	pool := testPool(t)
	ctx := context.Background()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM gold.feature_forecast_next_week`).Scan(&n); err != nil || n == 0 {
		t.Skipf("gold.feature_forecast_next_week unavailable (run dbt): %v", err)
	}
	v, _, _, err := ActiveModel(ctx, pool, ForecastModel)
	if err != nil {
		t.Skipf("no active forecast model: %v", err)
	}
	var fj, mj []byte
	_ = pool.QueryRow(ctx, `SELECT features, metrics FROM gold.model_registry WHERE model_name=$1 AND model_version=$2`,
		ForecastModel, v).Scan(&fj, &mj)
	_ = json.Unmarshal(fj, &want)
	sort.Strings(want)
	var m struct {
		CategoryRank map[string]int `json:"category_rank"`
	}
	_ = json.Unmarshal(mj, &m)
	return v, want, m.CategoryRank
}

func cleanupForecast(t *testing.T, version string) {
	pool := testPool(t)
	ctx := context.Background()
	_, _ = pool.Exec(ctx, `DELETE FROM gold.predictions WHERE model_name=$1 AND model_version=$2 AND metadata->>'source'=$3`,
		ForecastModel, version, ForecastSource)
	tx, err := pool.Begin(ctx)
	if err == nil {
		_, _ = tx.Exec(ctx, `SET LOCAL abi.audit_maintenance = 'on'`)
		_, _ = tx.Exec(ctx, `DELETE FROM gold.action_audit_log WHERE action_id IN (SELECT id FROM gold.action_queue WHERE rule = 'draft-po-on-demand-surge')`)
		_, _ = tx.Exec(ctx, `DELETE FROM gold.action_queue WHERE rule = 'draft-po-on-demand-surge'`)
		_ = tx.Commit(ctx)
	}
	_, _ = pool.Exec(ctx, `DELETE FROM gold.purchase_orders
WHERE forecast_week IN (SELECT to_char(week_start, 'YYYY-MM-DD') FROM gold.feature_forecast_next_week)`)
}

func TestForecastWorkerForecastsEveryTrainedCategoryOncePerVersion(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	version, want, rank := forecastContract(t)
	cleanupForecast(t, version)
	t.Cleanup(func() { cleanupForecast(t, version) })

	logger := slog.New(slog.DiscardHandler)
	srv, seen := strictSidecarFn(t, version, want, func(f map[string]float64) float64 { return f["orders_roll4_mean"] })
	svc := predict.NewService(pool, predict.NewScoreClient(srv.URL), logger)
	rules := []playbook.Rule{{Trigger: string(events.TypeForecastUpdated), Enabled: true}}
	w := NewForecastWorker(pool, svc, nil, rules, logger)

	var total, ranked int
	rows, err := pool.Query(ctx, `SELECT category FROM gold.feature_forecast_next_week`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var c string
		_ = rows.Scan(&c)
		total++
		if _, ok := rank[c]; ok {
			ranked++
		}
	}
	rows.Close()

	n, skipped, err := w.Once(ctx)
	if err != nil {
		t.Fatalf("pass: %v (the strict sidecar refuses any vector that is not exactly the registry's feature list)", err)
	}
	if n != ranked || skipped != total-ranked || len(*seen) != ranked {
		t.Fatalf("forecast %d, skipped %d, sidecar saw %d; want %d / %d (categories absent from the training rank are skipped, never guessed)",
			n, skipped, len(*seen), ranked, total-ranked)
	}
	var entity, week, src string
	if err := pool.QueryRow(ctx, `
SELECT entity_id, metadata->>'week', metadata->>'source'
FROM gold.predictions WHERE model_name=$1 AND model_version=$2 AND metadata->>'source'=$3 LIMIT 1`,
		ForecastModel, version, ForecastSource).Scan(&entity, &week, &src); err != nil {
		t.Fatal(err)
	}
	if entity == "" || week == "" || src != ForecastSource {
		t.Fatalf("persisted row: entity=%q week=%q source=%q", entity, week, src)
	}
	if n2, _, err := w.Once(ctx); err != nil || n2 != 0 {
		t.Fatalf("second pass forecast %d (err %v), want 0: the forecast week is fixed", n2, err)
	}
}
