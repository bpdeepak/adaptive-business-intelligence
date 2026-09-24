//go:build integration

// Train/serve parity for the fraud model (audit T4).
//
// Phase 3's headline claim is "feature parity, not approximation": the Go stream
// assembler must produce the same 19-feature vector the batch trainer saw, or
// the model scores skewed inputs. Until now only the feature NAMES were pinned.
// This test replays the dataset's earliest orders — the same rows, loaded by the
// same store.LoadReplayOrders and turned into events by the same
// stream.NewOrderPlaced as the live producer — through the real assembler and
// rings, and compares every feature to gold.feature_fraud_orders (the table the
// model was trained on).
//
// The earliest N orders are used because the rings start empty exactly like the
// batch window functions do at the start of the dataset, so every trailing
// window (velocity 24 h, account age, 90-day category benchmark) has identical
// history on both sides.
package scorewriter

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"abi/internal/store"
	"abi/internal/stream"
)

const parityOrders = 25000

func TestFraudFeatureParityWithBatchFeatureTable(t *testing.T) {
	dsn := os.Getenv("ABI_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://abi:abi@localhost:5432/abi?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("postgres unreachable (%v)", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("postgres unreachable (%v)", err)
	}
	// CI sets ABI_REQUIRE_PARITY=1 so a missing prerequisite FAILS the run instead
	// of silently skipping the one test that pins train/serve parity.
	unavailable := func(format string, args ...any) {
		t.Helper()
		if os.Getenv("ABI_REQUIRE_PARITY") != "" {
			t.Fatalf("parity prerequisites missing: "+format, args...)
		}
		t.Skipf(format, args...)
	}
	var nFeat, nOrders int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM gold.feature_fraud_orders`).Scan(&nFeat); err != nil || nFeat == 0 {
		unavailable("gold.feature_fraud_orders unavailable (run `make dbt ml-features`): %v", err)
	}
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM gold.fct_orders`).Scan(&nOrders)
	if nFeat != nOrders {
		// A --limit smoke build has windows computed over a subset: not comparable.
		unavailable("gold.feature_fraud_orders has %d rows but gold.fct_orders has %d (partial build)", nFeat, nOrders)
	}

	refs, err := LoadReferences(ctx, pool, slog.New(slog.DiscardHandler))
	if err != nil {
		// No registered model (e.g. CI trains with --skip-persist). Everything but
		// the training-time category rank is registry-independent; derive the rank
		// with the trainer's own formula (ml/common.py category_rank_from: total
		// order_value per primary category, descending, top = 0).
		refs = &References{
			ProductCategory: map[string]string{}, CategoryRank: map[string]int{},
			Models: map[string]ModelConfig{}, CustomerKey: map[string]string{},
		}
		if err := loadStaticReferences(ctx, pool, refs); err != nil {
			t.Fatalf("static references: %v", err)
		}
		rk, err := pool.Query(ctx, `
SELECT category_primary FROM gold.feature_fraud_orders
GROUP BY category_primary ORDER BY sum(order_value) DESC, category_primary`)
		if err != nil {
			t.Fatalf("derive category rank: %v", err)
		}
		for i := 0; rk.Next(); i++ {
			var c string
			if err := rk.Scan(&c); err != nil {
				t.Fatalf("scan rank: %v", err)
			}
			refs.CategoryRank[c] = i
		}
		rk.Close()
	}

	orders, err := store.LoadReplayOrders(ctx, pool)
	if err != nil {
		t.Fatalf("load replay orders: %v", err)
	}
	if len(orders) > parityOrders {
		orders = orders[:parityOrders]
	}

	// Batch truth for the same orders.
	type batchRow struct {
		vals     map[string]float64
		payType  string
		category string
	}
	batch := map[string]batchRow{}
	rows, err := pool.Query(ctx, `
SELECT order_id, category_primary, payment_type_primary,
       order_value, item_count, freight_share, payment_count, installments_max,
       categories_count, price_vs_benchmark, velocity_24h, account_age_days,
       order_hour, is_weekend, is_lost
FROM gold.feature_fraud_orders
ORDER BY order_purchase_timestamp, order_id
LIMIT $1`, parityOrders)
	if err != nil {
		t.Fatalf("query batch features: %v", err)
	}
	for rows.Next() {
		var id, cat, pay string
		var v [12]float64
		if err := rows.Scan(&id, &cat, &pay, &v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9], &v[10], &v[11]); err != nil {
			t.Fatalf("scan batch row: %v", err)
		}
		batch[id] = batchRow{category: cat, payType: pay, vals: map[string]float64{
			"order_value": v[0], "item_count": v[1], "freight_share": v[2], "payment_count": v[3],
			"installments_max": v[4], "categories_count": v[5], "price_vs_benchmark": v[6],
			"velocity_24h": v[7], "account_age_days": v[8], "order_hour": v[9],
			"is_weekend": v[10], "is_lost": v[11],
		}}
	}
	rows.Close()

	asm := NewFraudAssembler(refs)
	rings := NewRings()
	mismatches := map[string]int{}
	firstExample := map[string]string{}
	compared := 0
	for _, o := range orders {
		od := stream.NewOrderPlaced(o, o.PurchaseAt)
		want, ok := batch[od.OrderID]
		got := asm.Assemble(od, rings)
		rings.RecordOrder(od, refs, asm.primaryCategory(od.Items))
		if !ok {
			continue
		}
		compared++
		check := func(name string, g, w float64) {
			if math.Abs(g-w) > 1e-6*math.Max(1, math.Abs(w)) {
				mismatches[name]++
				if _, seen := firstExample[name]; !seen {
					firstExample[name] = fmt.Sprintf("order %s: stream=%v batch=%v", od.OrderID, g, w)
				}
			}
		}
		for name, w := range want.vals {
			check(name, got[name], w)
		}
		// one-hot payment type + category code, derived exactly as train_fraud.py does.
		for _, pt := range append(append([]string{}, knownPaymentTypes...), "unknown") {
			w := 0.0
			if want.payType == pt {
				w = 1
			}
			check("pay_"+pt, got["pay_"+pt], w)
		}
		wantCode := float64(len(refs.CategoryRank))
		if code, ok := refs.CategoryRank[want.category]; ok {
			wantCode = float64(code)
		}
		check("category_code", got["category_code"], wantCode)
	}
	if compared < parityOrders/2 {
		t.Fatalf("only %d of %d replayed orders found in the batch feature table", compared, len(orders))
	}
	for name, c := range mismatches {
		t.Errorf("feature %-20s differs on %d/%d orders; first: %s", name, c, compared, firstExample[name])
	}
}
