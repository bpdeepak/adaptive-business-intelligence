package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// These tests exercise the real SQL in PostgresStore against a live Postgres.
// They are skipped automatically when Postgres is unreachable OR the dbt gold
// layer hasn't been built, so plain `go test ./...` stays green everywhere.
//
//   - Local dev: docker compose up -d --wait && uv run dbt build, then run as-is
//     (the default DSN postgres://abi:abi@localhost:5432/abi matches the stack).
//   - CI: the workflow starts a Postgres service, builds dbt, and sets
//     ABI_TEST_DATABASE_URL explicitly (or relies on the same default).
func integrationStore(t *testing.T) *PostgresStore {
	t.Helper()

	dsn := os.Getenv("ABI_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://abi:abi@localhost:5432/abi?sslmode=disable"
	}

	probeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	probe, err := pgx.Connect(probeCtx, dsn)
	if err != nil {
		t.Skipf("postgres unreachable (%v) — skipping Postgres integration tests", err)
	}
	_ = probe.Close(probeCtx)

	pool, err := pgxpool.New(probeCtx, dsn)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(pool.Close)

	// The gold layer must exist — if not, tell the developer how to build it.
	var one int
	if err := pool.QueryRow(probeCtx, "SELECT 1 FROM gold.daily_revenue LIMIT 1").Scan(&one); err != nil {
		t.Skipf("gold layer not found (run `uv run dbt build --project-dir dbt --profiles-dir dbt`): %v", err)
	}
	return &PostgresStore{pool: pool}
}

func date(t *testing.T, s string) *time.Time {
	t.Helper()
	d, err := time.Parse("2006-01-02", s)
	if err != nil {
		t.Fatalf("parse date %q: %v", s, err)
	}
	return &d
}

func TestPgSummary(t *testing.T) {
	st := integrationStore(t)
	ctx := context.Background()

	sum, err := st.Summary(ctx, nil, nil)
	if err != nil {
		t.Fatalf("Summary: %v", err)
	}
	if sum.Revenue <= 0 {
		t.Errorf("revenue should be positive, got %.2f", sum.Revenue)
	}
	if sum.Orders <= 0 {
		t.Errorf("orders should be positive, got %d", sum.Orders)
	}
	if sum.AOV <= 0 {
		t.Errorf("aov should be positive, got %.2f", sum.AOV)
	}
	if sum.Customers <= 0 {
		t.Errorf("customers should be positive, got %d", sum.Customers)
	}
	if sum.DataStart == "" || sum.DataEnd == "" {
		t.Errorf("data range missing: %q → %q", sum.DataStart, sum.DataEnd)
	}
	if sum.DataStart >= sum.DataEnd {
		t.Errorf("data range unordered: %q >= %q", sum.DataStart, sum.DataEnd)
	}
	if sum.TopCategory == "" {
		t.Error("top category should be non-empty")
	}

	// Windowed summary must agree exactly with the underlying daily table.
	from, to := date(t, "2018-08-01"), date(t, "2018-08-15")
	win, err := st.Summary(ctx, from, to)
	if err != nil {
		t.Fatalf("Summary (windowed): %v", err)
	}
	var wantOrderCount int
	var wantRevenue float64
	err = st.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(order_count), 0)::int, COALESCE(SUM(revenue), 0)::float8
		FROM gold.daily_revenue
		WHERE date BETWEEN $1::date AND $2::date`, from, to).Scan(&wantOrderCount, &wantRevenue)
	if err != nil {
		t.Fatalf("reference query: %v", err)
	}
	if win.Orders != wantOrderCount {
		t.Errorf("windowed orders = %d, want %d", win.Orders, wantOrderCount)
	}
	if diff := abs(win.Revenue - wantRevenue); diff > 0.01 {
		t.Errorf("windowed revenue = %.2f, want %.2f (diff %.2f)", win.Revenue, wantRevenue, diff)
	}

	// A window whose to < from must be rejected by the handler layer — but the
	// store itself should not crash: it just filters to nothing.
	flipped, err := st.Summary(ctx, date(t, "2018-08-15"), date(t, "2018-08-01"))
	if err != nil {
		t.Fatalf("Summary (flipped window) should not error: %v", err)
	}
	_ = flipped
}

func TestPgRevenueDaily(t *testing.T) {
	st := integrationStore(t)
	ctx := context.Background()

	all, err := st.RevenueDaily(ctx, nil, nil)
	if err != nil {
		t.Fatalf("RevenueDaily: %v", err)
	}
	if len(all) < 100 {
		t.Fatalf("expected a full daily series (>=100 points), got %d", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i].Date <= all[i-1].Date {
			t.Fatalf("series not ascending at %d: %q then %q", i, all[i-1].Date, all[i].Date)
		}
	}

	from, to := date(t, "2018-08-01"), date(t, "2018-08-31")
	win, err := st.RevenueDaily(ctx, from, to)
	if err != nil {
		t.Fatalf("RevenueDaily (windowed): %v", err)
	}
	if len(win) == 0 {
		t.Fatal("windowed series empty")
	}
	for _, p := range win {
		if p.Date < "2018-08-01" || p.Date > "2018-08-31" {
			t.Errorf("point %q outside requested window", p.Date)
		}
	}
}

func TestPgOrdersDaily(t *testing.T) {
	st := integrationStore(t)
	ctx := context.Background()

	pts, err := st.OrdersDaily(ctx, nil, nil)
	if err != nil {
		t.Fatalf("OrdersDaily: %v", err)
	}
	if len(pts) == 0 {
		t.Fatal("orders daily empty")
	}
	for i := 1; i < len(pts); i++ {
		if pts[i].Date <= pts[i-1].Date {
			t.Fatalf("series not ascending at %d", i)
		}
	}
	for _, p := range pts {
		if p.Delivered > p.Orders {
			t.Errorf("delivered (%d) exceeds total (%d) on %s", p.Delivered, p.Orders, p.Date)
		}
		if p.Lost > p.Orders {
			t.Errorf("lost (%d) exceeds total (%d) on %s", p.Lost, p.Orders, p.Date)
		}
	}
}

func TestPgTopCategories(t *testing.T) {
	st := integrationStore(t)
	ctx := context.Background()

	cats, err := st.TopCategories(ctx, "revenue", 5)
	if err != nil {
		t.Fatalf("TopCategories(revenue): %v", err)
	}
	if len(cats) == 0 || len(cats) > 5 {
		t.Fatalf("expected 1..5 categories, got %d", len(cats))
	}
	for i := 1; i < len(cats); i++ {
		if cats[i].Revenue > cats[i-1].Revenue {
			t.Errorf("revenue not descending at %d: %.2f then %.2f", i, cats[i-1].Revenue, cats[i].Revenue)
		}
	}

	catsOrder, err := st.TopCategories(ctx, "orders", 3)
	if err != nil {
		t.Fatalf("TopCategories(orders): %v", err)
	}
	if len(catsOrder) > 3 {
		t.Errorf("limit ignored: %d", len(catsOrder))
	}
	for i := 1; i < len(catsOrder); i++ {
		if catsOrder[i].Orders > catsOrder[i-1].Orders {
			t.Errorf("orders not descending at %d", i)
		}
	}

	// Injection guard: the metric string is never interpolated into SQL.
	for _, bad := range []string{"revenue; DROP TABLE gold.fct_orders", "orders) --", "1=1", ""} {
		if _, err := st.TopCategories(ctx, bad, 5); err == nil {
			t.Errorf("TopCategories(%q) should have errored", bad)
		}
	}
}

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}
