package store

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"abi/internal/model"
)

// PostgresStore reads metrics from the dbt gold layer (schema "gold").
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore opens a pgx connection pool. Connections are lazy; the pool
// does not require the database to be reachable at construction time.
func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}
	return &PostgresStore{pool: pool}, nil
}

// Close releases the underlying pool.
func (s *PostgresStore) Close() { s.pool.Close() }

func (s *PostgresStore) Summary(ctx context.Context, from, to *time.Time) (model.Summary, error) {
	const q = `
SELECT
    COALESCE(SUM(rev.revenue), 0)::float8,
    COALESCE(SUM(rev.order_count), 0)::int,
    CASE WHEN SUM(rev.order_count) > 0
         THEN SUM(rev.revenue) / SUM(rev.order_count)
         ELSE 0 END::float8,
    (SELECT COUNT(DISTINCT f.customer_unique_id)::int
       FROM gold.fct_orders f
      WHERE f.order_status NOT IN ('canceled', 'unavailable')),
    (SELECT to_char(MIN(date), 'YYYY-MM-DD') FROM gold.daily_revenue),
    (SELECT to_char(MAX(date), 'YYYY-MM-DD') FROM gold.daily_revenue),
    (SELECT tc.product_category FROM gold.top_categories tc
      ORDER BY tc.revenue_rank LIMIT 1)
FROM gold.daily_revenue rev
WHERE ($1::date IS NULL OR rev.date >= $1::date)
  AND ($2::date IS NULL OR rev.date <= $2::date)`

	var out model.Summary
	var customers int32
	var dataStart, dataEnd, topCat *string
	err := s.pool.QueryRow(ctx, q, from, to).
		Scan(&out.Revenue, &out.Orders, &out.AOV, &customers, &dataStart, &dataEnd, &topCat)
	if err != nil {
		return model.Summary{}, fmt.Errorf("summary: %w", err)
	}
	out.Customers = int(customers)
	out.AOV = math.Round(out.AOV*100) / 100
	if dataStart != nil {
		out.DataStart = *dataStart
	}
	if dataEnd != nil {
		out.DataEnd = *dataEnd
	}
	if topCat != nil {
		out.TopCategory = *topCat
	}
	return out, nil
}

func (s *PostgresStore) RevenueDaily(ctx context.Context, from, to *time.Time) ([]model.SeriesPoint, error) {
	const q = `
SELECT to_char(date, 'YYYY-MM-DD'), revenue::float8
FROM gold.daily_revenue
WHERE ($1::date IS NULL OR date >= $1::date)
  AND ($2::date IS NULL OR date <= $2::date)
ORDER BY date`

	rows, err := s.pool.Query(ctx, q, from, to)
	if err != nil {
		return nil, fmt.Errorf("revenue daily: %w", err)
	}
	defer rows.Close()

	out := make([]model.SeriesPoint, 0, 700)
	for rows.Next() {
		var p model.SeriesPoint
		if err := rows.Scan(&p.Date, &p.Value); err != nil {
			return nil, fmt.Errorf("revenue daily scan: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *PostgresStore) OrdersDaily(ctx context.Context, from, to *time.Time) ([]model.OrderSeriesPoint, error) {
	const q = `
SELECT to_char(date, 'YYYY-MM-DD'),
       total_orders::int,
       delivered_orders::int,
       lost_orders::int
FROM gold.daily_orders
WHERE ($1::date IS NULL OR date >= $1::date)
  AND ($2::date IS NULL OR date <= $2::date)
ORDER BY date`

	rows, err := s.pool.Query(ctx, q, from, to)
	if err != nil {
		return nil, fmt.Errorf("orders daily: %w", err)
	}
	defer rows.Close()

	out := make([]model.OrderSeriesPoint, 0, 700)
	for rows.Next() {
		var p model.OrderSeriesPoint
		var total, delivered, lost int32
		if err := rows.Scan(&p.Date, &total, &delivered, &lost); err != nil {
			return nil, fmt.Errorf("orders daily scan: %w", err)
		}
		p.Orders, p.Delivered, p.Lost = int(total), int(delivered), int(lost)
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *PostgresStore) TopCategories(ctx context.Context, metric string, limit int) ([]model.Category, error) {
	orderCol := "revenue_rank"
	switch metric {
	case "revenue":
		orderCol = "revenue_rank"
	case "orders":
		orderCol = "order_rank"
	default:
		return nil, fmt.Errorf("unsupported metric %q", metric)
	}

	// orderCol comes from a fixed whitelist above, so this is injection-safe.
	q := fmt.Sprintf(`
SELECT product_category,
       order_count::int,
       revenue::float8,
       revenue_rank::int,
       order_rank::int
FROM gold.top_categories
ORDER BY %s
LIMIT $1`, orderCol)

	rows, err := s.pool.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("top categories: %w", err)
	}
	defer rows.Close()

	out := make([]model.Category, 0, limit)
	for rows.Next() {
		var c model.Category
		var orders, revRank, ordRank int32
		if err := rows.Scan(&c.Category, &orders, &c.Revenue, &revRank, &ordRank); err != nil {
			return nil, fmt.Errorf("top categories scan: %w", err)
		}
		c.Orders, c.RevenueRank, c.OrderRank = int(orders), int(revRank), int(ordRank)
		out = append(out, c)
	}
	return out, rows.Err()
}