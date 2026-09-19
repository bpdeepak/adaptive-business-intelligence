// Package store defines the data-access boundary used by the HTTP layer.
package store

import (
	"context"
	"time"

	"abi/internal/model"
)

// Store is the query surface the API depends on. The production implementation
// reads from the dbt gold layer; tests use a fake.
type Store interface {
	Summary(ctx context.Context, from, to *time.Time) (model.Summary, error)
	RevenueDaily(ctx context.Context, from, to *time.Time) ([]model.SeriesPoint, error)
	OrdersDaily(ctx context.Context, from, to *time.Time) ([]model.OrderSeriesPoint, error)
	TopCategories(ctx context.Context, metric string, limit int) ([]model.Category, error)

	// Realtime (Phase 1) surface backed by gold.realtime_metrics/anomalies.
	RecentRealtimeMetrics(ctx context.Context, n int) ([]model.RealtimeBucket, error)
	OpenAnomalies(ctx context.Context) ([]model.Anomaly, error)
	DismissAnomaly(ctx context.Context, id int64) error
}
