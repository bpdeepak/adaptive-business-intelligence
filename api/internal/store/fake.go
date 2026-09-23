package store

import (
	"context"
	"time"

	"abi/internal/model"
)

// FakeStore is an in-memory Store for tests. Any method whose *Fn is nil
// returns a benign default.
type FakeStore struct {
	SummaryFn       func(ctx context.Context, from, to *time.Time) (model.Summary, error)
	RevenueDailyFn  func(ctx context.Context, from, to *time.Time) ([]model.SeriesPoint, error)
	OrdersDailyFn   func(ctx context.Context, from, to *time.Time) ([]model.OrderSeriesPoint, error)
	TopCategoriesFn func(ctx context.Context, metric string, limit int) ([]model.Category, error)

	RecentMetricsFn func(ctx context.Context, n int) ([]model.RealtimeBucket, error)
	OpenAnomaliesFn func(ctx context.Context, detector string) ([]model.Anomaly, error)
	DismissFn       func(ctx context.Context, id int64) error
}

func (f *FakeStore) Summary(ctx context.Context, from, to *time.Time) (model.Summary, error) {
	if f.SummaryFn != nil {
		return f.SummaryFn(ctx, from, to)
	}
	return model.Summary{}, nil
}

func (f *FakeStore) RevenueDaily(ctx context.Context, from, to *time.Time) ([]model.SeriesPoint, error) {
	if f.RevenueDailyFn != nil {
		return f.RevenueDailyFn(ctx, from, to)
	}
	return []model.SeriesPoint{}, nil
}

func (f *FakeStore) OrdersDaily(ctx context.Context, from, to *time.Time) ([]model.OrderSeriesPoint, error) {
	if f.OrdersDailyFn != nil {
		return f.OrdersDailyFn(ctx, from, to)
	}
	return []model.OrderSeriesPoint{}, nil
}

func (f *FakeStore) TopCategories(ctx context.Context, metric string, limit int) ([]model.Category, error) {
	if f.TopCategoriesFn != nil {
		return f.TopCategoriesFn(ctx, metric, limit)
	}
	return []model.Category{}, nil
}

func (f *FakeStore) RecentRealtimeMetrics(ctx context.Context, n int) ([]model.RealtimeBucket, error) {
	if f.RecentMetricsFn != nil {
		return f.RecentMetricsFn(ctx, n)
	}
	return []model.RealtimeBucket{}, nil
}

func (f *FakeStore) OpenAnomalies(ctx context.Context, detector string) ([]model.Anomaly, error) {
	if f.OpenAnomaliesFn != nil {
		return f.OpenAnomaliesFn(ctx, detector)
	}
	return []model.Anomaly{}, nil
}

func (f *FakeStore) DismissAnomaly(ctx context.Context, id int64) error {
	if f.DismissFn != nil {
		return f.DismissFn(ctx, id)
	}
	return nil
}
