// Package model defines the API response payloads (the "contract").
package model

import "time"

// Summary is the headline set of business metrics for a period.
type Summary struct {
	Revenue     float64 `json:"revenue"`
	Orders      int     `json:"orders"`
	AOV         float64 `json:"aov"`
	Customers   int     `json:"customers"`
	TopCategory string  `json:"top_category"`
	// DataStart / DataEnd bound the observed dailies (data range of the dataset).
	DataStart string `json:"data_start"`
	DataEnd   string `json:"data_end"`
}

// SeriesPoint is a single (date, value) row of a daily time series.
type SeriesPoint struct {
	Date  string  `json:"date"`
	Value float64 `json:"value"`
}

// OrderSeriesPoint is a single day's order counts.
type OrderSeriesPoint struct {
	Date      string `json:"date"`
	Orders    int    `json:"orders"`
	Delivered int    `json:"delivered"`
	Lost      int    `json:"lost"`
}

// Category is one product category's aggregates for the top-categories panel.
type Category struct {
	Category    string  `json:"category"`
	Orders      int     `json:"orders"`
	Revenue     float64 `json:"revenue"`
	RevenueRank int     `json:"revenue_rank"`
	OrderRank   int     `json:"order_rank"`
}

// Meta carries response-level context in every envelope.
type Meta struct {
	GeneratedAt string  `json:"generated_at"`
	TookMS      float64 `json:"took_ms"`
}

// Envelope is the standard JSON response shape for the ABI API.
type Envelope struct {
	Data  any    `json:"data,omitempty"`
	Error string `json:"error,omitempty"`
	Meta  Meta   `json:"meta"`
}

// Health is the /healthz payload.
type Health struct {
	Status string `json:"status"`
}

func newMeta(t time.Time) Meta {
	return Meta{GeneratedAt: t.UTC().Format(time.RFC3339)}
}