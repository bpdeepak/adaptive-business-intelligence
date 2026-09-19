package model

import "time"

// ---------------------------------------------------------------------------
// Replay data (producer loads from gold; shared with the simulator).
// ---------------------------------------------------------------------------

// ReplayItem is one line item of a historical order, as needed by the producer
// to reconstruct order events with item detail.
type ReplayItem struct {
	ProductID string  `json:"product_id"`
	SellerID  string  `json:"seller_id"`
	Price     float64 `json:"price"`
	Freight   float64 `json:"freight_value"`
}

// ReplayOrder is one historical order the producer replays onto the stream.
type ReplayOrder struct {
	OrderID      string
	CustomerID   string
	Status       string
	PurchaseAt   time.Time
	PaymentValue float64
	IsLost       bool
	Items        []ReplayItem
}

// ---------------------------------------------------------------------------
// Realtime metrics (gold.realtime_metrics / gold.anomalies, SSE, gRPC).
// ---------------------------------------------------------------------------

// RealtimeBucket is one 1-minute bucket of live metrics.
type RealtimeBucket struct {
	BucketStart    string  `json:"bucket_start"` // RFC3339 UTC
	Revenue        float64 `json:"revenue"`
	Orders         int64   `json:"orders"`
	ActiveSessions int64   `json:"active_sessions"`
	AnomalyFlag    bool    `json:"anomaly_flag"`
	UpdatedAt      string  `json:"updated_at"`
}

// Anomaly is one detected anomaly row (gold.anomalies).
type Anomaly struct {
	ID          int64   `json:"id"`
	Metric      string  `json:"metric"`
	BucketStart string  `json:"bucket_start"`
	Observed    float64 `json:"observed"`
	Expected    float64 `json:"expected"`
	ZScore      float64 `json:"z_score"`
	Severity    string  `json:"severity"`
	Status      string  `json:"status"`
	DetectedAt  string  `json:"detected_at"`
}

// MetricsUpdate is one push on the SSE/gRPC live stream. The first update after
// a subscriber connects carries a recent Snapshot; subsequent updates carry the
// Current bucket and any new Anomaly.
type MetricsUpdate struct {
	Snapshot        []RealtimeBucket `json:"snapshot,omitempty"`
	Current         RealtimeBucket   `json:"current"`
	Anomaly         *Anomaly         `json:"anomaly,omitempty"`
	SpeedMultiplier float64          `json:"speed_multiplier"`
	Status          string           `json:"status"` // "live" | "replay"
}
