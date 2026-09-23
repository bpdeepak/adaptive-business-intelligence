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

// ReplayPayment is one payment row (silver.stg_order_payments) as needed by the
// producer and, through the order event, by the Phase 3 fraud feature assembler
// (payment_count, installments_max, and the pay_* one-hot of the primary type).
type ReplayPayment struct {
	Type         string  `json:"payment_type"`
	Installments int     `json:"payment_installments"`
	Value        float64 `json:"payment_value"`
}

// ReplayOrder is one historical order the producer replays onto the stream.
// Payments are loaded from silver.stg_order_payments so the stream score-writer
// sees the same payment detail the batch feature builder sees — they are part
// of the shared feature contract, not re-derived downstream.
type ReplayOrder struct {
	OrderID      string
	CustomerID   string
	Status       string
	PurchaseAt   time.Time
	PaymentValue float64
	IsLost       bool
	Items        []ReplayItem
	Payments     []ReplayPayment
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
	Detector    string  `json:"detector"` // "statistical" (Phase 1) | "model" (Phase 3 rate-based)
	BucketStart string  `json:"bucket_start"`
	Observed    float64 `json:"observed"`
	Expected    float64 `json:"expected"`
	ZScore      float64 `json:"z_score,omitempty"` // NULL for rate-based model anomalies
	Severity    string  `json:"severity"`
	Status      string  `json:"status"`
	DetectedAt  string  `json:"detected_at"`
}

// MetricsUpdate is one push on the SSE/gRPC live stream. The first update after
// a subscriber connects carries a recent Snapshot; subsequent updates carry the
// Current bucket and any new Anomaly. Source is always "live_replay" on this
// stream — included explicitly so an LLM tool layer cannot mistake the numbers
// for batch truth.
type MetricsUpdate struct {
	Snapshot        []RealtimeBucket `json:"snapshot,omitempty"`
	Current         RealtimeBucket   `json:"current"`
	Anomaly         *Anomaly         `json:"anomaly,omitempty"`
	SpeedMultiplier float64          `json:"speed_multiplier"`
	Status          string           `json:"status"` // "live" | "replay"
	Source          string           `json:"source,omitempty"`
}
