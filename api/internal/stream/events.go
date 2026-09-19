// Package stream defines the Phase 1 event-layer contract: the message
// envelope, the Redpanda topic names, and the typed payloads for every event
// kind the platform produces or consumes. It is shared by the producer
// (cmd/producer) and the realtime consumers (cmd/server).
package stream

import (
	"encoding/json"
	"fmt"
	"time"
)

// SchemaVersion is the envelope schema version carried by every message.
const SchemaVersion = 1

// Topic names. Keys are listed next to each topic:
//
//	ecommerce.orders.events                  key = customer_id
//	ecommerce.clickstream.events             key = session_id
//	ecommerce.catalog.price_changes          key = product_id
//	ecommerce.internal.training_labels       key = session_id   (restricted, Phase 2 ground truth)
//	ecommerce.anomalies                      key = metric
const (
	TopicOrders    = "ecommerce.orders.events"
	TopicClicks    = "ecommerce.clickstream.events"
	TopicPrices    = "ecommerce.catalog.price_changes"
	TopicLabels    = "ecommerce.internal.training_labels"
	TopicAnomalies = "ecommerce.anomalies"
)

// AllTopics lists every topic the platform uses (for admin/topic creation).
var AllTopics = []string{TopicOrders, TopicClicks, TopicPrices, TopicLabels, TopicAnomalies}

// Event types.
const (
	EventOrderPlaced   = "order.placed"
	EventPageView      = "page.view"
	EventCartAbandoned = "cart.abandoned"
	EventPriceChanged  = "price.changed"
	EventTrainingLabel = "training.session_label"
	EventAnomaly       = "anomaly.detected"
)

// Envelope is the wire format for every message. It deliberately mirrors the
// REST response envelope from Phase 0 (a `data` payload with context) so the
// platform has one envelope shape on both HTTP and Kafka.
type Envelope struct {
	SchemaVersion int       `json:"schema_version"`
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	OccurredAt    time.Time `json:"occurred_at"`
	ProducedAt    time.Time `json:"produced_at"`
	Key           string    `json:"key"`
	// LoopID tags which replay loop produced the event ("l0", "l1", ...). It is
	// the streaming counterpart of the batch loader's `_batch_id` and lets the
	// bronze-writer stamp exact provenance without coupling producer and
	// consumer run ids.
	LoopID  string          `json:"loop_id,omitempty"`
	Payload json.RawMessage `json:"payload"`
}

// NewEnvelope builds an envelope for an event, marshalling its typed payload.
func NewEnvelope(eventType, key string, occurredAt time.Time, loopID string, payload any) (Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal %s payload: %w", eventType, err)
	}
	return Envelope{
		SchemaVersion: SchemaVersion,
		EventID:       newEventID(loopID),
		EventType:     eventType,
		OccurredAt:    occurredAt,
		ProducedAt:    time.Now().UTC(),
		Key:           key,
		LoopID:        loopID,
		Payload:       raw,
	}, nil
}

// DecodePayload unmarshals an envelope's payload into the given typed payload.
func (e Envelope) DecodePayload(v any) error {
	return json.Unmarshal(e.Payload, v)
}

// newEventID returns a unique event id. Byte-level uniqueness comes from a
// random suffix; the loop suffix is belt-and-braces so a replay loop can never
// collide with an earlier loop's stored event (see Phase 1 spec, §4).
func newEventID(loopID string) string {
	id := randomHex(12)
	if loopID != "" {
		return fmt.Sprintf("%s-%s", loopID, id)
	}
	return id
}

// ---------------------------------------------------------------------------
// Typed payloads
// ---------------------------------------------------------------------------

// OrderItem is one line item of an order event. References the same product ids
// as the batch data so streaming and batch layers join cleanly.
type OrderItem struct {
	ProductID string  `json:"product_id"`
	SellerID  string  `json:"seller_id"`
	Price     float64 `json:"price"`
	Freight   float64 `json:"freight_value"`
	Quantity  int     `json:"quantity"`
}

// OrderPlaced is the payload of EventOrderPlaced. `Status`/`IsLost` and
// `PaymentValue` are finalized values from the historical dataset, so the
// aggregator applies the exact same revenue definition as the batch layer
// (status NOT IN ('canceled','unavailable') AND payment_value_total > 0).
type OrderPlaced struct {
	OrderID      string      `json:"order_id"`
	CustomerID   string      `json:"customer_id"`
	Status       string      `json:"order_status"`
	PurchaseTime time.Time   `json:"order_purchase_timestamp"`
	PaymentValue float64     `json:"payment_value_total"`
	IsLost       bool        `json:"is_lost"`
	Items        []OrderItem `json:"items"`
}

// Page types for clickstream events.
const (
	PageHome     = "home"
	PageCategory = "category"
	PageProduct  = "product"
	PageSearch   = "search"
	PageCart     = "cart"
	PageCheckout = "checkout"
)

// PageView is the payload of EventPageView. CustomerID is empty for anonymous
// (pre-login) sessions.
type PageView struct {
	SessionID  string `json:"session_id"`
	CustomerID string `json:"customer_id,omitempty"`
	PageType   string `json:"page_type"`
	ProductID  string `json:"product_id,omitempty"`
}

// CartAbandoned is the payload of EventCartAbandoned — the terminal event of a
// session that browsed but never converted.
type CartAbandoned struct {
	SessionID  string  `json:"session_id"`
	CustomerID string  `json:"customer_id,omitempty"`
	ItemsCount int     `json:"items_count"`
	CartValue  float64 `json:"cart_value"`
}

// PriceChanged is the payload of EventPriceChanged (synthetic catalog feed).
type PriceChanged struct {
	ProductID string    `json:"product_id"`
	OldPrice  float64   `json:"old_price"`
	NewPrice  float64   `json:"new_price"`
	ChangedAt time.Time `json:"changed_at"`
}

// SessionLabel is the payload of EventTrainingLabel on the restricted
// ecommerce.internal.training_labels topic. It is the ONLY place the synthetic
// ground-truth flag exists; it is never wrapped into a clickstream/order
// payload and no serving consumer reads it. Phase 2 trains a bot classifier
// against these labels.
type SessionLabel struct {
	SessionID    string `json:"session_id"`
	IsBot        bool   `json:"is_synthetic_bot"`
	LoopID       string `json:"loop_id"`
	IsConverting bool   `json:"is_converting"`
}

// AnomalyEvent is the payload of EventAnomaly on ecommerce.anomalies.
type AnomalyEvent struct {
	ID         int64     `json:"anomaly_id"`
	Metric     string    `json:"metric"`
	BucketTime time.Time `json:"bucket_start"`
	Observed   float64   `json:"observed"`
	Expected   float64   `json:"expected"`
	ZScore     float64   `json:"z_score"`
	Severity   string    `json:"severity"`
	DetectedAt time.Time `json:"detected_at"`
}
