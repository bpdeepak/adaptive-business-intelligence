// Package events is the in-process domain event bus of Phase 4. Producers
// (the stream score-writer, the realtime anomaly aggregator, the drift poller)
// publish typed events AFTER their DB writes; the playbook engine is a
// subscriber that proposes governed actions. It deliberately mirrors the
// Phase 1 broadcaster pattern (fan-out, drop-slow-consumers, never block the
// producer) so the scoring/anomaly hot path pays no latency for governance.
package events

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Type identifies one domain event. Only events the playbook machinery
// understands are defined — adding a producer is one Publish call here.
type Type string

const (
	// TypeOrderScored fires after an order's fraud prediction is persisted.
	TypeOrderScored Type = "order_scored"
	// TypeSessionScored fires after a session's bot prediction is persisted.
	TypeSessionScored Type = "session_scored"
	// TypeAnomalyDetected fires after any anomaly (statistical or model) row
	// is persisted to gold.anomalies.
	TypeAnomalyDetected Type = "anomaly_detected"
	// TypeForecastUpdated fires when a forecast family is (re)generated.
	// Phase 4 has no producer yet — the rule stays inert until the Phase 5
	// scheduled forecast worker lands; the type is part of the engine contract.
	TypeForecastUpdated Type = "forecast_updated"
	// TypeChurnScored fires when a customer's churn score is produced.
	// Phase 4 has no producer yet either (churn is batch-scored in Phase 2);
	// the retention playbooks are shipped dormant and activate the moment a
	// Phase 5 scheduled churn-scoring job emits this event.
	TypeChurnScored Type = "churn_scored"
	// TypeDriftComputed fires when the monitoring ticker observes a new
	// gold.model_drift row (PSI drift or the backtest-reproduction check) for a
	// model — this is what drives the drift-triggers-retrain-proposal rule.
	TypeDriftComputed Type = "drift_computed"
)

// Event is one domain fact on the bus. Payload keys are the flat JSON objects
// playbook conditions reference (e.g. prediction.model, prediction.score,
// registry.recommended_threshold, drift.status).
type Event struct {
	Type    Type
	At      time.Time
	Payload map[string]any
}

const (
	subBuffer = 64
)

// Bus fans one Event out to every subscriber. A slow subscriber (the buffer
// fills) drops rather than stalls the producers, mirroring the SSE
// broadcaster's backpressure contract.
type Bus struct {
	mu     sync.Mutex
	nextID int64
	subs   map[int64]chan Event

	// published counts events handed to Publish; dropped counts deliveries lost to
	// a full subscriber buffer. A drop is harmless for the dashboard but costs a
	// governance proposal until the reconciler re-derives it, so it must be
	// countable (exported as abi_domain_events_dropped_total).
	published atomic.Int64
	dropped   atomic.Int64
}

// Published is the number of events passed to Publish.
func (b *Bus) Published() int64 { return b.published.Load() }

// Dropped is the number of (event, subscriber) deliveries dropped because the
// subscriber's buffer was full.
func (b *Bus) Dropped() int64 { return b.dropped.Load() }

// New creates an empty bus.
func New() *Bus {
	return &Bus{subs: make(map[int64]chan Event)}
}

// Subscribe registers a subscriber and returns its receive channel plus an
// unsubscribe function. Events published before Subscribe are not replayed —
// the engine is wired before any producer starts, and freshly polled drift
// uses a DB watermark so nothing is lost across restarts.
func (b *Bus) Subscribe() (<-chan Event, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextID++
	id := b.nextID
	ch := make(chan Event, subBuffer)
	b.subs[id] = ch
	return ch, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		// The channel is deliberately NOT closed: reading from a closed
		// channel yields an immediate zero-value Event, which a stale reader
		// could mistake for a real (empty) event. Dropping the subscription
		// from the map stops all sends; the engine exits on ctx cancellation.
		delete(b.subs, id)
	}
}

// Publish delivers the event to every subscriber without blocking on any of
// them (full buffers drop the event for that subscriber).
func (b *Bus) Publish(e Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if e.At.IsZero() {
		e.At = time.Now().UTC()
	}
	b.published.Add(1)
	for _, ch := range b.subs {
		select {
		case ch <- e:
		default:
			// Slow consumer: drop (never block the producer). The authoritative
			// record is the DB row the producer already wrote and the governance
			// reconciler re-derives the proposal from it — but the drop is counted
			// so it is visible instead of silent.
			b.dropped.Add(1)
		}
	}
}

// PublishWait delivers the event to every subscriber, waiting up to `timeout`
// per subscriber for buffer space instead of dropping immediately. It is for
// BATCH producers (the churn scorer, the forecast worker): they are not on a
// latency-sensitive path and emit bursts of hundreds of events, which the
// drop-don't-block Publish would mostly shed (observed: 273 of 338). Waiting keeps
// live delivery the normal case, so the reconciler's reconciled=true marker keeps
// meaning "a genuine drop was recovered". A subscriber that still does not accept
// within the timeout (stuck, or unsubscribed mid-publish) counts as a drop, and the
// reconciler still covers it. Returns early with ctx's error if ctx ends.
func (b *Bus) PublishWait(ctx context.Context, e Event, timeout time.Duration) error {
	if e.At.IsZero() {
		e.At = time.Now().UTC()
	}
	b.mu.Lock()
	subs := make([]chan Event, 0, len(b.subs))
	for _, ch := range b.subs {
		subs = append(subs, ch)
	}
	b.mu.Unlock()
	b.published.Add(1)
	for _, ch := range subs {
		timer := time.NewTimer(timeout)
		select {
		case ch <- e:
			timer.Stop()
		case <-timer.C:
			b.dropped.Add(1)
		case <-ctx.Done():
			timer.Stop()
			b.dropped.Add(1)
			return ctx.Err()
		}
	}
	return nil
}

// SubscribeCount is the number of attached subscribers.
func (b *Bus) SubscribeCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs)
}