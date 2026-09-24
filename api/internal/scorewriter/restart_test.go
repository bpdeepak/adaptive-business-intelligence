package scorewriter

import (
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"abi/internal/stream"
	"abi/internal/telemetry"
)

// Restart-safety of the score-writer (audit C10b). Its state (rings, rate
// windows) used to be snapshotted every 30 s while offsets auto-committed every
// ~5 s, so after a crash the restored state and the stream position disagreed in
// both directions: events between snapshot and commit vanished from the rings,
// and redelivered ones were folded in twice. The snapshot now carries the
// consumer position, offsets are committed only after the snapshot is durable,
// and anything the snapshot already covers is skipped on redelivery.

func newTestService(slack time.Duration) *Service {
	refs := testRefs()
	return &Service{
		log:       slog.New(slog.DiscardHandler),
		refs:      refs,
		asm:       NewFraudAssembler(refs),
		rings:     NewRings(),
		rates:     map[string]*RateTracker{},
		gate:      NewGate(slack, 1000),
		jobs:      newDropQueue(100),
		seen:      map[string]struct{}{},
		processed: map[tp]int64{},
		resume:    map[tp]int64{},
		reg:       telemetry.NewRegistry(),
	}
}

var t0 = time.Date(2017, 3, 1, 10, 0, 0, 0, time.UTC)

func orderRecord(t *testing.T, offset int64, customer string, at time.Time) *kgo.Record {
	t.Helper()
	env, err := stream.NewEnvelope(stream.EventOrderPlaced, customer, at, "l0", stream.OrderPlaced{
		OrderID: "o-" + at.Format("150405") + "-" + customer, CustomerID: customer, PurchaseTime: at,
		PaymentValue: 10, Items: []stream.OrderItem{{ProductID: "p1", Price: 10}},
	})
	if err != nil {
		t.Fatalf("envelope: %v", err)
	}
	raw, _ := json.Marshal(env)
	return &kgo.Record{Topic: stream.TopicOrders, Partition: 0, Offset: offset, Value: raw}
}

func roundTrip(t *testing.T, st *State) *State {
	t.Helper()
	raw, err := json.Marshal(st) // what gold.scorewriter_state stores
	if err != nil {
		t.Fatal(err)
	}
	var out State
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return &out
}

func TestRestartDoesNotDoubleCountRedeliveredRecords(t *testing.T) {
	s1 := newTestService(0)
	var recs []*kgo.Record
	for i := 0; i < 8; i++ {
		recs = append(recs, orderRecord(t, int64(i), "c1", t0.Add(time.Duration(i)*time.Hour)))
	}
	s1.ingestAll(recs[:5]) // processed offsets 0..4, then the snapshot is taken
	st := roundTrip(t, s1.snapshot())
	if got := st.Offsets[tp{stream.TopicOrders, 0}.key()]; got != 5 {
		t.Fatalf("snapshot offset = %d, want 5 (next offset to process)", got)
	}

	// Crash + restart: a fresh service restores the snapshot. The commit lagged,
	// so the broker redelivers from offset 3: records 3,4 are duplicates.
	s2 := newTestService(0)
	s2.restoreFrom(st)
	s2.ingestAll(recs[3:8])

	if got := s2.processed[tp{stream.TopicOrders, 0}]; got != 8 {
		t.Errorf("position after resume = %d, want 8", got)
	}
	// 8 distinct orders, one per hour: at t0+8h the 24h velocity is exactly 8.
	// Folding the two redelivered ones in again would make it 10.
	if v, _ := s2.rings.Velocity("c1", float64(t0.Add(8*time.Hour).Unix())); v != 8 {
		t.Errorf("velocity = %d, want 8 (redelivered records were double-counted)", v)
	}
}

func TestSnapshotOffsetsCoverOnlyProcessedRecordsNotMerelyConsumed(t *testing.T) {
	// A 2-minute reorder slack holds recent events back; the snapshot position
	// must not advance past records still buffered in the gate, or a crash would
	// lose them (they were consumed, never processed).
	s := newTestService(2 * time.Minute)
	s.ingestAll([]*kgo.Record{
		orderRecord(t, 0, "a", t0),
		orderRecord(t, 1, "b", t0.Add(10*time.Minute)),
		orderRecord(t, 2, "c", t0.Add(20*time.Minute)),
	})
	if s.gate.Len() == 0 {
		t.Fatal("test setup: expected the newest event to still be held by the gate")
	}
	got := s.snapshot().Offsets[tp{stream.TopicOrders, 0}.key()]
	if got != 2 {
		t.Errorf("snapshot offset = %d, want 2: records 0 and 1 processed, record 2 still buffered", got)
	}
}

func TestRestoreFromIsNilSafe(t *testing.T) {
	// RestoreState used to dereference st.Rings unconditionally in its log line.
	s := newTestService(0)
	s.restoreFrom(nil)
	s.restoreFrom(&State{})
	s.restoreFrom(&State{Offsets: map[string]int64{tp{stream.TopicOrders, 0}.key(): 7}})
	if !s.alreadyProcessed(recPos{tp: tp{stream.TopicOrders, 0}, offset: 6}) {
		t.Error("offset-only snapshot (no rings) must still restore the resume position")
	}
	if s.alreadyProcessed(recPos{tp: tp{stream.TopicOrders, 0}, offset: 7}) {
		t.Error("the next unprocessed offset must not be skipped")
	}
}

func TestParseTPHandlesTopicNamesContainingTheSeparator(t *testing.T) {
	p := tp{topic: "a|b", partition: 3}
	got, ok := parseTP(p.key())
	if !ok || got != p {
		t.Fatalf("parseTP(%q) = %+v, %v", p.key(), got, ok)
	}
	if _, ok := parseTP("no-separator"); ok {
		t.Error("a malformed key must be rejected")
	}
}
