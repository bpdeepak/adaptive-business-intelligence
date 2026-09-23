package stream

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"abi/internal/model"
)

func base() time.Time {
	return time.Date(2016, 9, 4, 0, 0, 0, 0, time.UTC)
}

func sampleOrders() []model.ReplayOrder {
	b := base()
	return []model.ReplayOrder{
		{
			OrderID: "o1", CustomerID: "c1", Status: "delivered",
			PurchaseAt: b.Add(2 * time.Hour), PaymentValue: 100,
			Items: []model.ReplayItem{
				{ProductID: "p1", Price: 50, Freight: 5},
				{ProductID: "p2", Price: 45, Freight: 0},
			},
		},
		{
			OrderID: "o2", CustomerID: "c1", Status: "canceled",
			PurchaseAt: b.Add(3 * time.Hour), PaymentValue: 0, IsLost: true,
			Items: []model.ReplayItem{{ProductID: "p3", Price: 20, Freight: 0}},
		},
		{
			OrderID: "o3", CustomerID: "c2", Status: "delivered",
			PurchaseAt: b.Add(5 * time.Hour), PaymentValue: 250,
			Items: []model.ReplayItem{{ProductID: "p1", Price: 50, Freight: 8}},
		},
	}
}

func newTestSim(t *testing.T, orders []model.ReplayOrder, botRatio float64) (*Simulator, *Ring, time.Time) {
	t.Helper()
	products := []string{"p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8"}
	prices := map[string]float64{
		"p1": 50, "p2": 45, "p3": 20, "p4": 30, "p5": 12, "p6": 70, "p7": 9, "p8": 99,
	}
	rate := float64(SecondsPerDay) // 1 wall second == 1 simulated day
	ring := NewRing(100_000)
	sim := NewSimulator(SimConfig{
		Orders:     orders,
		Products:   products,
		Price:      prices,
		Rate:       rate,
		BotRatio:   botRatio,
		Conversion: 0.03,
		ClickMin:   3,
		ClickMax:   6,
	}, rand.New(rand.NewSource(42)), ring)
	return sim, ring, time.Now()
}

func drain(t *testing.T, ring *Ring) []Envelope {
	t.Helper()
	var out []Envelope
	for {
		e, ok := ring.Pop()
		if !ok {
			break
		}
		out = append(out, e)
	}
	return out
}

// TestSimulatorEmitsOrdersInOrder checks that a full day of simulated time
// produces every order event, in simulated-time order, with correct payload
// and key, and that click events for a session precede its order.
func TestSimulatorEmitsOrdersInOrder(t *testing.T) {
	sim, ring, w0 := newTestSim(t, sampleOrders(), 0.5)

	// One wall second replays a whole simulated day → all three orders fire.
	sim.Advance(w0.Add(time.Second))

	envs := drain(t, ring)
	if len(envs) == 0 {
		t.Fatal("no events emitted")
	}

	var orders []Envelope
	for _, e := range envs {
		if e.EventType == EventOrderPlaced {
			orders = append(orders, e)
		}
	}
	if len(orders) != 3 {
		t.Fatalf("expected 3 order events, got %d", len(orders))
	}

	// Strictly non-decreasing simulated occurrence time.
	for i := 1; i < len(envs); i++ {
		if envs[i].OccurredAt.Before(envs[i-1].OccurredAt) {
			t.Fatalf("events out of order at %d: %v after %v",
				i, envs[i].OccurredAt, envs[i-1].OccurredAt)
		}
	}

	// Order payloads carry item detail + the revenue filter fields.
	var o1 OrderPlaced
	if err := orders[0].DecodePayload(&o1); err != nil {
		t.Fatal(err)
	}
	if o1.OrderID != "o1" || len(o1.Items) != 2 || o1.PaymentValue != 100 {
		t.Fatalf("unexpected o1 payload: %+v", o1)
	}
	if orders[0].Key != "c1" {
		t.Fatalf("order key = %q, want customer c1", orders[0].Key)
	}

	// The canceled order is emitted too (status is a data field, not a filter).
	var o2 OrderPlaced
	if err := orders[1].DecodePayload(&o2); err != nil {
		t.Fatal(err)
	}
	if o2.Status != "canceled" || !o2.IsLost {
		t.Fatalf("expected canceled/lost order, got %+v", o2)
	}

	// A converting session's clicks must all precede its order event.
	seen := make(map[string][]time.Time) // session id → pageview times
	for _, e := range envs {
		if e.EventType != EventPageView {
			continue
		}
		var pv PageView
		if err := e.DecodePayload(&pv); err != nil {
			t.Fatal(err)
		}
		seen[pv.SessionID] = append(seen[pv.SessionID], e.OccurredAt)
	}
	var clicksBeforeOrder bool
	for _, e := range envs {
		if e.EventType != EventOrderPlaced {
			continue
		}
		var op OrderPlaced
		if err := e.DecodePayload(&op); err != nil {
			t.Fatal(err)
		}
		sess := fmt.Sprintf("c-%s-%s-%s", op.CustomerID, op.OrderID, e.LoopID)
		for _, ct := range seen[sess] {
			if ct.Before(e.OccurredAt) {
				clicksBeforeOrder = true
			} else {
				t.Fatalf("page view at %v not before order at %v", ct, e.OccurredAt)
			}
		}
		if len(seen[sess]) == 0 {
			t.Fatalf("order %s has no preceding clicks from session %s", op.OrderID, sess)
		}
	}
	if !clicksBeforeOrder {
		t.Fatal("expected at least one click before each converting order")
	}
}

// TestSimulatorLoopWrap verifies the loop boundary: after the data span, the
// simulator wraps to l1, events get a fresh loop suffix, and session ids are
// loop-scoped (no collisions across loops).
func TestSimulatorLoopWrap(t *testing.T) {
	sim, ring, w0 := newTestSim(t, sampleOrders(), 0.02)

	// Sim span = max(2h,24h) = 24h → at 1 day per wall second, wrap after 1s.
	sim.Advance(w0.Add(1500 * time.Millisecond))
	if sim.Loop() != "l1" {
		t.Fatalf("expected wrap to l1, got %q", sim.Loop())
	}
	envs := drain(t, ring)

	var l0OrderIDs, l1OrderIDs []string
	for _, e := range envs {
		if e.EventType != EventOrderPlaced {
			continue
		}
		if e.LoopID == "l0" {
			l0OrderIDs = append(l0OrderIDs, e.EventID)
		}
		if e.LoopID == "l1" {
			l1OrderIDs = append(l1OrderIDs, e.EventID)
		}
	}
	if len(l0OrderIDs) == 0 || len(l1OrderIDs) == 0 {
		t.Fatalf("expected order events in both l0 and l1, got l0=%d l1=%d",
			len(l0OrderIDs), len(l1OrderIDs))
	}
	if l0OrderIDs[0] == l1OrderIDs[0] {
		t.Fatal("event ids must differ across loops")
	}
}

// TestSessionEndEmittedForCompletedSessions pins the Phase 3 session.end
// contract: every completed session (converting + abandoned, bot + human) ends
// with a session.end payload whose statistics are internally consistent with
// the page views that preceded it — the feature vector a stream score-writer
// forwards verbatim to the bot model.
func TestSessionEndEmittedForCompletedSessions(t *testing.T) {
	sim, ring, w0 := newTestSim(t, sampleOrders(), 0.5)
	sim.Advance(w0.Add(time.Second))
	envs := drain(t, ring)

	// session id → sorted page-view times + page types.
	pages := map[string][]struct {
		at   time.Time
		page string
	}{}
	carts := map[string]time.Time{}
	var ends []Envelope
	for _, e := range envs {
		switch e.EventType {
		case EventPageView:
			var pv PageView
			if err := e.DecodePayload(&pv); err != nil {
				t.Fatal(err)
			}
			pages[pv.SessionID] = append(pages[pv.SessionID], struct {
				at   time.Time
				page string
			}{e.OccurredAt, pv.PageType})
		case EventCartAbandoned:
			var ca CartAbandoned
			if err := e.DecodePayload(&ca); err != nil {
				t.Fatal(err)
			}
			carts[ca.SessionID] = e.OccurredAt
		case EventSessionEnd:
			ends = append(ends, e)
		}
	}

	// Three converting sessions (one order each) + the day's abandoned count;
	// at 1 sim-day per wall-second every session completes.
	ordersPerDay := 3
	abandonPerDay := int(math.Round(float64(ordersPerDay) * (1 - 0.03) / 0.03))
	if len(ends) != ordersPerDay+abandonPerDay {
		t.Fatalf("session.end count = %d, want %d", len(ends), ordersPerDay+abandonPerDay)
	}

	// Session-end must be the session's final *clickstream* event: no page view
	// or cart-abandoned event for the same session may follow it. (Ground-truth
	// training labels live on a separate restricted topic and may trail it.)
	lastClick := map[string]int{}
	for i, e := range envs {
		if e.EventType == EventPageView || e.EventType == EventCartAbandoned {
			lastClick[e.Key] = i
		}
	}
	for _, e := range ends {
		var se SessionEnd
		if err := e.DecodePayload(&se); err != nil {
			t.Fatal(err)
		}
		if se.SessionID != e.Key {
			t.Fatalf("session.end key %q != payload session %q", e.Key, se.SessionID)
		}
		if want, ok := lastClick[e.Key]; ok && !e.OccurredAt.After(envs[want].OccurredAt) {
			t.Fatalf("session %s session.end at %v must be after its last click at %v",
				e.Key, e.OccurredAt, envs[want].OccurredAt)
		}

		ps := pages[se.SessionID]
		if se.ClickCount != len(ps) {
			t.Fatalf("session %s click_count=%d, have %d page views", se.SessionID, se.ClickCount, len(ps))
		}
		if se.IsConverting == 1 && !strings.HasPrefix(se.SessionID, "c-") {
			t.Fatalf("converting session id %q not prefixed c-", se.SessionID)
		}
		if se.IsConverting == 0 && !strings.HasPrefix(se.SessionID, "a-") {
			t.Fatalf("abandoned session id %q not prefixed a-", se.SessionID)
		}
		if se.DurationSeconds < 0 {
			t.Fatalf("session %s negative duration %.3f", se.SessionID, se.DurationSeconds)
		}
		// A 30 ms click burst (a multi-click synthetic bot) has an exact
		// duration and a zero coefficient of variation.
		if len(ps) >= 2 {
			uniform := true
			for i := 1; i < len(ps); i++ {
				if ps[i].at.Sub(ps[i-1].at) != 30*time.Millisecond {
					uniform = false
					break
				}
			}
			if uniform {
				if got, want := se.DurationSeconds, round3(float64(len(ps)-1)*0.03); got != want {
					t.Fatalf("session %s duration = %.4f, want %.4f (30 ms burst)", se.SessionID, got, want)
				}
				if se.ClickIntervalCV != 0 {
					t.Fatalf("session %s cv = %v, want 0 for a burst", se.SessionID, se.ClickIntervalCV)
				}
			}
		}
		// Page-type flags consistent with the observed page views.
		var types []string
		for _, p := range ps {
			types = append(types, p.page)
		}
		if se.PageTypesDistinct != len(uniqueStrings(types)) {
			t.Fatalf("session %s page_types_distinct=%d, want %d", se.SessionID, se.PageTypesDistinct, len(uniqueStrings(types)))
		}
		hasFlag := func(want string) int {
			for _, p := range ps {
				if p.page == want {
					return 1
				}
			}
			return 0
		}
		if se.HasSearch != hasFlag(PageSearch) || se.HasProductPage != hasFlag(PageProduct) ||
			se.HasCartPage != hasFlag(PageCart) || se.HasCheckoutPage != hasFlag(PageCheckout) {
			t.Fatalf("session %s page flags inconsistent with its page views: %+v", se.SessionID, se)
		}
		// Cart semantics: abandoned sessions may add a cart; converting never do.
		if se.IsConverting == 1 {
			if se.CartAdded != 0 || se.CartValue != 0 {
				t.Fatalf("converting session %s has cart state: %+v", se.SessionID, se)
			}
		} else {
			if _, had := carts[se.SessionID]; had != (se.CartAdded == 1) {
				t.Fatalf("session %s cart_added=%d but cart event present=%v", se.SessionID, se.CartAdded, had)
			}
			if se.CartAdded == 0 && se.CartValue != 0 {
				t.Fatalf("session %s cart_value=%v with no cart", se.SessionID, se.CartValue)
			}
			if se.CartAdded == 1 {
				if got := carts[se.SessionID]; !got.Before(e.OccurredAt) {
					t.Fatalf("session %s session.end must follow its cart event", se.SessionID)
				}
			}
		}
	}
}

func uniqueStrings(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// TestRingDropOldest verifies drop-oldest backpressure on the bounded ring.
func TestRingDropOldest(t *testing.T) {
	r := NewRing(3)
	for i := 0; i < 6; i++ {
		r.Push(Envelope{EventID: fmt.Sprintf("e%d", i)})
	}
	if got := r.Dropped(); got != 3 {
		t.Fatalf("dropped = %d, want 3", got)
	}
	var ids []string
	for {
		e, ok := r.Pop()
		if !ok {
			break
		}
		ids = append(ids, e.EventID)
	}
	want := []string{"e3", "e4", "e5"}
	if len(ids) != len(want) {
		t.Fatalf("got ids %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("got ids %v, want %v", ids, want)
		}
	}
}
