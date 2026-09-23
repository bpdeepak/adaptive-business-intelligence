package scorewriter

import (
	"testing"
	"time"
)

type tagged struct {
	ID  string
	Seq int
}

// tOf maps a Seq back to its input event time for assertion purposes.
func tOf(seq int) float64 {
	return map[int]float64{
		0: 100, 1: 101, 2: 102, 3: 99.5, 10: 99.7, 4: 200, 5: 300,
	}[seq]
}

// pushAll feeds records with the given times and collects everything the gate
// releases (simulating the consumer's bounded-disorder interleave).
func pushAll(g *gate, ts []float64) []tagged {
	var out []tagged
	for i, t := range ts {
		ready := g.Push(tagged{Seq: i}, t)
		for _, it := range ready {
			out = append(out, it.(tagged))
		}
	}
	return out
}

func TestGateReleasesInEventOrderWhenSlackElapses(t *testing.T) {
	g := NewGate(120*time.Second, 100000)
	// t0 events arrive near-ascending; then an older straggler (partition
	// interleave) arrives late but the slack covers it.
	released := pushAll(g, []float64{100, 101, 102, 99.5})
	if len(released) != 0 {
		t.Fatalf("released before slack elapsed: %v", released)
	}
	g.Push(tagged{Seq: 10}, 99.7)
	// Nothing releases until something is at least 120s ahead of the min.
	if got := g.Len(); got != 5 {
		t.Fatalf("buffered %d, want 5", got)
	}
	// Pushing 200 then 300 advances maxSeen past 220 → everything older than
	// 300-120=180 (all five) is provably ready, in event-time order.
	released = pushAll(g, []float64{200, 300})
	if len(released) != 5 {
		t.Fatalf("released %d, want 5", len(released))
	}
	last := -1.0
	for _, it := range released {
		tc := tOf(it.Seq)
		if tc < last {
			t.Fatalf("out of order: seq %d (t=%v) after t=%v", it.Seq, tc, last)
		}
		last = tc
	}
	if got := g.Len(); got != 2 { // 200, 300 still buffered
		t.Fatalf("remaining buffered %d, want 2", got)
	}
}

func TestGateForcedFlushBoundsMemory(t *testing.T) {
	g := NewGate(30*time.Second, 3)
	// Huge slack (30s vs 1s gaps) means nothing would ever release normally;
	// maxSize=3 forces a release each time the buffer hits capacity, and the
	// final stragglers stay buffered until Flush — so the heap is bounded to
	// maxSize and the full stream still emerges in ascending event order.
	released := pushAll(g, []float64{100, 101, 102, 103, 104, 105})
	if len(released) != 4 {
		t.Fatalf("released %d, want 4 (forced at capacity)", len(released))
	}
	if got := g.Len(); got != 2 {
		t.Fatalf("heap holds %d, want 2 (bounded to maxSize)", got)
	}
	prev := -1.0
	for _, it := range released {
		if float64(it.Seq) < prev {
			t.Fatalf("forced flush out of order")
		}
		prev = float64(it.Seq)
	}
	// Flush drains the remaining two, oldest first.
	rest := g.Flush()
	if len(rest) != 2 {
		t.Fatalf("flush returned %d, want 2", len(rest))
	}
	if rest[0].(tagged).Seq != 4 || rest[1].(tagged).Seq != 5 {
		t.Fatalf("flush order = %d,%d want 4,5", rest[0].(tagged).Seq, rest[1].(tagged).Seq)
	}
}

func TestGateFlushDrainsEverything(t *testing.T) {
	g := NewGate(120*time.Second, 100000)
	pushAll(g, []float64{10, 11, 12})
	rest := g.Flush()
	if len(rest) != 3 {
		t.Fatalf("flush returned %d, want 3", len(rest))
	}
	if got := g.Len(); got != 0 {
		t.Fatalf("gate not empty after flush: %d", got)
	}
}