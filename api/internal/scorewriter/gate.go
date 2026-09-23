package scorewriter

import (
	"container/heap"
	"time"
)

// gate restores a bounded-disorder event stream to event-time order.
//
// The orders topic is partitioned by customer, so a single customer's orders
// (velocity, account age) arrive in perfect order, but the category benchmark
// ring spans ALL partitions — every customer that buys the same category.
// Fetch batches are interleaved by Redpanda without a global timestamp order,
// so without reordering, "strictly before" windows could count a category order
// that arrived a few fetch batches early. Kafka gives at-least-once, not
// strict global ordering; this gate turns the pipeline's own guarantee into a
// bounded-disorder one: hold events until everything older than `slack`
// (event-time seconds) is present, then release in order.
type gate struct {
	slack   float64 // event-time seconds of tolerated disorder
	maxSize int     // hard cap: beyond this, force-release (memory bound)

	items   []gateItem
	maxSeen float64
}

type gateItem struct {
	t    float64 // event-time epoch seconds
	data any     // the parsed, ready-to-process event
}

// NewGate builds a reorder gate. slack is the event-time tolerance in seconds;
// maxSize caps buffered events before a forced release.
func NewGate(slack time.Duration, maxSize int) *gate {
	if maxSize <= 0 {
		maxSize = 100_000
	}
	return &gate{slack: slack.Seconds(), maxSize: maxSize, items: []gateItem{}}
}

// Push inserts one event and returns the events that are now safe to process
// in event-time order (empty when more buffering is needed).
func (g *gate) Push(data any, t float64) []any {
	heap.Push(&gateHeap{g}, gateItem{t: t, data: data})
	if t > g.maxSeen {
		g.maxSeen = t
	}
	return g.release(false)
}

// release pops the ordered tail that is provably ready: its event time is at
// least `slack` behind the newest buffered time (no still-unseen record can
// precede it), or the buffer hit maxSize (forced release).
func (g *gate) release(force bool) []any {
	var out []any
	for len(g.items) > 0 {
		minT := g.items[0].t
		if !force && len(g.items) < g.maxSize && minT > g.maxSeen-g.slack {
			break
		}
		it := heap.Pop(&gateHeap{g}).(gateItem)
		out = append(out, it.data)
	}
	return out
}

// Flush drains everything still buffered, oldest first (used at shutdown and
// in tests).
func (g *gate) Flush() []any {
	return g.release(true)
}

// Len reports how many events are buffered.
func (g *gate) Len() int { return len(g.items) }

// gateHeap adapts gate.items to container/heap (min by event time).
type gateHeap struct{ g *gate }

func (h gateHeap) Len() int           { return len(h.g.items) }
func (h gateHeap) Less(i, j int) bool { return h.g.items[i].t < h.g.items[j].t }
func (h gateHeap) Swap(i, j int)      { h.g.items[i], h.g.items[j] = h.g.items[j], h.g.items[i] }
func (h gateHeap) Push(x any)         { h.g.items = append(h.g.items, x.(gateItem)) }
func (h gateHeap) Pop() any {
	old := h.g.items
	n := len(old)
	it := old[n-1]
	h.g.items = old[:n-1]
	return it
}