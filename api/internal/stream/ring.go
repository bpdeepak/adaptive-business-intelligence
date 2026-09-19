package stream

import (
	"sync"
	"sync/atomic"
)

// Ring is a fixed-capacity FIFO of envelopes with drop-oldest semantics: when
// full, the oldest item is discarded to make room. This is the producer's
// backpressure valve — the replay never blocks on Kafka, it sheds load and
// counts what it shed (surfaced in producer logs and stats).
type Ring struct {
	cap     int
	mu      sync.Mutex
	buf     []Envelope
	head    int
	size    int
	dropped atomic.Uint64
	open    bool
}

// NewRing creates a Ring with the given capacity.
func NewRing(capacity int) *Ring {
	return &Ring{cap: capacity, buf: make([]Envelope, capacity), open: true}
}

// Push appends an envelope, dropping the oldest when full.
func (r *Ring) Push(e Envelope) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.open {
		return
	}
	if r.size == r.cap {
		// Drop the oldest (head) to make room.
		r.head = (r.head + 1) % r.cap
		r.size--
		r.dropped.Add(1)
	}
	r.buf[(r.head+r.size)%r.cap] = e
	r.size++
}

// Pop removes and returns the oldest envelope. ok is false when empty.
func (r *Ring) Pop() (Envelope, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.size == 0 {
		return Envelope{}, false
	}
	e := r.buf[r.head]
	r.buf[r.head] = Envelope{}
	r.head = (r.head + 1) % r.cap
	r.size--
	return e, true
}

// Close stops further pushes; pending items can still be popped.
func (r *Ring) Close() {
	r.mu.Lock()
	r.open = false
	r.mu.Unlock()
}

// Len returns the current number of buffered items.
func (r *Ring) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.size
}

// Dropped returns the number of events shed since creation.
func (r *Ring) Dropped() uint64 { return r.dropped.Load() }
