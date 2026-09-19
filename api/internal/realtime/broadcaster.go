package realtime

import (
	"sync"

	"abi/internal/model"
)

// Broadcaster fans out MetricsUpdate pushes to live subscribers (SSE clients,
// gRPC streams). Slow subscribers are dropped rather than allowed to block the
// aggregator; the caller re-synchronizes from the stored snapshot on reconnect.
type Broadcaster struct {
	mu     sync.Mutex
	nextID int64
	subs   map[int64]chan model.MetricsUpdate
	last   model.MetricsUpdate
}

// NewBroadcaster creates an empty broadcaster.
func NewBroadcaster() *Broadcaster {
	return &Broadcaster{subs: make(map[int64]chan model.MetricsUpdate)}
}

// Subscribe registers a subscriber and returns its receive channel plus an
// unsubscribe function. The channel is buffered; a consumer that fails to drain
// it will start seeing pushes dropped.
func (b *Broadcaster) Subscribe() (<-chan model.MetricsUpdate, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextID++
	id := b.nextID
	ch := make(chan model.MetricsUpdate, 8)
	b.subs[id] = ch
	return ch, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		delete(b.subs, id)
		close(ch)
	}
}

// Publish stores the update as the latest state and delivers it to every
// subscriber without blocking on any of them.
func (b *Broadcaster) Publish(u model.MetricsUpdate) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.last = u
	for id, ch := range b.subs {
		select {
		case ch <- u:
		default:
			// Slow consumer: drop this update; it will resync from the
			// snapshot/DB on the next update or on reconnect.
			_ = id
		}
	}
}

// Last returns the most recent MetricsUpdate published (zero value if none yet).
func (b *Broadcaster) Last() model.MetricsUpdate {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.last
}
