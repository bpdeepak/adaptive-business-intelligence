package realtime

import "abi/internal/model"

// LiveFeed is the read-side view of the realtime stack exposed to the HTTP
// (SSE) and gRPC surfaces. It wraps the broadcaster plus the pacing/status
// metadata the clients display.
type LiveFeed struct {
	bc     *Broadcaster
	speed  float64
	status string
}

// NewLiveFeed builds a feed over a broadcaster.
func NewLiveFeed(bc *Broadcaster, speed float64, status string) *LiveFeed {
	return &LiveFeed{bc: bc, speed: speed, status: status}
}

// Subscribe registers a subscriber on the broadcaster.
func (l *LiveFeed) Subscribe() (<-chan model.MetricsUpdate, func()) {
	return l.bc.Subscribe()
}

// Last returns the most recent published update.
func (l *LiveFeed) Last() model.MetricsUpdate { return l.bc.Last() }

// SpeedMultiplier is the replay pacing (simulated seconds per wall second).
func (l *LiveFeed) SpeedMultiplier() float64 { return l.speed }

// Status reports whether the stream is a live replay ("replay") or live
// production traffic ("live"); Phase 1 always replays.
func (l *LiveFeed) Status() string { return l.status }
