package stream

import "time"

// Clock is the producer's virtual clock. It maps elapsed wall time onto
// simulated ("data") time at a fixed rate.
//
//	SPEED_MULTIPLIER = 2880 means 1 simulated second of data per `1/2880`th of
//	a wall second, i.e. one historical day (86400 s) is replayed every 30 wall
//	seconds — the Phase 1 default.
type Clock struct {
	start time.Time // when the clock started (wall)
	base  time.Time // simulated time at clock start
	rate  float64   // simulated seconds per wall second
}

// NewClock creates a clock at `base` simulated time, advancing at `rate`
// simulated seconds per wall second (rate = a wall-clock-now won't matter since
// we pass `now` explicitly to Now).
func NewClock(base time.Time, rate float64) *Clock {
	return &Clock{start: time.Now(), base: base, rate: rate}
}

// Now returns the simulated time for the given wall time.
func (c *Clock) Now(wall time.Time) time.Time {
	elapsed := wall.Sub(c.start).Seconds()
	sim := elapsed * c.rate
	return c.base.Add(time.Duration(sim * float64(time.Second)))
}

// Rate returns simulated seconds per wall second.
func (c *Clock) Rate() float64 { return c.rate }

// LoopDuration returns the simulated duration of one full replay loop given the
// span of the source data.
func LoopDuration(dataSpan time.Duration) time.Duration { return dataSpan }

// SecondsPerDay is the simulated day length (data is days long, not 24h+DST).
const SecondsPerDay = 86400
