package realtime

import (
	"testing"
	"time"
)

// Bucket closure and eviction follow the stream's own event-time watermark. The
// replay stamps events with simulated time - years in the past at first, and
// after enough ~2-year loops, in the future - so a wall-clock rule either
// analysed a still-filling bucket on every flush or never closed a bucket at
// all (audit C7/C8).

func newClosureAgg() *Aggregator {
	return &Aggregator{flushEvery: 5 * time.Second, buckets: map[time.Time]*bucket{}}
}

func at(s string) time.Time {
	t, err := time.Parse("2006-01-02 15:04", s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func TestHistoricalReplayClosesBehindTheWatermarkNotTheWallClock(t *testing.T) {
	a := newClosureAgg()
	now := at("2026-09-24 15:00")
	a.watermark = at("2017-03-01 12:30")
	a.lastIngest = now // events are flowing right now

	if !a.closedLocked(at("2017-03-01 12:28"), now) {
		t.Error("a bucket two minutes behind the watermark must be closed")
	}
	if a.closedLocked(at("2017-03-01 12:30"), now) {
		t.Error("the newest bucket is still filling: the wall clock must not close it (it did, every flush)")
	}
	if a.closedLocked(at("2017-03-01 12:29"), now) {
		t.Error("one minute behind the watermark is still inside the grace window")
	}
}

func TestIdleStreamClosesTheNewestBucket(t *testing.T) {
	a := newClosureAgg()
	now := at("2026-09-24 15:00")
	a.watermark = at("2017-03-01 12:30")
	a.lastIngest = now.Add(-10 * time.Second) // idle for two flush intervals
	if !a.closedLocked(at("2017-03-01 12:30"), now) {
		t.Error("once the stream is idle the last bucket must close, or its anomaly is never evaluated")
	}
}

func TestFutureDatedReplayStillClosesBuckets(t *testing.T) {
	// After ~5 loops the simulated clock passes the wall clock. Under the old
	// `start < now-1min` rule none of these buckets ever closed: detection
	// stopped and memory grew without bound.
	a := newClosureAgg()
	now := at("2026-09-24 15:00")
	a.watermark = now.Add(48 * time.Hour)
	a.lastIngest = now
	if !a.closedLocked(a.watermark.Add(-5*time.Minute), now) {
		t.Error("a future-dated bucket well behind the watermark must close")
	}
	if a.closedLocked(a.watermark, now) {
		t.Error("the watermark bucket itself stays open while events flow")
	}
}

func TestLiveRealtimeBehaviourIsUnchanged(t *testing.T) {
	a := newClosureAgg()
	now := at("2026-09-24 15:00")
	a.watermark = now // events stamped ~ now
	a.lastIngest = now
	if !a.closedLocked(now.Add(-2*time.Minute), now) {
		t.Error("in live operation a bucket two minutes old closes as before")
	}
	if a.closedLocked(now.Add(-30*time.Second), now) {
		t.Error("the current minute is open")
	}
}

func TestEvictionFollowsTheWatermarkSoFillingBucketsSurvive(t *testing.T) {
	a := newClosureAgg()
	a.watermark = at("2017-03-01 12:30")
	if a.evictableLocked(at("2017-03-01 12:25")) {
		t.Error("a bucket five simulated minutes behind must be kept (the old wall-clock rule evicted every historical bucket immediately, so the next flush overwrote its sum with a smaller partial)")
	}
	if !a.evictableLocked(at("2017-03-01 12:19")) {
		t.Error("a bucket more than ten minutes behind the watermark is evictable")
	}
	// bounded on a future timeline too
	a.watermark = at("2028-01-01 00:00")
	if !a.evictableLocked(at("2027-12-31 23:00")) {
		t.Error("eviction must also bound memory when the timeline is ahead of the wall clock")
	}
}

func TestAdvanceTracksNewestEventTime(t *testing.T) {
	a := newClosureAgg()
	a.advanceLocked(at("2017-03-01 12:30"))
	a.advanceLocked(at("2017-03-01 12:10")) // late, out of order
	if !a.watermark.Equal(at("2017-03-01 12:30")) {
		t.Errorf("watermark = %v, want the newest event time", a.watermark)
	}
	if a.lastIngest.IsZero() {
		t.Error("lastIngest not recorded")
	}
}

func TestTimelineRestartIsNotIgnored(t *testing.T) {
	// The replay producer restarts at the dataset start. Without this, every new
	// bucket is older than the (restored) analysis cursor and is ignored forever.
	a := newClosureAgg()
	a.lastAnalyzed = at("2017-03-01 12:00")
	a.restoredThrough = a.lastAnalyzed
	a.watermark = at("2017-03-01 12:00")

	a.advanceLocked(at("2017-03-01 11:30")) // late event, well inside the tolerance
	if a.lastAnalyzed.IsZero() {
		t.Fatal("a late event must not reset the analysis cursor")
	}

	a.advanceLocked(at("2016-09-04 21:00")) // the source started over
	if !a.lastAnalyzed.IsZero() || !a.restoredThrough.IsZero() {
		t.Error("a backwards jump of years must reset the cursor so the new timeline is analysed")
	}
	if !a.watermark.Equal(at("2016-09-04 21:00")) {
		t.Errorf("watermark = %v, must restart with the new timeline", a.watermark)
	}
}
