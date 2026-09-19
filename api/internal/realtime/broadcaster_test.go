package realtime

import (
	"testing"
	"time"

	"abi/internal/model"
)

func TestBroadcasterFanOutAndLast(t *testing.T) {
	b := NewBroadcaster()

	ch1, unsub1 := b.Subscribe()
	defer unsub1()
	ch2, unsub2 := b.Subscribe()
	defer unsub2()

	u := model.MetricsUpdate{
		Current:         model.RealtimeBucket{BucketStart: "2026-01-01T00:00:00Z", Revenue: 12.5},
		SpeedMultiplier: 2880,
		Status:          "replay",
	}
	b.Publish(u)

	for i, ch := range []<-chan model.MetricsUpdate{ch1, ch2} {
		select {
		case got := <-ch:
			if got.Current.Revenue != 12.5 {
				t.Fatalf("subscriber %d got %+v", i, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d never received update", i)
		}
	}

	last := b.Last()
	if last.Current.BucketStart != "2026-01-01T00:00:00Z" {
		t.Fatalf("Last() = %+v", last)
	}
}

func TestBroadcasterDropsSlowSubscriber(t *testing.T) {
	b := NewBroadcaster()
	ch, unsub := b.Subscribe()
	defer unsub()

	// Flood the buffer repeatedly; the broadcaster must never block.
	for i := 0; i < 100; i++ {
		b.Publish(model.MetricsUpdate{Current: model.RealtimeBucket{Revenue: float64(i)}})
	}

	// The channel holds at most 8 (buffer cap); drain whatever survived.
	drained := 0
	for {
		select {
		case <-ch:
			drained++
			if drained > 8 {
				t.Fatalf("subscriber should never hold more than its buffer")
			}
		case <-time.After(200 * time.Millisecond):
			if drained == 0 {
				t.Fatal("expected at least one buffered update")
			}
			return
		}
	}
}
