package events

import (
	"context"
	"testing"
	"time"
)

func TestPublishDeliversToEverySubscriber(t *testing.T) {
	b := New()
	ch1, unsub1 := b.Subscribe()
	defer unsub1()
	ch2, unsub2 := b.Subscribe()
	defer unsub2()

	published := Event{Type: TypeOrderScored, At: time.Now(), Payload: map[string]any{"k": "v"}}
	b.Publish(published)

	for i, ch := range []<-chan Event{ch1, ch2} {
		select {
		case got := <-ch:
			if got.Type != published.Type {
				t.Errorf("sub %d got type %q, want %q", i, got.Type, published.Type)
			}
		case <-time.After(time.Second):
			t.Fatalf("sub %d did not receive the event", i)
		}
	}
}

func TestPublishNeverBlocksProducers(t *testing.T) {
	// A subscriber that never drains must not stall Publish: the producer's
	// fast path is the point of this bus.
	b := New()
	_, unsub := b.Subscribe() // no one drains the buffered channel
	defer unsub()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			b.Publish(Event{Type: TypeOrderScored})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked behind a slow subscriber")
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	b := New()
	ch, unsub := b.Subscribe()
	unsub()

	b.Publish(Event{Type: TypeAnomalyDetected})
	select {
	case got := <-ch:
		t.Fatalf("unsubscribed channel received %v", got)
	case <-time.After(50 * time.Millisecond):
		// expected: no delivery
	}
}

func TestSubscribeCount(t *testing.T) {
	b := New()
	if b.SubscribeCount() != 0 {
		t.Fatal("empty bus should have no subscribers")
	}
	_, unsub := b.Subscribe()
	if b.SubscribeCount() != 1 {
		t.Fatal("one subscriber expected")
	}
	unsub()
	if b.SubscribeCount() != 0 {
		t.Fatal("unsubscribe should decrement the count")
	}
}
func TestDroppedDeliveriesAreCounted(t *testing.T) {
	b := New()
	_, unsub := b.Subscribe() // never read: the buffer fills
	defer unsub()
	for i := 0; i < subBuffer+10; i++ {
		b.Publish(Event{Type: TypeOrderScored})
	}
	if b.Published() != int64(subBuffer+10) {
		t.Errorf("published = %d, want %d", b.Published(), subBuffer+10)
	}
	if b.Dropped() != 10 {
		t.Errorf("dropped = %d, want 10 (silent drops were the audit's H1 finding)", b.Dropped())
	}
}

func TestPublishWaitDeliversABurstWithoutDropping(t *testing.T) {
	b := New()
	ch, unsub := b.Subscribe()
	defer unsub()
	const burst = 5 * subBuffer
	received := make(chan int)
	go func() {
		n := 0
		for range ch {
			n++
			if n == burst {
				break
			}
		}
		received <- n
	}()
	for i := 0; i < burst; i++ {
		if err := b.PublishWait(context.Background(), Event{Type: TypeChurnScored}, time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if n := <-received; n != burst || b.Dropped() != 0 {
		t.Fatalf("received %d of %d, dropped %d", n, burst, b.Dropped())
	}
}

func TestPublishWaitCountsADropWhenASubscriberIsStuck(t *testing.T) {
	b := New()
	_, unsub := b.Subscribe() // never read
	defer unsub()
	for i := 0; i < subBuffer; i++ {
		b.Publish(Event{Type: TypeChurnScored}) // fill the buffer
	}
	start := time.Now()
	if err := b.PublishWait(context.Background(), Event{Type: TypeChurnScored}, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if b.Dropped() != 1 || time.Since(start) < 20*time.Millisecond {
		t.Fatalf("dropped=%d after %v; want exactly 1 drop after waiting the timeout", b.Dropped(), time.Since(start))
	}
}
