package batchscore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// A pass that fails (the model sidecar still starting at boot) is retried after
// the short retry delay, not a full poll interval (an hour by default).
func TestPollLoopRetriesAFailedPassSoon(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	var calls []time.Time
	done := make(chan struct{})
	go func() {
		pollLoop(ctx, time.Hour, 20*time.Millisecond, func() error {
			mu.Lock()
			defer mu.Unlock()
			calls = append(calls, time.Now())
			if len(calls) < 3 {
				return errors.New("sidecar not ready")
			}
			close(done)
			return nil
		})
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a failing pass was not retried within the retry delay")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 3 {
		t.Fatalf("calls = %d, want 3 (two failures, then success)", len(calls))
	}
}

// After a successful pass the loop waits the full interval.
func TestPollLoopWaitsTheFullIntervalAfterSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	n := 0
	pollLoop(ctx, time.Hour, 10*time.Millisecond, func() error { n++; return nil })
	if n != 1 {
		t.Fatalf("ticks = %d within 150ms, want 1 (next one is an hour away)", n)
	}
}
