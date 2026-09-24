//go:build integration

// Bronze landing must not lose events (audit, found by the full smoke run).
// flush() took the whole pending batch off the queue, wrote it as ONE statement
// (13 bound parameters per row, Postgres caps a statement at 65,535 = ~5,000 rows)
// and, on any error, only logged: the batch was silently gone. Bronze is what the
// realtime reconcile heals from.
package realtime

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"
)

func bronzeTestWriter(t *testing.T) (*BronzeWriter, string) {
	t.Helper()
	pool := retentionPool(t)
	marker := fmt.Sprintf("it-bronze-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM bronze.stream_events WHERE loop_id = $1`, marker)
	})
	return &BronzeWriter{pool: pool, log: slog.Default(), RunID: "it"}, marker
}

func testRows(marker string, n int) []eventRow {
	rows := make([]eventRow, n)
	for i := range rows {
		loop := marker
		rows[i] = eventRow{
			eventID: fmt.Sprintf("%s-%d", marker, i), schemaVersion: 1, eventType: "page.view",
			occurredAt: time.Date(2014, 1, 1, 0, 0, 0, 0, time.UTC), producedAt: time.Now(),
			key: "k", loopID: &loop, payload: []byte(`{"session_id":"s"}`),
			sourceFile: "stream:test", batchID: marker, topic: "t", partition: 0, offset: int64(i),
		}
	}
	return rows
}

func countRows(t *testing.T, w *BronzeWriter, marker string) int {
	t.Helper()
	var n int
	if err := w.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM bronze.stream_events WHERE loop_id = $1`, marker).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func TestFlushWritesABatchLargerThanThePostgresParameterLimit(t *testing.T) {
	w, marker := bronzeTestWriter(t)
	const n = 6000 // 6000 rows x 13 params = 78,000 > 65,535
	w.pending = testRows(marker, n)
	if err := w.flush(context.Background()); err != nil {
		t.Fatalf("flush of %d rows failed: %v", n, err)
	}
	if got := countRows(t, w, marker); got != n {
		t.Fatalf("landed %d of %d rows", got, n)
	}
	if len(w.pending) != 0 {
		t.Errorf("%d rows still pending after a successful flush", len(w.pending))
	}
}

func TestFailedFlushRequeuesWhatWasNotWritten(t *testing.T) {
	w, marker := bronzeTestWriter(t)
	w.pending = testRows(marker, 2500)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // every statement fails: stands in for a transient database outage
	if err := w.flush(ctx); err == nil {
		t.Fatal("flush with a dead context must report the failure")
	}
	if len(w.pending) != 2500 {
		t.Fatalf("pending = %d after a failed flush, want all 2500 re-queued (the old code discarded them)", len(w.pending))
	}
	// The retry succeeds and lands everything exactly once.
	if err := w.flush(context.Background()); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got := countRows(t, w, marker); got != 2500 {
		t.Fatalf("landed %d of 2500 after the retry", got)
	}
}

func TestRequeuePutsOlderRowsBeforeNewerOnes(t *testing.T) {
	w := &BronzeWriter{}
	newer := testRows("n", 1)
	w.pending = newer
	older := testRows("o", 2)
	w.requeue(older)
	if len(w.pending) != 3 || w.pending[0].eventID != older[0].eventID || w.pending[2].eventID != newer[0].eventID {
		t.Fatalf("requeue order wrong: %+v", w.pending)
	}
}
