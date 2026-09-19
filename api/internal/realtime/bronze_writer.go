package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"abi/internal/stream"
)

// BronzeWriter records every streaming event verbatim into
// bronze.stream_events with the same lineage contract the Phase 0 batch loader
// stamps (_source_file/_batch_id/_loaded_at) plus Kafka coordinates — the
// review-item guarantee that "which pipeline wrote this row" is answerable for
// streaming rows too.
type BronzeWriter struct {
	pool *pgxpool.Pool
	cl   *kgo.Client
	log  *slog.Logger
	// RunID uniquely identifies this writer process lifetime; used as _batch_id
	// for events whose envelope carries no loop id.
	RunID string

	mu        sync.Mutex
	pending   []eventRow
	nextFlush time.Time
}

// eventRow is one prepared bronze.stream_events row.
type eventRow struct {
	eventID       string
	schemaVersion int
	eventType     string
	occurredAt    time.Time
	producedAt    time.Time
	key           string
	loopID        *string
	payload       []byte // full envelope JSON
	sourceFile    string
	batchID       string
	topic         string
	partition     int32
	offset        int64
}

// NewBronzeWriter wires a BronzeWriter around its consumer and pool. The
// consumer must already subscribe to stream.AllTopics.
func NewBronzeWriter(pool *pgxpool.Pool, cl *kgo.Client, runID string, log *slog.Logger) *BronzeWriter {
	return &BronzeWriter{pool: pool, cl: cl, log: log, RunID: runID}
}

// Run consumes until ctx is cancelled, buffering rows and flushing in batches
// (size- or time-triggered).
func (w *BronzeWriter) Run(ctx context.Context) error {
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		w.consumeLoop(ctx)
	}()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.flush(context.Background())
			<-consumeDone
			return nil
		case <-ticker.C:
			w.mu.Lock()
			due := len(w.pending) > 0 && time.Now().After(w.nextFlush)
			w.mu.Unlock()
			if due {
				if err := w.flush(ctx); err != nil {
					w.log.Error("bronze-writer flush failed", "error", err)
				}
			}
		}
	}
}

func (w *BronzeWriter) consumeLoop(ctx context.Context) {
	for ctx.Err() == nil {
		fetches := w.cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return
		}
		records := fetches.Records()
		if len(records) == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		for _, rec := range records {
			w.ingest(rec)
		}
		w.mu.Lock()
		if len(w.pending) >= 256 {
			w.mu.Unlock()
			if err := w.flush(ctx); err != nil {
				w.log.Error("bronze-writer flush failed", "error", err)
			}
			continue
		}
		w.mu.Unlock()
	}
}

// ingest parses a record into a pending row but never blocks on the DB.
func (w *BronzeWriter) ingest(rec *kgo.Record) {
	var env stream.Envelope
	if err := json.Unmarshal(rec.Value, &env); err != nil {
		w.log.Warn("bronze-writer: undecodable record", "topic", rec.Topic, "offset", rec.Offset, "error", err)
		return
	}
	row := eventRow{
		eventID:       env.EventID,
		schemaVersion: env.SchemaVersion,
		eventType:     env.EventType,
		occurredAt:    env.OccurredAt,
		producedAt:    env.ProducedAt,
		key:           env.Key,
		payload:       rec.Value,
		sourceFile:    "stream:" + rec.Topic,
		batchID:       w.RunID,
		topic:         rec.Topic,
		partition:     rec.Partition,
		offset:        rec.Offset,
	}
	if env.LoopID != "" {
		row.batchID = env.LoopID
		row.loopID = &env.LoopID
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.pending) == 0 {
		w.nextFlush = time.Now().Add(2 * time.Second)
	}
	w.pending = append(w.pending, row)
}

// flush writes pending rows with a single multi-row INSERT ... ON CONFLICT DO
// NOTHING. At-least-once redelivery of an offset is therefore idempotent.
func (w *BronzeWriter) flush(ctx context.Context) error {
	w.mu.Lock()
	if len(w.pending) == 0 {
		w.mu.Unlock()
		return nil
	}
	rows := w.pending
	w.pending = nil
	w.mu.Unlock()

	cols := `(event_id, schema_version, event_type, occurred_at, produced_at, key, loop_id,
	          payload, _source_file, _batch_id, _loaded_at,
	          _kafka_topic, _kafka_partition, _kafka_offset)`
	values := make([]string, 0, len(rows))
	args := make([]any, 0, len(rows)*14)
	for _, r := range rows {
		base := len(args)
		values = append(values, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,now(),$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8,
			base+9, base+10, base+11, base+12, base+13))
		var loopVal any
		if r.loopID != nil {
			loopVal = *r.loopID
		}
		args = append(args,
			r.eventID, r.schemaVersion, r.eventType, r.occurredAt, r.producedAt,
			r.key, loopVal, r.payload, r.sourceFile, r.batchID,
			r.topic, r.partition, r.offset)
	}

	sql := `INSERT INTO bronze.stream_events ` + cols + ` VALUES ` + strings.Join(values, ",") +
		` ON CONFLICT (event_id) DO NOTHING`
	if _, err := w.pool.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("insert stream_events (%d rows): %w", len(rows), err)
	}
	return nil
}
