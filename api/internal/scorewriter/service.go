package scorewriter

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"abi/internal/model"
	"abi/internal/predict"
	"abi/internal/realtime"
	"abi/internal/stream"
	"abi/internal/telemetry"
)

// Service is the in-process stream score-writer: consumer group "score-writer"
// over the orders + clickstream topics, scoring every order (fraud) and every
// completed session (bot) through the sidecar, persisting explainable
// predictions with metadata.source="stream_score", and firing rate-based model
// anomalies from the at-risk proportion over 5-minute event-time windows.
//
// Unlike the Phase 1 statistical detector (a separate writer, detector=
// 'statistical'), model anomalies are whole-window rates — a single prediction
// can never trigger one.
type Service struct {
	pool    *pgxpool.Pool
	cl      *kgo.Client
	log     *slog.Logger
	predict *predict.Service
	bc      *realtime.Broadcaster

	refs   *References
	asm    *FraudAssembler
	rings  *Rings
	rates  map[string]*RateTracker
	gate   *gate
	jobs   *dropQueue
	state  *stateStore
	reg    *telemetry.Registry

	seenMu sync.Mutex
	seen   map[string]struct{}
	curLoop string

	speed      float64
	stateEvery time.Duration
}

// Options configures the score-writer service.
type Options struct {
	Pool        *pgxpool.Pool
	Client      *kgo.Client
	Predict     *predict.Service
	Broadcaster *realtime.Broadcaster
	Log         *slog.Logger
	Speed       float64
	Slack       time.Duration // event-time disorder tolerance (default 2 min)
	Buffer      int           // score-queue capacity (default 100_000)
	StateEvery  time.Duration // restart-state snapshot cadence (default 30s)
}

// New constructs the service: loads the reference tables, restores the last
// restart-state snapshot, and wires the reorder gate + rate trackers.
func New(ctx context.Context, opts Options) (*Service, error) {
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.Slack <= 0 {
		opts.Slack = 2 * time.Minute
	}
	if opts.Buffer <= 0 {
		opts.Buffer = 100_000
	}
	if opts.StateEvery <= 0 {
		opts.StateEvery = 30 * time.Second
	}

	refs, err := LoadReferences(ctx, opts.Pool, opts.Log)
	if err != nil {
		return nil, err
	}

	s := &Service{
		pool:       opts.Pool,
		cl:         opts.Client,
		log:        opts.Log,
		bc:         opts.Broadcaster,
		refs:       refs,
		asm:        NewFraudAssembler(refs),
		rings:      NewRings(),
		rates:      map[string]*RateTracker{},
		gate:       NewGate(opts.Slack, opts.Buffer),
		jobs:       newDropQueue(opts.Buffer),
		state:      &stateStore{pool: opts.Pool, log: opts.Log},
		seen:       map[string]struct{}{},
		reg:        telemetry.NewRegistry(), // overridden by Instrument() in main
		speed:      opts.Speed,
		stateEvery: opts.StateEvery,
	}
	// The sidecar is optional (empty ABI_SCORE_URL). When disabled the
	// score-writer keeps assembling + advancing its rings (batch parity) but
	// queues nothing and fires no model anomalies.
	if opts.Predict != nil && opts.Predict.Enabled() {
		s.predict = opts.Predict
	}
	s.rates["fraud_risk"] = NewRateTracker("fraud_risk", refs.Models["fraud_risk"])
	s.rates["bot_score"] = NewRateTracker("bot_score", refs.Models["bot_score"])
	s.RestoreState(ctx)
	return s, nil
}

// Instrument attaches the operational registry (nil-safe; optional).
func (s *Service) Instrument(reg *telemetry.Registry) { s.reg = reg }

// Models reports the model names the score-writer watches (for tests/docs).
func (s *Service) Models() []string {
	return []string{"fraud_risk", "bot_score"}
}

// Run consumes events until ctx is done: it parses + reorders (event-time
// order), advances the rings in that order, and hands scoring jobs to the
// drop-oldest queue drained by scoreWorker's goroutine. The caller runs
// scoreWorker separately (see Start).
func (s *Service) Run(ctx context.Context) error {
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		for ctx.Err() == nil {
			fetches := s.cl.PollFetches(ctx)
			if fetches.IsClientClosed() {
				return
			}
			records := fetches.Records()
			if len(records) == 0 {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			s.ingestAll(records)
		}
		// Shutdown drain: process whatever the gate still holds.
		s.processAll(s.gate.Flush())
	}()

	ticker := time.NewTicker(s.stateEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.SnapshotState(context.Background())
			<-consumeDone
			return nil
		case <-ticker.C:
			s.SnapshotState(ctx)
		}
	}
}

// ScoreWorker drains the score queue until ctx is done. Run it in its own
// goroutine so sidecar latency never backpressures the consumer.
func (s *Service) ScoreWorker(ctx context.Context) {
	for ctx.Err() == nil {
		job, ok := s.jobs.pop()
		if !ok {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		s.scoreOne(ctx, job)
	}
}

// ingestAll parses a fetch batch into the reorder gate and processes whatever
// is now ready, in event-time order.
func (s *Service) ingestAll(records []*kgo.Record) {
	var ready []any
	for _, rec := range records {
		var env stream.Envelope
		if err := json.Unmarshal(rec.Value, &env); err != nil {
			s.log.Warn("scorewriter: undecodable record", "topic", rec.Topic, "error", err)
			continue
		}
		switch {
		case rec.Topic == stream.TopicOrders && env.EventType == stream.EventOrderPlaced:
			var op stream.OrderPlaced
			if err := env.DecodePayload(&op); err != nil {
				s.log.Warn("scorewriter: bad order payload", "event_id", env.EventID, "error", err)
				continue
			}
			ready = append(ready, s.gate.Push(pendingOrder{op: op, loopID: env.LoopID}, float64(op.PurchaseTime.Unix()))...)
		case rec.Topic == stream.TopicClicks && env.EventType == stream.EventSessionEnd:
			var se stream.SessionEnd
			if err := env.DecodePayload(&se); err != nil {
				s.log.Warn("scorewriter: bad session.end payload", "event_id", env.EventID, "error", err)
				continue
			}
			ready = append(ready, s.gate.Push(pendingSession{se: se, loopID: env.LoopID, at: env.OccurredAt},
				float64(env.OccurredAt.Unix()))...)
		default:
			// Aggregator/bronze materialize page views, carts, labels; the
			// score-writer only consumes order.placed + session.end.
		}
	}
	s.processAll(ready)
}

type pendingOrder struct {
	op     stream.OrderPlaced
	loopID string
}

type pendingSession struct {
	se     stream.SessionEnd
	loopID string
	at     time.Time
}

// processAll handles a batch of gate-released events in order.
func (s *Service) processAll(items []any) {
	for _, it := range items {
		s.processEvent(it)
	}
}

func (s *Service) processEvent(data any) {
	switch e := data.(type) {
	case pendingOrder:
		s.processOrder(&e.op, e.loopID)
	case pendingSession:
		s.processSession(&e.se, e.loopID, e.at)
	}
}

// processOrder assembles + records one order and queues its fraud score. The
// rings advance for every order regardless of scoring outcome, exactly as the
// batch feature builder advanced its windows for every row.
func (s *Service) processOrder(op *stream.OrderPlaced, loopID string) {
	if !s.claim(loopID, "\x00"+op.OrderID) {
		return // at-least-once redelivery within a loop
	}
	cat := s.asm.primaryCategory(op.Items)
	feat := s.asm.Assemble(*op, s.rings)
	s.rings.RecordOrder(*op, s.refs, cat)

	if s.predict == nil {
		s.counter("abi_score_writer_skipped_total", "Events not scored because the sidecar is disabled.").Inc()
		return
	}
	if s.jobs.push(scoreJob{
		model: "fraud_risk", entity: op.OrderID, features: feat,
		evt: op.PurchaseTime, grain: "order",
	}) > 0 {
		s.counter("abi_score_writer_drops_total", "Score jobs dropped oldest-first when the queue overflowed.").Inc()
	}
}

// processSession maps the session.end payload's pre-aggregated vector onto the
// bot model features and queues its score. No session math is re-derived here.
func (s *Service) processSession(se *stream.SessionEnd, loopID string, at time.Time) {
	if !s.claim(loopID, "\x00"+se.SessionID) {
		return
	}
	feat := BotFeatures(*se)
	if s.predict == nil {
		s.counter("abi_score_writer_skipped_total", "Events not scored because the sidecar is disabled.").Inc()
		return
	}
	if s.jobs.push(scoreJob{
		model: "bot_score", entity: se.SessionID, features: feat,
		evt: at, grain: "session",
	}) > 0 {
		s.counter("abi_score_writer_drops_total", "Score jobs dropped oldest-first when the queue overflowed.").Inc()
	}
}

// claim dedupes one event identity per loop (order ids recur across loops by
// design; the score-writer must score every loop's copy).
func (s *Service) claim(loopID, entityKey string) bool {
	s.seenMu.Lock()
	defer s.seenMu.Unlock()
	if loopID != s.curLoop {
		s.seen = map[string]struct{}{}
		s.curLoop = loopID
	}
	key := loopID + entityKey
	if _, dup := s.seen[key]; dup {
		return false
	}
	s.seen[key] = struct{}{}
	return true
}

// scoreJob is one unit of scoring work queued for the sidecar.
type scoreJob struct {
	model    string
	entity   string
	features map[string]float64
	evt      time.Time
	grain    string
}

// scoreOne scores + persists one job and feeds the model's rate tracker; a
// fired rate anomaly is persisted and broadcast to the in-process SSE banner.
func (s *Service) scoreOne(ctx context.Context, j scoreJob) {
	md := json.RawMessage(fmt.Sprintf(`{"source":"stream_score","grain":"%s"}`, j.grain))
	resp, _, err := s.predict.ScoreAndPersist(ctx, j.model, j.entity, j.features, md)
	if err != nil {
		s.counter("abi_score_writer_errors_total", "Scoring or persistence failures.").Inc()
		s.log.Warn("scorewriter: score failed", "model", j.model, "entity", j.entity, "error", err)
		return
	}
	s.counter("abi_score_writer_scored_total", "Predictions scored and persisted from the stream.").Inc()
	if rt := s.rates[j.model]; rt != nil {
		if anom := rt.Observe(j.evt, resp.Prediction); anom != nil {
			s.persistModelAnomaly(ctx, anom)
		}
	}
}

// persistModelAnomaly writes a model-driven anomaly (detector='model',
// z_score stays NULL — rate anomalies carry no z-score) and surfaces it on the
// shared SSE/gRPC broadcaster with the last known live bucket as context.
func (s *Service) persistModelAnomaly(ctx context.Context, anom *model.Anomaly) {
	var out model.Anomaly
	err := s.pool.QueryRow(ctx, `
INSERT INTO gold.anomalies (metric, detector, bucket_start, observed, expected, z_score, severity, status, detected_at)
VALUES ($1, 'model', $2, $3, $4, NULL, $5, 'open', now())
RETURNING id, metric, detector,
  to_char(bucket_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
  observed, expected, COALESCE(z_score, 0)::float8, severity, status,
  to_char(detected_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')`,
		anom.Metric, anom.BucketStart, anom.Observed, anom.Expected, anom.Severity).
		Scan(&out.ID, &out.Metric, &out.Detector, &out.BucketStart, &out.Observed, &out.Expected,
			&out.ZScore, &out.Severity, &out.Status, &out.DetectedAt)
	if err != nil {
		s.log.Error("scorewriter: persist model anomaly", "metric", anom.Metric, "error", err)
		return
	}
	s.counter("abi_model_anomalies_total", "Model-driven (detector=model) anomalies persisted.").Inc()
	s.log.Warn("scorewriter: model anomaly fired",
		"metric", out.Metric, "observed", out.Observed, "expected", out.Expected)

	// Surface on the shared banner without disturbing the current live bucket.
	last := s.bc.Last()
	u := model.MetricsUpdate{
		Current:         last.Current,
		Anomaly:         &out,
		SpeedMultiplier: s.speed,
		Status:          "replay",
		Source:          model.SourceLiveReplay,
	}
	s.bc.Publish(u)
}

func (s *Service) counter(name, help string) *telemetry.Series {
	if s.reg == nil {
		return nil
	}
	return s.reg.Counter(name, help)
}

// dropQueue is the bounded scoring queue with drop-oldest backpressure: on
// overflow the oldest (earliest-event) job is discarded — rings and windows
// already advanced, so dropping only skips the persist, and the combined
// effect is a bounded backlog rather than unbounded memory.
type dropQueue struct {
	mu  sync.Mutex
	buf []scoreJob
	cap int
}

func newDropQueue(cap int) *dropQueue {
	return &dropQueue{buf: []scoreJob{}, cap: cap}
}

// push enqueues a job, returning the number dropped (0 or 1).
func (q *dropQueue) push(j scoreJob) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.cap > 0 && len(q.buf) >= q.cap {
		q.buf[0] = scoreJob{}
		q.buf = q.buf[1:]
		q.buf = append(q.buf, j)
		return 1
	}
	q.buf = append(q.buf, j)
	return 0
}

// pop removes the oldest job; ok=false when empty.
func (q *dropQueue) pop() (scoreJob, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.buf) == 0 {
		return scoreJob{}, false
	}
	j := q.buf[0]
	q.buf[0] = scoreJob{}
	q.buf = q.buf[1:]
	return j, true
}

func (q *dropQueue) size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.buf)
}