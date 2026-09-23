package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"abi/internal/model"
	"abi/internal/stream"
	"abi/internal/telemetry"
)

// Aggregator consumes the order + clickstream topics and maintains hot,
// one-minute metrics buckets in gold.realtime_metrics. Anomaly detection runs
// in-process against a running EWMA/z-score baseline and writes gold.anomalies
// plus a push on ecommerce.anomalies. Live updates fan out through the
// Broadcaster to SSE/gRPC subscribers.
type Aggregator struct {
	pool    *pgxpool.Pool
	cl      *kgo.Client
	anomPub *kgo.Client
	bc      *Broadcaster
	log     *slog.Logger

	speed  float64
	status string

	detector   *Detector
	flushEvery time.Duration

	tel *telemetry.Registry // optional Prometheus sink (nil-safe)

	mu           sync.Mutex
	buckets      map[time.Time]*bucket
	lastAnalyzed time.Time
}

type bucket struct {
	start    time.Time
	revenue  float64
	orders   map[string]struct{}
	sessions map[string]struct{}
	anomaly  bool
}

// NewAggregator wires an Aggregator around its consumers and broadcaster.
// `cl` must already be configured to consume TopicOrders + TopicClicks;
// `anomPub` is a separate producer used to publish anomaly events.
func NewAggregator(pool *pgxpool.Pool, cl, anomPub *kgo.Client, bc *Broadcaster,
	speed float64, status string, log *slog.Logger) *Aggregator {
	return &Aggregator{
		pool:       pool,
		cl:         cl,
		anomPub:    anomPub,
		bc:         bc,
		log:        log,
		speed:      speed,
		status:     status,
		detector:   NewDetector(),
		flushEvery: 5 * time.Second,
		buckets:    make(map[time.Time]*bucket),
	}
}

// Instrument attaches the optional Prometheus registry the aggregator reports
// into. Nil-safe: without a registry every counter is a no-op.
func (a *Aggregator) Instrument(reg *telemetry.Registry) {
	a.tel = reg
}

// Baseline returns the detector's current accumulator states (for observability
// of warm-up progress and for snapshot persistence).
func (a *Aggregator) Baseline() []MetricState {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.detector.Snapshot()
}

// Run consumes until ctx is cancelled, flushing buckets every flushEvery and on
// shutdown. It returns nil on graceful cancellation.
func (a *Aggregator) Run(ctx context.Context) error {
	// Resume the anomaly baseline from the last persisted snapshot so a restart
	// skips the 20-bucket warm-up instead of re-learning from zero (and firing a
	// false-positive burst while it does).
	if err := a.restoreBaseline(ctx); err != nil {
		a.log.Warn("aggregator: detector baseline not restored", "error", err)
	}
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		for ctx.Err() == nil {
			fetches := a.cl.PollFetches(ctx)
			if fetches.IsClientClosed() {
				return
			}
			records := fetches.Records()
			if len(records) == 0 {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			for _, rec := range records {
				a.ingest(rec)
			}
		}
	}()

	ticker := time.NewTicker(a.flushEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			a.flushAndDetect(context.Background())
			<-consumeDone
			return nil
		case <-ticker.C:
			if err := a.flushAndDetect(ctx); err != nil {
				a.log.Error("aggregator flush failed", "error", err)
				a.flushErrors().Inc()
			}
		}
	}
}

func (a *Aggregator) restoreBaseline(ctx context.Context) error {
	states, err := LoadDetectorState(ctx, a.pool)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.detector.Restore(states)
	a.mu.Unlock()
	if len(states) > 0 {
		a.log.Info("aggregator: detector baseline restored", "metrics", len(states))
	}
	return nil
}

// saveBaseline persists the current accumulators so the next start resumes
// here. Called after every flush (a few rows) — cheap insurance against any
// crash, not just a clean shutdown.
func (a *Aggregator) saveBaseline(ctx context.Context) {
	a.mu.Lock()
	states := a.detector.Snapshot()
	a.mu.Unlock()
	if err := SaveDetectorState(ctx, a.pool, states); err != nil {
		a.log.Debug("aggregator: save detector state", "error", err)
	}
}

func (a *Aggregator) flushErrors() *telemetry.Series {
	return a.tel.Counter("abi_aggregator_flush_errors_total", "Flush/detect cycles that could not be persisted.")
}

func (a *Aggregator) anomaliesDetected() *telemetry.Series {
	return a.tel.Counter("abi_anomalies_detected_total", "Anomalies persisted to gold.anomalies.")
}

// ingest folds one raw record into its minute bucket.
func (a *Aggregator) ingest(rec *kgo.Record) {
	var env stream.Envelope
	if err := json.Unmarshal(rec.Value, &env); err != nil {
		a.log.Warn("aggregator: undecodable record", "topic", rec.Topic, "error", err)
		return
	}
	start := env.OccurredAt.UTC().Truncate(time.Minute)

	switch rec.Topic {
	case stream.TopicOrders:
		var p stream.OrderPlaced
		if err := env.DecodePayload(&p); err != nil {
			a.log.Warn("aggregator: bad order payload", "event_id", env.EventID, "error", err)
			return
		}
		// Same revenue filter as the Phase 0 batch layer:
		// status NOT IN ('canceled','unavailable') AND payment_value_total > 0.
		if p.IsLost || p.PaymentValue <= 0 {
			return
		}
		a.observe(start, p.OrderID, p.PaymentValue)
	case stream.TopicClicks:
		var sessionID string
		switch env.EventType {
		case stream.EventPageView:
			var p stream.PageView
			if err := env.DecodePayload(&p); err != nil {
				a.log.Warn("aggregator: bad page.view payload", "event_id", env.EventID, "error", err)
				return
			}
			sessionID = p.SessionID
		case stream.EventCartAbandoned:
			var p stream.CartAbandoned
			if err := env.DecodePayload(&p); err != nil {
				a.log.Warn("aggregator: bad cart.abandoned payload", "event_id", env.EventID, "error", err)
				return
			}
			sessionID = p.SessionID
		default:
			return
		}
		if sessionID == "" {
			return
		}
		a.observeSession(start, sessionID)
	}
}

// observe records a revenue-bearing order in a minute bucket.
func (a *Aggregator) observe(start time.Time, orderID string, revenue float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.bucketLocked(start)
	if _, dup := b.orders[orderID]; dup {
		return // dedupe by order id (at-least-once redelivery)
	}
	b.orders[orderID] = struct{}{}
	b.revenue += revenue
}

// observeSession records a session as active in a minute bucket.
func (a *Aggregator) observeSession(start time.Time, sessionID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.bucketLocked(start)
	b.sessions[sessionID] = struct{}{}
}

func (a *Aggregator) bucketLocked(start time.Time) *bucket {
	b, ok := a.buckets[start]
	if !ok {
		b = &bucket{
			start:    start,
			orders:   make(map[string]struct{}),
			sessions: make(map[string]struct{}),
		}
		a.buckets[start] = b
	}
	return b
}

// flushAndDetect persists all buckets, runs anomaly detection over closed
// buckets, and broadcasts the latest state.
func (a *Aggregator) flushAndDetect(ctx context.Context) error {
	a.mu.Lock()
	now := time.Now().UTC()
	cutoff := now.Add(-time.Minute) // buckets strictly older than 1 min are closed
	starts := make([]time.Time, 0, len(a.buckets))
	for s := range a.buckets {
		starts = append(starts, s)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })

	var anomalies []model.Anomaly
	for _, s := range starts {
		b := a.buckets[s]
		if s.Before(cutoff) && s.After(a.lastAnalyzed) {
			a.detectBucketLocked(b, &anomalies)
		}
	}
	a.mu.Unlock()

	// Persist buckets (with updated anomaly flags) up to our retention window.
	if err := a.persistBuckets(ctx, starts); err != nil {
		return err
	}

	// Persist + publish anomalies.
	var lastAnom *model.Anomaly
	for i := range anomalies {
		anom, err := a.persistAnomaly(ctx, &anomalies[i])
		if err != nil {
			a.log.Error("aggregator: persist anomaly", "error", err)
			continue
		}
		a.anomaliesDetected().Inc()
		lastAnom = anom
		if err := a.publishAnomaly(ctx, anom); err != nil {
			a.log.Error("aggregator: publish anomaly", "error", err)
		}
	}

	// Keep the persisted anomaly baseline fresh (crash-safe resume).
	a.saveBaseline(ctx)

	// Broadcast the most recent bucket. It may already have been pruned by
	// persistBuckets when a flush was starved long enough for the newest bucket
	// to age past the retention window (host sleep, ticker stall): a stale
	// bucket is not "current state", so publish nothing for it rather than
	// panicking on a nil dereference.
	var current model.RealtimeBucket
	if len(starts) > 0 {
		latest := starts[len(starts)-1]
		a.mu.Lock()
		if b, ok := a.buckets[latest]; ok {
			current = model.RealtimeBucket{
				BucketStart:    b.start.UTC().Format(time.RFC3339),
				Revenue:        round2(b.revenue),
				Orders:         int64(len(b.orders)),
				ActiveSessions: int64(len(b.sessions)),
				AnomalyFlag:    b.anomaly,
				UpdatedAt:      now.UTC().Format(time.RFC3339),
			}
		}
		a.mu.Unlock()
	}
	if current.BucketStart != "" || lastAnom != nil {
		a.bc.Publish(model.MetricsUpdate{
			Current:         current,
			Anomaly:         lastAnom,
			SpeedMultiplier: a.speed,
			Status:          a.status,
			Source:          model.SourceLiveReplay,
		})
	}
	return nil
}

// detectBucketLocked runs the online detector over one closed bucket and
// records triggered anomalies. Caller holds a.mu.
func (a *Aggregator) detectBucketLocked(b *bucket, out *[]model.Anomaly) {
	ts := b.start.UTC()
	for _, m := range []string{"revenue", "orders"} {
		var observed float64
		if m == "revenue" {
			observed = b.revenue
		} else {
			observed = float64(len(b.orders))
		}
		res := a.detector.Evaluate(m, observed)
		if res.Triggered {
			b.anomaly = true
			*out = append(*out, model.Anomaly{
				Metric:      m,
				BucketStart: ts.Format(time.RFC3339),
				Observed:    round2(observed),
				Expected:    round2(res.Expected),
				ZScore:      round2(res.Z),
				Severity:    res.Severity,
				Status:      "open",
			})
		}
	}
	a.lastAnalyzed = b.start
}

// persistBuckets upserts every in-memory bucket into gold.realtime_metrics.
func (a *Aggregator) persistBuckets(ctx context.Context, starts []time.Time) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	persisted := make([]model.RealtimeBucket, 0, len(starts))
	for _, s := range starts {
		b := a.buckets[s]
		persisted = append(persisted, model.RealtimeBucket{
			BucketStart:    s.UTC().Format(time.RFC3339),
			Revenue:        round2(b.revenue),
			Orders:         int64(len(b.orders)),
			ActiveSessions: int64(len(b.sessions)),
			AnomalyFlag:    b.anomaly,
			UpdatedAt:      time.Now().UTC().Format(time.RFC3339),
		})
	}
	if len(persisted) == 0 {
		return nil
	}
	// Bulk upsert in one round trip.
	var sb []string
	args := make([]any, 0, len(persisted)*5)
	for _, p := range persisted {
		args = append(args, p.BucketStart, p.Revenue, p.Orders, p.ActiveSessions, p.AnomalyFlag)
		sb = append(sb, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d)",
			len(args)-4, len(args)-3, len(args)-2, len(args)-1, len(args)))
	}
	sql := `INSERT INTO gold.realtime_metrics (bucket_start, revenue, orders, active_sessions, anomaly_flag)
VALUES ` + joinArgs(sb) + `
ON CONFLICT (bucket_start) DO UPDATE SET
  revenue = EXCLUDED.revenue,
  orders = EXCLUDED.orders,
  active_sessions = EXCLUDED.active_sessions,
  anomaly_flag = EXCLUDED.anomaly_flag,
  updated_at = now()`
	if _, err := a.pool.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("upsert realtime metrics: %w", err)
	}
	// Drop buckets that slid outside the in-memory window.
	cut := time.Now().UTC().Add(-10 * time.Minute)
	for s := range a.buckets {
		if s.Before(cut) {
			delete(a.buckets, s)
		}
	}
	return nil
}

// persistAnomaly writes a triggered anomaly to gold.anomalies, returning the
// row with its assigned id. The statistical detector is a distinct writer kind
// from the Phase 3 model-driven rate anomalies (see scorewriter).
func (a *Aggregator) persistAnomaly(ctx context.Context, anom *model.Anomaly) (*model.Anomaly, error) {
	var out model.Anomaly
	err := a.pool.QueryRow(ctx, `
INSERT INTO gold.anomalies (metric, detector, bucket_start, observed, expected, z_score, severity, status, detected_at)
VALUES ($1, 'statistical', $2, $3, $4, $5, $6, 'open', now())
RETURNING id, metric, detector,
  to_char(bucket_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
  observed, expected, z_score, severity, status,
  to_char(detected_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')`,
		anom.Metric, anom.BucketStart, anom.Observed, anom.Expected, anom.ZScore, anom.Severity).
		Scan(&out.ID, &out.Metric, &out.Detector, &out.BucketStart, &out.Observed, &out.Expected,
			&out.ZScore, &out.Severity, &out.Status, &out.DetectedAt)
	if err != nil {
		return nil, fmt.Errorf("insert anomaly: %w", err)
	}
	return &out, nil
}

// publishAnomaly pushes an anomaly event onto ecommerce.anomalies so anything
// downstream (bronze recorder, future agents) sees it with full lineage.
func (a *Aggregator) publishAnomaly(ctx context.Context, anom *model.Anomaly) error {
	bt, err := time.Parse(time.RFC3339, anom.BucketStart)
	if err != nil {
		return fmt.Errorf("parse bucket_start: %w", err)
	}
	env, err := stream.NewEnvelope(stream.EventAnomaly, anom.Metric, bt, "", stream.AnomalyEvent{
		ID:         anom.ID,
		Metric:     anom.Metric,
		Detector:   anom.Detector,
		BucketTime: bt,
		Observed:   anom.Observed,
		Expected:   anom.Expected,
		ZScore:     anom.ZScore,
		Severity:   anom.Severity,
		DetectedAt: time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	val, err := json.Marshal(env)
	if err != nil {
		return err
	}
	rec := &kgo.Record{Topic: stream.TopicAnomalies, Key: []byte(anom.Metric), Value: val}
	if err := a.anomPub.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return err
	}
	return nil
}

// round2 rounds to 2 decimal places. The detector's variance floor keeps
// z-scores finite even on flat baselines, but an unexpected non-finite value
// (NaN creeping in from undecodable input) must still survive JSON broadcast
// and DB persistence — clamp to ±1e15 as a backstop only.
func round2(v float64) float64 {
	if math.IsInf(v, 0) || math.IsNaN(v) {
		if v > 0 {
			return 1e15
		}
		if v < 0 {
			return -1e15
		}
		return 0
	}
	return math.Round(v*100) / 100
}

// joinArgs joins a slice of argument placeholders with commas.
func joinArgs(pieces []string) string {
	var out string
	for i, p := range pieces {
		if i > 0 {
			out += ","
		}
		out += p
	}
	return out
}
