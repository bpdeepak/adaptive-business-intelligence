//go:build integration

// Integration tests for the Phase 1 realtime pipeline, running the real
// producers/consumers against a live Redpanda + Postgres stack.
//
// Skipped automatically when either service is unreachable so plain
// `go test ./...` stays green everywhere:
//
//   - Local dev: docker compose up -d --wait then run as-is (defaults match the
//     stack: kafka://localhost:29092, postgres://abi:abi@localhost:5432/abi).
//   - CI: the workflow starts Redpanda + Postgres and can override the
//     endpoints with ABI_TEST_KAFKA / ABI_TEST_DATABASE_URL.
//
// The test drives the real BronzeWriter + Aggregator end to end: it publishes a
// controlled series of 1-minute buckets (baseline + a revenue/orders spike) to
// the Kafka topics, lets the consumers fold them into bronze.stream_events and
// gold.realtime_metrics, and asserts the spike is detected into
// gold.anomalies with the correct lineage stamped on every raw row.
package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"abi/internal/kafka"
	"abi/internal/stream"
)

const (
	baselineBuckets = 24 // 1-minute buckets of normal traffic
	bucketOrders    = 8  // valid (revenue-bearing) orders per baseline bucket
	bucketLost      = 2  // canceled orders per bucket (must be excluded)
	bucketClicks    = 20 // total page views per bucket (4 sessions x 5 views)
	spikeOrders     = 60 // order burst in the final bucket (15x a baseline)
	orderValue      = 100.0
)

type realtimeStack struct {
	pool       *pgxpool.Pool
	seeds      []string
	marker     string // unique loop id for this test run
	markerFrom time.Time
}

// newRealtimeStack connects to Postgres + Kafka, bootstraps the realtime schema
// and topics, and returns a stack tagged with a unique marker loop id. Any
// unreachable dependency causes a skip.
func newRealtimeStack(t *testing.T) *realtimeStack {
	t.Helper()

	dsn := os.Getenv("ABI_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://abi:abi@localhost:5432/abi?sslmode=disable"
	}
	seeds := []string{"localhost:29092"}
	if v := os.Getenv("ABI_TEST_KAFKA"); v != "" {
		seeds = strings.Split(v, ",")
	}

	probeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(probeCtx, dsn)
	if err != nil {
		t.Skipf("postgres unreachable (%v) — skipping realtime integration tests", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(probeCtx); err != nil {
		t.Skipf("postgres unreachable (%v) — skipping realtime integration tests", err)
	}
	if err := EnsureSchema(probeCtx, pool); err != nil {
		t.Fatalf("ensure realtime schema: %v", err)
	}
	// Trim any leftover records from earlier runs (or a stray producer) so a
	// fresh consumer group sees exactly the records this run produces — the
	// bucket-level assertions below rely on a precise dataset. Destructive to
	// the topics by design; the producer regenerates everything on replay.
	if err := kafka.TrimTopics(probeCtx, seeds, stream.AllTopics, 1); err != nil {
		t.Skipf("kafka unreachable (%v) — skipping realtime integration tests", err)
	}

	// Buckets in the past 27..1 minutes keep every bucket inside the
	// aggregator's closed-bucket window right after start.
	from := time.Now().UTC().Truncate(time.Minute).Add(-time.Duration(baselineBuckets+2) * time.Minute)
	return &realtimeStack{
		pool:       pool,
		seeds:      seeds,
		marker:     fmt.Sprintf("itest-%d", time.Now().UnixNano()),
		markerFrom: from,
	}
}

// cleanup removes this run's rows from the streaming tables so repeated runs
// (and the CI job) start clean. Topic records are intentionally left in place:
// consumers use fresh groups and Postgres is the source of truth under test.
func (s *realtimeStack) cleanup(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := s.pool.Exec(ctx, `DELETE FROM bronze.stream_events WHERE loop_id = $1`, s.marker); err != nil {
		t.Logf("cleanup stream_events: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM gold.realtime_metrics WHERE bucket_start >= $1`, s.markerFrom); err != nil {
		t.Logf("cleanup realtime_metrics: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM gold.anomalies WHERE bucket_start >= $1`, s.markerFrom); err != nil {
		t.Logf("cleanup anomalies: %v", err)
	}
}

// orderRecord wraps an OrderPlaced payload in a marker-tagged envelope record.
func (s *realtimeStack) orderRecord(op stream.OrderPlaced) *kgo.Record {
	env, err := stream.NewEnvelope(stream.EventOrderPlaced, op.CustomerID, op.PurchaseTime, s.marker, op)
	if err != nil {
		panic(err)
	}
	val, _ := json.Marshal(env)
	return &kgo.Record{Topic: stream.TopicOrders, Key: []byte(op.CustomerID), Value: val}
}

// clickRecord wraps a PageView payload in a marker-tagged envelope record.
func (s *realtimeStack) clickRecord(sid string, at time.Time) *kgo.Record {
	env, err := stream.NewEnvelope(stream.EventPageView, sid, at, s.marker, stream.PageView{
		SessionID: sid, PageType: "home",
	})
	if err != nil {
		panic(err)
	}
	val, _ := json.Marshal(env)
	return &kgo.Record{Topic: stream.TopicClicks, Key: []byte(sid), Value: val}
}

// buildEvents publishes a deterministic series: `baselineBuckets` normal minute
// buckets followed by one spike bucket (with canceled orders interleaved that
// must be excluded). Returns the total number of records produced and the
// spike bucket's minute boundary.
func (s *realtimeStack) buildEvents(ctx context.Context, t *testing.T, prod *kgo.Client) (int, time.Time) {
	t.Helper()

	spikeAt := s.markerFrom.Add(time.Duration(baselineBuckets) * time.Minute)
	var recs []*kgo.Record

	for i := 0; i <= baselineBuckets; i++ {
		at := s.markerFrom.Add(time.Duration(i) * time.Minute)
		nOrders := bucketOrders
		if i == baselineBuckets {
			nOrders = spikeOrders
		}
		// Valid revenue-bearing orders.
		for j := 0; j < nOrders; j++ {
			recs = append(recs, s.orderRecord(stream.OrderPlaced{
				OrderID:      fmt.Sprintf("%s-o-%d-%d", s.marker, i, j),
				CustomerID:   fmt.Sprintf("c-%d-%d", i, j),
				Status:       "delivered",
				PurchaseTime: at.Add(time.Duration(j) * time.Second),
				PaymentValue: orderValue,
				Items: []stream.OrderItem{{
					ProductID: "test-prod", SellerID: "test-seller",
					Price: orderValue, Freight: 10, Quantity: 1,
				}},
			}))
		}
		// Canceled orders that must NOT count toward revenue/orders.
		for j := 0; j < bucketLost; j++ {
			recs = append(recs, s.orderRecord(stream.OrderPlaced{
				OrderID:      fmt.Sprintf("%s-l-%d-%d", s.marker, i, j),
				CustomerID:   fmt.Sprintf("c-lost-%d-%d", i, j),
				Status:       "canceled",
				PurchaseTime: at.Add(time.Duration(j) * time.Second),
				PaymentValue: 500.0,
				IsLost:       true,
				Items:        []stream.OrderItem{},
			}))
		}
		// Clickstream activity: 4 sessions x 5 page views within the minute.
		for j := 0; j < 4; j++ {
			sid := fmt.Sprintf("%s-s-%d-%d", s.marker, i, j)
			for k := 0; k < 5; k++ {
				recs = append(recs, s.clickRecord(sid, at.Add(time.Duration(k)*3*time.Second)))
			}
		}
	}

	if err := prod.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("produce test events: %v", err)
	}

	wantValid := baselineBuckets*bucketOrders + spikeOrders
	wantLost := (baselineBuckets + 1) * bucketLost
	wantClicks := (baselineBuckets + 1) * bucketClicks
	return wantValid + wantLost + wantClicks, spikeAt
}

// waitFor polls cond until it passes or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := cond()
		if err != nil {
			lastErr = err
		}
		if ok {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("timed out waiting for %s: %v", what, lastErr)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestRealtimePipelineEndToEnd(t *testing.T) {
	st := newRealtimeStack(t)
	t.Cleanup(func() { st.cleanup(t) })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Publish the controlled series before consumers start so reading from the
	// group's start offset picks up every test record in one pass.
	prod, err := kafka.Producer(st.seeds)
	if err != nil {
		t.Fatalf("test producer: %v", err)
	}
	defer prod.Close()
	expectedEvents, spikeAt := st.buildEvents(ctx, t, prod)

	// Wire the real consumers with throwaway groups (start-at-earliest).
	bronzeCl, err := kafka.Consumer(st.seeds, fmt.Sprintf("itest-bronze-%d", time.Now().UnixNano()), stream.AllTopics)
	if err != nil {
		t.Fatalf("bronze consumer: %v", err)
	}
	defer bronzeCl.Close()
	aggCl, err := kafka.Consumer(st.seeds, fmt.Sprintf("itest-agg-%d", time.Now().UnixNano()),
		[]string{stream.TopicOrders, stream.TopicClicks})
	if err != nil {
		t.Fatalf("aggregator consumer: %v", err)
	}
	defer aggCl.Close()
	anomPub, err := kafka.Producer(st.seeds)
	if err != nil {
		t.Fatalf("anomaly producer: %v", err)
	}
	defer anomPub.Close()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	bc := NewBroadcaster()
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	writer := NewBronzeWriter(st.pool, bronzeCl, "itest-run", log)
	agg := NewAggregator(st.pool, aggCl, anomPub, bc, 2880, "replay", log)
	writerDone := make(chan error, 1)
	aggDone := make(chan error, 1)
	go func() { writerDone <- writer.Run(runCtx) }()
	go func() { aggDone <- agg.Run(runCtx) }()

	wantValid := baselineBuckets*bucketOrders + spikeOrders

	// 1. Every test event lands in bronze.stream_events verbatim.
	waitFor(t, 60*time.Second, "bronze stream_events rows",
		func() (bool, error) {
			var n int
			err := st.pool.QueryRow(ctx,
				`SELECT count(*) FROM bronze.stream_events WHERE loop_id = $1`, st.marker).Scan(&n)
			return n == expectedEvents, err
		})

	// 2. Deterministic bucket aggregates: one row per minute, only valid orders.
	waitFor(t, 60*time.Second, "realtime_metrics buckets",
		func() (bool, error) {
			var buckets, orders int
			var revenue float64
			err := st.pool.QueryRow(ctx, `
				SELECT count(*), coalesce(sum(orders),0)::int,
				       coalesce(sum(revenue),0)::float8
				FROM gold.realtime_metrics WHERE bucket_start >= $1`, st.markerFrom).
				Scan(&buckets, &orders, &revenue)
			if err != nil {
				return false, err
			}
			return buckets == baselineBuckets+1 && orders == wantValid &&
				abs(revenue-float64(wantValid)*orderValue) < 0.01, nil
		})

	// 3. The spike bucket trips the orders detector into gold.anomalies.
	waitFor(t, 60*time.Second, "spike anomaly in gold.anomalies",
		func() (bool, error) {
			var n int
			err := st.pool.QueryRow(ctx, `
				SELECT count(*) FROM gold.anomalies
				WHERE bucket_start >= $1 AND metric = 'orders' AND status = 'open'`,
				st.markerFrom).Scan(&n)
			return n >= 1, err
		})

	// Stop the consumers so they flush their tails before we verify details.
	runCancel()
	select {
	case err := <-writerDone:
		if err != nil {
			t.Fatalf("bronze writer: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("bronze writer did not stop")
	}
	select {
	case err := <-aggDone:
		if err != nil {
			t.Fatalf("aggregator: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("aggregator did not stop")
	}

	// 4. Lineage precision: which pipeline wrote which row.
	srcs := map[string]bool{}
	{
		rows, err := st.pool.Query(ctx, `
			SELECT DISTINCT _source_file, _batch_id, _kafka_topic
			FROM bronze.stream_events WHERE loop_id = $1`, st.marker)
		if err != nil {
			t.Fatalf("lineage query: %v", err)
		}
		for rows.Next() {
			var src, batch, topic string
			if err := rows.Scan(&src, &batch, &topic); err != nil {
				rows.Close()
				t.Fatalf("lineage scan: %v", err)
			}
			srcs[src] = true
			if batch != st.marker {
				t.Errorf("row from %s has _batch_id=%q, want marker %q", src, batch, st.marker)
			}
			if topic == "" {
				t.Errorf("row from %s has empty _kafka_topic", src)
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatalf("lineage rows: %v", err)
		}
		rows.Close()
	}
	if !srcs["stream:ecommerce.orders.events"] || !srcs["stream:ecommerce.clickstream.events"] {
		t.Errorf("missing order/click sources in stream_events lineage: %v", srcs)
	}

	// 5. No double-counting and no lost/canceled leakage into revenue.
	var ordersTotal, sessionsTotal int
	var revenueTotal float64
	if err := st.pool.QueryRow(ctx, `
		SELECT coalesce(sum(orders),0)::int, coalesce(sum(active_sessions),0)::int,
		       coalesce(sum(revenue),0)::float8
		FROM gold.realtime_metrics WHERE bucket_start >= $1`, st.markerFrom).
		Scan(&ordersTotal, &sessionsTotal, &revenueTotal); err != nil {
		t.Fatalf("metric sums: %v", err)
	}
	if ordersTotal != wantValid {
		t.Errorf("orders = %d, want %d (lost/canceled must be excluded)", ordersTotal, wantValid)
	}
	wantSessions := (baselineBuckets + 1) * 4
	if sessionsTotal != wantSessions {
		t.Errorf("active_sessions = %d, want %d", sessionsTotal, wantSessions)
	}
	if abs(revenueTotal-float64(wantValid)*orderValue) > 0.01 {
		t.Errorf("revenue = %.2f, want %.2f", revenueTotal, float64(wantValid)*orderValue)
	}

	// 6. The spike row is unambiguous: observed == burst, severe, z >= 5, and
	// it points at the exact spike minute.
	var anObs, anExpected, anZ float64
	var severity string
	var bucketStart string
	if err := st.pool.QueryRow(ctx, `
		SELECT observed::float8, expected::float8, z_score, severity,
		       to_char(bucket_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
		FROM gold.anomalies
		WHERE bucket_start >= $1 AND metric = 'orders'
		ORDER BY z_score DESC LIMIT 1`, st.markerFrom).
		Scan(&anObs, &anExpected, &anZ, &severity, &bucketStart); err != nil {
		t.Fatalf("spike anomaly: %v", err)
	}
	if anObs != spikeOrders {
		t.Errorf("spike observed = %.0f, want %d", anObs, spikeOrders)
	}
	if severity != SeveritySevere {
		t.Errorf("spike severity = %q, want %q", severity, SeveritySevere)
	}
	if anZ < 5 {
		t.Errorf("spike z_score = %.2f, want >= 5", anZ)
	}
	wantSpike := spikeAt.UTC().Format(time.RFC3339)
	if bucketStart != wantSpike {
		t.Errorf("spike bucket = %s, want %s", bucketStart, wantSpike)
	}
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
