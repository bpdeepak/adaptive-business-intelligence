// Command producer replays the Olist historical dataset onto Redpanda as the
// Phase 1 event stream: orders (+ converting sessions), abandoned sessions,
// catalog price changes, and restricted bot-training labels. It uses a virtual
// clock (SPEED_MULTIPLIER simulated seconds per wall second) so the whole
// dataset (~774 days) is replayed in ~6.5 hours at the default pacing,
// looping forever with per-loop event-id suffixes.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"abi/internal/config"
	"abi/internal/kafka"
	"abi/internal/model"
	"abi/internal/stream"
	"abi/internal/telemetry"
)

// emitted counts successfully produced records (updated from callbacks).
var emitted atomic.Uint64

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	if err := run(logger, config.FromEnv()); err != nil {
		logger.Error("producer fatal", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, cfg config.Config) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 1. Load the replay dataset from the gold layer.
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer pool.Close()

	orders, err := loadReplayOrders(ctx, pool)
	if err != nil {
		return err
	}
	if len(orders) == 0 {
		return errors.New("no orders in gold.fct_orders; run the dbt build first")
	}
	products, err := loadProducts(ctx, pool)
	if err != nil {
		return err
	}
	startPrice, err := loadPrices(ctx, pool)
	if err != nil {
		return err
	}
	spanDays := int(orders[len(orders)-1].PurchaseAt.Sub(orders[0].PurchaseAt).Hours()/24) + 1
	logger.Info("replay dataset loaded",
		"orders", len(orders),
		"products", len(products),
		"span_days", spanDays,
		"estimated_loop", config.LoopDurationAtSpeed(float64(spanDays), cfg.SpeedMultiplier).String(),
	)

	// 2. Ensure topics exist (idempotent).
	if err := kafka.EnsureTopics(ctx, cfg.KafkaSeedBrokers, stream.AllTopics, 3); err != nil {
		return err
	}
	logger.Info("topics ensured", "topics", len(stream.AllTopics))

	// 3. Wire the simulator, bounded ring, and Kafka writer.
	producer, err := kafka.Producer(cfg.KafkaSeedBrokers)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer producer.Close()

	ring := stream.NewRing(cfg.RingCapacity)
	sim := stream.NewSimulator(stream.SimConfig{
		Orders:     orders,
		Products:   products,
		Price:      startPrice,
		Rate:       cfg.SpeedMultiplier,
		BotRatio:   cfg.BotRatio,
		Conversion: cfg.SessionConversionRate,
	}, rand.New(rand.NewSource(time.Now().UnixNano())), ring)

	go writeLoop(ctx, logger, producer, ring)

	// 4. Periodic stats + Prometheus instrumentation. The producer is the only
	// place that knows how much the ring is shedding, so its /metrics endpoint
	// is the first stop when dashboards show a data gap.
	reg := telemetry.NewRegistry()
	metricsDone := make(chan struct{})
	go func() {
		defer close(metricsDone)
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", reg.Handler())
		msrv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = msrv.Shutdown(shutdownCtx)
		}()
		logger.Info("metrics listening", "addr", cfg.MetricsAddr)
		if err := msrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Warn("metrics server failed", "error", err)
		}
	}()

	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		var lastEmitted, lastDropped uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e := emitted.Load()
				d := ring.Dropped()
				logger.Info("producer stats",
					"loop", sim.Loop(),
					"emitted", e, "emitted_delta", e-lastEmitted,
					"buffered", ring.Len(),
					"dropped_total", d, "dropped_delta", d-lastDropped,
					"speed", cfg.SpeedMultiplier)
				lastEmitted, lastDropped = e, d
			}
		}
	}()

	// Prometheus deltas are computed against the last sample so the counters
	// stay monotonic even though the underlying atomics are snapshot reads.
	go func() {
		events := reg.Counter("abi_producer_events_total", "Records acknowledged by the broker.")
		dropped := reg.Counter("abi_producer_dropped_records_total", "Records shed by the ring buffer (drop-oldest backpressure).")
		buffered := reg.Gauge("abi_producer_buffered_records", "Envelopes currently queued in the ring.")
		speed := reg.Gauge("abi_replay_speed_multiplier", "Simulated seconds per wall second.")
		loop := reg.Gauge("abi_replay_loop", "Current replay loop number.")
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		var lastE, lastD uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e, d := emitted.Load(), ring.Dropped()
				events.Add(float64(e - lastE))
				dropped.Add(float64(d - lastD))
				lastE, lastD = e, d
				buffered.Set(float64(ring.Len()))
				speed.Set(cfg.SpeedMultiplier)
				loop.Set(float64(sim.LoopIndex()))
			}
		}
	}()

	// 5. Replay forever.
	logger.Info("replay started",
		"speed_multiplier", cfg.SpeedMultiplier,
		"bot_ratio", cfg.BotRatio,
		"conversion", cfg.SessionConversionRate,
		"ring_capacity", cfg.RingCapacity,
	)
	if err := sim.Run(ctx); err != nil {
		return err
	}

	logger.Info("producer shutting down; flushing to kafka")
	finishCtx, cancelFinish := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFinish()
	if err := producer.Flush(finishCtx); err != nil {
		logger.Warn("final flush incomplete", "error", err)
	}
	<-statsDone
	<-metricsDone
	logger.Info("producer stopped",
		"emitted", emitted.Load(),
		"dropped", ring.Dropped())
	return nil
}

// writeLoop drains the ring into Kafka. The simulator emits in simulated-time
// order, so per-key ordering is preserved; backpressure is the ring's
// drop-oldest, never blocking the replay clock.
func writeLoop(ctx context.Context, logger *slog.Logger, pr *kgo.Client, ring *stream.Ring) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		env, ok := ring.Pop()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
			continue
		}
		val, err := json.Marshal(env)
		if err != nil {
			logger.Warn("marshal envelope", "event_id", env.EventID, "error", err)
			continue
		}
		rec := &kgo.Record{
			Topic: topicFor(env.EventType),
			Key:   []byte(env.Key),
			Value: val,
		}
		pr.Produce(ctx, rec, func(r *kgo.Record, err error) {
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					logger.Warn("produce failed",
						"topic", r.Topic, "key", string(r.Key), "error", err)
				}
				return
			}
			emitted.Add(1)
		})
	}
}

// topicFor maps an event type to its delivery topic.
func topicFor(eventType string) string {
	switch eventType {
	case stream.EventTrainingLabel:
		return stream.TopicLabels
	case stream.EventPriceChanged:
		return stream.TopicPrices
	case stream.EventAnomaly:
		return stream.TopicAnomalies
	case stream.EventOrderPlaced:
		return stream.TopicOrders
	default: // page.view / cart.abandoned
		return stream.TopicClicks
	}
}

// ---------------------------------------------------------------------------
// Replay data loading (gold → simulator)
// ---------------------------------------------------------------------------

func loadReplayOrders(ctx context.Context, pool *pgxpool.Pool) ([]model.ReplayOrder, error) {
	rows, err := pool.Query(ctx, `
SELECT o.order_id, o.customer_id, o.order_status, o.order_purchase_timestamp,
       o.payment_value_total::float8,
       (o.order_status IN ('canceled','unavailable')) AS is_lost
FROM gold.fct_orders o
ORDER BY o.order_purchase_timestamp, o.order_id`)
	if err != nil {
		return nil, fmt.Errorf("load replay orders: %w", err)
	}
	defer rows.Close()

	type base struct {
		orderID, customerID, status string
		at                          time.Time
		value                       float64
		lost                        bool
	}
	var bs []base
	for rows.Next() {
		var b base
		if err := rows.Scan(&b.orderID, &b.customerID, &b.status, &b.at, &b.value, &b.lost); err != nil {
			return nil, fmt.Errorf("scan replay order: %w", err)
		}
		bs = append(bs, b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	itemRows, err := pool.Query(ctx, `
SELECT order_id, product_id, seller_id, price::float8, freight_value::float8
FROM gold.fct_order_items`)
	if err != nil {
		return nil, fmt.Errorf("load replay items: %w", err)
	}
	defer itemRows.Close()
	items := make(map[string][]model.ReplayItem)
	for itemRows.Next() {
		var orderID, productID, sellerID string
		var price, freight float64
		if err := itemRows.Scan(&orderID, &productID, &sellerID, &price, &freight); err != nil {
			return nil, fmt.Errorf("scan replay item: %w", err)
		}
		items[orderID] = append(items[orderID], model.ReplayItem{
			ProductID: productID, SellerID: sellerID, Price: price, Freight: freight,
		})
	}
	if err := itemRows.Err(); err != nil {
		return nil, err
	}

	payRows, err := pool.Query(ctx, `
SELECT order_id, payment_type,
       COALESCE(payment_installments, 0)::int,
       payment_value::float8
FROM silver.stg_order_payments
ORDER BY order_id, payment_value DESC, payment_installments DESC, payment_type`)
	if err != nil {
		return nil, fmt.Errorf("load replay payments: %w", err)
	}
	defer payRows.Close()
	payments := make(map[string][]model.ReplayPayment)
	for payRows.Next() {
		var orderID, pType string
		var installments int
		var value float64
		if err := payRows.Scan(&orderID, &pType, &installments, &value); err != nil {
			return nil, fmt.Errorf("scan replay payment: %w", err)
		}
		payments[orderID] = append(payments[orderID], model.ReplayPayment{
			Type: pType, Installments: installments, Value: value,
		})
	}
	if err := payRows.Err(); err != nil {
		return nil, err
	}

	out := make([]model.ReplayOrder, 0, len(bs))
	for _, b := range bs {
		out = append(out, model.ReplayOrder{
			OrderID: b.orderID, CustomerID: b.customerID, Status: b.status,
			PurchaseAt: b.at, PaymentValue: b.value, IsLost: b.lost,
			Items: items[b.orderID], Payments: payments[b.orderID],
		})
	}
	return out, nil
}

func loadProducts(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	rows, err := pool.Query(ctx, `SELECT DISTINCT product_id FROM gold.fct_order_items ORDER BY product_id`)
	if err != nil {
		return nil, fmt.Errorf("load products: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func loadPrices(ctx context.Context, pool *pgxpool.Pool) (map[string]float64, error) {
	rows, err := pool.Query(ctx, `
SELECT DISTINCT ON (i.product_id) i.product_id, i.price::float8
FROM gold.fct_order_items i
JOIN gold.fct_orders o ON o.order_id = i.order_id
ORDER BY i.product_id, o.order_purchase_timestamp DESC`)
	if err != nil {
		return nil, fmt.Errorf("load start prices: %w", err)
	}
	defer rows.Close()
	out := make(map[string]float64)
	for rows.Next() {
		var p string
		var price float64
		if err := rows.Scan(&p, &price); err != nil {
			return nil, err
		}
		out[p] = price
	}
	return out, rows.Err()
}
