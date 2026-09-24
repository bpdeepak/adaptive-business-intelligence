// Command server is the Phase 1 realtime runtime: it runs the bronze-writer
// and realtime aggregator (with in-process anomaly detection) on Redpanda,
// persists to Postgres, and serves the REST/SSE dashboard plus the gRPC live
// metrics stream. It replaces cmd/api as the always-on process.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"abi/internal/agent"
	"abi/internal/actions"
	"abi/internal/config"
	"abi/internal/events"
	grpcapi "abi/internal/grpcapi"
	metricsv1 "abi/internal/grpcapi/metricsv1"
	httpapi "abi/internal/http"
	"abi/internal/kafka"
	"abi/internal/model"
	"abi/internal/monitor"
	"abi/internal/playbook"
	"abi/internal/predict"
	"abi/internal/realtime"
	"abi/internal/scorewriter"
	"abi/internal/store"
	"abi/internal/stream"
	"abi/internal/telemetry"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	if err := run(logger, config.FromEnv()); err != nil {
		logger.Error("server fatal", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, cfg config.Config) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 1. Postgres + realtime schema.
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer pool.Close()

	if err := realtime.EnsureSchema(ctx, pool); err != nil {
		return err
	}
	logger.Info("realtime schema ensured")

	if err := predict.EnsureSchema(ctx, pool); err != nil {
		return err
	}
	logger.Info("predict schema ensured")

	// Phase 4 governance + monitoring tables (idempotent on every boot).
	if err := actions.EnsureSchema(ctx, pool); err != nil {
		return err
	}
	logger.Info("actions schema ensured")
	if err := monitor.EnsureSchema(ctx, pool); err != nil {
		return err
	}
	logger.Info("monitor schema ensured")

	st, err := store.NewPostgresStore(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect store: %w", err)
	}
	defer st.Close()

	// 2. Kafka clients.
	anomPub, err := kafka.Producer(cfg.KafkaSeedBrokers)
	if err != nil {
		return fmt.Errorf("anomaly producer: %w", err)
	}
	defer anomPub.Close()

	bronzeCl, err := kafka.Consumer(cfg.KafkaSeedBrokers, cfg.ConsumerGroupBronze, stream.AllTopics)
	if err != nil {
		return fmt.Errorf("bronze consumer: %w", err)
	}
	defer bronzeCl.Close()

	rtCl, err := kafka.Consumer(cfg.KafkaSeedBrokers, cfg.ConsumerGroupRT,
		[]string{stream.TopicOrders, stream.TopicClicks})
	if err != nil {
		return fmt.Errorf("rt consumer: %w", err)
	}
	defer rtCl.Close()

	// 3. Broadcaster + live feed, seeded with the stored snapshot so SSE/gRPC
	// clients render immediately on connect.
	bc := realtime.NewBroadcaster()
	feed := realtime.NewLiveFeed(bc, cfg.SpeedMultiplier, "replay")
	seedBroadcaster(ctx, bc, feed, st)

	// 4. Consumers.
	writer := realtime.NewBronzeWriter(pool, bronzeCl, "server-uuid-"+time.Now().Format("20060102-150405"), logger)
	aggregator := realtime.NewAggregator(pool, rtCl, anomPub, bc, cfg.SpeedMultiplier, "replay", logger)

	// 4b. Observability: the pipeline's own health (consumer lag, flush errors,
	// anomaly counts, baseline progress) is exposed at GET /metrics on the main
	// HTTP listener in Prometheus text format.
	reg := telemetry.NewRegistry()
	aggregator.Instrument(reg)

	// 4c. Phase 4 governance reactor: one in-process domain bus, the playbook
	// engine turning domain events into governed proposals, the action
	// registry owning the approval queue + immutable audit log, and a drift
	// poller bridging the Python monitoring pipeline (gold.model_drift) onto
	// the bus. Wired before any consumer starts so the engine misses nothing.
	bus := events.New()
	aggregator.SetEvents(bus)

	actionsSvc := actions.New(pool, logger)

	rules, err := loadPlaybooks(cfg.PlaybookPath)
	if err != nil {
		return err
	}
	engine, err := playbook.NewEngine(bus, actionsSvc, rules, logger)
	if err != nil {
		return fmt.Errorf("playbook engine: %w", err)
	}
	govCtx, govCancel := context.WithCancel(ctx)
	defer govCancel()
	go func() {
		if err := engine.Run(govCtx); err != nil {
			logger.Error("playbook engine stopped", "error", err)
		}
	}()

	poller := monitor.NewPoller(pool, bus, logger.Info)
	driftCtx, driftCancel := context.WithCancel(ctx)
	defer driftCancel()
	go func() { _ = poller.Run(driftCtx, cfg.DriftPollEvery) }()

	// 5. HTTP (REST + SSE + dashboard + /metrics).
	srv := httpapi.New(st, logger)
	srv.AttachLive(feed)
	srv.AttachActions(actionsSvc)
	rootMux := http.NewServeMux()
	rootMux.Handle("/", srv)
	rootMux.Handle("GET /metrics", reg.Handler())

	// 5b. Phase 2 predictive layer: the model registry + explainable
	// predictions are read from Postgres, and live scoring is brokered through
	// the Python model sidecar (optional; empty ABI_SCORE_URL disables it).
	predictSvc := predict.NewService(pool, predict.NewScoreClient(cfg.ScoreURL), logger)
	predictSvc.Instrument(reg)
	predictSvc.Register(rootMux)
	srv.AttachPredict(predictSvc)
	predictCtx, predictCancel := context.WithCancel(ctx)
	defer predictCancel()
	go predictSvc.RunGaugeLoop(predictCtx, 30*time.Second)

	// 5d. Phase 3 NL→BI agent: proxies the Python agent sidecar (ml/agent/
	// server.py). Optional; empty ABI_AGENT_URL disables /api/v1/agent/query.
	agentSvc := agent.NewService(agent.NewAgentClient(cfg.AgentURL), logger)
	agentSvc.Instrument(reg)
	agentSvc.Register(rootMux)
	agentCtx, agentCancel := context.WithCancel(ctx)
	defer agentCancel()
	go agentSvc.RunGaugeLoop(agentCtx, 30*time.Second)

	// 5c. Phase 3 stream score-writer: consumes order.placed + session.end
	// through its own consumer group, assembles the batch-exact feature
	// vectors, scores via the same sidecar (stream_score rows), and fires
	// model-driven rate anomalies (detector='model'). It shares the in-process
	// broadcaster so its anomalies surface on the same SSE/gRPC live banner;
	// they are not written to ecommerce.anomalies (no consumer exists).
	if err := scorewriter.EnsureSchema(ctx, pool); err != nil {
		return err
	}
	swCl, err := kafka.Consumer(cfg.KafkaSeedBrokers, cfg.ConsumerGroupScoreWriter,
		[]string{stream.TopicOrders, stream.TopicClicks})
	if err != nil {
		return fmt.Errorf("score-writer consumer: %w", err)
	}
	defer swCl.Close()
	sw, err := scorewriter.New(ctx, scorewriter.Options{
		Pool:        pool,
		Client:      swCl,
		Predict:     predictSvc,
		Broadcaster: bc,
		Log:         logger,
		Speed:       cfg.SpeedMultiplier,
		Slack:       cfg.ScoreWriterSlack,
		Buffer:      cfg.ScoreWriterBuffer,
		StateEvery:  cfg.ScoreWriterStateEvery,
		Events:      bus,
	})
	if err != nil {
		return fmt.Errorf("score-writer init: %w", err)
	}
	sw.Instrument(reg)
	scoreCtx, scoreCancel := context.WithCancel(ctx)
	defer scoreCancel()

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           rootMux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// 6. gRPC (live metrics stream).
	grpcSrv := grpc.NewServer()
	metricsv1.RegisterMetricsServiceServer(grpcSrv, grpcapi.New(feed, logger))
	reflection.Register(grpcSrv)
	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}

	// 7. Retention janitor + self-healing reconciliation loop.
	retentionCtx, retentionCancel := context.WithCancel(ctx)
	defer retentionCancel()
	go retentionLoop(retentionCtx, logger, pool, cfg)

	reconcileCtx, reconcileCancel := context.WithCancel(ctx)
	defer reconcileCancel()
	go reconcileLoop(reconcileCtx, logger, pool, cfg)

	metricsCtx, metricsCancel := context.WithCancel(ctx)
	defer metricsCancel()
	go metricsLoop(metricsCtx, logger, reg, cfg, aggregator, bc)

	errCh := make(chan error, 5)
	go func() {
		logger.Info("http + SSE listening", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http: %w", err)
		}
	}()
	go func() {
		logger.Info("gRPC listening", "addr", cfg.GRPCAddr)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			errCh <- fmt.Errorf("grpc: %w", err)
		}
	}()
	go func() {
		logger.Info("bronze-writer consuming", "group", cfg.ConsumerGroupBronze)
		if err := writer.Run(ctx); err != nil {
			errCh <- fmt.Errorf("bronze-writer: %w", err)
		}
	}()
	go func() {
		logger.Info("aggregator consuming", "group", cfg.ConsumerGroupRT)
		if err := aggregator.Run(ctx); err != nil {
			errCh <- fmt.Errorf("aggregator: %w", err)
		}
	}()
	go func() {
		logger.Info("score-writer consuming", "group", cfg.ConsumerGroupScoreWriter)
		if err := sw.Run(scoreCtx); err != nil {
			errCh <- fmt.Errorf("score-writer: %w", err)
		}
	}()
	go func() {
		logger.Info("score-writer scoring", "group", cfg.ConsumerGroupScoreWriter)
		sw.ScoreWorker(scoreCtx)
	}()

	select {
	case err := <-errCh:
		cancel()
		return err
	case <-ctx.Done():
		logger.Info("shutting down")
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	grpcSrv.GracefulStop()
	logger.Info("server stopped")
	return nil
}

// loadPlaybooks resolves + loads the governance policy file. The server runs
// from the repo root in dev, but the binary may also be started from api/;
// the second candidate covers that. Either way a missing or invalid playbook
// is a boot failure, never a 3am surprise.
func loadPlaybooks(path string) ([]playbook.Rule, error) {
	if _, err := os.Stat(path); err == nil {
		return playbook.LoadRules(path)
	}
	alt := filepath.Join("..", path)
	if _, err := os.Stat(alt); err == nil {
		return playbook.LoadRules(alt)
	}
	return nil, fmt.Errorf("playbook file not found (tried %q and %q)", path, alt)
}

// seedBroadcaster publishes the stored recent buckets as the broadcaster's
// initial snapshot so a fresh SSE/gRPC client gets data immediately.
func seedBroadcaster(ctx context.Context, bc *realtime.Broadcaster, feed *realtime.LiveFeed, st *store.PostgresStore) {
	buckets, err := st.RecentRealtimeMetrics(ctx, 60)
	if err != nil {
		slog.Default().Warn("seed snapshot failed", "error", err)
		return
	}
	if len(buckets) == 0 {
		return
	}
	last := buckets[len(buckets)-1]
	bc.Publish(model.MetricsUpdate{
		Snapshot:        buckets,
		Current:         last,
		SpeedMultiplier: feed.SpeedMultiplier(),
		Status:          feed.Status(),
		Source:          model.SourceLiveReplay,
	})
}

// retentionLoop periodically trims the streaming tables back to their budgets:
// raw events, minute buckets, and the anomaly log.
func retentionLoop(ctx context.Context, logger *slog.Logger, pool *pgxpool.Pool, cfg config.Config) {
	keep, err1 := time.ParseDuration(cfg.RetentionKeep)
	bucketKeep, err2 := time.ParseDuration(cfg.RetentionBucketKeep)
	anomalyKeep, err3 := time.ParseDuration(cfg.RetentionAnomalies)
	if err1 != nil || err2 != nil || err3 != nil {
		logger.Warn("retention durations invalid; using defaults", "err1", err1, "err2", err2, "err3", err3)
		keep, bucketKeep, anomalyKeep = 12*time.Hour, 3*time.Hour, 720*time.Hour
	}
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := realtime.Retention(ctx, pool, keep, bucketKeep, anomalyKeep); err != nil {
				logger.Warn("retention purge failed", "error", err)
			}
		}
	}
}

// reconcileLoop recomputes the visible realtime_metrics window from the
// idempotent bronze layer on a schedule. This is the self-healing counterpart
// to at-least-once delivery: if a consumer restart redelivered a batch and
// double-counted revenue/orders, the next pass clamps the served buckets back
// to bronze's deduplicated truth.
func reconcileLoop(ctx context.Context, logger *slog.Logger, pool *pgxpool.Pool, cfg config.Config) {
	every, err1 := time.ParseDuration(cfg.ReconcileEvery)
	window, err2 := time.ParseDuration(cfg.RetentionBucketKeep)
	if err1 != nil || err2 != nil || every <= 0 || window <= 0 {
		logger.Warn("reconcile settings invalid; using defaults", "every", err1, "window", err2)
		every, window = time.Minute, 3*time.Hour
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := realtime.Reconcile(ctx, pool, window); err != nil {
				logger.Warn("realtime reconcile failed", "error", err)
			}
		}
	}
}

// metricsLoop refreshes the pipeline's own operational gauges: consumer lag for
// both groups, anomaly-baseline progress, and fan-out health.
func metricsLoop(ctx context.Context, logger *slog.Logger, reg *telemetry.Registry,
	cfg config.Config, aggregator *realtime.Aggregator, bc *realtime.Broadcaster) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshConsumerLag(ctx, logger, reg, cfg)
			for _, st := range aggregator.Baseline() {
				reg.GaugeLabel("abi_detector_baseline_buckets", "Closed buckets folded into the per-metric anomaly baseline.",
					map[string]string{"metric": st.Metric}).Set(float64(st.N))
			}
			reg.Gauge("abi_sse_subscribers", "Live SSE/gRPC subscribers currently attached.").
				Set(float64(bc.Subscribers()))
			reg.Counter("abi_broadcast_updates_total", "MetricsUpdate pushes published.").
				Set(float64(bc.Pushes()))
			reg.Counter("abi_broadcast_dropped_updates_total", "Pushes dropped for slow subscribers.").
				Set(float64(bc.Drops()))
		}
	}
}

// refreshConsumerLag probes both consumer groups and publishes per-partition
// lag gauges (end offset − committed offset).
func refreshConsumerLag(ctx context.Context, logger *slog.Logger, reg *telemetry.Registry, cfg config.Config) {
	groups := []struct {
		name   string
		topics []string
	}{
		{cfg.ConsumerGroupBronze, stream.AllTopics},
		{cfg.ConsumerGroupRT, []string{stream.TopicOrders, stream.TopicClicks}},
		{cfg.ConsumerGroupScoreWriter, []string{stream.TopicOrders, stream.TopicClicks}},
	}
	for _, g := range groups {
		lags, err := kafka.Lag(ctx, cfg.KafkaSeedBrokers, g.name, g.topics)
		if err != nil {
			logger.Warn("consumer lag probe", "group", g.name, "error", err)
			continue
		}
		for _, l := range lags {
			reg.GaugeLabel("abi_consumer_lag", "Unread records for a group/topic/partition.",
				map[string]string{
					"group":     l.Group,
					"topic":     l.Topic,
					"partition": strconv.Itoa(int(l.Partition)),
				}).Set(float64(l.Lag))
		}
	}
}
