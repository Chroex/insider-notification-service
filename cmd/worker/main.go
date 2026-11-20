// Command worker claims due notifications from Postgres, rate-limits per
// channel through Redis, calls the provider and records the outcome.
// It also serves health endpoints on a small admin port.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"notification-service/internal/config"
	"notification-service/internal/domain"
	"notification-service/internal/health"
	"notification-service/internal/observability"
	"notification-service/internal/platform"
	"notification-service/internal/provider"
	"notification-service/internal/queue"
	"notification-service/internal/ratelimit"
	"notification-service/internal/store"
	"notification-service/internal/worker"
)

// Compile-time check: the Prometheus metrics satisfy the engine's observer.
var _ worker.Observer = (*observability.WorkerMetrics)(nil)

func main() {
	if err := run(); err != nil {
		slog.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.LookupEnv)
	if err != nil {
		return err
	}
	log := observability.NewLogger(os.Stdout, "worker", cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := platform.NewPostgresPool(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer db.Close()

	rdb, err := platform.NewRedisClient(ctx, cfg.RedisURL)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()

	registry := observability.NewRegistry()
	metrics := observability.NewWorkerMetrics(registry)

	admin := http.NewServeMux()
	admin.Handle("GET /metrics", observability.MetricsHandler(registry))
	health.Register(admin, 2*time.Second,
		health.Check{Name: "postgres", Ping: db.Ping},
		health.Check{Name: "redis", Ping: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
	)
	srv := &http.Server{
		Addr:              cfg.WorkerAdminAddr,
		Handler:           admin,
		ReadHeaderTimeout: 5 * time.Second,
	}

	limits := make(map[domain.Channel]ratelimit.Limit, len(domain.Channels))
	for _, ch := range domain.Channels {
		limits[ch] = ratelimit.Limit{PerSecond: cfg.RateLimit.PerSecond[string(ch)], Burst: cfg.RateLimit.Burst}
	}

	engine := worker.New(
		queue.New(db),
		provider.NewClient(cfg.ProviderURL, cfg.Worker.ProviderTimeout),
		ratelimit.New(rdb, limits, ""),
		worker.Config{
			Channels:        domain.Channels,
			Concurrency:     cfg.Worker.Concurrency,
			ClaimBatchSize:  cfg.Worker.ClaimBatchSize,
			PollInterval:    cfg.Worker.PollInterval,
			LeaseDuration:   cfg.Worker.LeaseDuration,
			ReaperInterval:  cfg.Worker.ReaperInterval,
			ShutdownTimeout: cfg.ShutdownTimeout,
			Policy: worker.RetryPolicy{Backoff: worker.Backoff{
				Base:   cfg.Worker.RetryBaseDelay,
				Factor: cfg.Worker.RetryFactor,
				Max:    cfg.Worker.RetryMaxDelay,
			}},
			BreakerThreshold: cfg.Worker.BreakerThreshold,
			BreakerCooldown:  cfg.Worker.BreakerCooldown,
			Observer:         metrics,
		},
		log,
	)

	go observability.RefreshQueueDepth(ctx, cfg.Worker.QueueDepthInterval, store.New(db).QueueDepth, metrics, log)

	engineDone := make(chan struct{})
	go func() {
		defer close(engineDone)
		engine.Run(ctx)
	}()

	// The admin server outlives the signal: it keeps answering health checks
	// while the engine drains, and stops only once the engine is done.
	serverCtx, stopServer := context.WithCancel(context.Background())
	go func() {
		<-engineDone
		stopServer()
	}()
	serverErr := platform.RunHTTPServer(serverCtx, srv, cfg.ShutdownTimeout, log)
	if serverErr != nil {
		stop() // e.g. port in use: shut the engine down too instead of running headless
	}
	<-engineDone
	if serverErr != nil {
		return fmt.Errorf("worker admin: %w", serverErr)
	}
	log.Info("worker stopped cleanly")
	return nil
}
