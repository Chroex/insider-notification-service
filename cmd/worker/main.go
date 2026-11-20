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
	"notification-service/internal/worker"
)

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

	admin := http.NewServeMux()
	health.Register(admin, 2*time.Second,
		health.Check{Name: "postgres", Ping: db.Ping},
		health.Check{Name: "redis", Ping: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
	)
	srv := &http.Server{
		Addr:              cfg.WorkerAdminAddr,
		Handler:           admin,
		ReadHeaderTimeout: 5 * time.Second,
	}

	engine := worker.New(
		queue.New(db),
		provider.NewClient(cfg.ProviderURL, cfg.Worker.ProviderTimeout),
		worker.Config{
			Channels:        domain.Channels,
			Concurrency:     cfg.Worker.Concurrency,
			ClaimBatchSize:  cfg.Worker.ClaimBatchSize,
			PollInterval:    cfg.Worker.PollInterval,
			LeaseDuration:   cfg.Worker.LeaseDuration,
			ReaperInterval:  cfg.Worker.ReaperInterval,
			ShutdownTimeout: cfg.ShutdownTimeout,
			Policy:          worker.RetryPolicy{BaseDelay: cfg.Worker.RetryBaseDelay},
		},
		log,
	)

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
