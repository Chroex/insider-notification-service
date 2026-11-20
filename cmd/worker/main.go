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
	"notification-service/internal/health"
	"notification-service/internal/observability"
	"notification-service/internal/platform"
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

	// The dispatcher (claim -> rate limit -> send -> record) is added in a
	// later phase; for now the worker only exposes its admin endpoints.
	if err := platform.RunHTTPServer(ctx, srv, cfg.ShutdownTimeout, log); err != nil {
		return fmt.Errorf("worker admin: %w", err)
	}
	log.Info("worker stopped cleanly")
	return nil
}
