// Command api serves the HTTP API. It validates and persists notifications
// (an insert is the enqueue) and never calls the provider itself.
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
	"notification-service/internal/httpapi"
	"notification-service/internal/notification"
	"notification-service/internal/observability"
	"notification-service/internal/platform"
	"notification-service/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("api exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.LookupEnv)
	if err != nil {
		return err
	}
	log := observability.NewLogger(os.Stdout, "api", cfg.LogLevel)

	// Cancelled on SIGINT/SIGTERM; everything below shuts down from this ctx.
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

	svc := notification.NewService(store.New(db), notification.Config{
		Rules:        cfg.Validation,
		BatchMaxSize: cfg.BatchMaxSize,
		MaxAttempts:  cfg.MaxAttempts,
	})
	router := httpapi.NewRouter(httpapi.Options{
		Service:  svc,
		Logger:   log,
		Registry: observability.NewRegistry(),
		Checks: []health.Check{
			{Name: "postgres", Ping: db.Ping},
			{Name: "redis", Ping: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
		},
	})

	srv := &http.Server{
		Addr:              cfg.APIAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second, // guards against slowloris clients
	}
	if err := platform.RunHTTPServer(ctx, srv, cfg.ShutdownTimeout, log); err != nil {
		return fmt.Errorf("api: %w", err)
	}
	log.Info("api stopped cleanly")
	return nil
}
