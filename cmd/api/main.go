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
	"notification-service/internal/events"
	"notification-service/internal/health"
	"notification-service/internal/httpapi"
	"notification-service/internal/notification"
	"notification-service/internal/observability"
	"notification-service/internal/platform"
	"notification-service/internal/store"
	"notification-service/internal/ws"
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

	// Before any connection is created, so pgx/Redis instrumentation and
	// the HTTP middleware pick up the real tracer provider.
	shutdownTracing, err := observability.SetupTracing(ctx, "api")
	if err != nil {
		return err
	}
	defer flushTraces(shutdownTracing, log)

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
	// One Redis subscription per API process feeds every WebSocket client.
	hub := ws.NewHub(log)
	go hub.Run(ctx, rdb)

	router := httpapi.NewRouter(httpapi.Options{
		Service:            svc,
		Logger:             log,
		Registry:           observability.NewRegistry(),
		CORSAllowedOrigins: cfg.CORSAllowedOrigins,
		Events:             events.NewRedisPublisher(rdb, log),
		Hub:                hub,
		Checks: []health.Check{
			{Name: "postgres", Ping: db.Ping},
			// Accepting notifications needs Postgres only; without Redis
			// just the live WebSocket stream stops. So a Redis outage
			// degrades the API but keeps it in rotation.
			{Name: "redis", Optional: true, Ping: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
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

// flushTraces exports buffered spans before exit; bounded so a dead
// collector can't hold up shutdown.
func flushTraces(shutdown func(context.Context) error, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		log.Warn("flushing traces failed", "error", err)
	}
}
