// Command mockprovider is a local stand-in for webhook.site with
// configurable latency and failure injection (see internal/mockprovider).
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"notification-service/internal/config"
	"notification-service/internal/health"
	"notification-service/internal/mockprovider"
	"notification-service/internal/observability"
	"notification-service/internal/platform"
)

func main() {
	cfg, err := config.LoadMockProvider(os.LookupEnv)
	if err != nil {
		slog.Error("mockprovider config", "error", err)
		os.Exit(1)
	}
	log := observability.NewLogger(os.Stdout, "mockprovider", cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()
	health.Register(mux, time.Second) // no dependencies to check
	mux.Handle("/send", mockprovider.New(cfg, log))

	srv := &http.Server{Addr: cfg.Addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Info("mock provider config",
		"failure_rate", cfg.FailureRate, "failure_codes", cfg.FailureCodes,
		"latency_min", cfg.LatencyMin.String(), "latency_max", cfg.LatencyMax.String())
	if err := platform.RunHTTPServer(ctx, srv, 5*time.Second, log); err != nil {
		log.Error("mockprovider exited with error", "error", err)
		os.Exit(1)
	}
}
