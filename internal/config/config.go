// Package config loads service configuration from environment variables only
// (12-factor). Every setting has a sane default so `docker compose up` and
// local runs work without an .env file.
package config

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"notification-service/internal/validation"
)

// Config is shared by all binaries; each binary reads only the fields it needs.
type Config struct {
	LogLevel slog.Level

	DatabaseURL string
	// DBMaxConns caps the Postgres pool per process. Keep
	// replicas * DBMaxConns below the server's max_connections.
	DBMaxConns int
	RedisURL   string

	APIAddr         string // HTTP API listen address
	WorkerAdminAddr string // worker health/metrics listen address

	// ShutdownTimeout bounds how long a binary waits for in-flight work
	// (HTTP requests, provider calls) after SIGTERM.
	ShutdownTimeout time.Duration

	ProviderURL string

	// Validation holds per-channel content and recipient limits.
	Validation validation.Rules
	// BatchMaxSize caps items per POST /notifications/batch.
	BatchMaxSize int
	// MaxAttempts is the default delivery attempt budget per notification.
	MaxAttempts int
}

// Load reads configuration using lookup (normally os.LookupEnv).
func Load(lookup LookupFunc) (Config, error) {
	r := &envReader{lookup: lookup}

	cfg := Config{
		DatabaseURL:     r.string("DATABASE_URL", "postgres://notifications:notifications@localhost:5432/notifications?sslmode=disable"),
		DBMaxConns:      r.int("DB_MAX_CONNS", 10),
		RedisURL:        r.string("REDIS_URL", "redis://localhost:6379/0"),
		APIAddr:         r.string("API_ADDR", ":8080"),
		WorkerAdminAddr: r.string("WORKER_ADMIN_ADDR", ":8082"),
		ShutdownTimeout: r.duration("SHUTDOWN_TIMEOUT", 15*time.Second),
		ProviderURL:     r.string("PROVIDER_URL", "http://localhost:8090/send"),
		Validation: validation.Rules{
			SMSMaxChars:       r.int("SMS_MAX_CHARS", validation.DefaultRules.SMSMaxChars),
			EmailMaxChars:     r.int("EMAIL_MAX_CHARS", validation.DefaultRules.EmailMaxChars),
			PushMaxChars:      r.int("PUSH_MAX_CHARS", validation.DefaultRules.PushMaxChars),
			PushTokenMaxChars: r.int("PUSH_TOKEN_MAX_CHARS", validation.DefaultRules.PushTokenMaxChars),
		},
		BatchMaxSize: r.int("BATCH_MAX_SIZE", 1000),
		MaxAttempts:  r.int("MAX_ATTEMPTS", 5),
	}

	level, err := parseLogLevel(r.string("LOG_LEVEL", "info"))
	if err != nil {
		r.errs = append(r.errs, err)
	}
	cfg.LogLevel = level

	r.require(cfg.DBMaxConns > 0, "DB_MAX_CONNS must be positive")
	r.require(cfg.ShutdownTimeout > 0, "SHUTDOWN_TIMEOUT must be positive")
	r.require(cfg.Validation.SMSMaxChars > 0 && cfg.Validation.EmailMaxChars > 0 &&
		cfg.Validation.PushMaxChars > 0 && cfg.Validation.PushTokenMaxChars > 0,
		"*_MAX_CHARS limits must be positive")
	r.require(cfg.BatchMaxSize > 0, "BATCH_MAX_SIZE must be positive")
	r.require(cfg.MaxAttempts > 0, "MAX_ATTEMPTS must be positive")

	if err := r.err(); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}

func parseLogLevel(raw string) (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToUpper(raw))); err != nil {
		return slog.LevelInfo, fmt.Errorf("LOG_LEVEL: %q must be one of debug, info, warn, error", raw)
	}
	return level, nil
}
