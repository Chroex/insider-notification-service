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

	Worker    Worker
	RateLimit RateLimit
}

// RateLimit configures the per-channel token buckets shared by all worker
// replicas through Redis.
type RateLimit struct {
	// PerSecond is the sustained rate per channel (tokens refilled per second).
	PerSecond map[string]float64
	// Burst is the bucket capacity: how many sends may go out back-to-back
	// after an idle period. Kept well below PerSecond so a provider that
	// counts per-second windows never sees more than ~PerSecond+Burst.
	Burst int
}

// Worker tunes the delivery engine.
type Worker struct {
	// Concurrency is the number of parallel provider calls per channel.
	Concurrency int
	// ClaimBatchSize caps rows claimed per query (bounded by free slots).
	ClaimBatchSize int
	// PollInterval is how long an idle channel waits before claiming again.
	PollInterval time.Duration
	// LeaseDuration is how long a claimed row stays owned by a worker. If the
	// worker dies, the reaper re-queues the row after this. It must exceed
	// the longest legitimate hold time (a provider call plus DB writes).
	LeaseDuration time.Duration
	// ReaperInterval is how often expired leases are returned to the queue.
	ReaperInterval time.Duration
	// ProviderTimeout bounds one provider HTTP call.
	ProviderTimeout time.Duration
	// RetryBaseDelay is the delay before retrying a retryable failure.
	RetryBaseDelay time.Duration
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
		Worker: Worker{
			Concurrency:     r.int("WORKER_CONCURRENCY", 10),
			ClaimBatchSize:  r.int("CLAIM_BATCH_SIZE", 10),
			PollInterval:    r.duration("POLL_INTERVAL", 500*time.Millisecond),
			LeaseDuration:   r.duration("LEASE_DURATION", 30*time.Second),
			ReaperInterval:  r.duration("REAPER_INTERVAL", 10*time.Second),
			ProviderTimeout: r.duration("PROVIDER_TIMEOUT", 5*time.Second),
			RetryBaseDelay:  r.duration("RETRY_BASE_DELAY", 2*time.Second),
		},
		RateLimit: RateLimit{
			PerSecond: map[string]float64{
				"sms":   r.float("RATE_LIMIT_SMS_PER_SECOND", 100),
				"email": r.float("RATE_LIMIT_EMAIL_PER_SECOND", 100),
				"push":  r.float("RATE_LIMIT_PUSH_PER_SECOND", 100),
			},
			Burst: r.int("RATE_LIMIT_BURST", 10),
		},
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

	w := cfg.Worker
	r.require(w.Concurrency > 0 && w.ClaimBatchSize > 0, "WORKER_CONCURRENCY and CLAIM_BATCH_SIZE must be positive")
	r.require(w.PollInterval > 0 && w.ReaperInterval > 0 && w.ProviderTimeout > 0 && w.RetryBaseDelay > 0,
		"POLL_INTERVAL, REAPER_INTERVAL, PROVIDER_TIMEOUT and RETRY_BASE_DELAY must be positive")
	// A lease shorter than a provider call would let the reaper hand a row
	// to a second worker while the first is still sending it.
	r.require(w.LeaseDuration >= 2*w.ProviderTimeout,
		"LEASE_DURATION (%v) must be at least twice PROVIDER_TIMEOUT (%v)", w.LeaseDuration, w.ProviderTimeout)
	for ch, rate := range cfg.RateLimit.PerSecond {
		r.require(rate > 0, "RATE_LIMIT_%s_PER_SECOND must be positive", strings.ToUpper(ch))
	}
	r.require(cfg.RateLimit.Burst >= 1, "RATE_LIMIT_BURST must be at least 1")
	r.require(cfg.ShutdownTimeout > w.ProviderTimeout,
		"SHUTDOWN_TIMEOUT (%v) must exceed PROVIDER_TIMEOUT (%v) so in-flight sends can finish", cfg.ShutdownTimeout, w.ProviderTimeout)

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
