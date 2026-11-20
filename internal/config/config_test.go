package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func lookupFrom(env map[string]string) LookupFunc {
	return func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}
}

func TestLoad_Defaults(t *testing.T) {
	cfg, err := Load(lookupFrom(nil))
	if err != nil {
		t.Fatalf("Load with empty env: %v", err)
	}
	if cfg.APIAddr != ":8080" {
		t.Errorf("APIAddr = %q, want :8080", cfg.APIAddr)
	}
	if cfg.ShutdownTimeout != 15*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 15s", cfg.ShutdownTimeout)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v, want info", cfg.LogLevel)
	}
}

func TestLoad_Overrides(t *testing.T) {
	cfg, err := Load(lookupFrom(map[string]string{
		"API_ADDR":         ":9999",
		"SHUTDOWN_TIMEOUT": "3s",
		"LOG_LEVEL":        "debug",
		"PROVIDER_URL":     "https://webhook.site/abc",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.APIAddr != ":9999" || cfg.ShutdownTimeout != 3*time.Second ||
		cfg.LogLevel != slog.LevelDebug || cfg.ProviderURL != "https://webhook.site/abc" {
		t.Errorf("overrides not applied: %+v", cfg)
	}
}

func TestLoad_ReportsAllErrorsAtOnce(t *testing.T) {
	_, err := Load(lookupFrom(map[string]string{
		"SHUTDOWN_TIMEOUT": "soon",
		"LOG_LEVEL":        "loud",
		"DB_MAX_CONNS":     "many",
	}))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"SHUTDOWN_TIMEOUT", "LOG_LEVEL", "DB_MAX_CONNS"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestLoad_RejectsNonPositiveShutdownTimeout(t *testing.T) {
	_, err := Load(lookupFrom(map[string]string{"SHUTDOWN_TIMEOUT": "0s"}))
	if err == nil {
		t.Fatal("expected an error for zero shutdown timeout")
	}
}

