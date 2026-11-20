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
	if cfg.Validation.SMSMaxChars != 160 || cfg.BatchMaxSize != 1000 || cfg.MaxAttempts != 5 {
		t.Errorf("unexpected limit defaults: validation=%+v batch=%d attempts=%d",
			cfg.Validation, cfg.BatchMaxSize, cfg.MaxAttempts)
	}
}

func TestLoad_Overrides(t *testing.T) {
	cfg, err := Load(lookupFrom(map[string]string{
		"API_ADDR":         ":9999",
		"SHUTDOWN_TIMEOUT": "30s",
		"LOG_LEVEL":        "debug",
		"PROVIDER_URL":     "https://webhook.site/abc",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.APIAddr != ":9999" || cfg.ShutdownTimeout != 30*time.Second ||
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

func TestLoad_WorkerInvariants(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{"lease shorter than two provider timeouts", map[string]string{"LEASE_DURATION": "8s", "PROVIDER_TIMEOUT": "5s"}, "LEASE_DURATION"},
		{"shutdown not longer than provider timeout", map[string]string{"SHUTDOWN_TIMEOUT": "5s", "PROVIDER_TIMEOUT": "5s"}, "SHUTDOWN_TIMEOUT"},
		{"zero concurrency", map[string]string{"WORKER_CONCURRENCY": "0"}, "WORKER_CONCURRENCY"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(lookupFrom(tt.env))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("err = %v, want mention of %s", err, tt.wantErr)
			}
		})
	}
	cfg, err := Load(lookupFrom(nil))
	if err != nil || cfg.Worker.LeaseDuration != 30*time.Second || cfg.Worker.Concurrency != 10 {
		t.Errorf("worker defaults: %+v, %v", cfg.Worker, err)
	}
}

func TestLoad_RejectsNonPositiveShutdownTimeout(t *testing.T) {
	_, err := Load(lookupFrom(map[string]string{"SHUTDOWN_TIMEOUT": "0s"}))
	if err == nil {
		t.Fatal("expected an error for zero shutdown timeout")
	}
}

func TestLoadMockProvider(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{name: "defaults", env: nil},
		{name: "bad failure rate", env: map[string]string{"MOCK_FAILURE_RATE": "1.5"}, wantErr: "MOCK_FAILURE_RATE"},
		{name: "bad status code", env: map[string]string{"MOCK_FAILURE_CODES": "500,200"}, wantErr: "MOCK_FAILURE_CODES"},
		{name: "min above max", env: map[string]string{"MOCK_LATENCY_MIN": "2s", "MOCK_LATENCY_MAX": "1s"}, wantErr: "MOCK_LATENCY"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := LoadMockProvider(lookupFrom(tt.env))
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if len(cfg.FailureCodes) != 3 || cfg.Addr != ":8090" {
					t.Errorf("unexpected defaults: %+v", cfg)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %v, want mention of %s", err, tt.wantErr)
			}
		})
	}
}
