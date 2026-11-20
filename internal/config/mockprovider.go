package config

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

// MockProvider configures cmd/mockprovider, the local stand-in for
// webhook.site used for demos and load tests.
type MockProvider struct {
	LogLevel   slog.Level
	Addr       string
	LatencyMin time.Duration
	LatencyMax time.Duration
	// FailureRate is the probability (0..1) that a request fails with one of
	// FailureCodes chosen uniformly at random.
	FailureRate  float64
	FailureCodes []int
	// RetryAfter is sent as the Retry-After header on 429/503 responses
	// (0 disables the header).
	RetryAfter time.Duration
}

// LoadMockProvider reads MOCK_* variables.
func LoadMockProvider(lookup LookupFunc) (MockProvider, error) {
	r := &envReader{lookup: lookup}
	cfg := MockProvider{
		Addr:        r.string("MOCK_ADDR", ":8090"),
		LatencyMin:  r.duration("MOCK_LATENCY_MIN", 20*time.Millisecond),
		LatencyMax:  r.duration("MOCK_LATENCY_MAX", 120*time.Millisecond),
		FailureRate: r.float("MOCK_FAILURE_RATE", 0.1),
		RetryAfter:  r.duration("MOCK_RETRY_AFTER", 0),
	}
	codes, err := parseStatusCodes(r.string("MOCK_FAILURE_CODES", "500,503,429"))
	if err != nil {
		r.errs = append(r.errs, err)
	}
	cfg.FailureCodes = codes

	level, err := parseLogLevel(r.string("LOG_LEVEL", "info"))
	if err != nil {
		r.errs = append(r.errs, err)
	}
	cfg.LogLevel = level

	r.require(cfg.LatencyMin >= 0 && cfg.LatencyMax >= cfg.LatencyMin,
		"MOCK_LATENCY_MIN/MAX: need 0 <= min <= max, got %v..%v", cfg.LatencyMin, cfg.LatencyMax)
	r.require(cfg.FailureRate >= 0 && cfg.FailureRate <= 1,
		"MOCK_FAILURE_RATE must be within 0..1, got %v", cfg.FailureRate)

	if err := r.err(); err != nil {
		return MockProvider{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}

func parseStatusCodes(raw string) ([]int, error) {
	var codes []int
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		code, err := strconv.Atoi(part)
		if err != nil || code < 400 || code > 599 {
			return nil, fmt.Errorf("MOCK_FAILURE_CODES: %q is not a 4xx/5xx status code", part)
		}
		codes = append(codes, code)
	}
	if len(codes) == 0 {
		return nil, fmt.Errorf("MOCK_FAILURE_CODES must list at least one status code")
	}
	return codes, nil
}
