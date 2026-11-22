package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// LookupFunc matches os.LookupEnv. Injecting it keeps Load testable without
// mutating the real process environment.
type LookupFunc func(key string) (string, bool)

// envReader reads typed values with defaults and collects every parse error,
// so a misconfigured deployment reports all problems at once instead of one
// per restart.
type envReader struct {
	lookup LookupFunc
	errs   []error
}

func (r *envReader) string(key, def string) string {
	if v, ok := r.lookup(key); ok && v != "" {
		return v
	}
	return def
}

func (r *envReader) int(key string, def int) int {
	raw, ok := r.lookup(key)
	if !ok || raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %q is not an integer", key, raw))
		return def
	}
	return v
}

func (r *envReader) float(key string, def float64) float64 {
	raw, ok := r.lookup(key)
	if !ok || raw == "" {
		return def
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %q is not a number", key, raw))
		return def
	}
	return v
}

func (r *envReader) duration(key string, def time.Duration) time.Duration {
	raw, ok := r.lookup(key)
	if !ok || raw == "" {
		return def
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %q is not a duration (e.g. 5s, 2m)", key, raw))
		return def
	}
	return v
}

// weights reads three comma-separated non-negative integers ("70,20,10").
func (r *envReader) weights(key string, def [3]int) [3]int {
	raw, ok := r.lookup(key)
	if !ok || raw == "" {
		return def
	}
	parts := strings.Split(raw, ",")
	var out [3]int
	if len(parts) != len(out) {
		r.errs = append(r.errs, fmt.Errorf("%s: %q must be three comma-separated integers, e.g. 70,20,10", key, raw))
		return def
	}
	for i, p := range parts {
		v, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || v < 0 {
			r.errs = append(r.errs, fmt.Errorf("%s: %q must be three non-negative integers, e.g. 70,20,10", key, raw))
			return def
		}
		out[i] = v
	}
	return out
}

// require records an error when a cross-field or range rule is violated.
func (r *envReader) require(ok bool, format string, args ...any) {
	if !ok {
		r.errs = append(r.errs, fmt.Errorf(format, args...))
	}
}

func (r *envReader) err() error {
	return errors.Join(r.errs...)
}
