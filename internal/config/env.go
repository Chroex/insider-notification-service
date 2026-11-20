package config

import (
	"errors"
	"fmt"
	"strconv"
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

// require records an error when a cross-field or range rule is violated.
func (r *envReader) require(ok bool, format string, args ...any) {
	if !ok {
		r.errs = append(r.errs, fmt.Errorf(format, args...))
	}
}

func (r *envReader) err() error {
	return errors.Join(r.errs...)
}
