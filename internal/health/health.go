// Package health serves liveness and readiness endpoints for both binaries.
//
//   - Liveness (/health/live) answers "is the process running?". It never
//     checks dependencies: restarting a pod because Postgres is down would
//     not fix Postgres and would only add churn.
//   - Readiness (/health/ready) answers "can this instance do useful work?".
//     It pings the instance's dependencies so a load balancer stops routing
//     to it when a required one is down. An optional dependency that is
//     down is reported ("degraded") but keeps the instance in rotation.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Check is one dependency probe, e.g. a Postgres or Redis ping.
type Check struct {
	Name string
	Ping func(ctx context.Context) error
	// Optional marks a dependency the instance can work without, with a
	// feature missing: its failure makes readiness "degraded", not 503.
	Optional bool
}

type response struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

// Route patterns registered by Register.
const (
	LivePattern  = "GET /health/live"
	ReadyPattern = "GET /health/ready"
)

// Register mounts /health/live and /health/ready on mux.
func Register(mux *http.ServeMux, timeout time.Duration, checks ...Check) {
	mux.HandleFunc(LivePattern, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, response{Status: "ok"})
	})
	mux.HandleFunc(ReadyPattern, func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		results, healthy, degraded := runChecks(ctx, checks)
		switch {
		case !healthy:
			writeJSON(w, http.StatusServiceUnavailable, response{Status: "unavailable", Checks: results})
		case degraded:
			writeJSON(w, http.StatusOK, response{Status: "degraded", Checks: results})
		default:
			writeJSON(w, http.StatusOK, response{Status: "ok", Checks: results})
		}
	})
}

// runChecks probes all dependencies concurrently so readiness latency is
// the slowest check, not the sum of all checks.
func runChecks(ctx context.Context, checks []Check) (results map[string]string, healthy, degraded bool) {
	results = make(map[string]string, len(checks))
	healthy = true
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, c := range checks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.Ping(ctx)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				results[c.Name] = "error: " + err.Error()
				if c.Optional {
					degraded = true
				} else {
					healthy = false
				}
				return
			}
			results[c.Name] = "ok"
		}()
	}
	wg.Wait()
	return results, healthy, degraded
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
