package httpapi

import (
	"math"
	"net/http"
	"time"

	"notification-service/internal/domain"
	"notification-service/internal/store"
)

const (
	defaultSummaryWindow = time.Hour
	maxSummaryWindow     = 7 * 24 * time.Hour
)

type latencyResponse struct {
	P50 *float64 `json:"p50"`
	P95 *float64 `json:"p95"`
}

type attemptsResponse struct {
	Total          int      `json:"total"`
	Success        int      `json:"success"`
	RetryableError int      `json:"retryable_error"`
	PermanentError int      `json:"permanent_error"`
	Interrupted    int      `json:"interrupted"`
	SuccessRate    *float64 `json:"success_rate"`
}

type statsResponse struct {
	Sent              int              `json:"sent"`
	Failed            int              `json:"failed"`
	SuccessRate       *float64         `json:"success_rate"` // sent / (sent + failed); null with no data
	Attempts          attemptsResponse `json:"attempts"`
	ProviderLatencyMS latencyResponse  `json:"provider_latency_ms"`
	EndToEndLatencyMS latencyResponse  `json:"end_to_end_latency_ms"`
}

type queueDepthResponse struct {
	Total     int                       `json:"total"`
	ByChannel map[string]map[string]int `json:"by_channel"`
}

type summaryResponse struct {
	Window      string                   `json:"window"`
	Since       time.Time                `json:"since"`
	GeneratedAt time.Time                `json:"generated_at"`
	QueueDepth  queueDepthResponse       `json:"queue_depth"`
	Overall     statsResponse            `json:"overall"`
	ByChannel   map[string]statsResponse `json:"by_channel"`
}

// GET /api/v1/metrics/summary?window=1h
//
// A human-friendly view (no Prometheus needed): current queue depth plus
// delivery outcomes and p50/p95 latencies over the window, overall and per
// channel. Computed from Postgres, so it covers every worker replica.
func (h *handlers) metricsSummary(w http.ResponseWriter, r *http.Request) {
	window := defaultSummaryWindow
	if raw := r.URL.Query().Get("window"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < time.Minute || d > maxSummaryWindow {
			writeError(w, r, http.StatusBadRequest, codeInvalidParameter, "invalid query parameters",
				[]domain.FieldError{{Field: "window", Message: "must be a duration between 1m and 168h, e.g. 15m or 1h"}})
			return
		}
		window = d
	}

	sum, err := h.svc.Summary(r.Context(), window)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}

	resp := summaryResponse{
		Window:      window.String(),
		Since:       sum.Since.UTC(),
		GeneratedAt: time.Now().UTC(),
		QueueDepth:  queueDepthResponse{ByChannel: map[string]map[string]int{}},
		Overall:     toStatsResponse(sum.Overall),
		ByChannel:   map[string]statsResponse{},
	}
	for _, ch := range domain.Channels {
		byStatus := map[string]int{}
		for _, st := range domain.ActiveStatuses {
			n := sum.QueueDepth[ch][st]
			byStatus[string(st)] = n
			resp.QueueDepth.Total += n
		}
		resp.QueueDepth.ByChannel[string(ch)] = byStatus
		resp.ByChannel[string(ch)] = toStatsResponse(*sum.ByChannel[ch])
	}
	writeJSON(w, http.StatusOK, resp)
}

func toStatsResponse(s store.ChannelStats) statsResponse {
	a := attemptsResponse{
		Success:        s.AttemptsByOutcome[domain.OutcomeSuccess],
		RetryableError: s.AttemptsByOutcome[domain.OutcomeRetryableError],
		PermanentError: s.AttemptsByOutcome[domain.OutcomePermanentError],
		Interrupted:    s.AttemptsByOutcome[domain.OutcomeInterrupted],
	}
	a.Total = a.Success + a.RetryableError + a.PermanentError + a.Interrupted
	a.SuccessRate = ratio(a.Success, a.Total)
	return statsResponse{
		Sent:              s.Sent,
		Failed:            s.Failed,
		SuccessRate:       ratio(s.Sent, s.Sent+s.Failed),
		Attempts:          a,
		ProviderLatencyMS: latencyResponse{P50: round1(s.ProviderP50), P95: round1(s.ProviderP95)},
		EndToEndLatencyMS: latencyResponse{P50: round1(s.EndToEndP50), P95: round1(s.EndToEndP95)},
	}
}

// ratio returns part/whole rounded to 4 decimals, or nil when whole is 0
// (a rate with no data is unknown, not 0% or 100%).
func ratio(part, whole int) *float64 {
	if whole == 0 {
		return nil
	}
	r := math.Round(float64(part)/float64(whole)*10000) / 10000
	return &r
}

func round1(v *float64) *float64 {
	if v == nil {
		return nil
	}
	r := math.Round(*v*10) / 10
	return &r
}
