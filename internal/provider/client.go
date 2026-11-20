// Package provider is the HTTP client for the external delivery provider
// (webhook.site in the brief, cmd/mockprovider locally).
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Message is what we ask the provider to deliver.
type Message struct {
	NotificationID uuid.UUID
	CorrelationID  string
	To             string
	Channel        string
	Content        string
}

// requestBody is the provider contract: {"to", "channel", "content"}.
type requestBody struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// acceptedBody is the 202 response: {"messageId", "status", "timestamp"}.
type acceptedBody struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
}

// Result describes an accepted message.
type Result struct {
	StatusCode int
	MessageID  string // empty if the provider did not return one
}

// StatusError is returned when the provider answers with a non-2xx status.
// RetryAfter is parsed from the Retry-After header (0 when absent).
type StatusError struct {
	StatusCode int
	RetryAfter time.Duration
	Body       string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("provider returned HTTP %d: %s", e.StatusCode, e.Body)
}

// Client sends messages to one provider URL.
type Client struct {
	url     string
	http    *http.Client
	timeout time.Duration
	now     func() time.Time
}

// NewClient builds a client with a per-attempt timeout. The timeout is
// applied through the request context rather than http.Client.Timeout so a
// caller's own cancellation (shutdown) and the per-attempt limit compose.
func NewClient(url string, timeout time.Duration) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Many concurrent sends go to one host; the default of 2 idle
	// connections per host would force constant reconnects.
	transport.MaxIdleConnsPerHost = 100
	return &Client{
		url: url,
		// otelhttp adds a client span per call and the W3C traceparent
		// header, so a tracing-aware provider joins the same trace.
		http:    &http.Client{Transport: otelhttp.NewTransport(transport)},
		timeout: timeout,
		now:     time.Now,
	}
}

const maxResponseBytes = 64 << 10

// Send makes one delivery attempt. It returns:
//   - (Result, nil) when the provider accepted the message (any 2xx),
//   - (*StatusError) for a non-2xx answer,
//   - a transport error (timeout, connection refused, ...) otherwise.
//
// Classification into retryable or permanent is the caller's job
// (worker.Classify), so this package stays a thin, testable HTTP adapter.
func (c *Client) Send(ctx context.Context, msg Message) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	payload, err := json.Marshal(requestBody{To: msg.To, Channel: msg.Channel, Content: msg.Content})
	if err != nil {
		return Result{}, fmt.Errorf("encode provider request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(payload))
	if err != nil {
		return Result{}, fmt.Errorf("build provider request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Correlation-ID", msg.CorrelationID)
	// Our id lets a provider deduplicate retries of the same notification
	// (we deliver at-least-once; see docs/DECISIONS.md).
	req.Header.Set("X-Notification-ID", msg.NotificationID.String())

	resp, err := c.http.Do(req)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return Result{}, &StatusError{
			StatusCode: resp.StatusCode,
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), c.now()),
			Body:       truncate(strings.TrimSpace(string(body)), 200),
		}
	}

	// The brief's provider answers 202 with a messageId. We treat any 2xx as
	// accepted (webhook.site defaults to 200) and store the id when present.
	var accepted acceptedBody
	_ = json.Unmarshal(body, &accepted)
	return Result{StatusCode: resp.StatusCode, MessageID: accepted.MessageID}, nil
}

// parseRetryAfter supports both forms from RFC 9110: delay-seconds ("120")
// and an HTTP-date. Invalid or past values yield 0.
func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if secs, err := strconv.Atoi(value); err == nil {
		if secs < 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		if d := at.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// IsStatusError reports whether err is a provider HTTP status error and
// returns it.
func IsStatusError(err error) (*StatusError, bool) {
	var se *StatusError
	ok := errors.As(err, &se)
	return se, ok
}
