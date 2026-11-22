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
	"math"
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

// StatusError is returned when the provider answers with anything other than
// the contract's success status. RetryAfter is parsed from the Retry-After
// header (0 when absent). Expected is set when the answer was a different
// 2xx, so the error says what the contract required.
type StatusError struct {
	StatusCode int
	Expected   int
	RetryAfter time.Duration
	Body       string
}

func (e *StatusError) Error() string {
	if e.Expected != 0 {
		return fmt.Sprintf("provider returned HTTP %d, expected %d (the message may have been delivered): %s",
			e.StatusCode, e.Expected, e.Body)
	}
	return fmt.Sprintf("provider returned HTTP %d: %s", e.StatusCode, e.Body)
}

// Client sends messages to one provider URL.
type Client struct {
	url     string
	success int
	http    *http.Client
	timeout time.Duration
	now     func() time.Time
}

// NewClient builds a client with a per-attempt timeout. The timeout is
// applied through the request context rather than http.Client.Timeout so a
// caller's own cancellation (shutdown) and the per-attempt limit compose.
// success is the only status that means "accepted": 202 in the brief's
// provider contract (PROVIDER_SUCCESS_STATUS).
func NewClient(url string, timeout time.Duration, success int) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Many concurrent sends go to one host; the default of 2 idle
	// connections per host would force constant reconnects.
	transport.MaxIdleConnsPerHost = 100
	return &Client{
		url:     url,
		success: success,
		// otelhttp adds a client span per call and the W3C traceparent
		// header, so a tracing-aware provider joins the same trace.
		http: &http.Client{
			Transport: otelhttp.NewTransport(transport),
			// Never follow redirects. Go turns a 301/302/303 POST into a
			// body-less GET, so a 200 from the target would count as
			// "sent" although the provider never got the message; a
			// 307/308 would re-send recipient and content to another
			// host. The 3xx is returned as-is and classified permanent.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		timeout: timeout,
		now:     time.Now,
	}
}

const maxResponseBytes = 64 << 10

// Send makes one delivery attempt. It returns:
//   - (Result, nil) when the provider answered with the success status,
//   - (*StatusError) for any other answer, including a different 2xx,
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

	if resp.StatusCode != c.success {
		se := &StatusError{
			StatusCode: resp.StatusCode,
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), c.now()),
			Body:       truncate(strings.TrimSpace(string(body)), 200),
		}
		// A different 2xx is not the contract. It is not "sent" (a proxy or
		// login page can answer 200), and the caller classifies it as
		// permanent: the provider may have taken it, so a retry could
		// deliver it twice. Failing loudly beats a silent false "sent".
		if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
			se.Expected = c.success
		}
		return Result{}, se
	}

	// The contract's body carries a messageId; store it when present.
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
	if strings.Trim(value, "0123456789") == "" { // delay-seconds
		secs, err := strconv.ParseInt(value, 10, 64)
		if err != nil || secs > math.MaxInt64/int64(time.Second) {
			// Too large for a Duration: saturate rather than overflow, so
			// "wait a very long time" stays long (the worker caps it).
			return time.Duration(math.MaxInt64)
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
