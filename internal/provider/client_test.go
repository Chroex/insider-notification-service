package provider

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func testMessage() Message {
	return Message{
		NotificationID: uuid.MustParse("01900000-0000-7000-8000-000000000001"),
		CorrelationID:  "corr-1",
		To:             "+905551234567",
		Channel:        "sms",
		Content:        "hello",
	}
}

func TestSend_AcceptedSendsContractAndHeaders(t *testing.T) {
	var gotBody requestBody
	var gotHeaders http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"msg-123","status":"accepted","timestamp":"2026-01-01T00:00:00Z"}`))
	}))
	defer srv.Close()

	res, err := NewClient(srv.URL, time.Second, http.StatusAccepted).Send(context.Background(), testMessage())
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if res.StatusCode != http.StatusAccepted || res.MessageID != "msg-123" {
		t.Errorf("result = %+v", res)
	}
	if gotBody != (requestBody{To: "+905551234567", Channel: "sms", Content: "hello"}) {
		t.Errorf("body = %+v", gotBody)
	}
	if gotHeaders.Get("X-Correlation-ID") != "corr-1" ||
		gotHeaders.Get("X-Notification-ID") != "01900000-0000-7000-8000-000000000001" {
		t.Errorf("headers = %v", gotHeaders)
	}
}

// The brief's provider contract is "202 Accepted". Any other 2xx is not that
// contract: a misconfigured URL (a proxy or login page answering 200) must not
// be recorded as "sent", which would silently lose the message.
func TestSend_OtherSuccessCodesAreNotAccepted(t *testing.T) {
	for _, code := range []int{http.StatusOK, http.StatusCreated, http.StatusNoContent} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(code)
		}))
		_, err := NewClient(srv.URL, time.Second, http.StatusAccepted).Send(context.Background(), testMessage())
		se, ok := IsStatusError(err)
		if !ok || se.StatusCode != code {
			t.Errorf("HTTP %d: err = %v, want a StatusError with that code", code, err)
		} else if !strings.Contains(se.Error(), "expected 202") {
			t.Errorf("HTTP %d: error %q does not say what was expected", code, se.Error())
		}
		srv.Close()
	}
}

// PROVIDER_SUCCESS_STATUS lets a real provider with another contract be used.
func TestSend_ConfiguredSuccessStatusIsAccepted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	res, err := NewClient(srv.URL, time.Second, http.StatusOK).Send(context.Background(), testMessage())
	if err != nil || res.StatusCode != http.StatusOK || res.MessageID != "" {
		t.Errorf("Send = %+v, %v; want accepted with empty message id", res, err)
	}
}

// A redirect is never followed. Go's client would turn a 301/302/303 POST
// into a body-less GET, so a 200 from the target would mark a notification
// "sent" that the provider never received; a 307/308 would re-send the
// recipient and content to whatever host the redirect names.
func TestSend_RedirectIsNotFollowed(t *testing.T) {
	for _, code := range []int{http.StatusFound, http.StatusTemporaryRedirect} {
		targetHit := false
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			targetHit = true
			w.WriteHeader(http.StatusOK)
		}))
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target.URL, code)
		}))

		_, err := NewClient(srv.URL, time.Second, http.StatusAccepted).Send(context.Background(), testMessage())
		se, ok := IsStatusError(err)
		if !ok || se.StatusCode != code {
			t.Errorf("redirect %d: err = %v, want a StatusError with that code", code, err)
		}
		if targetHit {
			t.Errorf("redirect %d was followed", code)
		}
		srv.Close()
		target.Close()
	}
}

func TestSend_StatusErrorWithRetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"slow down"}`))
	}))
	defer srv.Close()

	_, err := NewClient(srv.URL, time.Second, http.StatusAccepted).Send(context.Background(), testMessage())
	se, ok := IsStatusError(err)
	if !ok {
		t.Fatalf("err = %v, want StatusError", err)
	}
	if se.StatusCode != 429 || se.RetryAfter != 7*time.Second || se.Body != `{"error":"slow down"}` {
		t.Errorf("status error = %+v", se)
	}
}

func TestSend_TimeoutIsATransportError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer srv.Close()

	start := time.Now()
	_, err := NewClient(srv.URL, 50*time.Millisecond, http.StatusAccepted).Send(context.Background(), testMessage())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want deadline exceeded", err)
	}
	if time.Since(start) > time.Second {
		t.Error("per-attempt timeout was not applied")
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		in   string
		want time.Duration
	}{
		{"", 0},
		{"30", 30 * time.Second},
		{"-5", 0},
		{"soon", 0},
		{now.Add(90 * time.Second).Format(http.TimeFormat), 90 * time.Second},
		{now.Add(-time.Minute).Format(http.TimeFormat), 0},
		// Too large for a Duration: saturate (the worker caps it at 1h)
		// rather than overflow to a negative value and retry too early.
		{"99999999999", time.Duration(math.MaxInt64)},
		{"9999999999999999999999999", time.Duration(math.MaxInt64)},
	}
	for _, tt := range tests {
		if got := parseRetryAfter(tt.in, now); got != tt.want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}
