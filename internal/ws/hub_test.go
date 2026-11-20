package ws

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"notification-service/internal/events"
)

func event(t *testing.T, n uuid.UUID, b *uuid.UUID) []byte {
	t.Helper()
	raw, err := json.Marshal(events.Event{NotificationID: n, BatchID: b, Channel: "sms", Status: "sent"})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestFilterMatches(t *testing.T) {
	n1, n2, b1 := uuid.New(), uuid.New(), uuid.New()
	ev := events.Event{NotificationID: n1, BatchID: &b1}
	noBatch := events.Event{NotificationID: n1}
	tests := []struct {
		name string
		f    Filter
		ev   events.Event
		want bool
	}{
		{"no filter", Filter{}, ev, true},
		{"notification match", Filter{NotificationID: &n1}, ev, true},
		{"notification mismatch", Filter{NotificationID: &n2}, ev, false},
		{"batch match", Filter{BatchID: &b1}, ev, true},
		{"batch filter, event without batch", Filter{BatchID: &b1}, noBatch, false},
	}
	for _, tt := range tests {
		if got := tt.f.matches(tt.ev); got != tt.want {
			t.Errorf("%s: matches = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestBroadcast_RoutesByFilterAndDropsSlowClients(t *testing.T) {
	h := NewHub(slog.New(slog.NewTextHandler(io.Discard, nil)))
	n1, n2 := uuid.New(), uuid.New()
	only1 := h.register(Filter{NotificationID: &n1})
	all := h.register(Filter{})

	h.Broadcast(event(t, n1, nil))
	h.Broadcast(event(t, n2, nil))
	if len(only1.send) != 1 || len(all.send) != 2 {
		t.Fatalf("queued: filtered=%d all=%d, want 1 and 2", len(only1.send), len(all.send))
	}

	// Overflow the unfiltered client's buffer: it must be marked slow, and
	// Broadcast must not block.
	for range clientBuffer {
		h.Broadcast(event(t, n2, nil))
	}
	select {
	case <-all.slow:
	default:
		t.Error("client with a full buffer was not marked slow")
	}
	select {
	case <-only1.slow:
		t.Error("an unaffected client was marked slow")
	default:
	}

	h.Broadcast([]byte("not json")) // ignored, no panic
	h.unregister(only1)
	h.unregister(all)
	if h.Clients() != 0 {
		t.Errorf("clients = %d after unregister", h.Clients())
	}
}

func TestParseFilter(t *testing.T) {
	id := uuid.New()
	tests := []struct {
		query   string
		wantOK  bool
		wantBad string
		check   func(Filter) bool
	}{
		{"", true, "", func(f Filter) bool { return f.NotificationID == nil && f.BatchID == nil }},
		{"notification_id=" + id.String(), true, "", func(f Filter) bool { return *f.NotificationID == id }},
		{"batch_id=" + id.String(), true, "", func(f Filter) bool { return *f.BatchID == id }},
		{"batch_id=nope", false, "batch_id", nil},
		{"notification_id=123", false, "notification_id", nil},
	}
	for _, tt := range tests {
		r := httptest.NewRequest("GET", "/ws?"+tt.query, nil)
		f, bad, ok := ParseFilter(r)
		if ok != tt.wantOK || bad != tt.wantBad || (ok && !tt.check(f)) {
			t.Errorf("ParseFilter(%q) = %+v, %q, %v", tt.query, f, bad, ok)
		}
	}
}
