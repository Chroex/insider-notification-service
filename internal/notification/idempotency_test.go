package notification

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"notification-service/internal/domain"
)

func TestCheckReplay(t *testing.T) {
	stored := "abc"
	tests := []struct {
		name       string
		storedHash *string
		newHash    string
		wantErr    error
	}{
		{"same payload is a replay", &stored, "abc", nil},
		{"different payload is a conflict", &stored, "xyz", domain.ErrIdempotencyConflict},
		{"row without a hash never matches", nil, "abc", domain.ErrIdempotencyConflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkReplay(tt.storedHash, tt.newHash); !errors.Is(err, tt.wantErr) {
				t.Errorf("checkReplay = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestRequestHash_Normalization(t *testing.T) {
	base := CreateInput{Channel: "sms", Recipient: "+905551234567", Content: "hi"}
	hash := func(in CreateInput) string { return requestHash("notification", canonicalize(in)) }

	explicitNormal := base
	explicitNormal.Priority = "normal"
	if hash(base) != hash(explicitNormal) {
		t.Error("omitted priority and explicit \"normal\" must hash the same")
	}

	utc := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	istanbul := utc.In(time.FixedZone("TRT", 3*60*60))
	a, b := base, base
	a.ScheduledAt, b.ScheduledAt = &utc, &istanbul
	if hash(a) != hash(b) {
		t.Error("the same instant in different time zones must hash the same")
	}

	for name, changed := range map[string]CreateInput{
		"content":   {Channel: "sms", Recipient: "+905551234567", Content: "hello"},
		"recipient": {Channel: "sms", Recipient: "+905551234568", Content: "hi"},
		"priority":  {Channel: "sms", Recipient: "+905551234567", Content: "hi", Priority: "high"},
		"scheduled": {Channel: "sms", Recipient: "+905551234567", Content: "hi", ScheduledAt: &utc},
	} {
		if hash(changed) == hash(base) {
			t.Errorf("changing %s must change the hash", name)
		}
	}

	if requestHash("notification", canonicalize(base)) == requestHash("batch", canonicalize(base)) {
		t.Error("hash kind must be part of the hash")
	}
}

func TestNormalizeKey(t *testing.T) {
	if k, err := normalizeKey("  "); k != nil || err != nil {
		t.Errorf("blank key = %v, %v; want nil, nil", k, err)
	}
	if k, err := normalizeKey(" order-42 "); err != nil || k == nil || *k != "order-42" {
		t.Errorf("trimmed key = %v, %v", k, err)
	}
	var ve *domain.ValidationError
	if _, err := normalizeKey(strings.Repeat("k", MaxIdempotencyKeyLength+1)); !errors.As(err, &ve) {
		t.Errorf("long key err = %v, want ValidationError", err)
	}
}

// Batch item order on idempotent replay relies on UUIDv7 ids generated in
// one process being strictly increasing.
func TestNewUUIDv7_Monotonic(t *testing.T) {
	prev := newUUIDv7()
	for range 10_000 {
		next := newUUIDv7()
		if strings.Compare(next.String(), prev.String()) <= 0 {
			t.Fatalf("UUIDv7 not increasing: %s then %s", prev, next)
		}
		prev = next
	}
	if prev.Version() != uuid.Version(7) {
		t.Errorf("version = %d, want 7", prev.Version())
	}
}
