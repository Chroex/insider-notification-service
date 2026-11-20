package notification

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"notification-service/internal/domain"
)

// MaxIdempotencyKeyLength bounds the header so it cannot bloat the index.
const MaxIdempotencyKeyLength = 255

// canonicalItem is the normalized form of one create request that the
// request hash is computed from. Defaults are applied first, so a request
// that omits priority and one that sends "normal" hash identically: they
// mean the same thing and must not be reported as a conflict.
type canonicalItem struct {
	Channel     string  `json:"channel"`
	Recipient   string  `json:"recipient"`
	Content     string  `json:"content"`
	Priority    string  `json:"priority"`
	ScheduledAt *string `json:"scheduled_at"`
}

func canonicalize(in CreateInput) canonicalItem {
	c := canonicalItem{
		Channel:   in.Channel,
		Recipient: in.Recipient,
		Content:   in.Content,
		Priority:  string(effectivePriority(in.Priority)),
	}
	if in.ScheduledAt != nil {
		// UTC + fixed format: "2030-01-01T03:00:00+03:00" and
		// "2030-01-01T00:00:00Z" are the same instant and hash the same.
		ts := in.ScheduledAt.UTC().Format(time.RFC3339Nano)
		c.ScheduledAt = &ts
	}
	return c
}

// requestHash returns a stable SHA-256 of the normalized payload. A hash
// (rather than storing the full request) keeps the comparison cheap and
// avoids duplicating message content in another column.
func requestHash(kind string, payload any) string {
	raw, err := json.Marshal(payload)
	if err != nil {
		// Only plain strings and slices of structs are marshalled here.
		panic(fmt.Sprintf("marshal idempotency payload: %v", err))
	}
	sum := sha256.Sum256(append([]byte(kind+":"), raw...))
	return hex.EncodeToString(sum[:])
}

// checkReplay decides what an existing row found under the same
// Idempotency-Key means:
//   - same payload hash: a client retry. Return the original (HTTP 200).
//   - different hash: the key was reused for a different request. This is a
//     client bug; silently returning the old resource would hide it (HTTP 409).
func checkReplay(storedHash *string, newHash string) error {
	if storedHash != nil && *storedHash == newHash {
		return nil
	}
	return domain.ErrIdempotencyConflict
}

// normalizeKey trims the header value and enforces limits. An empty result
// means "no idempotency requested".
func normalizeKey(raw string) (*string, error) {
	key := strings.TrimSpace(raw)
	if key == "" {
		return nil, nil
	}
	if len(key) > MaxIdempotencyKeyLength {
		return nil, &domain.ValidationError{Fields: []domain.FieldError{{
			Field:   "Idempotency-Key",
			Message: fmt.Sprintf("must be at most %d characters", MaxIdempotencyKeyLength),
		}}}
	}
	return &key, nil
}

func effectivePriority(raw string) domain.Priority {
	if raw == "" {
		return domain.PriorityNormal
	}
	return domain.Priority(raw)
}
