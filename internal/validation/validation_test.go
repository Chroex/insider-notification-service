package validation

import (
	"strings"
	"testing"
	"time"

	"notification-service/internal/domain"
)

var now = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func ptr[T any](v T) *T { return &v }

// fields returns the names of invalid fields, in order.
func fields(errs []domain.FieldError) []string {
	out := make([]string, 0, len(errs))
	for _, e := range errs {
		out = append(out, e.Field)
	}
	return out
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name string
		req  Request
		want []string // invalid fields; nil means valid
	}{
		// SMS
		{"sms valid", Request{Channel: "sms", Recipient: "+905551234567", Content: "hi"}, nil},
		{"sms 160 chars ok", Request{Channel: "sms", Recipient: "+905551234567", Content: strings.Repeat("a", 160)}, nil},
		{"sms 161 chars", Request{Channel: "sms", Recipient: "+905551234567", Content: strings.Repeat("a", 161)}, []string{"content"}},
		{"sms counts runes not bytes", Request{Channel: "sms", Recipient: "+905551234567", Content: strings.Repeat("ş", 160)}, nil},
		{"sms missing plus", Request{Channel: "sms", Recipient: "905551234567", Content: "hi"}, []string{"recipient"}},
		{"sms leading zero country", Request{Channel: "sms", Recipient: "+0555123", Content: "hi"}, []string{"recipient"}},
		{"sms too many digits", Request{Channel: "sms", Recipient: "+1234567890123456", Content: "hi"}, []string{"recipient"}},
		{"sms letters", Request{Channel: "sms", Recipient: "+90555abc", Content: "hi"}, []string{"recipient"}},

		// Email
		{"email valid", Request{Channel: "email", Recipient: "user@example.com", Content: "hi"}, nil},
		{"email display name rejected", Request{Channel: "email", Recipient: "User <user@example.com>", Content: "hi"}, []string{"recipient"}},
		{"email no domain dot", Request{Channel: "email", Recipient: "user@localhost", Content: "hi"}, []string{"recipient"}},
		{"email garbage", Request{Channel: "email", Recipient: "not-an-email", Content: "hi"}, []string{"recipient"}},
		{"email 10000 ok", Request{Channel: "email", Recipient: "u@example.com", Content: strings.Repeat("a", 10_000)}, nil},
		{"email 10001", Request{Channel: "email", Recipient: "u@example.com", Content: strings.Repeat("a", 10_001)}, []string{"content"}},

		// Push
		{"push valid", Request{Channel: "push", Recipient: "fcm-token-123", Content: "hi"}, nil},
		{"push empty token", Request{Channel: "push", Recipient: "", Content: "hi"}, []string{"recipient"}},
		{"push token with space", Request{Channel: "push", Recipient: "abc def", Content: "hi"}, []string{"recipient"}},
		{"push token too long", Request{Channel: "push", Recipient: strings.Repeat("x", 513), Content: "hi"}, []string{"recipient"}},
		{"push 257 chars", Request{Channel: "push", Recipient: "tok", Content: strings.Repeat("a", 257)}, []string{"content"}},

		// Common rules
		{"unknown channel stops channel rules", Request{Channel: "fax", Recipient: "", Content: "hi"}, []string{"channel"}},
		{"content required", Request{Channel: "sms", Recipient: "+905551234567", Content: "   "}, []string{"content"}},
		{"bad priority", Request{Channel: "sms", Recipient: "+905551234567", Content: "hi", Priority: "urgent"}, []string{"priority"}},
		{"explicit priority ok", Request{Channel: "sms", Recipient: "+905551234567", Content: "hi", Priority: "low"}, nil},
		{"scheduled in future ok", Request{Channel: "sms", Recipient: "+905551234567", Content: "hi", ScheduledAt: ptr(now.Add(time.Minute))}, nil},
		{"scheduled in past", Request{Channel: "sms", Recipient: "+905551234567", Content: "hi", ScheduledAt: ptr(now.Add(-time.Minute))}, []string{"scheduled_at"}},
		{"scheduled exactly now", Request{Channel: "sms", Recipient: "+905551234567", Content: "hi", ScheduledAt: ptr(now)}, []string{"scheduled_at"}},
		{"reports all errors at once", Request{Channel: "sms", Recipient: "bad", Content: "", Priority: "x"}, []string{"priority", "content", "recipient"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fields(DefaultRules.Validate(tt.req, now))
			if strings.Join(got, ",") != strings.Join(tt.want, ",") {
				t.Errorf("invalid fields = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidate_LimitsAreConfigurable(t *testing.T) {
	rules := DefaultRules
	rules.SMSMaxChars = 10
	errs := rules.Validate(Request{Channel: "sms", Recipient: "+905551234567", Content: strings.Repeat("a", 11)}, now)
	if len(errs) != 1 || !strings.Contains(errs[0].Message, "at most 10") {
		t.Errorf("errs = %+v, want one content error mentioning the configured limit", errs)
	}
}
