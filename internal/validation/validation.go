// Package validation holds per-channel content and recipient rules. Limits
// come from configuration so they can change without a code release; see
// README "Assumptions" for why each default was chosen.
package validation

import (
	"fmt"
	"net/mail"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"notification-service/internal/domain"
)

// Rules are the configurable limits. Lengths count Unicode characters
// (runes), not bytes, because that is what a user perceives as "length".
type Rules struct {
	SMSMaxChars       int // 160 = one GSM-7 SMS segment
	EmailMaxChars     int
	PushMaxChars      int
	PushTokenMaxChars int
}

// DefaultRules mirror the documented assumptions.
var DefaultRules = Rules{
	SMSMaxChars:       160,
	EmailMaxChars:     10_000,
	PushMaxChars:      256,
	PushTokenMaxChars: 512,
}

// Request is the subset of a create request that validation inspects.
type Request struct {
	Channel     string
	Recipient   string
	Content     string
	Priority    string // empty means "use the default"
	ScheduledAt *time.Time
}

// e164 is "+" followed by a country code (no leading zero) and up to 15 digits in total.
var e164 = regexp.MustCompile(`^\+[1-9][0-9]{1,14}$`)

const maxEmailLength = 254 // RFC 5321 path limit

// Validate returns every problem found (empty slice when valid). now is
// injected so scheduled_at checks are deterministic in tests.
func (r Rules) Validate(req Request, now time.Time) []domain.FieldError {
	var errs []domain.FieldError
	add := func(field, format string, args ...any) {
		errs = append(errs, domain.FieldError{Field: field, Message: fmt.Sprintf(format, args...)})
	}

	if req.Priority != "" && !domain.Priority(req.Priority).Valid() {
		add("priority", "must be one of high, normal, low")
	}
	if req.ScheduledAt != nil && !req.ScheduledAt.After(now) {
		add("scheduled_at", "must be in the future")
	}
	if strings.TrimSpace(req.Content) == "" {
		add("content", "is required")
	}

	channel := domain.Channel(req.Channel)
	if !channel.Valid() {
		add("channel", "must be one of sms, email, push")
		// Recipient and length rules depend on the channel, so stop here.
		return errs
	}

	switch channel {
	case domain.ChannelSMS:
		if !e164.MatchString(req.Recipient) {
			add("recipient", "must be a phone number in E.164 format, e.g. +905551234567")
		}
		r.checkLength(&errs, req.Content, r.SMSMaxChars)
	case domain.ChannelEmail:
		if !validEmail(req.Recipient) {
			add("recipient", "must be a valid email address, e.g. user@example.com")
		}
		r.checkLength(&errs, req.Content, r.EmailMaxChars)
	case domain.ChannelPush:
		switch {
		case req.Recipient == "":
			add("recipient", "device token is required")
		case strings.IndexFunc(req.Recipient, unicode.IsSpace) >= 0:
			add("recipient", "device token must not contain whitespace")
		case utf8.RuneCountInString(req.Recipient) > r.PushTokenMaxChars:
			add("recipient", "device token must be at most %d characters", r.PushTokenMaxChars)
		}
		r.checkLength(&errs, req.Content, r.PushMaxChars)
	}
	return errs
}

func (r Rules) checkLength(errs *[]domain.FieldError, content string, limit int) {
	if n := utf8.RuneCountInString(content); n > limit {
		*errs = append(*errs, domain.FieldError{
			Field:   "content",
			Message: fmt.Sprintf("must be at most %d characters for this channel, got %d", limit, n),
		})
	}
}

// validEmail accepts a bare address only ("user@example.com"), not a display
// name form ("User <user@example.com>"), and requires a dotted domain.
func validEmail(s string) bool {
	if s == "" || len(s) > maxEmailLength {
		return false
	}
	addr, err := mail.ParseAddress(s)
	if err != nil || addr.Address != s {
		return false
	}
	at := strings.LastIndexByte(s, '@')
	return strings.Contains(s[at+1:], ".")
}
