package domain

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by the store and service layers. The HTTP layer
// maps them to status codes in exactly one place (httpapi/errors.go).
var (
	ErrNotFound = errors.New("not found")
	// ErrIdempotencyConflict: the Idempotency-Key was already used with a
	// different request payload.
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different payload")
	// ErrTemplateNameTaken: template names are unique.
	ErrTemplateNameTaken = errors.New("template name already exists")
)

// InvalidTransitionError reports a status change that is not allowed from
// the notification's current status, e.g. cancelling a sent notification.
type InvalidTransitionError struct {
	From   Status
	Action string
}

func (e *InvalidTransitionError) Error() string {
	return fmt.Sprintf("cannot %s a notification in status %q", e.Action, e.From)
}

// FieldError describes one invalid input field. Index is set for batch items
// so clients can tell which item failed.
type FieldError struct {
	Index   *int   `json:"index,omitempty"`
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationError carries every field problem found, not just the first, so
// a client can fix a request in one round trip.
type ValidationError struct {
	Fields []FieldError
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation failed: %d invalid field(s)", len(e.Fields))
}
