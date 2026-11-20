package httpapi

import (
	"errors"
	"net/http"

	"notification-service/internal/domain"
	"notification-service/internal/observability"
)

// Error codes are stable, machine-readable identifiers; messages are for humans.
const (
	codeInvalidJSON         = "invalid_json"
	codeInvalidParameter    = "invalid_parameter"
	codeValidationFailed    = "validation_failed"
	codeNotFound            = "not_found"
	codeIdempotencyConflict = "idempotency_conflict"
	codeInvalidState        = "invalid_state"
	codePayloadTooLarge     = "payload_too_large"
	codeInternal            = "internal_error"
)

// errorBody is the single error shape used by every endpoint:
// { "error": { "code", "message", "details" }, "correlation_id" }.
type errorBody struct {
	Error         errorDetail `json:"error"`
	CorrelationID string      `json:"correlation_id"`
}

type errorDetail struct {
	Code    string              `json:"code"`
	Message string              `json:"message"`
	Details []domain.FieldError `json:"details,omitempty"`
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string, details []domain.FieldError) {
	writeJSON(w, status, errorBody{
		Error:         errorDetail{Code: code, Message: message, Details: details},
		CorrelationID: observability.CorrelationID(r.Context()),
	})
}

// writeServiceError maps domain errors to HTTP in one place. Anything
// unrecognised is logged and returned as a generic 500 so internal details
// (SQL, hostnames) never leak to clients.
func writeServiceError(w http.ResponseWriter, r *http.Request, err error) {
	var validationErr *domain.ValidationError
	var transitionErr *domain.InvalidTransitionError
	switch {
	case errors.As(err, &validationErr):
		writeError(w, r, http.StatusUnprocessableEntity, codeValidationFailed,
			"request validation failed", validationErr.Fields)
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, r, http.StatusNotFound, codeNotFound, "resource not found", nil)
	case errors.Is(err, domain.ErrIdempotencyConflict):
		writeError(w, r, http.StatusConflict, codeIdempotencyConflict,
			"Idempotency-Key was already used with a different request payload", nil)
	case errors.As(err, &transitionErr):
		writeError(w, r, http.StatusConflict, codeInvalidState, transitionErr.Error(), nil)
	default:
		observability.Logger(r.Context()).Error("request failed", "error", err)
		writeError(w, r, http.StatusInternalServerError, codeInternal, "internal server error", nil)
	}
}
