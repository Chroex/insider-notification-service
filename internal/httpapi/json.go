package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const (
	maxSingleBodyBytes = 1 << 20  // 1 MiB: one email at 10k chars is ~40 KiB
	maxBatchBodyBytes  = 16 << 20 // 16 MiB: 1000 emails at 10k chars each
)

// requestError is a client error found while decoding, before the service
// layer is involved.
type requestError struct {
	status  int
	code    string
	message string
}

func (e *requestError) Error() string { return e.message }

// decodeJSON reads exactly one JSON object into dst. Unknown fields are
// rejected so typos like "recipent" fail loudly instead of being ignored.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any, maxBytes int64) *requestError {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBytes))
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		var syntaxErr *json.SyntaxError
		var typeErr *json.UnmarshalTypeError
		switch {
		case errors.As(err, &maxErr):
			return &requestError{http.StatusRequestEntityTooLarge, codePayloadTooLarge,
				fmt.Sprintf("request body must be at most %d bytes", maxErr.Limit)}
		case errors.Is(err, io.EOF):
			return &requestError{http.StatusBadRequest, codeInvalidJSON, "request body is empty"}
		case errors.As(err, &syntaxErr), errors.Is(err, io.ErrUnexpectedEOF):
			return &requestError{http.StatusBadRequest, codeInvalidJSON, "request body is not valid JSON"}
		case errors.As(err, &typeErr):
			return &requestError{http.StatusBadRequest, codeInvalidJSON,
				fmt.Sprintf("field %q has the wrong type", typeErr.Field)}
		case strings.HasPrefix(err.Error(), "json: unknown field "):
			field := strings.TrimPrefix(err.Error(), "json: unknown field ")
			return &requestError{http.StatusBadRequest, codeInvalidJSON, "unknown field " + field}
		default:
			// e.g. a malformed RFC 3339 timestamp in scheduled_at
			return &requestError{http.StatusBadRequest, codeInvalidJSON, "invalid request body: " + err.Error()}
		}
	}
	if dec.More() {
		return &requestError{http.StatusBadRequest, codeInvalidJSON, "request body must contain a single JSON object"}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
