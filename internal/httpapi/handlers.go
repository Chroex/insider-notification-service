package httpapi

import (
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"

	"notification-service/internal/domain"
	"notification-service/internal/notification"
	"notification-service/internal/observability"
	"notification-service/internal/store"
)

const (
	headerIdempotencyKey = "Idempotency-Key"
	// headerReplayed tells a client its retry was deduplicated.
	headerReplayed = "Idempotent-Replayed"

	defaultPageSize = 50
	maxPageSize     = 200
)

type handlers struct {
	svc     *notification.Service
	metrics *observability.APIMetrics
}

// POST /api/v1/notifications
func (h *handlers) createNotification(w http.ResponseWriter, r *http.Request) {
	var req createNotificationRequest
	if reqErr := decodeJSON(w, r, &req, maxSingleBodyBytes); reqErr != nil {
		writeError(w, r, reqErr.status, reqErr.code, reqErr.message, nil)
		return
	}

	n, created, err := h.svc.Create(r.Context(), req.toInput(),
		r.Header.Get(headerIdempotencyKey), observability.CorrelationID(r.Context()))
	if err != nil {
		writeServiceError(w, r, err)
		return
	}

	w.Header().Set("Location", "/api/v1/notifications/"+n.ID.String())
	if created {
		h.metrics.NotificationCreated(n.Channel, n.Priority)
	}
	if !created {
		w.Header().Set(headerReplayed, "true")
		writeJSON(w, http.StatusOK, toNotificationResponse(n))
		return
	}
	writeJSON(w, http.StatusCreated, toNotificationResponse(n))
}

// POST /api/v1/notifications/batch
func (h *handlers) createBatch(w http.ResponseWriter, r *http.Request) {
	var req createBatchRequest
	if reqErr := decodeJSON(w, r, &req, maxBatchBodyBytes); reqErr != nil {
		writeError(w, r, reqErr.status, reqErr.code, reqErr.message, nil)
		return
	}

	inputs := make([]notification.CreateInput, len(req.Notifications))
	for i, item := range req.Notifications {
		inputs[i] = item.toInput()
	}

	result, created, err := h.svc.CreateBatch(r.Context(), inputs,
		r.Header.Get(headerIdempotencyKey), observability.CorrelationID(r.Context()))
	if err != nil {
		writeServiceError(w, r, err)
		return
	}

	resp := createBatchResponse{
		BatchID:   result.Batch.ID,
		Total:     result.Batch.Total,
		CreatedAt: result.Batch.CreatedAt.UTC(),
		Items:     make([]batchItemRef, len(result.IDs)),
	}
	for i, id := range result.IDs {
		resp.Items[i] = batchItemRef{Index: i, ID: id}
	}

	w.Header().Set("Location", "/api/v1/batches/"+result.Batch.ID.String())
	if created {
		for _, in := range inputs {
			h.metrics.NotificationCreated(domain.Channel(in.Channel), notification.EffectivePriority(in.Priority))
		}
	}
	if !created {
		w.Header().Set(headerReplayed, "true")
		writeJSON(w, http.StatusOK, resp)
		return
	}
	writeJSON(w, http.StatusCreated, resp)
}

// GET /api/v1/notifications/{id}
func (h *handlers) getNotification(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}
	n, attempts, err := h.svc.Get(r.Context(), id)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toDetailResponse(n, attempts))
}

// POST /api/v1/notifications/{id}/cancel
func (h *handlers) cancelNotification(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}
	n, err := h.svc.Cancel(r.Context(), id)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toNotificationResponse(n))
}

// POST /api/v1/notifications/{id}/retry
func (h *handlers) retryNotification(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}
	n, err := h.svc.Retry(r.Context(), id)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toNotificationResponse(n))
}

// GET /api/v1/notifications?status=&channel=&from=&to=&limit=&cursor=
func (h *handlers) listNotifications(w http.ResponseWriter, r *http.Request) {
	filter, fieldErrs := parseListFilter(r.URL.Query())
	if len(fieldErrs) > 0 {
		writeError(w, r, http.StatusBadRequest, codeInvalidParameter, "invalid query parameters", fieldErrs)
		return
	}
	items, next, err := h.svc.List(r.Context(), filter)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, listResponse[notificationResponse]{
		Data:       toNotificationResponses(items),
		NextCursor: encodeCursor(next),
	})
}

// GET /api/v1/batches/{id}?limit=&cursor=
func (h *handlers) getBatch(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	var fieldErrs []domain.FieldError
	limit := parseLimit(q, &fieldErrs)
	after := parseCursor(q, &fieldErrs)
	if len(fieldErrs) > 0 {
		writeError(w, r, http.StatusBadRequest, codeInvalidParameter, "invalid query parameters", fieldErrs)
		return
	}

	summary, items, next, err := h.svc.GetBatch(r.Context(), id, limit, after)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toBatchResponse(summary, items, encodeCursor(next)))
}

// POST /api/v1/templates
func (h *handlers) createTemplate(w http.ResponseWriter, r *http.Request) {
	var req createTemplateRequest
	if reqErr := decodeJSON(w, r, &req, maxSingleBodyBytes); reqErr != nil {
		writeError(w, r, reqErr.status, reqErr.code, reqErr.message, nil)
		return
	}
	t, err := h.svc.CreateTemplate(r.Context(), notification.TemplateInput{
		Name: req.Name, Channel: req.Channel, Body: req.Body,
	})
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	w.Header().Set("Location", "/api/v1/templates/"+t.ID.String())
	writeJSON(w, http.StatusCreated, toTemplateResponse(t))
}

// GET /api/v1/templates/{id}
func (h *handlers) getTemplate(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}
	t, err := h.svc.GetTemplate(r.Context(), id)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTemplateResponse(t))
}

// notFound returns the standard error body for unknown routes.
func notFound(w http.ResponseWriter, r *http.Request) {
	writeError(w, r, http.StatusNotFound, codeNotFound, "no route for "+r.Method+" "+r.URL.Path, nil)
}

// --- request parsing helpers ---

func pathUUID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidParameter, "invalid path parameter",
			[]domain.FieldError{{Field: "id", Message: "must be a UUID"}})
		return uuid.Nil, false
	}
	return id, true
}

func parseListFilter(q url.Values) (store.ListFilter, []domain.FieldError) {
	var errs []domain.FieldError
	f := store.ListFilter{Limit: parseLimit(q, &errs), After: parseCursor(q, &errs)}

	if v := q.Get("status"); v != "" {
		if s := domain.Status(v); s.Valid() {
			f.Status = &s
		} else {
			errs = append(errs, domain.FieldError{Field: "status", Message: "unknown status"})
		}
	}
	if v := q.Get("channel"); v != "" {
		if c := domain.Channel(v); c.Valid() {
			f.Channel = &c
		} else {
			errs = append(errs, domain.FieldError{Field: "channel", Message: "must be one of sms, email, push"})
		}
	}
	f.From = parseTime(q, "from", &errs)
	f.To = parseTime(q, "to", &errs)
	if f.From != nil && f.To != nil && !f.From.Before(*f.To) {
		errs = append(errs, domain.FieldError{Field: "to", Message: "must be after from"})
	}
	return f, errs
}

func parseLimit(q url.Values, errs *[]domain.FieldError) int {
	raw := q.Get("limit")
	if raw == "" {
		return defaultPageSize
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > maxPageSize {
		*errs = append(*errs, domain.FieldError{Field: "limit",
			Message: "must be an integer between 1 and " + strconv.Itoa(maxPageSize)})
		return defaultPageSize
	}
	return n
}

func parseCursor(q url.Values, errs *[]domain.FieldError) *store.Cursor {
	raw := q.Get("cursor")
	if raw == "" {
		return nil
	}
	c, err := store.DecodeCursor(raw)
	if err != nil {
		*errs = append(*errs, domain.FieldError{Field: "cursor", Message: "is invalid; use next_cursor from a previous response"})
		return nil
	}
	return &c
}

func parseTime(q url.Values, field string, errs *[]domain.FieldError) *time.Time {
	raw := q.Get(field)
	if raw == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		*errs = append(*errs, domain.FieldError{Field: field, Message: "must be an RFC 3339 timestamp, e.g. 2026-01-02T15:04:05Z"})
		return nil
	}
	return &t
}

func encodeCursor(c *store.Cursor) *string {
	if c == nil {
		return nil
	}
	s := c.Encode()
	return &s
}
