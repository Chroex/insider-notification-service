//go:build integration

package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	legacyrouter "github.com/getkin/kin-openapi/routers/legacy"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"notification-service/internal/events"
	"notification-service/internal/httpapi"
	"notification-service/internal/notification"
	"notification-service/internal/store"
	"notification-service/internal/testutil"
	"notification-service/internal/validation"
)

var (
	db       *testutil.Postgres
	server   *httptest.Server
	recorder = &recordingPublisher{}
	// spec routes requests to api/openapi.yaml operations so every response
	// in this suite is validated against the documented contract.
	spec routers.Router
)

// TestMain starts one Postgres container for the whole package; each test
// truncates tables instead of paying for a new container.
func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	db, err = testutil.StartPostgres(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration setup:", err)
		os.Exit(1)
	}
	svc := notification.NewService(store.New(db.Pool), notification.Config{
		Rules: validation.DefaultRules, BatchMaxSize: 1000, MaxAttempts: 5,
	})
	server = httptest.NewServer(httpapi.NewRouter(httpapi.Options{
		Service: svc,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Events:  recorder,
	}))
	if spec, err = loadSpecRouter(server.URL); err != nil {
		fmt.Fprintln(os.Stderr, "load openapi spec:", err)
		os.Exit(1)
	}

	code := m.Run()
	server.Close()
	db.Close(ctx)
	os.Exit(code)
}

// --- helpers ---

type response struct {
	Status int
	Header http.Header
	Body   map[string]any
}

func call(t *testing.T, method, path string, body any, headers ...string) response {
	t.Helper()
	var reader io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		reader = strings.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, server.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	validateAgainstSpec(t, req, resp, raw)

	out := response{Status: resp.StatusCode, Header: resp.Header, Body: map[string]any{}}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out.Body); err != nil {
			t.Fatalf("%s %s: decode body: %v", method, path, err)
		}
	}
	return out
}

func loadSpecRouter(serverURL string) (routers.Router, error) {
	doc, err := openapi3.NewLoader().LoadFromFile("../../api/openapi.yaml")
	if err != nil {
		return nil, err
	}
	doc.Servers = openapi3.Servers{{URL: serverURL}} // the test server, not localhost:8080
	return legacyrouter.NewRouter(doc)
}

// validateAgainstSpec checks status code, headers and body of a response
// against the operation documented in api/openapi.yaml. Undocumented status
// codes fail too. Requests to unknown routes (the 404 catch-all) have no
// operation to check against and are skipped; TestOpenAPISpecMatchesRoutes
// guarantees every real route is documented.
func validateAgainstSpec(t *testing.T, req *http.Request, resp *http.Response, body []byte) {
	t.Helper()
	route, params, err := spec.FindRoute(req)
	if err != nil {
		return
	}
	input := &openapi3filter.ResponseValidationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{
			Request: req, PathParams: params, Route: route,
		},
		Status:  resp.StatusCode,
		Header:  resp.Header,
		Body:    io.NopCloser(bytes.NewReader(body)),
		Options: &openapi3filter.Options{IncludeResponseStatus: true, MultiError: true},
	}
	if err := openapi3filter.ValidateResponse(context.Background(), input); err != nil {
		t.Errorf("%s %s -> %d does not match api/openapi.yaml:\n%v\nbody: %s",
			req.Method, req.URL.Path, resp.StatusCode, err, truncate(string(body), 500))
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func expectStatus(t *testing.T, r response, want int) {
	t.Helper()
	if r.Status != want {
		t.Fatalf("status = %d, want %d; body = %v", r.Status, want, r.Body)
	}
}

func errorCode(r response) string {
	e, _ := r.Body["error"].(map[string]any)
	code, _ := e["code"].(string)
	return code
}

func sms(content string) map[string]any {
	return map[string]any{"channel": "sms", "recipient": "+905551234567", "content": content}
}

func countRows(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.Pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// --- create / get ---

func TestCreateAndGet(t *testing.T) {
	db.Reset(t)
	created := call(t, "POST", "/api/v1/notifications", map[string]any{
		"channel": "email", "recipient": "user@example.com", "content": "Welcome!", "priority": "high",
	})
	expectStatus(t, created, http.StatusCreated)
	id := created.Body["id"].(string)
	if created.Header.Get("Location") != "/api/v1/notifications/"+id {
		t.Errorf("Location = %q", created.Header.Get("Location"))
	}
	if created.Body["status"] != "pending" || created.Body["priority"] != "high" || created.Body["max_attempts"] != 5.0 {
		t.Errorf("unexpected create body: %v", created.Body)
	}
	if ts, _ := created.Body["created_at"].(string); !strings.HasSuffix(ts, "Z") {
		t.Errorf("created_at = %q, want UTC (Z suffix)", ts)
	}

	got := call(t, "GET", "/api/v1/notifications/"+id, nil)
	expectStatus(t, got, http.StatusOK)
	if got.Body["id"] != id || got.Body["content"] != "Welcome!" {
		t.Errorf("unexpected get body: %v", got.Body)
	}
	if attempts, ok := got.Body["delivery_attempts"].([]any); !ok || len(attempts) != 0 {
		t.Errorf("delivery_attempts = %v, want empty array", got.Body["delivery_attempts"])
	}
}

func TestCreate_DefaultsPriorityToNormal(t *testing.T) {
	db.Reset(t)
	r := call(t, "POST", "/api/v1/notifications", sms("hi"))
	expectStatus(t, r, http.StatusCreated)
	if r.Body["priority"] != "normal" {
		t.Errorf("priority = %v, want normal", r.Body["priority"])
	}
}

func TestCreate_Scheduled(t *testing.T) {
	db.Reset(t)
	at := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	body := sms("later")
	body["scheduled_at"] = at.Format(time.RFC3339)
	r := call(t, "POST", "/api/v1/notifications", body)
	expectStatus(t, r, http.StatusCreated)
	if r.Body["status"] != "scheduled" || r.Body["next_attempt_at"] != at.Format(time.RFC3339) ||
		r.Body["scheduled_at"] != at.Format(time.RFC3339) {
		t.Errorf("scheduled body = %v", r.Body)
	}
}

func TestCreate_Rejections(t *testing.T) {
	db.Reset(t)
	tests := []struct {
		name       string
		body       any
		wantStatus int
		wantCode   string
	}{
		{"validation lists every field", map[string]any{"channel": "sms", "recipient": "123", "content": ""},
			http.StatusUnprocessableEntity, "validation_failed"},
		{"unknown field", `{"channel":"sms","recipent":"+905551234567","content":"hi"}`,
			http.StatusBadRequest, "invalid_json"},
		{"malformed json", `{"channel":`, http.StatusBadRequest, "invalid_json"},
		{"empty body", ``, http.StatusBadRequest, "invalid_json"},
		{"two objects", `{"channel":"sms"}{"channel":"sms"}`, http.StatusBadRequest, "invalid_json"},
		{"past schedule", map[string]any{"channel": "sms", "recipient": "+905551234567", "content": "x",
			"scheduled_at": "2001-01-01T00:00:00Z"}, http.StatusUnprocessableEntity, "validation_failed"},
		// Postgres cannot store U+0000; before validation caught it, the
		// INSERT failed and the client got a 500.
		{"NUL in content", `{"channel":"sms","recipient":"+905551234567","content":"a\u0000b"}`,
			http.StatusUnprocessableEntity, "validation_failed"},
		{"NUL in push token", `{"channel":"push","recipient":"tok\u0000en","content":"hi"}`,
			http.StatusUnprocessableEntity, "validation_failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := call(t, "POST", "/api/v1/notifications", tt.body)
			expectStatus(t, r, tt.wantStatus)
			if errorCode(r) != tt.wantCode {
				t.Errorf("error code = %q, want %q", errorCode(r), tt.wantCode)
			}
			if r.Body["correlation_id"] == "" || r.Body["correlation_id"] == nil {
				t.Error("error body must include correlation_id")
			}
		})
	}
	if n := countRows(t, "SELECT count(*) FROM notifications"); n != 0 {
		t.Errorf("rejected requests inserted %d rows", n)
	}

	r := call(t, "POST", "/api/v1/notifications", map[string]any{"channel": "sms", "recipient": "123", "content": ""})
	details := r.Body["error"].(map[string]any)["details"].([]any)
	if len(details) != 2 {
		t.Errorf("details = %v, want content and recipient errors", details)
	}
}

func TestGet_NotFoundAndBadID(t *testing.T) {
	r := call(t, "GET", "/api/v1/notifications/0190a8c4-0000-7000-8000-000000000000", nil)
	expectStatus(t, r, http.StatusNotFound)
	r = call(t, "GET", "/api/v1/notifications/not-a-uuid", nil)
	expectStatus(t, r, http.StatusBadRequest)
	r = call(t, "GET", "/api/v1/nope", nil)
	expectStatus(t, r, http.StatusNotFound)
	if errorCode(r) != "not_found" {
		t.Errorf("unknown route code = %q", errorCode(r))
	}
}

// --- correlation id ---

func TestCorrelationID_PropagatedAndStored(t *testing.T) {
	db.Reset(t)
	r := call(t, "POST", "/api/v1/notifications", sms("hi"), "X-Correlation-ID", "req-abc-123")
	expectStatus(t, r, http.StatusCreated)
	if r.Header.Get("X-Correlation-ID") != "req-abc-123" || r.Body["correlation_id"] != "req-abc-123" {
		t.Errorf("correlation id not propagated: header=%q body=%v", r.Header.Get("X-Correlation-ID"), r.Body["correlation_id"])
	}

	// Unsafe values are replaced with a generated id rather than logged.
	r = call(t, "POST", "/api/v1/notifications", sms("hi"), "X-Correlation-ID", "bad value; with spaces")
	if got := r.Header.Get("X-Correlation-ID"); got == "" || strings.Contains(got, " ") {
		t.Errorf("unsafe correlation id not replaced: %q", got)
	}
}

// --- idempotency ---

func TestIdempotency_ReplayAndConflict(t *testing.T) {
	db.Reset(t)
	first := call(t, "POST", "/api/v1/notifications", sms("hi"), "Idempotency-Key", "order-42")
	expectStatus(t, first, http.StatusCreated)

	// Same payload; priority made explicit, which is semantically identical.
	sameMeaning := sms("hi")
	sameMeaning["priority"] = "normal"
	replay := call(t, "POST", "/api/v1/notifications", sameMeaning, "Idempotency-Key", "order-42")
	expectStatus(t, replay, http.StatusOK)
	if replay.Body["id"] != first.Body["id"] || replay.Header.Get("Idempotent-Replayed") != "true" {
		t.Errorf("replay returned id %v (want %v), replayed header %q",
			replay.Body["id"], first.Body["id"], replay.Header.Get("Idempotent-Replayed"))
	}

	conflict := call(t, "POST", "/api/v1/notifications", sms("different"), "Idempotency-Key", "order-42")
	expectStatus(t, conflict, http.StatusConflict)
	if errorCode(conflict) != "idempotency_conflict" {
		t.Errorf("code = %q", errorCode(conflict))
	}

	if n := countRows(t, "SELECT count(*) FROM notifications"); n != 1 {
		t.Errorf("rows = %d, want 1", n)
	}
}

func TestIdempotency_ConcurrentRequestsCreateOneRow(t *testing.T) {
	db.Reset(t)
	const clients = 20
	var wg sync.WaitGroup
	ids := make(chan string, clients)
	for range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := call(t, "POST", "/api/v1/notifications", sms("race"), "Idempotency-Key", "same-key")
			if r.Status != http.StatusCreated && r.Status != http.StatusOK {
				t.Errorf("status = %d, body = %v", r.Status, r.Body)
				return
			}
			ids <- r.Body["id"].(string)
		}()
	}
	wg.Wait()
	close(ids)

	unique := map[string]bool{}
	for id := range ids {
		unique[id] = true
	}
	if len(unique) != 1 {
		t.Errorf("clients saw %d different ids, want 1", len(unique))
	}
	if n := countRows(t, "SELECT count(*) FROM notifications"); n != 1 {
		t.Errorf("rows = %d, want 1", n)
	}
}

// --- cancel ---

func TestCancel(t *testing.T) {
	db.Reset(t)
	id := call(t, "POST", "/api/v1/notifications", sms("hi")).Body["id"].(string)

	r := call(t, "POST", "/api/v1/notifications/"+id+"/cancel", nil)
	expectStatus(t, r, http.StatusOK)
	if r.Body["status"] != "cancelled" {
		t.Errorf("status = %v", r.Body["status"])
	}

	again := call(t, "POST", "/api/v1/notifications/"+id+"/cancel", nil)
	expectStatus(t, again, http.StatusConflict)
	if errorCode(again) != "invalid_state" {
		t.Errorf("code = %q", errorCode(again))
	}

	r = call(t, "POST", "/api/v1/notifications/0190a8c4-0000-7000-8000-000000000000/cancel", nil)
	expectStatus(t, r, http.StatusNotFound)
}

// forceStatus moves a notification to status the way a worker would,
// including the columns the schema's invariants tie to it (a lease while
// processing, sent_at once sent).
func forceStatus(t *testing.T, id, status string) {
	t.Helper()
	if _, err := db.Pool.Exec(context.Background(), `
		UPDATE notifications
		SET status = $1,
		    locked_until = CASE WHEN $1 = 'processing' THEN now() + interval '30 seconds' END,
		    sent_at      = CASE WHEN $1 = 'sent' THEN now() END
		WHERE id = $2`, status, id); err != nil {
		t.Fatal(err)
	}
}

func TestCancel_NotAllowedOnceProcessingOrSent(t *testing.T) {
	for _, status := range []string{"processing", "sent", "failed"} {
		t.Run(status, func(t *testing.T) {
			db.Reset(t)
			id := call(t, "POST", "/api/v1/notifications", sms("hi")).Body["id"].(string)
			forceStatus(t, id, status) // simulate the worker having moved it on
			r := call(t, "POST", "/api/v1/notifications/"+id+"/cancel", nil)
			expectStatus(t, r, http.StatusConflict)
			msg := r.Body["error"].(map[string]any)["message"].(string)
			if !strings.Contains(msg, status) {
				t.Errorf("message %q should name the current status", msg)
			}
		})
	}
}

// --- list ---

func TestList_FiltersAndCursorPagination(t *testing.T) {
	db.Reset(t)
	var smsIDs []string
	for i := range 5 {
		smsIDs = append(smsIDs, call(t, "POST", "/api/v1/notifications", sms(fmt.Sprint("m", i))).Body["id"].(string))
	}
	for range 2 {
		call(t, "POST", "/api/v1/notifications", map[string]any{"channel": "push", "recipient": "tok", "content": "x"})
	}

	// Walk all SMS pages with limit=2: expect 2 + 2 + 1, newest first, no duplicates.
	var seen []string
	cursor := ""
	for page := 0; ; page++ {
		path := "/api/v1/notifications?channel=sms&limit=2"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		r := call(t, "GET", path, nil)
		expectStatus(t, r, http.StatusOK)
		for _, item := range r.Body["data"].([]any) {
			seen = append(seen, item.(map[string]any)["id"].(string))
		}
		next, _ := r.Body["next_cursor"].(string)
		if next == "" {
			break
		}
		if page > 5 {
			t.Fatal("pagination did not terminate")
		}
		cursor = next
	}
	if len(seen) != 5 {
		t.Fatalf("saw %d sms notifications, want 5: %v", len(seen), seen)
	}
	for i := range seen { // newest first = reverse creation order
		if seen[i] != smsIDs[len(smsIDs)-1-i] {
			t.Errorf("order mismatch at %d: got %s want %s", i, seen[i], smsIDs[len(smsIDs)-1-i])
		}
	}

	// A new row inserted mid-pagination must not shift later pages.
	first := call(t, "GET", "/api/v1/notifications?channel=sms&limit=2", nil)
	call(t, "POST", "/api/v1/notifications", sms("newer"))
	second := call(t, "GET", "/api/v1/notifications?channel=sms&limit=2&cursor="+first.Body["next_cursor"].(string), nil)
	if got := second.Body["data"].([]any)[0].(map[string]any)["id"]; got != smsIDs[2] {
		t.Errorf("page 2 shifted after insert: first id %v, want %s", got, smsIDs[2])
	}

	cancelled := call(t, "POST", "/api/v1/notifications/"+smsIDs[0]+"/cancel", nil)
	expectStatus(t, cancelled, http.StatusOK)
	r := call(t, "GET", "/api/v1/notifications?status=cancelled", nil)
	if data := r.Body["data"].([]any); len(data) != 1 || data[0].(map[string]any)["id"] != smsIDs[0] {
		t.Errorf("status filter returned %v", data)
	}
}

func TestList_TimeRange(t *testing.T) {
	db.Reset(t)
	call(t, "POST", "/api/v1/notifications", sms("now"))
	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	past := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)

	if n := len(call(t, "GET", "/api/v1/notifications?from="+past, nil).Body["data"].([]any)); n != 1 {
		t.Errorf("from=past returned %d, want 1", n)
	}
	if n := len(call(t, "GET", "/api/v1/notifications?from="+future, nil).Body["data"].([]any)); n != 0 {
		t.Errorf("from=future returned %d, want 0", n)
	}
}

func TestList_InvalidParameters(t *testing.T) {
	for _, q := range []string{"status=bogus", "channel=fax", "limit=0", "limit=1000", "cursor=garbage", "from=yesterday",
		"from=2026-02-01T00:00:00Z&to=2026-01-01T00:00:00Z"} {
		r := call(t, "GET", "/api/v1/notifications?"+q, nil)
		if r.Status != http.StatusBadRequest || errorCode(r) != "invalid_parameter" {
			t.Errorf("?%s: status %d code %q, want 400 invalid_parameter", q, r.Status, errorCode(r))
		}
	}
}

// --- batches ---

func TestBatch_CreateAndGet(t *testing.T) {
	db.Reset(t)
	items := []any{sms("a"), sms("b"),
		map[string]any{"channel": "email", "recipient": "u@example.com", "content": "c", "priority": "high"}}
	r := call(t, "POST", "/api/v1/notifications/batch", map[string]any{"notifications": items})
	expectStatus(t, r, http.StatusCreated)
	batchID := r.Body["batch_id"].(string)
	refs := r.Body["items"].([]any)
	if r.Body["total"] != 3.0 || len(refs) != 3 {
		t.Fatalf("unexpected batch body: %v", r.Body)
	}

	// Cancel one so the summary has two statuses.
	cancelID := refs[1].(map[string]any)["id"].(string)
	call(t, "POST", "/api/v1/notifications/"+cancelID+"/cancel", nil)

	page1 := call(t, "GET", "/api/v1/batches/"+batchID+"?limit=2", nil)
	expectStatus(t, page1, http.StatusOK)
	counts := page1.Body["status_counts"].(map[string]any)
	if counts["pending"] != 2.0 || counts["cancelled"] != 1.0 || counts["sent"] != 0.0 {
		t.Errorf("status_counts = %v", counts)
	}
	page2 := call(t, "GET", "/api/v1/batches/"+batchID+"?limit=2&cursor="+page1.Body["next_cursor"].(string), nil)
	if page2.Body["next_cursor"] != nil {
		t.Errorf("expected last page, next_cursor = %v", page2.Body["next_cursor"])
	}

	// Items come back in request order across pages.
	var gotContent []string
	for _, p := range []response{page1, page2} {
		for _, it := range p.Body["items"].([]any) {
			gotContent = append(gotContent, it.(map[string]any)["content"].(string))
		}
	}
	if strings.Join(gotContent, "") != "abc" {
		t.Errorf("batch items order = %v, want [a b c]", gotContent)
	}
}

func TestBatch_RejectsWholeBatchListingIndexes(t *testing.T) {
	db.Reset(t)
	items := []any{sms("ok"), sms(""), map[string]any{"channel": "push", "recipient": "", "content": "x"}}
	r := call(t, "POST", "/api/v1/notifications/batch", map[string]any{"notifications": items})
	expectStatus(t, r, http.StatusUnprocessableEntity)

	var indexes []float64
	for _, d := range r.Body["error"].(map[string]any)["details"].([]any) {
		indexes = append(indexes, d.(map[string]any)["index"].(float64))
	}
	if fmt.Sprint(indexes) != "[1 2]" {
		t.Errorf("failing indexes = %v, want [1 2]", indexes)
	}
	if n := countRows(t, "SELECT count(*) FROM notifications"); n != 0 {
		t.Errorf("rows = %d, want 0 (all-or-nothing)", n)
	}
	if n := countRows(t, "SELECT count(*) FROM batches"); n != 0 {
		t.Errorf("batches = %d, want 0", n)
	}
}

func TestBatch_SizeLimits(t *testing.T) {
	db.Reset(t)
	r := call(t, "POST", "/api/v1/notifications/batch", map[string]any{"notifications": []any{}})
	expectStatus(t, r, http.StatusUnprocessableEntity)

	full := make([]any, 1000)
	for i := range full {
		full[i] = sms(fmt.Sprint("bulk ", i))
	}
	r = call(t, "POST", "/api/v1/notifications/batch", map[string]any{"notifications": full})
	expectStatus(t, r, http.StatusCreated)
	if n := countRows(t, "SELECT count(*) FROM notifications WHERE batch_id = $1", r.Body["batch_id"]); n != 1000 {
		t.Errorf("rows = %d, want 1000", n)
	}

	r = call(t, "POST", "/api/v1/notifications/batch", map[string]any{"notifications": append(full, sms("one too many"))})
	expectStatus(t, r, http.StatusUnprocessableEntity)
}

func TestBatch_Idempotency(t *testing.T) {
	db.Reset(t)
	body := map[string]any{"notifications": []any{sms("a"), sms("b")}}
	first := call(t, "POST", "/api/v1/notifications/batch", body, "Idempotency-Key", "campaign-7")
	expectStatus(t, first, http.StatusCreated)

	replay := call(t, "POST", "/api/v1/notifications/batch", body, "Idempotency-Key", "campaign-7")
	expectStatus(t, replay, http.StatusOK)
	if replay.Body["batch_id"] != first.Body["batch_id"] ||
		fmt.Sprint(replay.Body["items"]) != fmt.Sprint(first.Body["items"]) {
		t.Errorf("replay = %v, want same batch and item ids as %v", replay.Body, first.Body)
	}

	conflict := call(t, "POST", "/api/v1/notifications/batch",
		map[string]any{"notifications": []any{sms("a")}}, "Idempotency-Key", "campaign-7")
	expectStatus(t, conflict, http.StatusConflict)

	if n := countRows(t, "SELECT count(*) FROM notifications"); n != 2 {
		t.Errorf("rows = %d, want 2", n)
	}
}

func TestBatch_NotFound(t *testing.T) {
	r := call(t, "GET", "/api/v1/batches/0190a8c4-0000-7000-8000-000000000000", nil)
	expectStatus(t, r, http.StatusNotFound)
}

// Cancel and a worker claim race on the same row; both are conditional
// UPDATEs, so exactly one must win every time.
func TestCancel_RacesWithWorkerClaim(t *testing.T) {
	db.Reset(t)
	for i := range 30 {
		id := call(t, "POST", "/api/v1/notifications", sms(fmt.Sprint("race ", i))).Body["id"].(string)

		var wg sync.WaitGroup
		var cancelStatus int
		var claimed int64
		wg.Add(2)
		go func() {
			defer wg.Done()
			cancelStatus = call(t, "POST", "/api/v1/notifications/"+id+"/cancel", nil).Status
		}()
		go func() {
			defer wg.Done()
			tag, err := db.Pool.Exec(context.Background(), `
				UPDATE notifications SET status = 'processing', locked_until = now() + interval '30 seconds'
				WHERE id = $1 AND status IN ('scheduled', 'pending', 'retrying')`, id)
			if err != nil {
				t.Error(err)
				return
			}
			claimed = tag.RowsAffected()
		}()
		wg.Wait()

		cancelWon := cancelStatus == http.StatusOK
		claimWon := claimed == 1
		if cancelWon == claimWon {
			t.Fatalf("iteration %d: cancel=%d claimed=%d; exactly one should win", i, cancelStatus, claimed)
		}
		if !cancelWon && cancelStatus != http.StatusConflict {
			t.Fatalf("iteration %d: losing cancel returned %d, want 409", i, cancelStatus)
		}
	}
}

// --- manual retry ---

func TestRetry_RequeuesFailedWithFreshBudget(t *testing.T) {
	db.Reset(t)
	id := call(t, "POST", "/api/v1/notifications", sms("hi")).Body["id"].(string)
	// Simulate the worker exhausting 5 attempts.
	if _, err := db.Pool.Exec(context.Background(), `UPDATE notifications
		SET status = 'failed', attempts = 5, last_error = 'giving up after 5 attempts' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}

	r := call(t, "POST", "/api/v1/notifications/"+id+"/retry", nil)
	expectStatus(t, r, http.StatusOK)
	// attempts keeps counting (history and fencing); the budget moves up.
	if r.Body["status"] != "pending" || r.Body["attempts"] != 5.0 || r.Body["max_attempts"] != 10.0 {
		t.Errorf("after retry: %v", r.Body)
	}
	if r.Body["next_attempt_at"] == nil {
		t.Error("a re-queued notification should expose next_attempt_at")
	}

	again := call(t, "POST", "/api/v1/notifications/"+id+"/retry", nil)
	expectStatus(t, again, http.StatusConflict)
	if errorCode(again) != "invalid_state" {
		t.Errorf("code = %q", errorCode(again))
	}
}

func TestRetry_OnlyFromFailed(t *testing.T) {
	db.Reset(t)
	for _, status := range []string{"pending", "processing", "sent", "cancelled"} {
		id := call(t, "POST", "/api/v1/notifications", sms(status)).Body["id"].(string)
		forceStatus(t, id, status)
		r := call(t, "POST", "/api/v1/notifications/"+id+"/retry", nil)
		if r.Status != http.StatusConflict {
			t.Errorf("retry from %s: status %d, want 409", status, r.Status)
		}
	}
	r := call(t, "POST", "/api/v1/notifications/0190a8c4-0000-7000-8000-000000000000/retry", nil)
	expectStatus(t, r, http.StatusNotFound)
}

// --- observability ---

func getText(t *testing.T, path string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

// metricValue returns the value of one exact series line in /metrics output
// (0 if absent).
func metricValue(t *testing.T, body, series string) float64 {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, series+" ") {
			var v float64
			if _, err := fmt.Sscanf(strings.TrimPrefix(line, series+" "), "%g", &v); err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
			return v
		}
	}
	return 0
}

func TestMetricsEndpoint(t *testing.T) {
	db.Reset(t)
	const created = `notifications_created_total{channel="email",priority="low"}`
	const posted = `http_requests_total{method="POST",route="POST /api/v1/notifications",status="201"}`
	before := getText(t, "/metrics")

	body := map[string]any{"channel": "email", "recipient": "m@example.com", "content": "hi", "priority": "low"}
	first := call(t, "POST", "/api/v1/notifications", body, "Idempotency-Key", "metrics-1")
	call(t, "POST", "/api/v1/notifications", body, "Idempotency-Key", "metrics-1") // replay: not counted
	call(t, "POST", "/api/v1/notifications/batch", map[string]any{"notifications": []any{body, body}})
	call(t, "GET", "/api/v1/notifications/"+first.Body["id"].(string), nil)

	after := getText(t, "/metrics")
	if d := metricValue(t, after, created) - metricValue(t, before, created); d != 3 {
		t.Errorf("created delta = %v, want 3 (1 single + 2 batch, replay excluded)", d)
	}
	if d := metricValue(t, after, posted) - metricValue(t, before, posted); d != 1 {
		t.Errorf("POST 201 delta = %v, want 1", d)
	}
	if !strings.Contains(after, `route="GET /api/v1/notifications/{id}"`) {
		t.Error("GET by id should be labelled with the route pattern")
	}
	if strings.Contains(after, first.Body["id"].(string)) {
		t.Error("a notification id leaked into metric labels (unbounded cardinality)")
	}
	if !strings.Contains(after, "go_goroutines") {
		t.Error("runtime collectors missing")
	}
}

func TestMetricsSummary(t *testing.T) {
	db.Reset(t)
	ctx := context.Background()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := db.Pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	insert := func(channel, status string, ageMS int, sent bool) string {
		t.Helper()
		var id string
		sentAt := "NULL"
		if sent {
			sentAt = "now()"
		}
		if err := db.Pool.QueryRow(ctx, `
			INSERT INTO notifications (correlation_id, channel, recipient, content, status, attempts,
				created_at, sent_at, next_attempt_at)
			VALUES ('c', $1, 'r', 'x', $2, 1, now() - $3 * interval '1 millisecond', `+sentAt+`, now())
			RETURNING id`, channel, status, ageMS).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	attempt := func(id string, durationMS int, outcome string, age string) {
		exec(`INSERT INTO delivery_attempts (notification_id, attempt_no, started_at, duration_ms, outcome)
			VALUES ($1, (SELECT count(*) + 1 FROM delivery_attempts WHERE notification_id = $1),
			        now() - $4::interval, $2, $3)`, id, durationMS, outcome, age)
	}

	// In the window: 4 sms sent (end-to-end 1s..4s) and 1 failed.
	durations := []int{10, 20, 30, 40}
	for i := range 4 {
		id := insert("sms", "sent", (i+1)*1000, true)
		attempt(id, durations[i], "success", "1 minute")
	}
	failed := insert("sms", "failed", 0, false)
	attempt(failed, 1000, "permanent_error", "1 minute")
	// A call lost to a worker crash: counted, but its unknown (NULL)
	// duration must not distort the latency percentiles.
	exec(`INSERT INTO delivery_attempts (notification_id, attempt_no, started_at, outcome)
		VALUES ($1, 2, now() - interval '1 minute', 'interrupted')`, failed)
	// Queue: 2 pending email, 1 retrying push.
	insert("email", "pending", 0, false)
	insert("email", "pending", 0, false)
	insert("push", "retrying", 0, false)
	// Outside the 1h window: must be ignored.
	old := insert("sms", "sent", 0, true)
	exec(`UPDATE notifications SET sent_at = now() - interval '2 hours' WHERE id = $1`, old)
	attempt(old, 99999, "success", "2 hours")

	r := call(t, "GET", "/api/v1/metrics/summary?window=1h", nil)
	expectStatus(t, r, http.StatusOK)
	overall := r.Body["overall"].(map[string]any)
	attempts := overall["attempts"].(map[string]any)
	provider := overall["provider_latency_ms"].(map[string]any)
	e2e := overall["end_to_end_latency_ms"].(map[string]any)

	// percentile_cont over [10,20,30,40,1000]: p50 = 30, p95 = 40 + 0.8*960 = 808.
	// End-to-end over [1000,2000,3000,4000]: p50 = 2500, p95 = 3000 + 0.85*1000 = 3850.
	checks := map[string][2]any{
		"sent":                  {overall["sent"], 4.0},
		"failed":                {overall["failed"], 1.0},
		"success_rate":          {overall["success_rate"], 0.8},
		"attempts.total":        {attempts["total"], 6.0},
		"attempts.permanent":    {attempts["permanent_error"], 1.0},
		"attempts.interrupted":  {attempts["interrupted"], 1.0},
		"attempts.success_rate": {attempts["success_rate"], 0.6667},
		"provider.p50":          {provider["p50"], 30.0},
		"provider.p95":          {provider["p95"], 808.0},
		"e2e.p50":               {e2e["p50"], 2500.0},
		"e2e.p95":               {e2e["p95"], 3850.0},
		"window":                {r.Body["window"], "1h0m0s"},
	}
	for name, c := range checks {
		if c[0] != c[1] {
			t.Errorf("%s = %v, want %v", name, c[0], c[1])
		}
	}

	email := r.Body["by_channel"].(map[string]any)["email"].(map[string]any)
	if email["success_rate"] != nil || email["provider_latency_ms"].(map[string]any)["p50"] != nil {
		t.Errorf("email has no data in the window; rates and latencies must be null: %v", email)
	}
	depth := r.Body["queue_depth"].(map[string]any)
	byChannel := depth["by_channel"].(map[string]any)
	if depth["total"] != 3.0 ||
		byChannel["email"].(map[string]any)["pending"] != 2.0 ||
		byChannel["push"].(map[string]any)["retrying"] != 1.0 {
		t.Errorf("queue_depth = %v", depth)
	}

	for _, bad := range []string{"soon", "10s", "800h"} {
		if r := call(t, "GET", "/api/v1/metrics/summary?window="+bad, nil); r.Status != http.StatusBadRequest {
			t.Errorf("window=%s: status %d, want 400", bad, r.Status)
		}
	}
}

// --- templates ---

func createTemplate(t *testing.T, name, channel, body string) string {
	t.Helper()
	r := call(t, "POST", "/api/v1/templates", map[string]any{"name": name, "channel": channel, "body": body})
	expectStatus(t, r, http.StatusCreated)
	return r.Body["id"].(string)
}

func TestTemplates_CreateAndGet(t *testing.T) {
	db.Reset(t)
	r := call(t, "POST", "/api/v1/templates", map[string]any{
		"name": "order-shipped", "channel": "sms",
		"body": "Hi {{.first_name}}, order {{.order_id}} has shipped.",
	})
	expectStatus(t, r, http.StatusCreated)
	if fmt.Sprint(r.Body["variables"]) != "[first_name order_id]" {
		t.Errorf("variables = %v", r.Body["variables"])
	}
	got := call(t, "GET", "/api/v1/templates/"+r.Body["id"].(string), nil)
	expectStatus(t, got, http.StatusOK)
	if got.Body["name"] != "order-shipped" || got.Body["body"] != r.Body["body"] {
		t.Errorf("get = %v", got.Body)
	}

	dup := call(t, "POST", "/api/v1/templates", map[string]any{"name": "order-shipped", "channel": "sms", "body": "x"})
	expectStatus(t, dup, http.StatusConflict)
	if errorCode(dup) != "template_name_taken" {
		t.Errorf("code = %q", errorCode(dup))
	}

	bad := call(t, "POST", "/api/v1/templates", map[string]any{"name": "", "channel": "fax", "body": "{{.unclosed"})
	expectStatus(t, bad, http.StatusUnprocessableEntity)
	if n := len(bad.Body["error"].(map[string]any)["details"].([]any)); n != 3 {
		t.Errorf("want name, channel and body errors, got %d: %v", n, bad.Body)
	}
	nul := call(t, "POST", "/api/v1/templates", map[string]any{"name": "nul", "channel": "sms", "body": "Hi\x00"})
	expectStatus(t, nul, http.StatusUnprocessableEntity)

	expectStatus(t, call(t, "GET", "/api/v1/templates/0190a8c4-0000-7000-8000-000000000000", nil), http.StatusNotFound)
}

func TestTemplates_RenderIntoNotification(t *testing.T) {
	db.Reset(t)
	tid := createTemplate(t, "welcome", "email", "Welcome {{.first_name}}! {{if .vip}}Enjoy free shipping.{{end}}")

	r := call(t, "POST", "/api/v1/notifications", map[string]any{
		"channel": "email", "recipient": "ayse@example.com",
		"template_id": tid, "variables": map[string]any{"first_name": "Ayşe", "vip": true},
	})
	expectStatus(t, r, http.StatusCreated)
	if r.Body["content"] != "Welcome Ayşe! Enjoy free shipping." || r.Body["template_id"] != tid {
		t.Errorf("rendered notification = %v", r.Body)
	}
	// The stored row holds the final text, so workers never render.
	var stored string
	if err := db.Pool.QueryRow(context.Background(), "SELECT content FROM notifications WHERE id = $1",
		r.Body["id"]).Scan(&stored); err != nil || stored != "Welcome Ayşe! Enjoy free shipping." {
		t.Errorf("stored content = %q, %v", stored, err)
	}
}

func TestTemplates_Rejections(t *testing.T) {
	db.Reset(t)
	smsTpl := createTemplate(t, "otp", "sms", "Code {{.code}} for {{.name}}")
	longTpl := createTemplate(t, "long", "sms", "{{.text}} - from Insider")

	tests := []struct {
		name      string
		body      map[string]any
		wantField string
	}{
		{"missing variables listed", map[string]any{"channel": "sms", "recipient": "+905551234567",
			"template_id": smsTpl, "variables": map[string]any{"name": "x"}}, "variables"},
		{"channel mismatch", map[string]any{"channel": "push", "recipient": "tok",
			"template_id": smsTpl, "variables": map[string]any{"code": 1, "name": "x"}}, "template_id"},
		{"unknown template", map[string]any{"channel": "sms", "recipient": "+905551234567",
			"template_id": "0190a8c4-0000-7000-8000-000000000000"}, "template_id"},
		{"content and template", map[string]any{"channel": "sms", "recipient": "+905551234567", "content": "hi",
			"template_id": smsTpl, "variables": map[string]any{"code": 1, "name": "x"}}, "content"},
		{"variables without template", map[string]any{"channel": "sms", "recipient": "+905551234567",
			"content": "hi", "variables": map[string]any{"a": 1}}, "variables"},
		{"rendered output too long for sms", map[string]any{"channel": "sms", "recipient": "+905551234567",
			"template_id": longTpl, "variables": map[string]any{"text": strings.Repeat("x", 150)}}, "content"},
		{"NUL in an unused variable", map[string]any{"channel": "sms", "recipient": "+905551234567",
			"template_id": smsTpl, "variables": map[string]any{"code": 1, "name": "x", "extra": "\x00"}}, "variables"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := call(t, "POST", "/api/v1/notifications", tt.body)
			expectStatus(t, r, http.StatusUnprocessableEntity)
			details := r.Body["error"].(map[string]any)["details"].([]any)
			if len(details) != 1 || details[0].(map[string]any)["field"] != tt.wantField {
				t.Errorf("details = %v, want exactly one %q error", details, tt.wantField)
			}
		})
	}
	r := call(t, "POST", "/api/v1/notifications", tests[0].body)
	if msg := r.Body["error"].(map[string]any)["details"].([]any)[0].(map[string]any)["message"]; msg != "missing required variables: code" {
		t.Errorf("message = %v", msg)
	}
}

func TestTemplates_InBatchAndIdempotency(t *testing.T) {
	db.Reset(t)
	tid := createTemplate(t, "flash", "push", "{{.first_name}}, flash sale ends at {{.hour}}:00!")
	item := func(name string) map[string]any {
		return map[string]any{"channel": "push", "recipient": "tok-" + name, "template_id": tid,
			"variables": map[string]any{"first_name": name, "hour": 18}}
	}
	bad := map[string]any{"channel": "push", "recipient": "tok-x", "template_id": tid,
		"variables": map[string]any{"first_name": "x"}}

	r := call(t, "POST", "/api/v1/notifications/batch", map[string]any{"notifications": []any{item("Can"), bad}})
	expectStatus(t, r, http.StatusUnprocessableEntity)
	if d := r.Body["error"].(map[string]any)["details"].([]any)[0].(map[string]any); d["index"] != 1.0 || d["field"] != "variables" {
		t.Errorf("batch template error = %v", d)
	}

	body := map[string]any{"notifications": []any{item("Can"), item("Elif")}}
	first := call(t, "POST", "/api/v1/notifications/batch", body, "Idempotency-Key", "flash-1")
	expectStatus(t, first, http.StatusCreated)
	expectStatus(t, call(t, "POST", "/api/v1/notifications/batch", body, "Idempotency-Key", "flash-1"), http.StatusOK)

	changed := map[string]any{"notifications": []any{item("Can"), item("Deniz")}}
	expectStatus(t, call(t, "POST", "/api/v1/notifications/batch", changed, "Idempotency-Key", "flash-1"), http.StatusConflict)

	id := first.Body["items"].([]any)[1].(map[string]any)["id"].(string)
	if got := call(t, "GET", "/api/v1/notifications/"+id, nil).Body["content"]; got != "Elif, flash sale ends at 18:00!" {
		t.Errorf("content = %v", got)
	}
}

// --- status events ---

// recordingPublisher captures published events.
type recordingPublisher struct {
	mu  sync.Mutex
	evs []events.Event
}

func (r *recordingPublisher) Publish(_ context.Context, evs ...events.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evs = append(r.evs, evs...)
}

func (r *recordingPublisher) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.evs))
	for i, ev := range r.evs {
		out[i] = ev.Status
	}
	r.evs = nil
	return out
}

func TestStatusEvents_PublishedByAPI(t *testing.T) {
	db.Reset(t)
	recorder.take()

	id := call(t, "POST", "/api/v1/notifications", sms("hi"), "Idempotency-Key", "ev-1").Body["id"].(string)
	call(t, "POST", "/api/v1/notifications", sms("hi"), "Idempotency-Key", "ev-1") // replay: no event
	call(t, "POST", "/api/v1/notifications/"+id+"/cancel", nil)
	call(t, "POST", "/api/v1/notifications/"+id+"/cancel", nil) // 409: no event
	later := sms("later")
	later["scheduled_at"] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	call(t, "POST", "/api/v1/notifications/batch", map[string]any{"notifications": []any{sms("a"), later}})

	if got := strings.Join(recorder.take(), ","); got != "pending,cancelled,pending,scheduled" {
		t.Errorf("published statuses = %s", got)
	}
}

func TestStatusStream_DisabledWithoutHub(t *testing.T) {
	// The test router has no hub (Redis), so the endpoint reports itself
	// unavailable instead of hanging; the response is checked against the spec.
	if r := call(t, "GET", "/ws", nil); r.Status != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", r.Status)
	}
}

// --- tracing ---

// An incoming W3C traceparent is continued by the API and stored on the row,
// so the worker can join the same trace later.
func TestTracing_TraceParentStoredOnCreate(t *testing.T) {
	db.Reset(t)
	tp := sdktrace.NewTracerProvider()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })

	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	r := call(t, "POST", "/api/v1/notifications", sms("traced"),
		"Traceparent", "00-"+traceID+"-00f067aa0ba902b7-01")
	expectStatus(t, r, http.StatusCreated)

	var stored *string
	if err := db.Pool.QueryRow(context.Background(),
		"SELECT trace_parent FROM notifications WHERE id = $1", r.Body["id"]).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored == nil || !strings.Contains(*stored, traceID) {
		t.Errorf("stored trace_parent = %v, want trace %s", stored, traceID)
	}
}
