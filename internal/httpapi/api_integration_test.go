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

	"notification-service/internal/httpapi"
	"notification-service/internal/notification"
	"notification-service/internal/store"
	"notification-service/internal/testutil"
	"notification-service/internal/validation"
)

var (
	db     *testutil.Postgres
	server *httptest.Server
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
	server = httptest.NewServer(httpapi.NewRouter(svc, slog.New(slog.NewTextHandler(io.Discard, nil))))

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
	out := response{Status: resp.StatusCode, Header: resp.Header, Body: map[string]any{}}
	if err := json.NewDecoder(resp.Body).Decode(&out.Body); err != nil && err != io.EOF {
		t.Fatalf("%s %s: decode body: %v", method, path, err)
	}
	return out
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

func TestCancel_NotAllowedOnceProcessingOrSent(t *testing.T) {
	for _, status := range []string{"processing", "sent", "failed"} {
		t.Run(status, func(t *testing.T) {
			db.Reset(t)
			id := call(t, "POST", "/api/v1/notifications", sms("hi")).Body["id"].(string)
			// Simulate the worker having moved it on.
			if _, err := db.Pool.Exec(context.Background(),
				"UPDATE notifications SET status = $1 WHERE id = $2", status, id); err != nil {
				t.Fatal(err)
			}
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
		if _, err := db.Pool.Exec(context.Background(),
			"UPDATE notifications SET status = $1 WHERE id = $2", status, id); err != nil {
			t.Fatal(err)
		}
		r := call(t, "POST", "/api/v1/notifications/"+id+"/retry", nil)
		if r.Status != http.StatusConflict {
			t.Errorf("retry from %s: status %d, want 409", status, r.Status)
		}
	}
	r := call(t, "POST", "/api/v1/notifications/0190a8c4-0000-7000-8000-000000000000/retry", nil)
	expectStatus(t, r, http.StatusNotFound)
}
