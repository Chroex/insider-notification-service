package notification

import (
	"context"
	"errors"
	"testing"
	"time"

	"notification-service/internal/domain"
	"notification-service/internal/store"
	"notification-service/internal/validation"
)

// fakeStore records inserts and simulates the unique idempotency index.
type fakeStore struct {
	Store   // unimplemented methods panic if called
	byKey   map[string]domain.Notification
	inserts int
}

func (f *fakeStore) InsertNotification(_ context.Context, n domain.Notification) (domain.Notification, bool, error) {
	f.inserts++
	if n.IdempotencyKey != nil {
		if existing, ok := f.byKey[*n.IdempotencyKey]; ok {
			return existing, false, nil
		}
		f.byKey[*n.IdempotencyKey] = n
	}
	return n, true, nil
}

func newTestService() (*Service, *fakeStore) {
	fs := &fakeStore{byKey: map[string]domain.Notification{}}
	svc := NewService(fs, Config{Rules: validation.DefaultRules, BatchMaxSize: 3, MaxAttempts: 5})
	svc.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
	return svc, fs
}

var validSMS = CreateInput{Channel: "sms", Recipient: "+905551234567", Content: "hi"}

func TestCreate_AppliesDefaults(t *testing.T) {
	svc, _ := newTestService()
	n, created, err := svc.Create(context.Background(), validSMS, "", "corr-1")
	if err != nil || !created {
		t.Fatalf("Create = %v, %v", created, err)
	}
	if n.Priority != domain.PriorityNormal || n.Status != domain.StatusPending ||
		n.MaxAttempts != 5 || n.CorrelationID != "corr-1" || n.ID.Version() != 7 {
		t.Errorf("unexpected defaults: %+v", n)
	}
}

func TestCreate_ScheduledIsClaimableAtScheduledTime(t *testing.T) {
	svc, _ := newTestService()
	at := svc.now().Add(time.Hour)
	in := validSMS
	in.ScheduledAt = &at
	n, _, err := svc.Create(context.Background(), in, "", "c")
	if err != nil {
		t.Fatal(err)
	}
	if n.Status != domain.StatusScheduled || !n.NextAttemptAt.Equal(at) {
		t.Errorf("status=%s next_attempt_at=%v, want scheduled at %v", n.Status, n.NextAttemptAt, at)
	}
}

func TestCreate_InvalidInputNeverReachesStore(t *testing.T) {
	svc, fs := newTestService()
	_, _, err := svc.Create(context.Background(), CreateInput{Channel: "sms"}, "", "c")
	var ve *domain.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("err = %v, want ValidationError", err)
	}
	if fs.inserts != 0 {
		t.Errorf("store called %d times for invalid input", fs.inserts)
	}
}

func TestCreate_Idempotency(t *testing.T) {
	svc, _ := newTestService()
	ctx := context.Background()

	first, created, err := svc.Create(ctx, validSMS, "key-1", "c")
	if err != nil || !created {
		t.Fatalf("first create = %v, %v", created, err)
	}

	replay, created, err := svc.Create(ctx, validSMS, "key-1", "c")
	if err != nil || created || replay.ID != first.ID {
		t.Errorf("replay = id %s created %v err %v; want original id, created=false", replay.ID, created, err)
	}

	different := validSMS
	different.Content = "something else"
	if _, _, err := svc.Create(ctx, different, "key-1", "c"); !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Errorf("different payload err = %v, want ErrIdempotencyConflict", err)
	}
}

func TestCreateBatch_ReportsEveryFailingIndex(t *testing.T) {
	svc, _ := newTestService()
	items := []CreateInput{validSMS, {Channel: "sms", Recipient: "bad", Content: "x"}, {Channel: "fax", Content: "x"}}
	_, _, err := svc.CreateBatch(context.Background(), items, "", "c")
	var ve *domain.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("err = %v, want ValidationError", err)
	}
	var indexes []int
	for _, f := range ve.Fields {
		indexes = append(indexes, *f.Index)
	}
	if len(indexes) != 2 || indexes[0] != 1 || indexes[1] != 2 {
		t.Errorf("failing indexes = %v, want [1 2]", indexes)
	}
}

func TestCreateBatch_SizeLimits(t *testing.T) {
	svc, _ := newTestService()
	for _, n := range []int{0, 4} { // BatchMaxSize is 3 in tests
		items := make([]CreateInput, n)
		for i := range items {
			items[i] = validSMS
		}
		var ve *domain.ValidationError
		if _, _, err := svc.CreateBatch(context.Background(), items, "", "c"); !errors.As(err, &ve) || ve.Fields[0].Field != "notifications" {
			t.Errorf("batch of %d: err = %v, want notifications size error", n, err)
		}
	}
}

// Compile-time check that the real store satisfies the interface.
var _ Store = (*store.Store)(nil)
