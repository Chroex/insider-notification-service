//go:build integration

package ws_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"notification-service/internal/events"
	"notification-service/internal/testutil"
	"notification-service/internal/ws"
)

var rdb *testutil.Redis

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	if rdb, err = testutil.StartRedis(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "integration setup:", err)
		os.Exit(1)
	}
	code := m.Run()
	rdb.Close(ctx)
	os.Exit(code)
}

// Full path: publisher (as the worker uses it) -> Redis pub/sub -> hub ->
// WebSocket client with a batch filter.
func TestStatusStream_EndToEnd(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub := ws.NewHub(log)
	hubCtx, stopHub := context.WithCancel(context.Background())
	defer stopHub()
	go hub.Run(hubCtx, rdb.Client)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f, _, _ := ws.ParseFilter(r)
		hub.Serve(w, r, f, nil)
	}))
	defer srv.Close()

	batchA, batchB := uuid.New(), uuid.New()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"?batch_id="+batchA.String(), nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close() // handshake response; the socket stays open
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.CloseNow() }()
	waitFor(t, func() bool { return hub.Clients() == 1 })
	time.Sleep(100 * time.Millisecond) // let the Redis SUBSCRIBE settle

	pub := events.NewRedisPublisher(rdb.Client, log)
	n1, n2 := uuid.New(), uuid.New()
	pub.Publish(ctx,
		events.Event{NotificationID: uuid.New(), BatchID: &batchB, Channel: "sms", Status: "sent"}, // filtered out
		events.Event{NotificationID: n1, BatchID: &batchA, Channel: "sms", Status: "processing"},
		events.Event{NotificationID: n2, BatchID: &batchA, Channel: "email", Status: "sent"},
	)

	var got []string
	for range 2 {
		_, raw, err := conn.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var ev events.Event
		if err := json.Unmarshal(raw, &ev); err != nil {
			t.Fatal(err)
		}
		if *ev.BatchID != batchA {
			t.Errorf("received event for another batch: %+v", ev)
		}
		got = append(got, ev.NotificationID.String()+":"+ev.Status)
	}
	if want := n1.String() + ":processing," + n2.String() + ":sent"; strings.Join(got, ",") != want {
		t.Errorf("events = %v, want %s", got, want)
	}

	// Hub shutdown closes clients with "going away" instead of leaving them open.
	stopHub()
	_, _, err = conn.Read(ctx)
	if websocket.CloseStatus(err) != websocket.StatusGoingAway {
		t.Errorf("close status = %v (err %v), want 1001 going away", websocket.CloseStatus(err), err)
	}
	waitFor(t, func() bool { return hub.Clients() == 0 })
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
