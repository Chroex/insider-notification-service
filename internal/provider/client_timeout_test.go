package provider_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"notification-service/internal/provider"
)

// H-maxsend: the limiter assumes a send never lasts longer than
// PROVIDER_TIMEOUT + 1s. Check the slow paths: headers fast then a body
// trickled forever, and a server that accepts but never answers.
func TestSend_IsBoundedByTimeoutEvenWhenTheServerStalls(t *testing.T) {
	trickle := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		f := w.(http.Flusher)
		for range 100 {
			_, _ = w.Write([]byte(" "))
			f.Flush()
			select {
			case <-time.After(100 * time.Millisecond):
			case <-r.Context().Done():
				return
			}
		}
	}))
	defer trickle.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0") // never accepted
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	for name, url := range map[string]string{"trickle": trickle.URL, "hang": "http://" + ln.Addr().String()} {
		c := provider.NewClient(url, 300*time.Millisecond, http.StatusAccepted)
		start := time.Now()
		_, err := c.Send(context.Background(), provider.Message{To: "x", Channel: "sms", Content: "y"})
		if d := time.Since(start); d > 800*time.Millisecond {
			t.Errorf("%s: Send took %v with a 300ms timeout (err %v)", name, d, err)
		}
	}
}
