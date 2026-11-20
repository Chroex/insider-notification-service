package store

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCursorRoundTrip(t *testing.T) {
	// Microsecond precision matches Postgres timestamptz; it must survive.
	in := Cursor{CreatedAt: time.Date(2026, 3, 4, 5, 6, 7, 123456000, time.UTC), ID: uuid.New()}
	out, err := DecodeCursor(in.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !out.CreatedAt.Equal(in.CreatedAt) || out.ID != in.ID {
		t.Errorf("round trip = %+v, want %+v", out, in)
	}
}

func TestDecodeCursor_Rejects(t *testing.T) {
	for _, token := range []string{
		"",
		"not base64!",
		"bm90LWpzb24",                          // "not-json"
		Cursor{CreatedAt: time.Now()}.Encode(), // nil id
		Cursor{ID: uuid.New()}.Encode(),        // zero time
	} {
		if _, err := DecodeCursor(token); !errors.Is(err, ErrInvalidCursor) {
			t.Errorf("DecodeCursor(%q) err = %v, want ErrInvalidCursor", token, err)
		}
	}
}
