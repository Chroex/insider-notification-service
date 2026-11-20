package store

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Cursor is a keyset pagination position: the (created_at, id) of the last
// row on the previous page. id breaks ties between rows created in the same
// transaction (a batch shares one created_at).
//
// Why keyset over OFFSET: OFFSET n makes Postgres read and discard n rows,
// so deep pages get slower linearly; and when new rows are inserted while a
// client pages, OFFSET shifts and the client sees duplicates or skips rows.
// A keyset cursor seeks straight to the position via the index and is stable
// under concurrent inserts.
type Cursor struct {
	CreatedAt time.Time `json:"t"`
	ID        uuid.UUID `json:"id"`
}

var ErrInvalidCursor = errors.New("invalid cursor")

// Encode returns an opaque, URL-safe token. Clients must treat it as opaque
// so the format can change without breaking them.
func (c Cursor) Encode() string {
	c.CreatedAt = c.CreatedAt.UTC() // token independent of the server's time zone
	raw, _ := json.Marshal(c)       // cannot fail for this struct
	return base64.RawURLEncoding.EncodeToString(raw)
}

// DecodeCursor parses a token produced by Encode.
func DecodeCursor(token string) (Cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return Cursor{}, ErrInvalidCursor
	}
	var c Cursor
	if err := json.Unmarshal(raw, &c); err != nil || c.ID == uuid.Nil || c.CreatedAt.IsZero() {
		return Cursor{}, ErrInvalidCursor
	}
	return c, nil
}
