package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"notification-service/internal/domain"
)

const uniqueViolation = "23505"

// InsertTemplate stores a new template. A duplicate name returns
// domain.ErrTemplateNameTaken (the unique index arbitrates races).
func (s *Store) InsertTemplate(ctx context.Context, t domain.Template) (domain.Template, error) {
	err := s.pool.QueryRow(ctx, `
		INSERT INTO templates (id, name, channel, body) VALUES ($1, $2, $3, $4)
		RETURNING created_at`, t.ID, t.Name, t.Channel, t.Body).Scan(&t.CreatedAt)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		return domain.Template{}, domain.ErrTemplateNameTaken
	}
	if err != nil {
		return domain.Template{}, fmt.Errorf("insert template: %w", err)
	}
	return t, nil
}

// GetTemplate returns domain.ErrNotFound when id does not exist.
func (s *Store) GetTemplate(ctx context.Context, id uuid.UUID) (domain.Template, error) {
	var t domain.Template
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, channel, body, created_at FROM templates WHERE id = $1`, id).
		Scan(&t.ID, &t.Name, &t.Channel, &t.Body, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Template{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Template{}, fmt.Errorf("get template: %w", err)
	}
	return t, nil
}
