//go:build integration

// Package testutil starts disposable infrastructure for integration tests.
// It is compiled only with -tags=integration so unit builds never depend on
// Docker.
package testutil

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"notification-service/migrations"
)

// PostgresImage matches the version used in compose.
const PostgresImage = "postgres:16-alpine"

// Postgres is a running, migrated test database.
type Postgres struct {
	Pool      *pgxpool.Pool
	terminate func(context.Context) error
}

// StartPostgres runs a fresh Postgres container and applies every *.up.sql
// migration in order. Call Close when done (typically from TestMain).
func StartPostgres(ctx context.Context) (*Postgres, error) {
	container, err := tcpostgres.Run(ctx, PostgresImage,
		tcpostgres.WithDatabase("notifications"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, fmt.Errorf("start postgres container: %w", err)
	}
	terminate := func(ctx context.Context) error { return container.Terminate(ctx) }

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = terminate(ctx)
		return nil, fmt.Errorf("connection string: %w", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		_ = terminate(ctx)
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := applyMigrations(ctx, pool); err != nil {
		pool.Close()
		_ = terminate(ctx)
		return nil, err
	}
	return &Postgres{Pool: pool, terminate: terminate}, nil
}

// applyMigrations executes the same SQL files golang-migrate runs in compose.
// Exec without arguments uses the simple protocol, which allows multiple
// statements per file.
func applyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	files, err := fs.Glob(migrations.FS, "*.up.sql")
	if err != nil {
		return err
	}
	sort.Strings(files) // zero-padded version prefixes sort correctly
	for _, name := range files {
		sql, err := fs.ReadFile(migrations.FS, name)
		if err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
	}
	return nil
}

// Reset empties every table so each test starts from a clean state.
func (p *Postgres) Reset(t testing.TB) {
	t.Helper()
	tables := []string{"delivery_attempts", "notifications", "batches", "templates"}
	if _, err := p.Pool.Exec(context.Background(),
		"TRUNCATE "+strings.Join(tables, ", ")+" RESTART IDENTITY CASCADE"); err != nil {
		t.Fatalf("reset database: %v", err)
	}
}

// Close releases the pool and removes the container.
func (p *Postgres) Close(ctx context.Context) {
	p.Pool.Close()
	_ = p.terminate(ctx)
}
