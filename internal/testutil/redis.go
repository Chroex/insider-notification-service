//go:build integration

package testutil

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// RedisImage matches the version used in compose.
const RedisImage = "redis:7-alpine"

// Redis is a running test Redis.
type Redis struct {
	Client    *goredis.Client
	terminate func(context.Context) error
}

// StartRedis runs a fresh Redis container.
func StartRedis(ctx context.Context) (*Redis, error) {
	container, err := tcredis.Run(ctx, RedisImage)
	if err != nil {
		return nil, fmt.Errorf("start redis container: %w", err)
	}
	terminate := func(ctx context.Context) error { return container.Terminate(ctx) }

	url, err := container.ConnectionString(ctx)
	if err != nil {
		_ = terminate(ctx)
		return nil, fmt.Errorf("redis connection string: %w", err)
	}
	opts, err := goredis.ParseURL(url)
	if err != nil {
		_ = terminate(ctx)
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	client := goredis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		_ = terminate(ctx)
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return &Redis{Client: client, terminate: terminate}, nil
}

// Close releases the client and removes the container.
func (r *Redis) Close(ctx context.Context) {
	_ = r.Client.Close()
	_ = r.terminate(ctx)
}
