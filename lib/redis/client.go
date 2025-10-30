package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/shreyas/dronc/lib/env"
)

var Client *redis.Client

// Initialize sets up the Redis client with connection pool
func Initialize(ctx context.Context) error {
	Client = redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", env.RedisHost(), env.RedisPort()),
		Password:     env.RedisPassword(),
		DB:           env.RedisDB(),
		PoolSize:     env.RedisPoolSize(),
		MinIdleConns: 2,
	})

	// Test connection
	if err := Client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return nil
}

// Close closes the Redis client connection
func Close() error {
	if Client != nil {
		return Client.Close()
	}
	return nil
}

// Ping checks if Redis is reachable
func Ping(ctx context.Context) error {
	if Client == nil {
		return fmt.Errorf("redis client not initialized")
	}
	return Client.Ping(ctx).Err()
}
