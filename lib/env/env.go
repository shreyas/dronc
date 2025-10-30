package env

import (
	"os"
	"strconv"
)

// RedisHost returns the Redis host from environment
func RedisHost() string {
	if host := os.Getenv("REDIS_HOST"); host != "" {
		return host
	}
	return "localhost"
}

// RedisPort returns the Redis port from environment
func RedisPort() string {
	if port := os.Getenv("REDIS_PORT"); port != "" {
		return port
	}
	return "6379"
}

// RedisPassword returns the Redis password from environment
func RedisPassword() string {
	return os.Getenv("REDIS_PASSWORD")
}

// RedisDB returns the Redis database number from environment
func RedisDB() int {
	if db := os.Getenv("REDIS_DB"); db != "" {
		if dbNum, err := strconv.Atoi(db); err == nil {
			return dbNum
		}
	}
	return 0
}

// RedisPoolSize returns the Redis connection pool size from environment
func RedisPoolSize() int {
	if poolSize := os.Getenv("REDIS_POOL_SIZE"); poolSize != "" {
		if size, err := strconv.Atoi(poolSize); err == nil {
			return size
		}
	}
	return 10
}

// HTTPPort returns the HTTP server port from environment
func HTTPPort() string {
	if port := os.Getenv("HTTP_PORT"); port != "" {
		return port
	}
	return "80"
}
