# Dronc

A Redis-backed job scheduler built with Go.

## Purpose

Dronc is a distributed job scheduler that leverages Redis for persistent storage and queue management, enabling reliable task scheduling and execution.

## Project Structure

```
dronc/
├── scheduler/    # Core scheduler implementation
├── api/          # REST API layer for job scheduling
├── lib/          # Redis clients and external API clients
├── cmd/          # Application entry point
└── dist/         # Compiled binaries (ignored by git)
```

## Dependencies

- **Go 1.24+** - Programming language
- **Docker & Docker Compose** - Container orchestration
- **Redis 7.4** - Data store and message broker
- **go-redis/v9** - Redis client library
- **gin-gonic/gin** - HTTP web framework
- **uber-go/zap** - Structured logging

## Makefile Commands

| Command | Description |
|---------|-------------|
| `make build` | Download dependencies and build the binary |
| `make build-only` | Build binary without downloading dependencies |
| `make run` | Build and run the application |
| `make deps` | Download and tidy Go dependencies |
| `make test` | Run all tests |
| `make lint` | Run golangci-lint |
| `make lint-fix` | Run golangci-lint with auto-fix |
| `make fmt` | Format code with gofmt and goimports |
| `make clean` | Remove build artifacts (dist/) |
| `make clean-all` | Remove build artifacts and Go module cache |

## API Endpoints

### Health Checks

- **`GET /health`** - Basic health check
  - Returns: `{"status": "healthy"}` if dronc is up and running

- **`GET /health/deep`** - Deep health check with Redis connectivity
  - Returns: `{"status": "healthy", "checks": {"redis": "healthy"}}` if dronc and all its dependencies are healthy
  - Returns 503 if Redis is unreachable

### Routes

- **`GET /`** - Root endpoint
  - Returns: `"Dronc - Redis-backed Job Scheduler"`

## Quick Start

### Local Development
```bash
make build
make run
```

### Docker Deployment
```bash
docker-compose up
```

The application will be available at `http://localhost:8080` with Redis data persisted to `./data/redis/`.
