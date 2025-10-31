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

### Job Scheduling

#### Schedule API Caller Job

**`POST /v1/schedule/api-caller`**

Schedule a job that calls an HTTP API endpoint at specified intervals.

**Request Body:**
```json
{
  "schedule": "0 0 * * *",
  "api": "https://example.com/webhook",
  "type": "ATMOST_ONCE"
}
```

**Request Fields:**
- `schedule` (string, required): Cron expression defining when the job runs
  - Examples:
    - `"0 0 * * *"` - Daily at midnight
    - `"*/5 * * * *"` - Every 5 minutes
    - `"0 9-17 * * MON-FRI"` - Every hour from 9 AM to 5 PM on weekdays
- `api` (string, required): HTTP/HTTPS URL to call when job executes
- `type` (string, required): Job execution guarantee
  - `"ATMOST_ONCE"` - Job executes at most once per scheduled interval (may skip on failures)
  - `"ATLEAST_ONCE"` - Job executes at least once per scheduled interval (retries on failures)

**Success Response (201 Created):**
```json
{
  "job_id": "a3f5c8d9e2b1f4a6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0",
  "schedule": "0 0 * * *",
  "api": "https://example.com/webhook",
  "type": "ATMOST_ONCE"
}
```

**Error Response (400 Bad Request):**
```json
{
  "error": {
    "code": "ERROR_CODE",
    "message": "Detailed error message",
    "field": "field_name"
  }
}
```

**Error Codes:**
- `INVALID_REQUEST` - Missing required fields or malformed JSON
- `VALIDATION_ERROR` - Invalid URL format or invalid type value
- `JOB_CREATION_FAILED` - Invalid cron expression or unsupported URL scheme

**Example Usage:**
```bash
curl -X POST http://localhost:8080/v1/schedule/api-caller \
  -H "Content-Type: application/json" \
  -d '{
    "schedule": "*/10 * * * *",
    "api": "https://api.example.com/status-check",
    "type": "ATLEAST_ONCE"
  }'
```

### Routes

- **`GET /`** - Root endpoint
  - Returns: `"Dronc - Redis-backed Job Scheduler"`

## Observability

### Track Executions

Retrieve recent execution events (useful for debugging job runs and latency analysis).

- **`GET /v1/track/executions`** - Returns recent execution events recorded in Redis streams.

Query parameters:
- `limit` (optional, positive integer): maximum number of events to return. Default: 50. Maximum: 200.
- `job_id` (optional): filter events to a specific job identifier.

Response shape:

```json
{
  "count": 2,
  "executions": [
    {
      "id": "169f3a8b-0b8c-...",
      "job_id": "capi:...",
      "scheduled_time": 1698764400,
      "execution_time": 1698764412,
      "status_code": 200,
      "success": true,
      "time_taken_ms": 28
    }
  ]
}
```

Notes:
- `time_taken_ms` is recorded for each execution and represents the elapsed time in milliseconds for the job's call.
- If Redis isn't initialized, the endpoint responds with a 500 and an error code `REDIS_NOT_INITIALIZED`.
- Invalid `limit` values return 400 with `INVALID_REQUEST`.


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
