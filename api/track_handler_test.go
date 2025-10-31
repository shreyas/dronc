package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	redisClient "github.com/shreyas/dronc/lib/redis"
	"github.com/shreyas/dronc/scheduler/repository"
)

func setupRedisForTests(t *testing.T) (*redis.Client, func()) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	redisClient.Client = client

	cleanup := func() {
		redisClient.Client = nil
		_ = client.Close()
		mr.Close()
	}

	return client, cleanup
}

func TestTrackExecutions_ReturnsRecentEvents(t *testing.T) {
	gin.SetMode(gin.TestMode)

	client, cleanup := setupRedisForTests(t)
	defer cleanup()

	repo := repository.NewExecEventsRepository(client)
	ctx := context.Background()

	events := []repository.ExecutionEvent{
		{
			JobID:           "job-1",
			ScheduledTime:   100,
			ExecutionTime:   150,
			StatusCode:      200,
			Success:         true,
			TimeTakenMillis: 30,
		},
		{
			JobID:           "job-2",
			ScheduledTime:   200,
			ExecutionTime:   260,
			StatusCode:      500,
			Success:         false,
			TimeTakenMillis: 45,
		},
	}

	for _, event := range events {
		if err := repo.SaveExecutionEvent(ctx, event); err != nil {
			t.Fatalf("failed to save execution event: %v", err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/track/executions", nil)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = req

	TrackExecutions(c)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	var resp executionEventsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Count != 2 {
		t.Fatalf("expected count 2, got %d", resp.Count)
	}

	if len(resp.Executions) != 2 {
		t.Fatalf("expected 2 executions, got %d", len(resp.Executions))
	}

	if resp.Executions[0].JobID != "job-2" || resp.Executions[1].JobID != "job-1" {
		t.Fatalf("expected events ordered by recency")
	}

	if resp.Executions[0].TimeTakenMillis != 45 {
		t.Errorf("expected time_taken_ms to match stored value")
	}
}

func TestTrackExecutions_WithLimitAndFilter(t *testing.T) {
	gin.SetMode(gin.TestMode)

	client, cleanup := setupRedisForTests(t)
	defer cleanup()

	repo := repository.NewExecEventsRepository(client)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		event := repository.ExecutionEvent{
			JobID:           "job-1",
			ScheduledTime:   int64(100 + i),
			ExecutionTime:   int64(150 + i),
			StatusCode:      200,
			Success:         true,
			TimeTakenMillis: int64(25 + i),
		}
		if err := repo.SaveExecutionEvent(ctx, event); err != nil {
			t.Fatalf("failed to save execution event: %v", err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/track/executions?limit=2&job_id=job-1", nil)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = req

	TrackExecutions(c)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	var resp executionEventsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Count != 2 {
		t.Fatalf("expected count 2, got %d", resp.Count)
	}

	for _, execution := range resp.Executions {
		if execution.JobID != "job-1" {
			t.Fatalf("expected only job-1 executions, got %s", execution.JobID)
		}
	}
}

func TestTrackExecutions_InvalidLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)

	_, cleanup := setupRedisForTests(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/v1/track/executions?limit=foo", nil)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = req

	TrackExecutions(c)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", w.Code)
	}

	var resp errorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Error.Code != "INVALID_REQUEST" {
		t.Fatalf("expected error code INVALID_REQUEST, got %s", resp.Error.Code)
	}
}

func TestTrackExecutions_RedisNotInitialized(t *testing.T) {
	gin.SetMode(gin.TestMode)
	redisClient.Client = nil

	req := httptest.NewRequest(http.MethodGet, "/v1/track/executions", nil)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = req

	TrackExecutions(c)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", w.Code)
	}

	var resp errorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Error.Code != "REDIS_NOT_INITIALIZED" {
		t.Fatalf("expected error code REDIS_NOT_INITIALIZED, got %s", resp.Error.Code)
	}
}
