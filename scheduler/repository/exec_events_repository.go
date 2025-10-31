package repository

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// ExecEventsRepository handles Redis operations for execution events
type ExecEventsRepository struct {
	client *redis.Client
}

// NewExecEventsRepository creates a new ExecEventsRepository
func NewExecEventsRepository(client *redis.Client) *ExecEventsRepository {
	return &ExecEventsRepository{client: client}
}

// SaveExecutionEvent saves an execution event to Redis streams
func (r *ExecEventsRepository) SaveExecutionEvent(ctx context.Context, event ExecutionEvent) error {
	values := map[string]interface{}{
		"job_id":         event.JobID,
		"scheduled_time": event.ScheduledTime,
		"execution_time": event.ExecutionTime,
		"status_code":    event.StatusCode,
		"success":        strconv.FormatBool(event.Success),
	}

	_, err := r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: execEventsKey,
		Values: values,
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to save execution event to redis stream: %w", err)
	}

	return nil
}
