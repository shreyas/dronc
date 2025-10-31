package repository

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

const (
	defaultExecEventsLimit int64 = 50
	maxExecEventsLimit     int64 = 200
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
		"time_taken_ms":  event.TimeTakenMillis,
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

// ListExecutionEvents returns a list of execution events ordered from most recent to oldest.
func (r *ExecEventsRepository) ListExecutionEvents(ctx context.Context, query ExecutionEventsQuery) ([]ExecutionEventRecord, error) {
	if query.Limit < 0 {
		query.Limit = 0
	}

	limit := query.Limit
	if limit == 0 {
		limit = defaultExecEventsLimit
	}

	if limit > maxExecEventsLimit {
		limit = maxExecEventsLimit
	}

	// Always request at least the maximum limit from Redis so we can filter client-side when job filters are applied.
	fetchCount := int(maxExecEventsLimit)
	if query.JobID == "" {
		fetchCount = int(limit)
	}

	// todo: search index in future maybe
	messages, err := r.client.XRevRangeN(ctx, execEventsKey, "+", "-", int64(fetchCount)).Result()
	if err != nil {
		if err == redis.Nil {
			return []ExecutionEventRecord{}, nil
		}
		return nil, fmt.Errorf("failed to list execution events: %w", err)
	}

	events := make([]ExecutionEventRecord, 0, len(messages))
	for _, msg := range messages {
		parsed, err := mapStreamValuesToExecutionEvent(msg.Values)
		if err != nil {
			return nil, fmt.Errorf("failed to parse execution event %s: %w", msg.ID, err)
		}

		if query.JobID != "" && parsed.JobID != query.JobID {
			continue
		}

		events = append(events, ExecutionEventRecord{
			ID:             msg.ID,
			ExecutionEvent: parsed,
		})

		if int64(len(events)) >= limit {
			break
		}
	}

	if len(events) == 0 {
		return []ExecutionEventRecord{}, nil
	}

	return events, nil
}

func mapStreamValuesToExecutionEvent(values map[string]interface{}) (ExecutionEvent, error) {
	jobID, err := parseString(values["job_id"])
	if err != nil {
		return ExecutionEvent{}, fmt.Errorf("invalid job_id: %w", err)
	}

	scheduledTime, err := parseInt64(values["scheduled_time"])
	if err != nil {
		return ExecutionEvent{}, fmt.Errorf("invalid scheduled_time: %w", err)
	}

	executionTime, err := parseInt64(values["execution_time"])
	if err != nil {
		return ExecutionEvent{}, fmt.Errorf("invalid execution_time: %w", err)
	}

	statusCodeValue, err := parseInt(values["status_code"])
	if err != nil {
		return ExecutionEvent{}, fmt.Errorf("invalid status_code: %w", err)
	}

	success, err := parseBool(values["success"])
	if err != nil {
		return ExecutionEvent{}, fmt.Errorf("invalid success flag: %w", err)
	}

	timeTaken, err := parseInt64(values["time_taken_ms"])
	if err != nil {
		return ExecutionEvent{}, fmt.Errorf("invalid time_taken_ms: %w", err)
	}

	return ExecutionEvent{
		JobID:           jobID,
		ScheduledTime:   scheduledTime,
		ExecutionTime:   executionTime,
		StatusCode:      statusCodeValue,
		Success:         success,
		TimeTakenMillis: timeTaken,
	}, nil
}

func parseString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case fmt.Stringer:
		return v.String(), nil
	case nil:
		return "", fmt.Errorf("missing value")
	default:
		return fmt.Sprint(v), nil
	}
}

func parseInt64(value interface{}) (int64, error) {
	str, err := parseString(value)
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(str, 10, 64)
}

func parseInt(value interface{}) (int, error) {
	val, err := parseInt64(value)
	if err != nil {
		return 0, err
	}
	return int(val), nil
}

func parseBool(value interface{}) (bool, error) {
	str, err := parseString(value)
	if err != nil {
		return false, err
	}
	return strconv.ParseBool(str)
}
