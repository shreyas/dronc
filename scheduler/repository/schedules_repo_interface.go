package repository

import "context"

const schedulesKey = redisKeyPrefix + "schedules"

// SchedulesRepositoryInterface defines operations for managing job schedules
type SchedulesRepositoryInterface interface {
	// AddSchedules adds one or more scheduled occurrences for a job to the sorted set
	// The timestamps are used as scores, and the member is "jobID:timestamp" to ensure uniqueness
	AddSchedules(ctx context.Context, jobID string, timestamps ...int64) error
}
