package repository

import "context"

const schedulesKey = redisKeyPrefix + "schedules"
const dueJobsKey = redisKeyPrefix + "due_jobs"

// SchedulesRepositoryInterface defines operations for managing job schedules
type SchedulesRepositoryInterface interface {
	// AddSchedules adds one or more scheduled occurrences for a job to the sorted set
	// The timestamps are used as scores, and the member is "jobID:timestamp" to ensure uniqueness
	AddSchedules(ctx context.Context, jobID string, timestamps ...int64) error

	// FindDueJobs finds all jobs due until the specified timestamp, moves them to the due_jobs set,
	// removes them from the schedules sorted set, and pushes the job IDs to the provided channel
	FindDueJobs(ctx context.Context, untilTimestamp int64, jobsChan chan<- string) error
}
