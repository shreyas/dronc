package repository

import (
	"context"
	"fmt"

	"github.com/shreyas/dronc/scheduler/job"
)

// redisKeyPrefix is the prefix for all dronc-related Redis keys
const redisKeyPrefix = "dronc:"

// buildRedisKey constructs a Redis key with the dronc prefix
func buildRedisKey(jobID string) string {
	return fmt.Sprintf("%s%s", redisKeyPrefix, jobID)
}

// JobsRepositoryInterface defines operations for job persistence
type JobsRepositoryInterface interface {
	// SaveApiCallerJob stores an ApiCallerJob in Redis
	SaveApiCallerJob(ctx context.Context, j *job.ApiCallerJob) error

	// GetApiCallerJob retrieves an ApiCallerJob by job ID
	GetApiCallerJob(ctx context.Context, jobID string) (*job.ApiCallerJob, error)

	// MultiGetApiCallerJobs retrieves multiple ApiCallerJobs from Redis in a single pipelined operation
	// Returns: successful lookups map, failed job IDs, error
	MultiGetApiCallerJobs(ctx context.Context, jobIDs []string) (map[string]*job.ApiCallerJob, []string, error)

	// Delete removes a job from Redis by job ID
	Delete(ctx context.Context, jobID string) error

	// Exists checks if a job exists in Redis by job ID
	Exists(ctx context.Context, jobID string) (bool, error)
}
