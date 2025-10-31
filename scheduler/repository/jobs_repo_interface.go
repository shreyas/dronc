package repository

import (
	"context"
	"github.com/shreyas/dronc/scheduler/job"
)

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

	// MoveJobToProcessingAndScheduleNext atomically moves a job-spec from due_jobs to processing_jobs
	// and schedules the next occurrence in the schedules sorted set
	MoveJobToProcessingAndScheduleNext(ctx context.Context, jobSpec string, nextTimestamp int64) error

	// MoveJobToFailed moves a job-spec from processing_jobs to failed_atlo_jobs
	MoveJobToFailed(ctx context.Context, jobSpec string) error

	// RemoveFromProcessing removes a job-spec from the processing_jobs set
	RemoveFromProcessing(ctx context.Context, jobSpec string) error
}
