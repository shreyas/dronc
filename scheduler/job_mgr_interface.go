package scheduler

import "context"

// JobsManagerInterface defines operations for managing jobs
type JobsManagerInterface interface {
	// SetupNewJob stores a new job in the repository
	// Accepts any job type and routes to appropriate storage method
	SetupNewJob(ctx context.Context, j interface{}) error

	// StartDueJobsFinder begins the due jobs finder goroutine that runs every second
	// It finds jobs that are due and pushes them to the due jobs channel
	StartDueJobsFinder(ctx context.Context)

	// StartJobBatchProcessor begins the job batch processor goroutine
	// It reads due job-specs in batches, fetches job details, and routes to guarantee-specific channels
	StartJobBatchProcessor(ctx context.Context)

	// StartAtloProcessor begins the at-least-once job processor goroutine
	// It reads job execution requests from the atloChannel and spawns worker goroutines for each job
	StartAtloProcessor(ctx context.Context)

	// StartAtmoProcessor begins the at-most-once job processor goroutine
	// It reads job execution requests from the atmoChannel and spawns worker goroutines for each job
	StartAtmoProcessor(ctx context.Context)

	// GetDueJobsChannel returns a read-only channel for consuming due job-specs
	GetDueJobsChannel() <-chan string

	// GetAtLeastOnceChannel returns a read-only channel for consuming AtLeastOnce job execution requests
	GetAtLeastOnceChannel() <-chan JobExecutionRequest

	// GetAtMostOnceChannel returns a read-only channel for consuming AtMostOnce job execution requests
	GetAtMostOnceChannel() <-chan JobExecutionRequest

	// Run starts all job manager components (goroutines)
	Run(ctx context.Context)
}
