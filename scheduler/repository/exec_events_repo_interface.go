package repository

import "context"

// ExecutionEvent represents an event when a job is executed
type ExecutionEvent struct {
	JobID         string
	ScheduledTime int64
	ExecutionTime int64
	StatusCode    int
	Success       bool
	// TimeTakenMillis indicates how long the job took to execute in milliseconds
	TimeTakenMillis int64
}

// ExecEventsRepositoryInterface defines operations for managing job execution events
type ExecEventsRepositoryInterface interface {
	// SaveExecutionEvent saves an execution event to Redis streams
	SaveExecutionEvent(ctx context.Context, event ExecutionEvent) error
}
