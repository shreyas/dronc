package repository

import "context"

// ExecutionEvent represents an event when a job is executed
type ExecutionEvent struct {
	JobID         string
	ScheduledTime int64
	ExecutionTime int64
	StatusCode    int
	Success       bool
}

// ExecEventsRepositoryInterface defines operations for managing job execution events
type ExecEventsRepositoryInterface interface {
	// SaveExecutionEvent saves an execution event to Redis streams
	SaveExecutionEvent(ctx context.Context, event ExecutionEvent) error
}
