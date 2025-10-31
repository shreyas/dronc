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

// ExecutionEventRecord represents a persisted execution event including its stream identifier.
type ExecutionEventRecord struct {
	ID string
	ExecutionEvent
}

// ExecutionEventsQuery represents filters when listing execution events.
type ExecutionEventsQuery struct {
	// JobID optionally filters execution events by job identifier.
	JobID string
	// Limit caps the number of events returned; zero uses the default.
	Limit int64
}

// ExecEventsRepositoryInterface defines operations for managing job execution events
type ExecEventsRepositoryInterface interface {
	// SaveExecutionEvent saves an execution event to Redis streams
	SaveExecutionEvent(ctx context.Context, event ExecutionEvent) error
	// ListExecutionEvents retrieves execution events applying optional filters and limits.
	ListExecutionEvents(ctx context.Context, query ExecutionEventsQuery) ([]ExecutionEventRecord, error)
}
