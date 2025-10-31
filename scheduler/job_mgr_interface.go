package scheduler

import "context"

// JobsManagerInterface defines operations for managing jobs
type JobsManagerInterface interface {
	// SetupNewJob stores a new job in the repository
	// Accepts any job type and routes to appropriate storage method
	SetupNewJob(ctx context.Context, j interface{}) error
}
