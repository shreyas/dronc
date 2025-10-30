package job

import (
	"fmt"

	"github.com/gorhill/cronexpr"
)

// JobRunGuarantee defines various types of run guarantees for scheduled jobs
type JobRunGuarantee int

const (
	AtMostOnce JobRunGuarantee = iota
	AtLeastOnce
)

// Job represents an abstract scheduled job
type Job struct {
	ID       string
	Schedule string
	Type     JobRunGuarantee
}

// newJob creates a new Job with validated schedule (used by concrete implementations)
func newJob(schedule string, jobType JobRunGuarantee) (*Job, error) {
	// Validate cron expression
	if _, err := cronexpr.Parse(schedule); err != nil {
		return nil, fmt.Errorf("invalid cron expression: %w", err)
	}

	return &Job{
		Schedule: schedule,
		Type:     jobType,
		// ID is left empty - concrete implementations must set it
	}, nil
}
