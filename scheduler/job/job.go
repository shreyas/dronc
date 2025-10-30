package job

import (
	"fmt"
	"strings"

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
	ID        string
	Schedule  string
	Type      JobRunGuarantee
	namespace string
}

// newJob creates a new Job with validated schedule (used by concrete implementations)
func newJob(schedule, namespace string, jobType JobRunGuarantee) (*Job, error) {
	// Validate namespace
	if len(strings.TrimSpace(namespace)) == 0 {
		return nil, fmt.Errorf("namespace cannot be empty")
	}

	// Validate cron expression
	if _, err := cronexpr.Parse(schedule); err != nil {
		return nil, fmt.Errorf("invalid cron expression: %w", err)
	}

	return &Job{
		Schedule:  schedule,
		Type:      jobType,
		namespace: namespace,
		// ID is left empty - concrete implementations must set it
	}, nil
}

// Namespace returns the namespace of the job
func (job *Job) Namespace() string {
	return job.namespace
}
