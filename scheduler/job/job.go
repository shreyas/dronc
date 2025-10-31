package job

import (
	"fmt"
	"time"

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

// Namespace returns the namespace used to construct the id for a job instance
func (j *Job) Namespace() string {
	//TODO implement me
	panic("implement me")
}

// AtMostOnce indicates if the job is scheduled to run at most once
func (j *Job) AtMostOnce() bool {
	return j.Type == AtMostOnce
}

// AtLeastOnce indicates if the job is scheduled to run at least once
func (j *Job) AtLeastOnce() bool {
	return j.Type == AtLeastOnce
}

// NextOccurance calculates the next occurrence of the job after the given Unix timestamp
func (j *Job) NextOccurance(after int64) (int64, error) {
	// Parse the cron expression
	expr, err := cronexpr.Parse(j.Schedule)
	if err != nil {
		return 0, fmt.Errorf("failed to parse cron expression: %w", err)
	}

	// Convert Unix timestamp to time.Time
	afterTime := time.Unix(after, 0)

	// Calculate next occurrence
	nextTime := expr.Next(afterTime)

	// Return Unix timestamp
	return nextTime.Unix(), nil
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
