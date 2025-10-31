package scheduler

import (
	"context"
	"fmt"

	"github.com/shreyas/dronc/scheduler/job"
	"github.com/shreyas/dronc/scheduler/repository"
)

// JobsManager orchestrates job operations including storage
type JobsManager struct {
	repo repository.JobsRepositoryInterface
}

// NewJobsManager creates a new JobsManager instance
func NewJobsManager(repo repository.JobsRepositoryInterface) JobsManagerInterface {
	return &JobsManager{
		repo: repo,
	}
}

// SetupNewJob stores a new job in the repository
// It routes to the appropriate repository method based on the job type
func (m *JobsManager) SetupNewJob(ctx context.Context, j interface{}) error {
	switch jobType := j.(type) {
	case *job.ApiCallerJob:
		if err := m.repo.SaveApiCallerJob(ctx, jobType); err != nil {
			return fmt.Errorf("failed to save api caller job: %w", err)
		}
	default:
		return fmt.Errorf("unsupported job type: %T", j)
	}

	// TODO: schedule next n occurances of the job

	return nil
}
