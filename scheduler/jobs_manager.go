package scheduler

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/shreyas/dronc/scheduler/job"
	"github.com/shreyas/dronc/scheduler/repository"
)

// JobsManager orchestrates job operations including storage
type JobsManager struct {
	repo *repository.JobsRepository
}

// NewJobsManager creates a new JobsManager instance
func NewJobsManager(redisClient *redis.Client) *JobsManager {
	return &JobsManager{
		repo: repository.NewJobsRepository(redisClient),
	}
}

// SetupNewJob stores a new job in Redis
// It routes to the appropriate repository method based on the job type
func (m *JobsManager) SetupNewJob(ctx context.Context, j interface{}) error {
	switch jobType := j.(type) {
	case *job.ApiCallerJob:
		if err := m.repo.SaveApiCallerJob(ctx, jobType); err != nil {
			return fmt.Errorf("failed to save api caller job: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unsupported job type: %T", j)
	}
}
