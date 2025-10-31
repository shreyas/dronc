package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/shreyas/dronc/lib/redis"
	"github.com/shreyas/dronc/scheduler/job"
	"github.com/shreyas/dronc/scheduler/repository"
)

// JobsManager orchestrates job operations including storage
type JobsManager struct {
	repo          repository.JobsRepositoryInterface
	schedulesRepo repository.SchedulesRepositoryInterface

	// configurations
	// how many schedules to generate on setup of a new job
	numSchedulesToGenerate int
}

// NewJobsManager creates a new JobsManager instance
// Both repositories are optional - if nil, default implementations are created
// todo: use varargs based optional arguments for better aesthetics
func NewJobsManager(repo repository.JobsRepositoryInterface, schedulesRepo repository.SchedulesRepositoryInterface) JobsManagerInterface {
	// Create default job repository if not provided
	if repo == nil {
		repo = repository.NewJobsRepository(redis.Client)
	}

	// Create default schedules repository if not provided
	if schedulesRepo == nil {
		schedulesRepo = repository.NewSchedulesRepository(redis.Client)
	}

	return &JobsManager{
		repo:          repo,
		schedulesRepo: schedulesRepo,

		// config options
		numSchedulesToGenerate: 5,
	}
}

// SetupNewJob stores a new job in the repository and schedules its next occurrences
// It routes to the appropriate repository method based on the job type
func (m *JobsManager) SetupNewJob(ctx context.Context, j interface{}) error {
	var jobID string
	var jobInterface job.JobInterface

	switch jobType := j.(type) {
	case *job.ApiCallerJob:
		if err := m.repo.SaveApiCallerJob(ctx, jobType); err != nil {
			return fmt.Errorf("failed to save api caller job: %w", err)
		}
		jobID = jobType.ID
		jobInterface = jobType
	default:
		return fmt.Errorf("unsupported job type: %T", j)
	}

	// Generate next N occurrences of the job
	if err := m.scheduleNextOccurrences(ctx, jobID, jobInterface, m.numSchedulesToGenerate); err != nil {
		return fmt.Errorf("failed to schedule job occurrences: %w", err)
	}

	return nil
}

// scheduleNextOccurrences generates and stores the next N occurrences for a job
func (m *JobsManager) scheduleNextOccurrences(ctx context.Context, jobID string, j job.JobInterface, count int) error {
	timestamps := make([]int64, 0, count)

	// Start with current time
	currentTime := time.Now().Unix()

	// Generate N occurrences iteratively
	for i := 0; i < count; i++ {
		nextOccurrence, err := j.NextOccurance(currentTime)
		if err != nil {
			return fmt.Errorf("failed to calculate next occurrence: %w", err)
		}

		timestamps = append(timestamps, nextOccurrence)

		// Use this occurrence as the base for calculating the next one
		currentTime = nextOccurrence
	}

	// Add all schedules to Redis in a batch
	if err := m.schedulesRepo.AddSchedules(ctx, jobID, timestamps...); err != nil {
		return fmt.Errorf("failed to add schedules to repository: %w", err)
	}

	return nil
}
