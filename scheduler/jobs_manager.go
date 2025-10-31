package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/shreyas/dronc/lib/logger"
	"github.com/shreyas/dronc/lib/redis"
	"github.com/shreyas/dronc/scheduler/job"
	"github.com/shreyas/dronc/scheduler/repository"
)

// JobsManager orchestrates job operations including storage
type JobsManager struct {
	repo          repository.JobsRepositoryInterface
	schedulesRepo repository.SchedulesRepositoryInterface

	// dueJobsChan is the channel where due job-specs are pushed
	dueJobsChan chan string

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
		dueJobsChan:   make(chan string, 1000), // Buffered channel for handling bursts

		// config options
		numSchedulesToGenerate: 5,
	}
}

// GetDueJobsChannel returns a read-only channel for consuming due job-specs
func (m *JobsManager) GetDueJobsChannel() <-chan string {
	return m.dueJobsChan
}

// StartDueJobsFinder begins the due jobs finder goroutine that runs every second
// It finds jobs that are due and pushes them to the due jobs channel
// The goroutine runs until the context is cancelled and recovers from all panics
func (m *JobsManager) StartDueJobsFinder(ctx context.Context) {
	go func() {
		// Recover from any panics to prevent goroutine death
		defer func() {
			if r := recover(); r != nil {
				logger.Error("due jobs finder goroutine panicked and recovered", "panic", r)
				// Restart the goroutine after a panic
				logger.Info("restarting due jobs finder goroutine after panic")
				m.StartDueJobsFinder(ctx)

				// todo: maybe it's better to let it panic and fail fast, rather than keep panicking and recovering
			}
		}()

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		logger.Info("due jobs finder goroutine started")

		for {
			select {
			case <-ctx.Done():
				logger.Info("due jobs finder goroutine stopping due to context cancellation")
				return
			case <-ticker.C:
				// Find due jobs up to current time
				currentTime := time.Now().Unix()
				err := m.schedulesRepo.FindDueJobs(ctx, currentTime, m.dueJobsChan)
				if err != nil {
					logger.Error("failed to find due jobs", "error", err)
					// Continue running even on error
				}
			}
		}
	}()
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
