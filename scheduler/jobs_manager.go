package scheduler

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/shreyas/dronc/lib/logger"
	"github.com/shreyas/dronc/lib/redis"
	"github.com/shreyas/dronc/scheduler/job"
	"github.com/shreyas/dronc/scheduler/repository"
)

// JobExecutionRequest represents a job ready for execution with its job-spec
type JobExecutionRequest struct {
	Job     *job.ApiCallerJob
	JobSpec string // format: "jobID:timestamp"
}

// JobsManager orchestrates job operations including storage
type JobsManager struct {
	repo          repository.JobsRepositoryInterface
	schedulesRepo repository.SchedulesRepositoryInterface

	// dueJobsChan is the channel where due job-specs are pushed
	dueJobsChan chan string

	// atloChannel is the channel for AtLeastOnce guarantee jobs
	atloChannel chan JobExecutionRequest

	// atmoChannel is the channel for AtMostOnce guarantee jobs
	atmoChannel chan JobExecutionRequest

	// configurations
	// how many schedules to generate on setup of a new job
	numSchedulesToGenerate int
}

// NewJobsManager creates a new JobsManager instance
// Both repositories are optional - if nil, default implementations are created
// todo: use varargs based optional arguments for better aesthetics
func NewJobsManager(jobsRepo repository.JobsRepositoryInterface, schedulesRepo repository.SchedulesRepositoryInterface) JobsManagerInterface {
	// Create default job repository if not provided
	if jobsRepo == nil {
		jobsRepo = repository.NewJobsRepository(redis.Client)
	}

	// Create default schedules repository if not provided
	if schedulesRepo == nil {
		schedulesRepo = repository.NewSchedulesRepository(redis.Client)
	}

	return &JobsManager{
		repo:          jobsRepo,
		schedulesRepo: schedulesRepo,
		dueJobsChan:   make(chan string, 1000),               // Buffered channel for handling bursts
		atloChannel:   make(chan JobExecutionRequest, 10000), // AtLeastOnce jobs
		atmoChannel:   make(chan JobExecutionRequest, 10000), // AtMostOnce jobs

		// config options
		numSchedulesToGenerate: 5,
	}
}

// GetDueJobsChannel returns a read-only channel for consuming due job-specs
func (m *JobsManager) GetDueJobsChannel() <-chan string {
	return m.dueJobsChan
}

// GetAtLeastOnceChannel returns a read-only channel for consuming AtLeastOnce job execution requests
func (m *JobsManager) GetAtLeastOnceChannel() <-chan JobExecutionRequest {
	return m.atloChannel
}

// GetAtMostOnceChannel returns a read-only channel for consuming AtMostOnce job execution requests
func (m *JobsManager) GetAtMostOnceChannel() <-chan JobExecutionRequest {
	return m.atmoChannel
}

// StartJobBatchProcessor begins the job batch processor goroutine
// It reads due job-specs in batches, fetches job details from Redis, and routes to guarantee-specific channels
// The goroutine runs until the context is cancelled and recovers from all panics
func (m *JobsManager) StartJobBatchProcessor(ctx context.Context) {
	go func() {
		// Recover from any panics to prevent goroutine death
		defer func() {
			if r := recover(); r != nil {
				logger.Error("job batch processor goroutine panicked and recovered", "panic", r)
				// Restart the goroutine after a panic
				logger.Info("restarting job batch processor goroutine after panic")
				m.StartJobBatchProcessor(ctx)
			}
		}()

		logger.Info("job batch processor goroutine started")

		// todo: check if the bachsize is optimal. maybe make it env var to tune it easily later
		const batchSize = 500
		const batchTimeout = 100 * time.Millisecond

		for {
			select {
			case <-ctx.Done():
				logger.Info("job batch processor goroutine stopping due to context cancellation")
				return
			default:
				// Collect a batch of job-specs
				batch := m.collectBatch(ctx, batchSize, batchTimeout)

				if len(batch) == 0 {
					// No jobs collected, continue
					continue
				}

				jobIDs, jobSpecMap := m.extractJobIDs(batch)
				if len(jobIDs) == 0 {
					continue
				}

				// Fetch jobs from Redis using pipelined multi-get
				jobs, failedIDs, err := m.repo.MultiGetApiCallerJobs(ctx, jobIDs)
				if err != nil {
					logger.Error("failed to multi-get jobs from repository", "error", err)
					continue
				}

				// Log failed lookups
				for _, failedID := range failedIDs {
					logger.Error("failed to fetch job from repository", "jobID", failedID)
				}

				// todo: compare sizes of received jobs and jobs returned by multi-get, log error if there's a mismatch

				// Route jobs to appropriate channels based on guarantee type
				if m.routeJobsForProcessing(ctx, jobs, jobSpecMap) {
					// if context was cancelled during routing, exit
					return
				}
			}
		}
	}()
}

// routeJobsForProcessing routes jobs to appropriate guarantee-specific channels
func (m *JobsManager) routeJobsForProcessing(ctx context.Context, jobs map[string]*job.ApiCallerJob, jobSpecMap map[string]string) bool {
	for jobID, apiJob := range jobs {
		jobSpec := jobSpecMap[jobID]

		req := JobExecutionRequest{
			Job:     apiJob,
			JobSpec: jobSpec,
		}

		// Route based on job guarantee type
		switch apiJob.Type {
		case job.AtLeastOnce:
			select {
			case m.atloChannel <- req:
				// Successfully queued
			case <-ctx.Done():
				logger.Info("job batch processor stopping while routing jobs")
				return true
			}
		case job.AtMostOnce:
			select {
			case m.atmoChannel <- req:
				// Successfully queued
			case <-ctx.Done():
				logger.Info("job batch processor stopping while routing jobs")
				return true
			}
		default:
			logger.Error("unknown job guarantee type", "jobID", jobID, "type", apiJob.Type)
		}
	}

	return false
}

// extractJobIDs Extract job IDs from job-specs (format: "jobID:timestamp")
func (m *JobsManager) extractJobIDs(batch []string) ([]string, map[string]string) {
	jobIDs := make([]string, 0, len(batch))
	jobSpecMap := make(map[string]string) // jobID -> jobSpec

	for _, jobSpec := range batch {
		// Extract jobID from "jobID:timestamp" by finding last colon
		lastColon := strings.LastIndex(jobSpec, ":")

		if lastColon == -1 {
			logger.Error("invalid job-spec format, skipping", "jobSpec", jobSpec)
			continue
		}

		jobID := jobSpec[:lastColon]
		jobIDs = append(jobIDs, jobID)

		jobSpecMap[jobID] = jobSpec
	}

	return jobIDs, jobSpecMap
}

// collectBatch collects up to batchSize job-specs from the due jobs channel
// or waits up to batchTimeout, whichever comes first
func (m *JobsManager) collectBatch(ctx context.Context, batchSize int, batchTimeout time.Duration) []string {
	batch := make([]string, 0, batchSize)
	timeout := time.After(batchTimeout)

	for len(batch) < batchSize {
		select {
		case <-ctx.Done():
			return batch
		case jobSpec := <-m.dueJobsChan:
			batch = append(batch, jobSpec)
		case <-timeout:
			return batch
		}
	}

	return batch
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
