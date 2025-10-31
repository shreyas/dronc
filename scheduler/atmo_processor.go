package scheduler

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/shreyas/dronc/lib/logger"
	"github.com/shreyas/dronc/scheduler/job"
	"github.com/shreyas/dronc/scheduler/repository"
)

// atmoProcessor manages the processing of at-most-once jobs
type atmoProcessor struct {
	jobsRepo       repository.JobsRepositoryInterface
	execEventsRepo repository.ExecEventsRepositoryInterface
	httpClient     *http.Client
	wg             sync.WaitGroup
}

// newAtmoProcessor creates a new processor for at-most-once (atmo) jobs
func newAtmoProcessor(jobsRepo repository.JobsRepositoryInterface, execEventsRepo repository.ExecEventsRepositoryInterface) *atmoProcessor {
	// Create HTTP client with 90 second timeout, no retries
	client := &http.Client{
		Timeout: 90 * time.Second,
	}

	return &atmoProcessor{
		jobsRepo:       jobsRepo,
		execEventsRepo: execEventsRepo,
		httpClient:     client,
	}
}

// Process processes a single at-most-once job
// It spawns a worker goroutine that handles the job execution
func (p *atmoProcessor) Process(ctx context.Context, req JobExecutionRequest) {
	p.wg.Add(1)
	go p.processJob(ctx, req)
}

// Wait waits for all worker goroutines to complete
func (p *atmoProcessor) Wait() {
	p.wg.Wait()
}

// processJob is the internal worker that processes a single job
func (p *atmoProcessor) processJob(ctx context.Context, req JobExecutionRequest) {
	defer p.wg.Done()

	// Extract scheduled timestamp from job-spec (format: "jobID:timestamp")
	scheduledTime, err := extractTimestampFromJobSpec(req.JobSpec)
	if err != nil {
		logger.Error("failed to extract timestamp from job-spec", "jobSpec", req.JobSpec, "error", err)
		return
	}

	// Calculate 5th occurrence from now (since we always have next 5 instances rescheduled)
	nextTimestamp, err := p.calculateFifthOccurrence(req.Job, time.Now().Unix())
	if err != nil {
		logger.Error("failed to calculate 5th occurrence", "jobID", req.Job.ID, "error", err)
		return
	}

	// Atomically remove job from due_jobs and schedule next occurrence
	// This is done BEFORE calling the API (at-most-once guarantee)
	if err := p.jobsRepo.RemoveFromDueAndScheduleNext(ctx, req.JobSpec, nextTimestamp); err != nil {
		logger.Error("failed to remove from due and schedule next", "jobSpec", req.JobSpec, "error", err)
		return
	}

	// Call API with single attempt (no retries) and measure time taken
	start := time.Now()
	statusCode, success := p.callAPI(ctx, req.Job.API)
	timeTakenMs := time.Since(start).Milliseconds()

	executionTime := time.Now().Unix()

	// Save execution event to Redis streams (regardless of success/failure)
	event := repository.ExecutionEvent{
		JobID:           req.Job.ID,
		ScheduledTime:   scheduledTime,
		ExecutionTime:   executionTime,
		StatusCode:      statusCode,
		Success:         success,
		TimeTakenMillis: timeTakenMs,
	}

	if err := p.execEventsRepo.SaveExecutionEvent(ctx, event); err != nil {
		logger.Error("failed to save execution event", "jobID", req.Job.ID, "error", err)
	}

	if success {
		logger.Info("successfully processed atmo job", "jobID", req.Job.ID, "statusCode", statusCode)
	} else {
		logger.Error("atmo job failed", "jobID", req.Job.ID, "statusCode", statusCode)
	}
}

// calculateFifthOccurrence calculates the 5th occurrence from the given timestamp
func (p *atmoProcessor) calculateFifthOccurrence(j job.JobInterface, fromTime int64) (int64, error) {
	currentTime := fromTime

	// Calculate 5 occurrences iteratively
	for i := 0; i < 5; i++ {
		nextOccurrence, err := j.NextOccurance(currentTime)
		if err != nil {
			return 0, fmt.Errorf("failed to calculate occurrence %d: %w", i+1, err)
		}
		currentTime = nextOccurrence
	}

	return currentTime, nil
}

// callAPI calls the API with a single attempt (no retries)
// Returns the status code and whether the call was successful
func (p *atmoProcessor) callAPI(ctx context.Context, apiURL string) (int, bool) {
	req, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		logger.Error("failed to create http request", "url", apiURL, "error", err)
		return 0, false
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		logger.Error("api call failed", "url", apiURL, "error", err)
		return 0, false
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logger.Error("failed to close response body", "url", apiURL, "error", err)
		}
	}()

	statusCode := resp.StatusCode

	// Consider 2xx status codes as success
	if statusCode >= 200 && statusCode < 300 {
		logger.Info("api call succeeded", "url", apiURL, "statusCode", statusCode)
		return statusCode, true
	}

	logger.Error("api call failed", "url", apiURL, "statusCode", statusCode)
	return statusCode, false
}
