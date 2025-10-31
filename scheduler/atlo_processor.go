package scheduler

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/shreyas/dronc/lib/logger"
	"github.com/shreyas/dronc/scheduler/job"
	"github.com/shreyas/dronc/scheduler/repository"
)

// atloProcessor manages the processing of at-least-once jobs
type atloProcessor struct {
	jobsRepo       repository.JobsRepositoryInterface
	execEventsRepo repository.ExecEventsRepositoryInterface
	httpClient     *retryablehttp.Client
	wg             sync.WaitGroup
}

// newAtloProcessor creates a new atloProcessor
func newAtloProcessor(jobsRepo repository.JobsRepositoryInterface, execEventsRepo repository.ExecEventsRepositoryInterface) *atloProcessor {
	// Create retryable HTTP client with exponential backoff
	client := retryablehttp.NewClient()
	client.RetryMax = 5
	client.RetryWaitMin = 1 * time.Second
	client.RetryWaitMax = 30 * time.Second
	client.Logger = nil // Disable retryablehttp's default logging

	return &atloProcessor{
		jobsRepo:       jobsRepo,
		execEventsRepo: execEventsRepo,
		httpClient:     client,
	}
}

// Process processes a single at-least-once job
// It spawns a worker goroutine that handles the job execution
func (p *atloProcessor) Process(ctx context.Context, req JobExecutionRequest) {
	p.wg.Add(1)
	go p.processJob(ctx, req)
}

// Wait waits for all worker goroutines to complete
func (p *atloProcessor) Wait() {
	p.wg.Wait()
}

// processJob is the internal worker that processes a single job
func (p *atloProcessor) processJob(ctx context.Context, req JobExecutionRequest) {
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

	// Atomically move job to processing and schedule next occurrence
	if err := p.jobsRepo.MoveJobToProcessingAndScheduleNext(ctx, req.JobSpec, nextTimestamp); err != nil {
		logger.Error("failed to move job to processing", "jobSpec", req.JobSpec, "error", err)
		return
	}

	// todo: we need a job specific dispatcher here. the execution has to be job type agnostic
	// Call API with exponential backoff and retries; measure time taken
	start := time.Now()
	statusCode, success := p.callAPI(ctx, req.Job.API)
	timeTakenMs := time.Since(start).Milliseconds()

	executionTime := time.Now().Unix()

	if success {
		// Save execution event to Redis streams
		event := repository.ExecutionEvent{
			JobID:           req.Job.ID,
			ScheduledTime:   scheduledTime,
			ExecutionTime:   executionTime,
			StatusCode:      statusCode,
			Success:         true,
			TimeTakenMillis: timeTakenMs,
		}

		if err := p.execEventsRepo.SaveExecutionEvent(ctx, event); err != nil {
			logger.Error("failed to save execution event", "jobID", req.Job.ID, "error", err)
			// Continue to remove from processing even if event save fails
		}

		// Remove job from processing set
		if err := p.jobsRepo.RemoveFromProcessing(ctx, req.JobSpec); err != nil {
			logger.Error("failed to remove job from processing", "jobSpec", req.JobSpec, "error", err)
		} else {
			logger.Info("successfully processed atlo job", "jobID", req.Job.ID, "statusCode", statusCode)
		}
	} else {
		// All retries failed, move to failed jobs set
		if err := p.jobsRepo.MoveJobToFailed(ctx, req.JobSpec); err != nil {
			logger.Error("failed to move job to failed set", "jobSpec", req.JobSpec, "error", err)
		} else {
			logger.Error("atlo job failed after all retries", "jobID", req.Job.ID, "statusCode", statusCode)
		}

		// Still save the failed execution event
		event := repository.ExecutionEvent{
			JobID:           req.Job.ID,
			ScheduledTime:   scheduledTime,
			ExecutionTime:   executionTime,
			StatusCode:      statusCode,
			Success:         false,
			TimeTakenMillis: timeTakenMs,
		}

		if err := p.execEventsRepo.SaveExecutionEvent(ctx, event); err != nil {
			logger.Error("failed to save failed execution event", "jobID", req.Job.ID, "error", err)
		}
	}
}

// callAPI calls the API with exponential backoff and up to 5 retries
// Returns the status code and whether the call was successful
func (p *atloProcessor) callAPI(ctx context.Context, apiURL string) (int, bool) {
	req, err := retryablehttp.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		logger.Error("failed to create http request", "url", apiURL, "error", err)
		return 0, false
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		logger.Error("api call failed after all retries", "url", apiURL, "error", err)
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

// calculateFifthOccurrence calculates the 5th occurrence from the given timestamp
func (p *atloProcessor) calculateFifthOccurrence(j job.JobInterface, fromTime int64) (int64, error) {
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

// extractTimestampFromJobSpec extracts the timestamp from a job-spec (format: "jobID:timestamp")
func extractTimestampFromJobSpec(jobSpec string) (int64, error) {
	lastColon := strings.LastIndex(jobSpec, ":")
	if lastColon == -1 {
		return 0, fmt.Errorf("invalid job-spec format: %s", jobSpec)
	}

	timestampStr := jobSpec[lastColon+1:]
	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	return timestamp, nil
}
