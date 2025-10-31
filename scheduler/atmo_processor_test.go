package scheduler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shreyas/dronc/scheduler/job"
	"github.com/shreyas/dronc/scheduler/repository"
)

// mockJobsRepo implements JobsRepositoryInterface for testing
type mockJobsRepoAtmo struct {
	removeFromDueCalled        bool
	moveToProcessingCalled     bool
	moveToFailedCalled         bool
	removeFromProcessingCalled bool
}

func (m *mockJobsRepoAtmo) SaveApiCallerJob(ctx context.Context, j *job.ApiCallerJob) error {
	return nil
}

func (m *mockJobsRepoAtmo) GetApiCallerJob(ctx context.Context, jobID string) (*job.ApiCallerJob, error) {
	return nil, nil
}

func (m *mockJobsRepoAtmo) MultiGetApiCallerJobs(ctx context.Context, jobIDs []string) (map[string]*job.ApiCallerJob, []string, error) {
	return nil, nil, nil
}

func (m *mockJobsRepoAtmo) Delete(ctx context.Context, jobID string) error {
	return nil
}

func (m *mockJobsRepoAtmo) Exists(ctx context.Context, jobID string) (bool, error) {
	return false, nil
}

func (m *mockJobsRepoAtmo) MoveJobToProcessingAndScheduleNext(ctx context.Context, jobSpec string, nextTimestamp int64) error {
	m.moveToProcessingCalled = true
	return nil
}

func (m *mockJobsRepoAtmo) MoveJobToFailed(ctx context.Context, jobSpec string) error {
	m.moveToFailedCalled = true
	return nil
}

func (m *mockJobsRepoAtmo) RemoveFromProcessing(ctx context.Context, jobSpec string) error {
	m.removeFromProcessingCalled = true
	return nil
}

func (m *mockJobsRepoAtmo) RemoveFromDueAndScheduleNext(ctx context.Context, jobSpec string, nextTimestamp int64) error {
	m.removeFromDueCalled = true
	return nil
}

// mockExecEventsRepoAtmo implements ExecEventsRepositoryInterface for testing
type mockExecEventsRepoAtmo struct {
	savedEvents []repository.ExecutionEvent
}

func (m *mockExecEventsRepoAtmo) SaveExecutionEvent(ctx context.Context, event repository.ExecutionEvent) error {
	m.savedEvents = append(m.savedEvents, event)
	return nil
}

func (m *mockExecEventsRepoAtmo) ListExecutionEvents(ctx context.Context, query repository.ExecutionEventsQuery) ([]repository.ExecutionEventRecord, error) {
	return nil, nil
}

func TestAtmoProcessor_callAPI_Success(t *testing.T) {
	// Create a test server that always succeeds
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	processor := newAtmoProcessor(nil, nil)
	ctx := context.Background()

	statusCode, success := processor.callAPI(ctx, server.URL)

	if !success {
		t.Errorf("expected success but got failure")
	}
	if statusCode != http.StatusOK {
		t.Errorf("expected status code 200, got %d", statusCode)
	}
}

func TestAtmoProcessor_callAPI_Failure(t *testing.T) {
	// Create a test server that always fails
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	processor := newAtmoProcessor(nil, nil)
	ctx := context.Background()

	statusCode, success := processor.callAPI(ctx, server.URL)

	if success {
		t.Errorf("expected failure but got success")
	}
	if statusCode != http.StatusInternalServerError {
		t.Errorf("expected status code 500, got %d", statusCode)
	}
}

func TestAtmoProcessor_callAPI_ContextCancelled(t *testing.T) {
	// Create a test server with delay
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	processor := newAtmoProcessor(nil, nil)

	// Create a context that will be cancelled immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	statusCode, success := processor.callAPI(ctx, server.URL)

	if success {
		t.Errorf("expected failure due to context cancellation")
	}
	if statusCode != 0 {
		t.Errorf("expected status code 0, got %d", statusCode)
	}
}

func TestAtmoProcessor_Process_Success(t *testing.T) {
	// Create a test server that always succeeds
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create mock repositories
	mockJobs := &mockJobsRepoAtmo{}
	mockExecEvents := &mockExecEventsRepoAtmo{}

	processor := newAtmoProcessor(mockJobs, mockExecEvents)

	// Create a test job
	apiJob, err := job.NewApiCallerJob(job.ApiCallerJobRequest{
		Schedule: "*/5 * * * *",
		Type:     job.AtMostOnce,
		API:      server.URL,
	})
	if err != nil {
		t.Fatalf("failed to create test job: %v", err)
	}

	req := JobExecutionRequest{
		Job:     apiJob,
		JobSpec: apiJob.ID + ":1698764400",
	}

	ctx := context.Background()
	processor.Process(ctx, req)
	processor.Wait()

	// Verify repository calls
	if !mockJobs.removeFromDueCalled {
		t.Errorf("expected RemoveFromDueAndScheduleNext to be called")
	}
	if mockJobs.moveToProcessingCalled {
		t.Errorf("did not expect MoveJobToProcessingAndScheduleNext to be called for ATMO jobs")
	}

	// Verify execution event was saved
	if len(mockExecEvents.savedEvents) != 1 {
		t.Errorf("expected 1 execution event, got %d", len(mockExecEvents.savedEvents))
	}
	if len(mockExecEvents.savedEvents) > 0 {
		event := mockExecEvents.savedEvents[0]
		if !event.Success {
			t.Errorf("expected success=true in execution event")
		}
		if event.StatusCode != http.StatusOK {
			t.Errorf("expected status code 200, got %d", event.StatusCode)
		}

		if event.TimeTakenMillis < 0 {
			t.Errorf("expected TimeTakenMillis >= 0, got %d", event.TimeTakenMillis)
		}
	}
}

func TestAtmoProcessor_Process_Failure(t *testing.T) {
	// Create a test server that always fails
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	// Create mock repositories
	mockJobs := &mockJobsRepoAtmo{}
	mockExecEvents := &mockExecEventsRepoAtmo{}

	processor := newAtmoProcessor(mockJobs, mockExecEvents)

	// Create a test job
	apiJob, err := job.NewApiCallerJob(job.ApiCallerJobRequest{
		Schedule: "*/5 * * * *",
		Type:     job.AtMostOnce,
		API:      server.URL,
	})
	if err != nil {
		t.Fatalf("failed to create test job: %v", err)
	}

	req := JobExecutionRequest{
		Job:     apiJob,
		JobSpec: apiJob.ID + ":1698764400",
	}

	ctx := context.Background()
	processor.Process(ctx, req)
	processor.Wait()

	// Verify repository calls
	if !mockJobs.removeFromDueCalled {
		t.Errorf("expected RemoveFromDueAndScheduleNext to be called")
	}

	// Verify execution event was saved (even for failures)
	if len(mockExecEvents.savedEvents) != 1 {
		t.Errorf("expected 1 execution event, got %d", len(mockExecEvents.savedEvents))
	}
	if len(mockExecEvents.savedEvents) > 0 {
		event := mockExecEvents.savedEvents[0]
		if event.Success {
			t.Errorf("expected success=false in execution event")
		}
		if event.StatusCode != http.StatusInternalServerError {
			t.Errorf("expected status code 500, got %d", event.StatusCode)
		}

		if event.TimeTakenMillis < 0 {
			t.Errorf("expected TimeTakenMillis >= 0, got %d", event.TimeTakenMillis)
		}
	}
}

func TestAtmoProcessor_calculateFifthOccurrence(t *testing.T) {
	processor := newAtmoProcessor(nil, nil)

	apiJob, err := job.NewApiCallerJob(job.ApiCallerJobRequest{
		Schedule: "*/5 * * * *", // Every 5 minutes
		Type:     job.AtMostOnce,
		API:      "https://example.com/api",
	})
	if err != nil {
		t.Fatalf("failed to create test job: %v", err)
	}

	startTime := int64(1698764400) // Some fixed timestamp

	fifthOccurrence, err := processor.calculateFifthOccurrence(apiJob, startTime)
	if err != nil {
		t.Errorf("calculateFifthOccurrence() error = %v, want nil", err)
	}

	// Verify that the 5th occurrence is 25 minutes (5 * 5) after start time
	expectedDiff := int64(25 * 60) // 25 minutes in seconds
	actualDiff := fifthOccurrence - startTime

	if actualDiff < expectedDiff || actualDiff > expectedDiff+60 {
		t.Errorf("expected 5th occurrence to be ~%d seconds after start, got %d", expectedDiff, actualDiff)
	}
}
