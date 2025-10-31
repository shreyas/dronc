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

// mockJobsRepo is a mock implementation of JobsRepositoryInterface
type mockJobsRepo struct {
	moveToProcessingCalled     bool
	moveToFailedCalled         bool
	removeFromProcessingCalled bool
	moveToProcessingErr        error
	moveToFailedErr            error
	removeFromProcessingErr    error
}

func (m *mockJobsRepo) SaveApiCallerJob(ctx context.Context, j *job.ApiCallerJob) error {
	return nil
}

func (m *mockJobsRepo) GetApiCallerJob(ctx context.Context, jobID string) (*job.ApiCallerJob, error) {
	return nil, nil
}

func (m *mockJobsRepo) MultiGetApiCallerJobs(ctx context.Context, jobIDs []string) (map[string]*job.ApiCallerJob, []string, error) {
	return nil, nil, nil
}

func (m *mockJobsRepo) Delete(ctx context.Context, jobID string) error {
	return nil
}

func (m *mockJobsRepo) Exists(ctx context.Context, jobID string) (bool, error) {
	return false, nil
}

func (m *mockJobsRepo) MoveJobToProcessingAndScheduleNext(ctx context.Context, jobSpec string, nextTimestamp int64) error {
	m.moveToProcessingCalled = true
	return m.moveToProcessingErr
}

func (m *mockJobsRepo) MoveJobToFailed(ctx context.Context, jobSpec string) error {
	m.moveToFailedCalled = true
	return m.moveToFailedErr
}

func (m *mockJobsRepo) RemoveFromProcessing(ctx context.Context, jobSpec string) error {
	m.removeFromProcessingCalled = true
	return m.removeFromProcessingErr
}

func (m *mockJobsRepo) RemoveFromDueAndScheduleNext(ctx context.Context, jobSpec string, nextTimestamp int64) error {
	return nil
}

// mockExecEventsRepo is a mock implementation of ExecEventsRepositoryInterface
type mockExecEventsRepo struct {
	savedEvents []repository.ExecutionEvent
	saveErr     error
}

func (m *mockExecEventsRepo) SaveExecutionEvent(ctx context.Context, event repository.ExecutionEvent) error {
	m.savedEvents = append(m.savedEvents, event)
	return m.saveErr
}

func TestAtloProcessor_extractTimestampFromJobSpec(t *testing.T) {
	tests := []struct {
		name      string
		jobSpec   string
		expected  int64
		expectErr bool
	}{
		{
			name:      "valid job-spec",
			jobSpec:   "capi:abc123:1698764400",
			expected:  1698764400,
			expectErr: false,
		},
		{
			name:      "job-spec with multiple colons",
			jobSpec:   "capi:abc:def:1698764400",
			expected:  1698764400,
			expectErr: false,
		},
		{
			name:      "invalid job-spec - no colon",
			jobSpec:   "invalid",
			expected:  0,
			expectErr: true,
		},
		{
			name:      "invalid job-spec - invalid timestamp",
			jobSpec:   "capi:abc123:invalid",
			expected:  0,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := extractTimestampFromJobSpec(tt.jobSpec)
			if tt.expectErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if result != tt.expected {
					t.Errorf("extractTimestampFromJobSpec(%s) = %d, want %d", tt.jobSpec, result, tt.expected)
				}
			}
		})
	}
}

func TestAtloProcessor_callAPI_Success(t *testing.T) {
	// Create a test server that returns 200 OK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	processor := newAtloProcessor(nil, nil)
	ctx := context.Background()

	statusCode, success := processor.callAPI(ctx, server.URL)

	if !success {
		t.Errorf("expected success but got failure")
	}
	if statusCode != http.StatusOK {
		t.Errorf("expected status code 200, got %d", statusCode)
	}
}

func TestAtloProcessor_callAPI_SuccessAfterRetries(t *testing.T) {
	// Skip this test in short mode as it has delays
	if testing.Short() {
		t.Skip("skipping test with retries in short mode")
	}

	// Create a test server that fails first 2 times, then succeeds
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	processor := newAtloProcessor(nil, nil)
	ctx := context.Background()

	statusCode, success := processor.callAPI(ctx, server.URL)

	if !success {
		t.Errorf("expected success but got failure")
	}
	if statusCode != http.StatusOK {
		t.Errorf("expected status code 200, got %d", statusCode)
	}
	if callCount != 3 {
		t.Errorf("expected 3 calls, got %d", callCount)
	}
}

func TestAtloProcessor_callAPI_AllFail(t *testing.T) {
	// Skip this test in short mode as it has many retries with delays
	if testing.Short() {
		t.Skip("skipping test with retries in short mode")
	}

	// Create a test server that always returns 500
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	processor := newAtloProcessor(nil, nil)
	ctx := context.Background()

	statusCode, success := processor.callAPI(ctx, server.URL)

	if success {
		t.Errorf("expected failure but got success")
	}
	// When all retries fail, retryablehttp gives up and returns error without response
	// So status code is 0 (no successful response received)
	if statusCode != 0 {
		t.Errorf("expected status code 0, got %d", statusCode)
	}
	// Should try 6 times total (initial + 5 retries)
	if callCount != 6 {
		t.Errorf("expected 6 calls, got %d", callCount)
	}
}

func TestAtloProcessor_callAPI_ContextCancelled(t *testing.T) {
	// Create a test server with delay
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	processor := newAtloProcessor(nil, nil)
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel context immediately
	cancel()

	statusCode, success := processor.callAPI(ctx, server.URL)

	if success {
		t.Errorf("expected failure due to context cancellation")
	}
	if statusCode != 0 {
		t.Errorf("expected status code 0, got %d", statusCode)
	}
}

func TestAtloProcessor_Process_Success(t *testing.T) {
	// Create a test server that returns 200 OK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create mock repositories
	mockJobs := &mockJobsRepo{}
	mockExecEvents := &mockExecEventsRepo{}

	processor := newAtloProcessor(mockJobs, mockExecEvents)

	// Create a test job
	apiJob, err := job.NewApiCallerJob(job.ApiCallerJobRequest{
		Schedule: "*/5 * * * *",
		Type:     job.AtLeastOnce,
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
	if !mockJobs.moveToProcessingCalled {
		t.Errorf("expected MoveJobToProcessingAndScheduleNext to be called")
	}
	if mockJobs.moveToFailedCalled {
		t.Errorf("did not expect MoveJobToFailed to be called")
	}
	if !mockJobs.removeFromProcessingCalled {
		t.Errorf("expected RemoveFromProcessing to be called")
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
		if event.JobID != apiJob.ID {
			t.Errorf("expected job ID %s, got %s", apiJob.ID, event.JobID)
		}
	}
}

func TestAtloProcessor_Process_AllRetriesFail(t *testing.T) {
	// Skip this test in short mode as it has many retries with delays
	if testing.Short() {
		t.Skip("skipping test with retries in short mode")
	}

	// Create a test server that always fails
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	// Create mock repositories
	mockJobs := &mockJobsRepo{}
	mockExecEvents := &mockExecEventsRepo{}

	processor := newAtloProcessor(mockJobs, mockExecEvents)

	// Create a test job
	apiJob, err := job.NewApiCallerJob(job.ApiCallerJobRequest{
		Schedule: "*/5 * * * *",
		Type:     job.AtLeastOnce,
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
	if !mockJobs.moveToProcessingCalled {
		t.Errorf("expected MoveJobToProcessingAndScheduleNext to be called")
	}
	if !mockJobs.moveToFailedCalled {
		t.Errorf("expected MoveJobToFailed to be called")
	}
	if mockJobs.removeFromProcessingCalled {
		t.Errorf("did not expect RemoveFromProcessing to be called when job fails")
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
		// When all retries fail, retryablehttp gives up and returns error without response
		// So status code is 0 (no successful response received)
		if event.StatusCode != 0 {
			t.Errorf("expected status code 0, got %d", event.StatusCode)
		}
	}
}
