package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/shreyas/dronc/lib/logger"
	"github.com/shreyas/dronc/scheduler/job"
	"github.com/shreyas/dronc/scheduler/repository"
)

func init() {
	// Initialize logger for tests
	_ = logger.Initialize()
}

func setupTestRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	return client, mr
}

func TestJobsManager_SetupNewJob_ApiCallerJob(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := repository.NewJobsRepository(client)
	schedulesRepo := repository.NewSchedulesRepository(client)
	manager := NewJobsManager(repo, schedulesRepo)
	ctx := context.Background()

	apiJob, err := job.NewApiCallerJob(job.ApiCallerJobRequest{
		Schedule: "0 0 * * *",
		Type:     job.AtMostOnce,
		API:      "https://example.com/api",
	})
	if err != nil {
		t.Fatalf("Failed to create ApiCallerJob: %v", err)
	}

	// Setup new job
	err = manager.SetupNewJob(ctx, apiJob)
	if err != nil {
		t.Errorf("SetupNewJob() error = %v, want nil", err)
	}

	// Verify job was saved in Redis
	key := "dronc:" + apiJob.ID
	exists, err := client.Exists(ctx, key).Result()
	if err != nil {
		t.Fatalf("Failed to check key existence: %v", err)
	}
	if exists != 1 {
		t.Errorf("Job key does not exist in Redis after SetupNewJob()")
	}

	// Verify hash contents
	hash, err := client.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatalf("Failed to get hash from Redis: %v", err)
	}

	// Verify ID is NOT in the hash
	if _, exists := hash["id"]; exists {
		t.Errorf("Saved job should not include 'id' field in hash")
	}

	if hash["schedule"] != "0 0 * * *" {
		t.Errorf("Saved job schedule = %v, want %v", hash["schedule"], "0 0 * * *")
	}
	if hash["api"] != "https://example.com/api" {
		t.Errorf("Saved job api = %v, want %v", hash["api"], "https://example.com/api")
	}

	// Verify schedules were created in the sorted set
	schedulesKey := "dronc:schedules"
	schedules, err := client.ZRangeWithScores(ctx, schedulesKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("Failed to read schedules from Redis: %v", err)
	}

	// Should have 5 schedules (numSchedulesToGenerate)
	if len(schedules) != 5 {
		t.Errorf("Expected 5 schedules, got %d", len(schedules))
	}

	// Verify all schedules are formatted correctly (jobID:timestamp)
	for i, schedule := range schedules {
		expectedPrefix := apiJob.ID + ":"
		member := schedule.Member.(string)
		if len(member) <= len(expectedPrefix) || member[:len(expectedPrefix)] != expectedPrefix {
			t.Errorf("Schedule %d member = %v, want format %v<timestamp>", i, member, expectedPrefix)
		}
	}

	// Verify schedules are in chronological order
	for i := 1; i < len(schedules); i++ {
		if schedules[i].Score <= schedules[i-1].Score {
			t.Errorf("Schedules not in chronological order: %v <= %v", schedules[i].Score, schedules[i-1].Score)
		}
	}
}

func TestJobsManager_SetupNewJob_UnsupportedType(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := repository.NewJobsRepository(client)
	schedulesRepo := repository.NewSchedulesRepository(client)
	manager := NewJobsManager(repo, schedulesRepo)
	ctx := context.Background()

	// Try to setup an unsupported job type
	unsupportedJob := "string is not a job type"

	err := manager.SetupNewJob(ctx, unsupportedJob)
	if err == nil {
		t.Errorf("SetupNewJob() error = nil, want error for unsupported job type")
	}

	// Verify error message mentions unsupported type
	expectedErrSubstring := "unsupported job type"
	if err != nil && len(err.Error()) > 0 {
		if !contains(err.Error(), expectedErrSubstring) {
			t.Errorf("SetupNewJob() error = %v, want error containing %v", err, expectedErrSubstring)
		}
	}
}

func TestJobsManager_SetupNewJob_MultipleJobs(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := repository.NewJobsRepository(client)
	schedulesRepo := repository.NewSchedulesRepository(client)
	manager := NewJobsManager(repo, schedulesRepo)
	ctx := context.Background()

	jobs := []job.ApiCallerJobRequest{
		{
			Schedule: "0 0 * * *",
			Type:     job.AtMostOnce,
			API:      "https://example.com/api1",
		},
		{
			Schedule: "*/5 * * * *",
			Type:     job.AtLeastOnce,
			API:      "https://example.com/api2",
		},
		{
			Schedule: "0 12 * * *",
			Type:     job.AtMostOnce,
			API:      "https://example.com/api3",
		},
	}

	var savedIDs []string

	for _, jobReq := range jobs {
		apiJob, err := job.NewApiCallerJob(jobReq)
		if err != nil {
			t.Fatalf("Failed to create ApiCallerJob: %v", err)
		}

		err = manager.SetupNewJob(ctx, apiJob)
		if err != nil {
			t.Errorf("SetupNewJob() error = %v, want nil", err)
		}

		savedIDs = append(savedIDs, apiJob.ID)
	}

	// Verify all jobs exist in Redis
	for _, jobID := range savedIDs {
		key := "dronc:" + jobID
		exists, err := client.Exists(ctx, key).Result()
		if err != nil {
			t.Errorf("Exists() error = %v for job %v", err, jobID)
		}
		if exists != 1 {
			t.Errorf("Job %v does not exist in Redis after SetupNewJob()", jobID)
		}
	}
}

func TestJobsManager_SetupNewJob_IdempotencyCheck(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := repository.NewJobsRepository(client)
	schedulesRepo := repository.NewSchedulesRepository(client)
	manager := NewJobsManager(repo, schedulesRepo)
	ctx := context.Background()

	// Create the same job twice (same parameters)
	req := job.ApiCallerJobRequest{
		Schedule: "0 0 * * *",
		Type:     job.AtMostOnce,
		API:      "https://example.com/api",
	}

	apiJob1, err := job.NewApiCallerJob(req)
	if err != nil {
		t.Fatalf("Failed to create ApiCallerJob: %v", err)
	}

	apiJob2, err := job.NewApiCallerJob(req)
	if err != nil {
		t.Fatalf("Failed to create ApiCallerJob: %v", err)
	}

	// Both jobs should have the same ID (idempotency)
	if apiJob1.ID != apiJob2.ID {
		t.Errorf("Job IDs differ for identical requests: %v != %v", apiJob1.ID, apiJob2.ID)
	}

	// Setup first job
	err = manager.SetupNewJob(ctx, apiJob1)
	if err != nil {
		t.Errorf("SetupNewJob() first call error = %v, want nil", err)
	}

	// Setup second job (should overwrite)
	err = manager.SetupNewJob(ctx, apiJob2)
	if err != nil {
		t.Errorf("SetupNewJob() second call error = %v, want nil", err)
	}

	// Verify only one job exists in Redis
	key := "dronc:" + apiJob1.ID
	exists, err := client.Exists(ctx, key).Result()
	if err != nil {
		t.Fatalf("Failed to check key existence: %v", err)
	}
	if exists != 1 {
		t.Errorf("Expected exactly 1 key in Redis, got %v", exists)
	}
}

func TestJobsManager_StartDueJobsFinder_FindsDueJobs(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := repository.NewJobsRepository(client)
	schedulesRepo := repository.NewSchedulesRepository(client)
	manager := NewJobsManager(repo, schedulesRepo)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Add some jobs that are due now (in the past)
	jobID := "capi:test"
	pastTime := int64(1704110390) // Some time in the past

	err := schedulesRepo.AddSchedules(ctx, jobID, pastTime)
	if err != nil {
		t.Fatalf("Failed to add schedules: %v", err)
	}

	// Start the StartDueJobsFinder
	manager.StartDueJobsFinder(ctx)

	// Get the channel
	dueJobsChan := manager.GetDueJobsChannel()

	// Wait for the due job to be found (with timeout)
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 3*time.Second)
	defer timeoutCancel()

	select {
	case jobSpec := <-dueJobsChan:
		expectedJobSpec := "capi:test:1704110390"
		if jobSpec != expectedJobSpec {
			t.Errorf("Received job spec = %v, want %v", jobSpec, expectedJobSpec)
		}
	case <-timeoutCtx.Done():
		t.Errorf("Timeout waiting for due job to be found")
	}

	// Verify the job was removed from schedules
	schedules, err := client.ZRangeWithScores(ctx, "dronc:schedules", 0, -1).Result()
	if err != nil {
		t.Fatalf("Failed to read schedules: %v", err)
	}
	if len(schedules) != 0 {
		t.Errorf("Expected 0 schedules remaining, got %d", len(schedules))
	}
}

func TestJobsManager_StartDueJobsFinder_GracefulShutdown(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := repository.NewJobsRepository(client)
	schedulesRepo := repository.NewSchedulesRepository(client)
	manager := NewJobsManager(repo, schedulesRepo)
	ctx, cancel := context.WithCancel(context.Background())

	// Start the StartDueJobsFinder
	manager.StartDueJobsFinder(ctx)

	// Cancel the context to trigger shutdown
	cancel()

	// Give it a moment to shutdown gracefully
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer shutdownCancel()
	<-shutdownCtx.Done()

	// If we reach here without hanging, the shutdown was graceful
	// No assertion needed - the test passes if it completes
}

func TestJobsManager_StartDueJobsFinder_HandlesNoJobs(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := repository.NewJobsRepository(client)
	schedulesRepo := repository.NewSchedulesRepository(client)
	manager := NewJobsManager(repo, schedulesRepo)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the StartDueJobsFinder without adding any jobs
	manager.StartDueJobsFinder(ctx)

	// Get the channel
	dueJobsChan := manager.GetDueJobsChannel()

	// Create a timeout context
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 2*time.Second)
	defer timeoutCancel()

	// Verify no jobs are found
	select {
	case jobSpec := <-dueJobsChan:
		t.Errorf("Received unexpected job spec: %v", jobSpec)
	case <-timeoutCtx.Done():
		// Expected - no jobs should be found
	}
}

func TestJobsManager_StartDueJobsFinder_FindsMultipleDueJobs(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := repository.NewJobsRepository(client)
	schedulesRepo := repository.NewSchedulesRepository(client)
	manager := NewJobsManager(repo, schedulesRepo)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Add multiple jobs that are due
	job1ID := "capi:job1"
	job2ID := "capi:job2"
	pastTime1 := int64(1704110390)
	pastTime2 := int64(1704110395)

	err := schedulesRepo.AddSchedules(ctx, job1ID, pastTime1)
	if err != nil {
		t.Fatalf("Failed to add schedules for job1: %v", err)
	}

	err = schedulesRepo.AddSchedules(ctx, job2ID, pastTime2)
	if err != nil {
		t.Fatalf("Failed to add schedules for job2: %v", err)
	}

	// Start the StartDueJobsFinder
	manager.StartDueJobsFinder(ctx)

	// Get the channel
	dueJobsChan := manager.GetDueJobsChannel()

	// Collect found jobs
	foundJobs := make(map[string]bool)
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 3*time.Second)
	defer timeoutCancel()

	for i := 0; i < 2; i++ {
		select {
		case jobSpec := <-dueJobsChan:
			foundJobs[jobSpec] = true
		case <-timeoutCtx.Done():
			t.Fatalf("Timeout waiting for job %d", i+1)
		}
	}

	// Verify both jobs were found
	expectedJob1 := "capi:job1:1704110390"
	expectedJob2 := "capi:job2:1704110395"

	if !foundJobs[expectedJob1] {
		t.Errorf("Expected to find job spec %v", expectedJob1)
	}
	if !foundJobs[expectedJob2] {
		t.Errorf("Expected to find job spec %v", expectedJob2)
	}
}

func TestJobsManager_GetDueJobsChannel(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := repository.NewJobsRepository(client)
	schedulesRepo := repository.NewSchedulesRepository(client)
	manager := NewJobsManager(repo, schedulesRepo)

	// Get the channel
	ch := manager.GetDueJobsChannel()

	if ch == nil {
		t.Errorf("GetDueJobsChannel() returned nil")
	}

	// Verify it's a read-only channel (compile-time check via type)
	// This test mainly ensures the method exists and returns a channel
}

// Helper function for substring matching
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
