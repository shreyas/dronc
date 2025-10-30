package scheduler

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/shreyas/dronc/scheduler/job"
)

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

	manager := NewJobsManager(client)
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
	if hash["namespace"] != "capi" {
		t.Errorf("Saved job namespace = %v, want %v", hash["namespace"], "capi")
	}
}

func TestJobsManager_SetupNewJob_UnsupportedType(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	manager := NewJobsManager(client)
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

	manager := NewJobsManager(client)
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

	manager := NewJobsManager(client)
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
