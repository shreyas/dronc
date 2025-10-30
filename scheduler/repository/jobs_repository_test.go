package repository

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

func TestJobsRepository_SaveApiCallerJob(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewJobsRepository(client)
	ctx := context.Background()

	apiJob, err := job.NewApiCallerJob(job.ApiCallerJobRequest{
		Schedule: "0 0 * * *",
		Type:     job.AtMostOnce,
		API:      "https://example.com/api",
	})
	if err != nil {
		t.Fatalf("Failed to create ApiCallerJob: %v", err)
	}

	// Save job
	err = repo.SaveApiCallerJob(ctx, apiJob)
	if err != nil {
		t.Errorf("SaveApiCallerJob() error = %v, want nil", err)
	}

	// Verify job was saved in Redis
	key := "dronc:" + apiJob.ID
	exists, err := client.Exists(ctx, key).Result()
	if err != nil {
		t.Fatalf("Failed to check key existence: %v", err)
	}
	if exists != 1 {
		t.Errorf("Job key does not exist in Redis after SaveApiCallerJob()")
	}

	// Verify hash fields
	hash, err := client.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatalf("Failed to get hash from Redis: %v", err)
	}

	// Verify ID is NOT stored in the hash
	if _, exists := hash["id"]; exists {
		t.Errorf("Saved hash should not include 'id' field")
	}

	if hash["schedule"] != "0 0 * * *" {
		t.Errorf("Saved hash schedule = %v, want %v", hash["schedule"], "0 0 * * *")
	}
	if hash["api"] != "https://example.com/api" {
		t.Errorf("Saved hash api = %v, want %v", hash["api"], "https://example.com/api")
	}
	if hash["namespace"] != "capi" {
		t.Errorf("Saved hash namespace = %v, want %v", hash["namespace"], "capi")
	}
}

func TestJobsRepository_Get(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewJobsRepository(client)
	ctx := context.Background()

	apiJob, err := job.NewApiCallerJob(job.ApiCallerJobRequest{
		Schedule: "*/5 * * * *",
		Type:     job.AtLeastOnce,
		API:      "https://example.com/webhook",
	})
	if err != nil {
		t.Fatalf("Failed to create ApiCallerJob: %v", err)
	}

	// Save job first
	err = repo.SaveApiCallerJob(ctx, apiJob)
	if err != nil {
		t.Fatalf("SaveApiCallerJob() error = %v", err)
	}

	// Get job
	result, err := repo.Get(ctx, apiJob.ID)
	if err != nil {
		t.Errorf("Get() error = %v, want nil", err)
	}

	// Verify retrieved data (ID should NOT be in hash)
	if _, exists := result["id"]; exists {
		t.Errorf("Get() should not return 'id' field in hash")
	}

	if result["schedule"] != "*/5 * * * *" {
		t.Errorf("Get() schedule = %v, want %v", result["schedule"], "*/5 * * * *")
	}
	if result["type"] != "1" {
		t.Errorf("Get() type = %v, want %v", result["type"], "1")
	}
	if result["api"] != "https://example.com/webhook" {
		t.Errorf("Get() api = %v, want %v", result["api"], "https://example.com/webhook")
	}
	if result["namespace"] != "capi" {
		t.Errorf("Get() namespace = %v, want %v", result["namespace"], "capi")
	}
}

func TestJobsRepository_Get_NotFound(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewJobsRepository(client)
	ctx := context.Background()

	// Try to get non-existent job
	_, err := repo.Get(ctx, "capi:nonexistent")
	if err == nil {
		t.Errorf("Get() error = nil, want error for non-existent job")
	}
}

func TestJobsRepository_Delete(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewJobsRepository(client)
	ctx := context.Background()

	apiJob, err := job.NewApiCallerJob(job.ApiCallerJobRequest{
		Schedule: "0 0 * * *",
		Type:     job.AtMostOnce,
		API:      "https://example.com/api",
	})
	if err != nil {
		t.Fatalf("Failed to create ApiCallerJob: %v", err)
	}

	// Save job first
	err = repo.SaveApiCallerJob(ctx, apiJob)
	if err != nil {
		t.Fatalf("SaveApiCallerJob() error = %v", err)
	}

	// Delete job
	err = repo.Delete(ctx, apiJob.ID)
	if err != nil {
		t.Errorf("Delete() error = %v, want nil", err)
	}

	// Verify job was deleted
	key := "dronc:" + apiJob.ID
	exists, err := client.Exists(ctx, key).Result()
	if err != nil {
		t.Fatalf("Failed to check key existence: %v", err)
	}
	if exists != 0 {
		t.Errorf("Job key still exists in Redis after Delete()")
	}
}

func TestJobsRepository_Exists(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewJobsRepository(client)
	ctx := context.Background()

	apiJob, err := job.NewApiCallerJob(job.ApiCallerJobRequest{
		Schedule: "0 0 * * *",
		Type:     job.AtMostOnce,
		API:      "https://example.com/api",
	})
	if err != nil {
		t.Fatalf("Failed to create ApiCallerJob: %v", err)
	}

	// Check non-existent job
	exists, err := repo.Exists(ctx, apiJob.ID)
	if err != nil {
		t.Errorf("Exists() error = %v, want nil", err)
	}
	if exists {
		t.Errorf("Exists() = true, want false for non-existent job")
	}

	// Save job
	err = repo.SaveApiCallerJob(ctx, apiJob)
	if err != nil {
		t.Fatalf("SaveApiCallerJob() error = %v", err)
	}

	// Check existing job
	exists, err = repo.Exists(ctx, apiJob.ID)
	if err != nil {
		t.Errorf("Exists() error = %v, want nil", err)
	}
	if !exists {
		t.Errorf("Exists() = false, want true for existing job")
	}
}

func TestJobsRepository_SaveMultipleJobs(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewJobsRepository(client)
	ctx := context.Background()

	// Create and save multiple jobs
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

		err = repo.SaveApiCallerJob(ctx, apiJob)
		if err != nil {
			t.Errorf("SaveApiCallerJob() error = %v, want nil", err)
		}

		savedIDs = append(savedIDs, apiJob.ID)
	}

	// Verify all jobs exist
	for _, jobID := range savedIDs {
		exists, err := repo.Exists(ctx, jobID)
		if err != nil {
			t.Errorf("Exists() error = %v for job %v", err, jobID)
		}
		if !exists {
			t.Errorf("Job %v does not exist after SaveApiCallerJob()", jobID)
		}
	}
}
