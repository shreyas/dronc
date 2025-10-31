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
}

func TestJobsRepository_GetApiCallerJob(t *testing.T) {
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
	retrievedJob, err := repo.GetApiCallerJob(ctx, apiJob.ID)
	if err != nil {
		t.Errorf("GetApiCallerJob() error = %v, want nil", err)
	}

	// Verify retrieved job matches original
	if retrievedJob.ID != apiJob.ID {
		t.Errorf("GetApiCallerJob() ID = %v, want %v", retrievedJob.ID, apiJob.ID)
	}
	if retrievedJob.Schedule != "*/5 * * * *" {
		t.Errorf("GetApiCallerJob() schedule = %v, want %v", retrievedJob.Schedule, "*/5 * * * *")
	}
	if retrievedJob.Type != job.AtLeastOnce {
		t.Errorf("GetApiCallerJob() type = %v, want %v", retrievedJob.Type, job.AtLeastOnce)
	}
	if retrievedJob.API != "https://example.com/webhook" {
		t.Errorf("GetApiCallerJob() api = %v, want %v", retrievedJob.API, "https://example.com/webhook")
	}
	// Note: namespace is not reconstructed from Redis as it's determined by job type
}

func TestJobsRepository_GetApiCallerJob_NotFound(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewJobsRepository(client)
	ctx := context.Background()

	// Try to get non-existent job
	_, err := repo.GetApiCallerJob(ctx, "capi:nonexistent")
	if err == nil {
		t.Errorf("GetApiCallerJob() error = nil, want error for non-existent job")
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

func TestJobsRepository_MoveJobToProcessingAndScheduleNext(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewJobsRepository(client)
	ctx := context.Background()

	// Create test data
	jobSpec := "test-job:1698764400"
	nextTimestamp := int64(1698764700) // 5 minutes later

	// Add job-spec to due_jobs set
	err := client.SAdd(ctx, "dronc:due_jobs", jobSpec).Err()
	if err != nil {
		t.Fatalf("Failed to add job to due_jobs: %v", err)
	}

	// Call the method
	err = repo.MoveJobToProcessingAndScheduleNext(ctx, jobSpec, nextTimestamp)
	if err != nil {
		t.Errorf("MoveJobToProcessingAndScheduleNext() error = %v, want nil", err)
	}

	// Verify job was removed from due_jobs
	isMember, err := client.SIsMember(ctx, "dronc:due_jobs", jobSpec).Result()
	if err != nil {
		t.Fatalf("Failed to check due_jobs membership: %v", err)
	}
	if isMember {
		t.Errorf("Job still exists in due_jobs after move")
	}

	// Verify job was added to processing_jobs
	isMember, err = client.SIsMember(ctx, "dronc:processing_jobs", jobSpec).Result()
	if err != nil {
		t.Fatalf("Failed to check processing_jobs membership: %v", err)
	}
	if !isMember {
		t.Errorf("Job does not exist in processing_jobs after move")
	}

	// Verify next occurrence was scheduled
	score, err := client.ZScore(ctx, "dronc:schedules", jobSpec).Result()
	if err != nil {
		t.Fatalf("Failed to check schedules score: %v", err)
	}
	if int64(score) != nextTimestamp {
		t.Errorf("Scheduled timestamp = %v, want %v", int64(score), nextTimestamp)
	}
}

func TestJobsRepository_MoveJobToProcessingAndScheduleNext_NotFound(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewJobsRepository(client)
	ctx := context.Background()

	// Try to move a job-spec that doesn't exist
	jobSpec := "non-existent-job:1698764400"
	nextTimestamp := int64(1698764700)

	err := repo.MoveJobToProcessingAndScheduleNext(ctx, jobSpec, nextTimestamp)
	if err == nil {
		t.Errorf("MoveJobToProcessingAndScheduleNext() error = nil, want error")
	}
	if err != nil && err.Error() != "job-spec not found in due_jobs set: non-existent-job:1698764400" {
		t.Errorf("MoveJobToProcessingAndScheduleNext() error = %v, want 'job-spec not found in due_jobs set'", err)
	}
}

func TestJobsRepository_MoveJobToProcessingAndScheduleNext_DoesNotOverwriteExistingSchedule(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewJobsRepository(client)
	ctx := context.Background()

	jobSpec := "test-job:1698764400"
	existingTimestamp := int64(1698764500)
	newTimestamp := int64(1698764700)

	// Pre-add job-spec to schedules with an existing timestamp
	err := client.ZAdd(ctx, "dronc:schedules", redis.Z{
		Score:  float64(existingTimestamp),
		Member: jobSpec,
	}).Err()
	if err != nil {
		t.Fatalf("Failed to add job to schedules: %v", err)
	}

	// Add job-spec to due_jobs set
	err = client.SAdd(ctx, "dronc:due_jobs", jobSpec).Err()
	if err != nil {
		t.Fatalf("Failed to add job to due_jobs: %v", err)
	}

	// Call the method with a new timestamp
	err = repo.MoveJobToProcessingAndScheduleNext(ctx, jobSpec, newTimestamp)
	if err != nil {
		t.Errorf("MoveJobToProcessingAndScheduleNext() error = %v, want nil", err)
	}

	// Verify the schedule was NOT overwritten (NX flag in ZADD)
	score, err := client.ZScore(ctx, "dronc:schedules", jobSpec).Result()
	if err != nil {
		t.Fatalf("Failed to check schedules score: %v", err)
	}
	if int64(score) != existingTimestamp {
		t.Errorf("Scheduled timestamp = %v, want %v (should not have been overwritten)", int64(score), existingTimestamp)
	}
}

func TestJobsRepository_MoveJobToFailed(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewJobsRepository(client)
	ctx := context.Background()

	jobSpec := "test-job:1698764400"

	// Add job-spec to processing_jobs set
	err := client.SAdd(ctx, "dronc:processing_jobs", jobSpec).Err()
	if err != nil {
		t.Fatalf("Failed to add job to processing_jobs: %v", err)
	}

	// Call the method
	err = repo.MoveJobToFailed(ctx, jobSpec)
	if err != nil {
		t.Errorf("MoveJobToFailed() error = %v, want nil", err)
	}

	// Verify job was removed from processing_jobs
	isMember, err := client.SIsMember(ctx, "dronc:processing_jobs", jobSpec).Result()
	if err != nil {
		t.Fatalf("Failed to check processing_jobs membership: %v", err)
	}
	if isMember {
		t.Errorf("Job still exists in processing_jobs after move")
	}

	// Verify job was added to failed_atlo_jobs
	isMember, err = client.SIsMember(ctx, "dronc:failed_atlo_jobs", jobSpec).Result()
	if err != nil {
		t.Fatalf("Failed to check failed_atlo_jobs membership: %v", err)
	}
	if !isMember {
		t.Errorf("Job does not exist in failed_atlo_jobs after move")
	}
}

func TestJobsRepository_MoveJobToFailed_NotFound(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewJobsRepository(client)
	ctx := context.Background()

	// Try to move a job-spec that doesn't exist
	jobSpec := "non-existent-job:1698764400"

	err := repo.MoveJobToFailed(ctx, jobSpec)
	if err == nil {
		t.Errorf("MoveJobToFailed() error = nil, want error")
	}
	if err != nil && err.Error() != "job-spec not found in processing_jobs set: non-existent-job:1698764400" {
		t.Errorf("MoveJobToFailed() error = %v, want 'job-spec not found in processing_jobs set'", err)
	}
}

func TestJobsRepository_RemoveFromProcessing(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewJobsRepository(client)
	ctx := context.Background()

	jobSpec := "test-job:1698764400"

	// Add job-spec to processing_jobs set
	err := client.SAdd(ctx, "dronc:processing_jobs", jobSpec).Err()
	if err != nil {
		t.Fatalf("Failed to add job to processing_jobs: %v", err)
	}

	// Call the method
	err = repo.RemoveFromProcessing(ctx, jobSpec)
	if err != nil {
		t.Errorf("RemoveFromProcessing() error = %v, want nil", err)
	}

	// Verify job was removed from processing_jobs
	isMember, err := client.SIsMember(ctx, "dronc:processing_jobs", jobSpec).Result()
	if err != nil {
		t.Fatalf("Failed to check processing_jobs membership: %v", err)
	}
	if isMember {
		t.Errorf("Job still exists in processing_jobs after removal")
	}
}

func TestJobsRepository_RemoveFromProcessing_NotFound(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewJobsRepository(client)
	ctx := context.Background()

	// Try to remove a job-spec that doesn't exist
	jobSpec := "non-existent-job:1698764400"

	err := repo.RemoveFromProcessing(ctx, jobSpec)
	if err == nil {
		t.Errorf("RemoveFromProcessing() error = nil, want error")
	}
	if err != nil && err.Error() != "job-spec not found in processing_jobs set: non-existent-job:1698764400" {
		t.Errorf("RemoveFromProcessing() error = %v, want 'job-spec not found in processing_jobs set'", err)
	}
}
