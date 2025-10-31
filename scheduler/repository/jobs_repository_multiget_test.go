package repository

import (
	"context"
	"testing"

	"github.com/shreyas/dronc/scheduler/job"
)

func TestJobsRepository_MultiGetApiCallerJobs_Success(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewJobsRepository(client)
	ctx := context.Background()

	// Create and save multiple jobs
	job1, _ := job.NewApiCallerJob(job.ApiCallerJobRequest{
		Schedule: "0 0 * * *",
		Type:     job.AtMostOnce,
		API:      "https://example.com/api1",
	})
	job2, _ := job.NewApiCallerJob(job.ApiCallerJobRequest{
		Schedule: "*/5 * * * *",
		Type:     job.AtLeastOnce,
		API:      "https://example.com/api2",
	})

	_ = repo.SaveApiCallerJob(ctx, job1)
	_ = repo.SaveApiCallerJob(ctx, job2)

	// Multi-get both jobs
	jobIDs := []string{job1.ID, job2.ID}
	jobs, failedIDs, err := repo.MultiGetApiCallerJobs(ctx, jobIDs)

	if err != nil {
		t.Errorf("MultiGetApiCallerJobs() error = %v, want nil", err)
	}

	if len(failedIDs) != 0 {
		t.Errorf("Expected 0 failed IDs, got %d: %v", len(failedIDs), failedIDs)
	}

	if len(jobs) != 2 {
		t.Fatalf("Expected 2 jobs, got %d", len(jobs))
	}

	if jobs[job1.ID].API != job1.API {
		t.Errorf("Job1 API = %v, want %v", jobs[job1.ID].API, job1.API)
	}

	if jobs[job2.ID].API != job2.API {
		t.Errorf("Job2 API = %v, want %v", jobs[job2.ID].API, job2.API)
	}
}

func TestJobsRepository_MultiGetApiCallerJobs_MixedResults(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewJobsRepository(client)
	ctx := context.Background()

	// Create and save only one job
	job1, _ := job.NewApiCallerJob(job.ApiCallerJobRequest{
		Schedule: "0 0 * * *",
		Type:     job.AtMostOnce,
		API:      "https://example.com/api1",
	})
	_ = repo.SaveApiCallerJob(ctx, job1)

	// Try to get one existing and one non-existing job
	jobIDs := []string{job1.ID, "capi:nonexistent"}
	jobs, failedIDs, err := repo.MultiGetApiCallerJobs(ctx, jobIDs)

	if err != nil {
		t.Errorf("MultiGetApiCallerJobs() error = %v, want nil", err)
	}

	if len(jobs) != 1 {
		t.Errorf("Expected 1 successful job, got %d", len(jobs))
	}

	if len(failedIDs) != 1 {
		t.Errorf("Expected 1 failed ID, got %d", len(failedIDs))
	}

	if failedIDs[0] != "capi:nonexistent" {
		t.Errorf("Failed ID = %v, want capi:nonexistent", failedIDs[0])
	}
}

func TestJobsRepository_MultiGetApiCallerJobs_AllNotFound(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewJobsRepository(client)
	ctx := context.Background()

	// Try to get non-existing jobs
	jobIDs := []string{"capi:job1", "capi:job2"}
	jobs, failedIDs, err := repo.MultiGetApiCallerJobs(ctx, jobIDs)

	if err != nil {
		t.Errorf("MultiGetApiCallerJobs() error = %v, want nil", err)
	}

	if len(jobs) != 0 {
		t.Errorf("Expected 0 successful jobs, got %d", len(jobs))
	}

	if len(failedIDs) != 2 {
		t.Errorf("Expected 2 failed IDs, got %d", len(failedIDs))
	}
}

func TestJobsRepository_MultiGetApiCallerJobs_EmptyInput(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewJobsRepository(client)
	ctx := context.Background()

	// Empty input
	jobIDs := []string{}
	jobs, failedIDs, err := repo.MultiGetApiCallerJobs(ctx, jobIDs)

	if err != nil {
		t.Errorf("MultiGetApiCallerJobs() error = %v, want nil", err)
	}

	if len(jobs) != 0 {
		t.Errorf("Expected 0 jobs, got %d", len(jobs))
	}

	if len(failedIDs) != 0 {
		t.Errorf("Expected 0 failed IDs, got %d", len(failedIDs))
	}
}
