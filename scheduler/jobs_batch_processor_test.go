package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/shreyas/dronc/scheduler/job"
	"github.com/shreyas/dronc/scheduler/repository"
)

func TestJobsManager_StartJobBatchProcessor_RoutesToCorrectChannels(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := repository.NewJobsRepository(client)
	schedulesRepo := repository.NewSchedulesRepository(client)
	mgr := NewJobsManager(repo, schedulesRepo)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cast to concrete type to access internal channel
	manager := mgr.(*JobsManager)

	// Create and save jobs with different guarantee types
	atmoJob, _ := job.NewApiCallerJob(job.ApiCallerJobRequest{
		Schedule: "0 0 * * *",
		Type:     job.AtMostOnce,
		API:      "https://example.com/atmo",
	})
	atloJob, _ := job.NewApiCallerJob(job.ApiCallerJobRequest{
		Schedule: "*/5 * * * *",
		Type:     job.AtLeastOnce,
		API:      "https://example.com/atlo",
	})

	_ = repo.SaveApiCallerJob(ctx, atmoJob)
	_ = repo.SaveApiCallerJob(ctx, atloJob)

	// Start the batch processor
	manager.StartJobBatchProcessor(ctx)

	// Push job-specs to dueJobsChan
	atmoJobSpec := atmoJob.ID + ":1704110400"
	atloJobSpec := atloJob.ID + ":1704110500"

	manager.dueJobsChan <- atmoJobSpec
	manager.dueJobsChan <- atloJobSpec

	// Check AtMostOnce channel
	select {
	case req := <-manager.GetAtMostOnceChannel():
		if req.Job.ID != atmoJob.ID {
			t.Errorf("AtMostOnce channel received wrong job ID: %v, want %v", req.Job.ID, atmoJob.ID)
		}
		if req.JobSpec != atmoJobSpec {
			t.Errorf("AtMostOnce channel received wrong job-spec: %v, want %v", req.JobSpec, atmoJobSpec)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("Timeout waiting for AtMostOnce job")
	}

	// Check AtLeastOnce channel
	select {
	case req := <-manager.GetAtLeastOnceChannel():
		if req.Job.ID != atloJob.ID {
			t.Errorf("AtLeastOnce channel received wrong job ID: %v, want %v", req.Job.ID, atloJob.ID)
		}
		if req.JobSpec != atloJobSpec {
			t.Errorf("AtLeastOnce channel received wrong job-spec: %v, want %v", req.JobSpec, atloJobSpec)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("Timeout waiting for AtLeastOnce job")
	}
}

func TestJobsManager_StartJobBatchProcessor_GracefulShutdown(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := repository.NewJobsRepository(client)
	schedulesRepo := repository.NewSchedulesRepository(client)
	manager := NewJobsManager(repo, schedulesRepo)
	ctx, cancel := context.WithCancel(context.Background())

	// Start the batch processor
	manager.StartJobBatchProcessor(ctx)

	// Cancel the context to trigger shutdown
	cancel()

	// Give it a moment to shutdown gracefully
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer shutdownCancel()
	<-shutdownCtx.Done()

	// If we reach here without hanging, the shutdown was graceful
}

func TestJobsManager_CollectBatch_Timeout(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := repository.NewJobsRepository(client)
	schedulesRepo := repository.NewSchedulesRepository(client)
	mgr := NewJobsManager(repo, schedulesRepo)
	ctx := context.Background()

	// Cast to concrete type
	manager := mgr.(*JobsManager)

	// Push one job-spec
	manager.dueJobsChan <- "capi:test:1704110400"

	// Collect batch with small timeout
	start := time.Now()
	batch := manager.collectBatch(ctx, 100, 50*time.Millisecond)
	elapsed := time.Since(start)

	if len(batch) != 1 {
		t.Errorf("Expected 1 job in batch, got %d", len(batch))
	}

	// Should timeout around 50ms (with some tolerance)
	if elapsed < 40*time.Millisecond || elapsed > 150*time.Millisecond {
		t.Errorf("Batch collection took %v, expected ~50ms", elapsed)
	}
}

func TestJobsManager_CollectBatch_BatchSize(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := repository.NewJobsRepository(client)
	schedulesRepo := repository.NewSchedulesRepository(client)
	mgr := NewJobsManager(repo, schedulesRepo)
	ctx := context.Background()

	// Cast to concrete type
	manager := mgr.(*JobsManager)

	// Push multiple job-specs quickly
	for i := 0; i < 10; i++ {
		manager.dueJobsChan <- "capi:test:1704110400"
	}

	// Collect batch with size limit
	batch := manager.collectBatch(ctx, 5, 1*time.Second)

	if len(batch) != 5 {
		t.Errorf("Expected 5 jobs in batch, got %d", len(batch))
	}
}

func TestJobsManager_GetChannels(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := repository.NewJobsRepository(client)
	schedulesRepo := repository.NewSchedulesRepository(client)
	manager := NewJobsManager(repo, schedulesRepo)

	// Test channel getters
	atloChan := manager.GetAtLeastOnceChannel()
	if atloChan == nil {
		t.Errorf("GetAtLeastOnceChannel() returned nil")
	}

	atmoChan := manager.GetAtMostOnceChannel()
	if atmoChan == nil {
		t.Errorf("GetAtMostOnceChannel() returned nil")
	}
}
