package repository

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/shreyas/dronc/scheduler/job"
)

// JobsRepository handles Redis operations for jobs
type JobsRepository struct {
	client *redis.Client
}

// NewJobsRepository creates a new JobsRepository
func NewJobsRepository(client *redis.Client) *JobsRepository {
	return &JobsRepository{client: client}
}

// SaveApiCallerJob stores an ApiCallerJob in Redis as a hash
func (r *JobsRepository) SaveApiCallerJob(ctx context.Context, j *job.ApiCallerJob) error {
	key := buildRedisKey(j.ID)
	hash := apiCallerJobToRedisHash(j)

	err := r.client.HSet(ctx, key, hash).Err()
	if err != nil {
		return fmt.Errorf("failed to save api caller job to redis: %w", err)
	}

	return nil
}

// GetApiCallerJob retrieves an ApiCallerJob from Redis by job ID
func (r *JobsRepository) GetApiCallerJob(ctx context.Context, jobID string) (*job.ApiCallerJob, error) {
	key := buildRedisKey(jobID)

	result, err := r.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get job from redis: %w", err)
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	// Use mapper to reconstruct job from hash
	apiJob, err := apiCallerJobFromRedisHash(jobID, result)
	if err != nil {
		return nil, fmt.Errorf("failed to reconstruct job from redis data: %w", err)
	}

	return apiJob, nil
}

// Delete removes a job from Redis by job ID
func (r *JobsRepository) Delete(ctx context.Context, jobID string) error {
	key := buildRedisKey(jobID)

	err := r.client.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("failed to delete job from redis: %w", err)
	}

	return nil
}

// Exists checks if a job exists in Redis by job ID
func (r *JobsRepository) Exists(ctx context.Context, jobID string) (bool, error) {
	key := buildRedisKey(jobID)

	count, err := r.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check job existence in redis: %w", err)
	}

	return count > 0, nil
}

// MultiGetApiCallerJobs retrieves multiple ApiCallerJobs from Redis in a single pipelined operation
// Returns: successful lookups map, failed job IDs, error
func (r *JobsRepository) MultiGetApiCallerJobs(ctx context.Context, jobIDs []string) (map[string]*job.ApiCallerJob, []string, error) {
	if len(jobIDs) == 0 {
		return make(map[string]*job.ApiCallerJob), []string{}, nil
	}

	// Use pipeline for batch fetching
	pipe := r.client.Pipeline()

	// Queue all HGETALL commands
	cmds := make(map[string]*redis.MapStringStringCmd)
	for _, jobID := range jobIDs {
		key := buildRedisKey(jobID)
		cmds[jobID] = pipe.HGetAll(ctx, key)
	}

	// Execute pipeline
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, nil, fmt.Errorf("failed to execute multi-get pipeline: %w", err)
	}

	// Process results
	successfulJobs := make(map[string]*job.ApiCallerJob)
	failedJobIDs := make([]string, 0)

	for jobID, cmd := range cmds {
		result, err := cmd.Result()
		if err != nil {
			// Job fetch failed
			failedJobIDs = append(failedJobIDs, jobID)
			continue
		}

		if len(result) == 0 {
			// Job not found
			failedJobIDs = append(failedJobIDs, jobID)
			continue
		}

		// Reconstruct job from hash
		apiJob, err := apiCallerJobFromRedisHash(jobID, result)
		if err != nil {
			// Job reconstruction failed
			failedJobIDs = append(failedJobIDs, jobID)
			continue
		}

		successfulJobs[jobID] = apiJob
	}

	return successfulJobs, failedJobIDs, nil
}
