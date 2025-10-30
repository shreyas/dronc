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
	mapper := newApiCallerJobMapper(j)
	key := mapper.redisKey()
	hash := mapper.toRedisHash()

	err := r.client.HSet(ctx, key, hash).Err()
	if err != nil {
		return fmt.Errorf("failed to save api caller job to redis: %w", err)
	}

	return nil
}

// Get retrieves a job from Redis by job ID
func (r *JobsRepository) Get(ctx context.Context, jobID string) (map[string]string, error) {
	key := fmt.Sprintf("dronc:%s", jobID)

	result, err := r.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get job from redis: %w", err)
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	return result, nil
}

// Delete removes a job from Redis by job ID
func (r *JobsRepository) Delete(ctx context.Context, jobID string) error {
	key := fmt.Sprintf("dronc:%s", jobID)

	err := r.client.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("failed to delete job from redis: %w", err)
	}

	return nil
}

// Exists checks if a job exists in Redis by job ID
func (r *JobsRepository) Exists(ctx context.Context, jobID string) (bool, error) {
	key := fmt.Sprintf("dronc:%s", jobID)

	count, err := r.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check job existence in redis: %w", err)
	}

	return count > 0, nil
}
