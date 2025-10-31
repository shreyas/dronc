package repository

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

// SchedulesRepository handles Redis operations for job schedules
type SchedulesRepository struct {
	client *redis.Client
}

// NewSchedulesRepository creates a new SchedulesRepository
func NewSchedulesRepository(client *redis.Client) *SchedulesRepository {
	return &SchedulesRepository{client: client}
}

// AddSchedules adds one or more scheduled occurrences for a job to the sorted set
// The timestamps are used as scores, and the member is "jobID:timestamp" to ensure uniqueness
func (r *SchedulesRepository) AddSchedules(ctx context.Context, jobID string, timestamps ...int64) error {
	if len(timestamps) == 0 {
		return nil // Nothing to add
	}

	// Batch everything
	pipe := r.client.Pipeline()

	// todo: change this eventually by having a per-second-sorted-set (total 60) to distribute the load and members better
	// Add each timestamp as a score with "jobID:timestamp" as member to ensure uniqueness
	for _, ts := range timestamps {
		member := fmt.Sprintf("%s:%d", jobID, ts)
		pipe.ZAdd(ctx, schedulesKey, redis.Z{
			Score:  float64(ts),
			Member: member,
		})
	}

	// Execute all commands in the pipeline
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to add schedules to redis: %w", err)
	}

	return nil
}

// FindDueJobs finds all jobs due until the specified timestamp, moves them to the due_jobs set,
// removes them from the schedules sorted set, and pushes the job-specs (in the format jobID:timestamp) to the provided channel
func (r *SchedulesRepository) FindDueJobs(ctx context.Context, untilTimestamp int64, jobsChan chan<- string) error {
	// Lua script to atomically find due jobs, move them to due_jobs set, and remove from schedules
	var findDueJobsScript = redis.NewScript(`
local schedules_key = KEYS[1]
local due_jobs_key = KEYS[2]
local until_timestamp = ARGV[1]

-- Find all due jobs
local due_jobs = redis.call('ZRANGEBYSCORE', schedules_key, '-inf', until_timestamp)

if #due_jobs > 0 then
    -- Add to due_jobs set
    redis.call('SADD', due_jobs_key, unpack(due_jobs))

    -- Remove from schedules
    redis.call('ZREMRANGEBYSCORE', schedules_key, '-inf', until_timestamp)
end

return due_jobs
`)
	// Execute the Lua script
	result, err := findDueJobsScript.Run(ctx, r.client, []string{schedulesKey, dueJobsKey}, untilTimestamp).Result()
	if err != nil {
		return fmt.Errorf("failed to find due jobs: %w", err)
	}

	// Parse the result (array of jobs in format "jobID:timestamp")
	jobs, ok := result.([]interface{})
	if !ok {
		return fmt.Errorf("unexpected result type from Lua script: %T", result)
	}

	// Extract job IDs and push to channel
	for _, jobspec := range jobs {

		// Push to channel
		select {
		case jobsChan <- jobspec.(string):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
