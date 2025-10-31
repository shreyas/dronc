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
