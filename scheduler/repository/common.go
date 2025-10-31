package repository

import "fmt"

// redisKeyPrefix is the prefix for all dronc-related Redis keys
const redisKeyPrefix = "dronc:"

// execEventsKey is the redis key for exec-events redis stream
const execEventsKey = redisKeyPrefix + "exec-events"

// processingJobsKey is the redis key for the set holding jobs being processed
const processingJobsKey = redisKeyPrefix + "processing_jobs"

// failedAtloJobsKey is the redis key for the set holding failed at-least-once jobs
const failedAtloJobsKey = redisKeyPrefix + "failed_atlo_jobs"

// buildRedisKey constructs a Redis key with the dronc prefix
func buildRedisKey(jobID string) string {
	return fmt.Sprintf("%s%s", redisKeyPrefix, jobID)
}
