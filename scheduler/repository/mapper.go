package repository

import (
	"fmt"
	"strconv"

	"github.com/shreyas/dronc/scheduler/job"
)

// apiCallerJobToRedisHash converts an ApiCallerJob to a Redis hash map (without ID)
func apiCallerJobToRedisHash(j *job.ApiCallerJob) map[string]interface{} {
	return map[string]interface{}{
		"schedule": j.Schedule,
		"type":     strconv.Itoa(int(j.Type)),
		"api":      j.API,
	}
}

// apiCallerJobFromRedisHash reconstructs an ApiCallerJob from Redis hash data
// jobID must be provided as it's not stored in the hash
func apiCallerJobFromRedisHash(jobID string, data map[string]string) (*job.ApiCallerJob, error) {
	// Validate required fields
	schedule, ok := data["schedule"]
	if !ok {
		return nil, fmt.Errorf("missing required field: schedule")
	}

	typeStr, ok := data["type"]
	if !ok {
		return nil, fmt.Errorf("missing required field: type")
	}

	api, ok := data["api"]
	if !ok {
		return nil, fmt.Errorf("missing required field: api")
	}

	// Parse type
	typeInt, err := strconv.Atoi(typeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid type value: %w", err)
	}

	// Create job with reconstructed fields
	apiJob := &job.ApiCallerJob{
		Job: job.Job{
			ID:       jobID,
			Schedule: schedule,
			Type:     job.JobRunGuarantee(typeInt),
		},
		API: api,
	}

	return apiJob, nil
}
