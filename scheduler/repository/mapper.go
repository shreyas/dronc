package repository

import (
	"fmt"
	"strconv"

	"github.com/shreyas/dronc/scheduler/job"
)

// jobMapper is an internal interface for converting jobs to/from Redis hash format
//
//nolint:unused // This interface is used internally by repository methods
type jobMapper interface {
	redisKey() string
	toRedisHash() map[string]interface{}
}

// apiCallerJobMapper implements jobMapper for ApiCallerJob
type apiCallerJobMapper struct {
	job *job.ApiCallerJob
}

// newApiCallerJobMapper creates a new mapper for ApiCallerJob
func newApiCallerJobMapper(j *job.ApiCallerJob) *apiCallerJobMapper {
	return &apiCallerJobMapper{job: j}
}

// redisKey returns the Redis key with "dronc:" prefix
func (m *apiCallerJobMapper) redisKey() string {
	return fmt.Sprintf("dronc:%s", m.job.ID)
}

// toRedisHash converts the job to a Redis hash map (without ID)
func (m *apiCallerJobMapper) toRedisHash() map[string]interface{} {
	return map[string]interface{}{
		"schedule":  m.job.Schedule,
		"type":      strconv.Itoa(int(m.job.Type)),
		"api":       m.job.API,
		"namespace": m.job.Namespace(),
	}
}
