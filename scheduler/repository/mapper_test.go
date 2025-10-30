package repository

import (
	"testing"

	"github.com/shreyas/dronc/scheduler/job"
)

func Test_apiCallerJobMapper_redisKey(t *testing.T) {
	apiJob, err := job.NewApiCallerJob(job.ApiCallerJobRequest{
		Schedule: "0 0 * * *",
		Type:     job.AtMostOnce,
		API:      "https://example.com/api",
	})
	if err != nil {
		t.Fatalf("Failed to create ApiCallerJob: %v", err)
	}

	mapper := newApiCallerJobMapper(apiJob)
	key := mapper.redisKey()

	// Key should be "dronc:{job-id}"
	expectedPrefix := "dronc:capi:"
	if len(key) < len(expectedPrefix) || key[:len(expectedPrefix)] != expectedPrefix {
		t.Errorf("redisKey() = %v, want key starting with %v", key, expectedPrefix)
	}

	// Key should include the job ID
	expectedKey := "dronc:" + apiJob.ID
	if key != expectedKey {
		t.Errorf("redisKey() = %v, want %v", key, expectedKey)
	}
}

func Test_apiCallerJobMapper_toRedisHash(t *testing.T) {
	apiJob, err := job.NewApiCallerJob(job.ApiCallerJobRequest{
		Schedule: "0 0 * * *",
		Type:     job.AtMostOnce,
		API:      "https://example.com/api",
	})
	if err != nil {
		t.Fatalf("Failed to create ApiCallerJob: %v", err)
	}

	mapper := newApiCallerJobMapper(apiJob)
	hash := mapper.toRedisHash()

	// Check all required fields are present (ID should NOT be present)
	requiredFields := []string{"schedule", "type", "api", "namespace"}
	for _, field := range requiredFields {
		if _, exists := hash[field]; !exists {
			t.Errorf("toRedisHash() missing field %v", field)
		}
	}

	// Verify ID is NOT in the hash
	if _, exists := hash["id"]; exists {
		t.Errorf("toRedisHash() should not include 'id' field")
	}

	// Validate field values
	if hash["schedule"] != "0 0 * * *" {
		t.Errorf("toRedisHash() schedule = %v, want %v", hash["schedule"], "0 0 * * *")
	}

	if hash["type"] != "0" {
		t.Errorf("toRedisHash() type = %v, want %v", hash["type"], "0")
	}

	if hash["api"] != "https://example.com/api" {
		t.Errorf("toRedisHash() api = %v, want %v", hash["api"], "https://example.com/api")
	}

	if hash["namespace"] != "capi" {
		t.Errorf("toRedisHash() namespace = %v, want %v", hash["namespace"], "capi")
	}
}

func Test_apiCallerJobMapper_typeConversion(t *testing.T) {
	tests := []struct {
		name         string
		jobType      job.JobRunGuarantee
		expectedType string
	}{
		{
			name:         "AtMostOnce type",
			jobType:      job.AtMostOnce,
			expectedType: "0",
		},
		{
			name:         "AtLeastOnce type",
			jobType:      job.AtLeastOnce,
			expectedType: "1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apiJob, err := job.NewApiCallerJob(job.ApiCallerJobRequest{
				Schedule: "0 0 * * *",
				Type:     tt.jobType,
				API:      "https://example.com/api",
			})
			if err != nil {
				t.Fatalf("Failed to create ApiCallerJob: %v", err)
			}

			mapper := newApiCallerJobMapper(apiJob)
			hash := mapper.toRedisHash()

			if hash["type"] != tt.expectedType {
				t.Errorf("toRedisHash() type = %v, want %v", hash["type"], tt.expectedType)
			}
		})
	}
}
