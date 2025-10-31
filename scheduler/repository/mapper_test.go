package repository

import (
	"testing"

	"github.com/shreyas/dronc/scheduler/job"
)

func Test_apiCallerJobToRedisHash(t *testing.T) {
	apiJob, err := job.NewApiCallerJob(job.ApiCallerJobRequest{
		Schedule: "0 0 * * *",
		Type:     job.AtMostOnce,
		API:      "https://example.com/api",
	})
	if err != nil {
		t.Fatalf("Failed to create ApiCallerJob: %v", err)
	}

	hash := apiCallerJobToRedisHash(apiJob)

	// Check all required fields are present (ID should NOT be present)
	requiredFields := []string{"schedule", "type", "api"}
	for _, field := range requiredFields {
		if _, exists := hash[field]; !exists {
			t.Errorf("apiCallerJobToRedisHash() missing field %v", field)
		}
	}

	// Verify ID is NOT in the hash
	if _, exists := hash["id"]; exists {
		t.Errorf("apiCallerJobToRedisHash() should not include 'id' field")
	}

	// Validate field values
	if hash["schedule"] != "0 0 * * *" {
		t.Errorf("apiCallerJobToRedisHash() schedule = %v, want %v", hash["schedule"], "0 0 * * *")
	}

	if hash["type"] != "0" {
		t.Errorf("apiCallerJobToRedisHash() type = %v, want %v", hash["type"], "0")
	}

	if hash["api"] != "https://example.com/api" {
		t.Errorf("apiCallerJobToRedisHash() api = %v, want %v", hash["api"], "https://example.com/api")
	}
}

func Test_apiCallerJobToRedisHash_typeConversion(t *testing.T) {
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

			hash := apiCallerJobToRedisHash(apiJob)

			if hash["type"] != tt.expectedType {
				t.Errorf("apiCallerJobToRedisHash() type = %v, want %v", hash["type"], tt.expectedType)
			}
		})
	}
}

func Test_apiCallerJobFromRedisHash(t *testing.T) {
	tests := []struct {
		name      string
		jobID     string
		data      map[string]string
		wantErr   bool
		wantJob   *job.ApiCallerJob
		errSubstr string
	}{
		{
			name:  "valid hash data",
			jobID: "capi:abc123",
			data: map[string]string{
				"schedule":  "0 0 * * *",
				"type":      "0",
				"api":       "https://example.com/api",
				"namespace": "capi",
			},
			wantErr: false,
			wantJob: &job.ApiCallerJob{},
		},
		{
			name:  "missing schedule field",
			jobID: "capi:abc123",
			data: map[string]string{
				"type":      "0",
				"api":       "https://example.com/api",
				"namespace": "capi",
			},
			wantErr:   true,
			errSubstr: "missing required field: schedule",
		},
		{
			name:  "missing type field",
			jobID: "capi:abc123",
			data: map[string]string{
				"schedule":  "0 0 * * *",
				"api":       "https://example.com/api",
				"namespace": "capi",
			},
			wantErr:   true,
			errSubstr: "missing required field: type",
		},
		{
			name:  "invalid type value",
			jobID: "capi:abc123",
			data: map[string]string{
				"schedule":  "0 0 * * *",
				"type":      "invalid",
				"api":       "https://example.com/api",
				"namespace": "capi",
			},
			wantErr:   true,
			errSubstr: "invalid type value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apiJob, err := apiCallerJobFromRedisHash(tt.jobID, tt.data)

			if tt.wantErr {
				if err == nil {
					t.Errorf("apiCallerJobFromRedisHash() error = nil, want error")
					return
				}
				if tt.errSubstr != "" && !contains(err.Error(), tt.errSubstr) {
					t.Errorf("apiCallerJobFromRedisHash() error = %v, want error containing %v", err, tt.errSubstr)
				}
				return
			}

			if err != nil {
				t.Errorf("apiCallerJobFromRedisHash() error = %v, want nil", err)
				return
			}

			// Verify reconstructed job
			if apiJob.ID != tt.jobID {
				t.Errorf("apiCallerJobFromRedisHash() ID = %v, want %v", apiJob.ID, tt.jobID)
			}
			if apiJob.Schedule != tt.data["schedule"] {
				t.Errorf("apiCallerJobFromRedisHash() Schedule = %v, want %v", apiJob.Schedule, tt.data["schedule"])
			}
			if apiJob.API != tt.data["api"] {
				t.Errorf("apiCallerJobFromRedisHash() API = %v, want %v", apiJob.API, tt.data["api"])
			}
		})
	}
}

func Test_apiCallerJobRoundTrip(t *testing.T) {
	// Test that we can convert job -> hash -> job and get the same data
	originalJob, err := job.NewApiCallerJob(job.ApiCallerJobRequest{
		Schedule: "*/5 * * * *",
		Type:     job.AtLeastOnce,
		API:      "https://example.com/webhook",
	})
	if err != nil {
		t.Fatalf("Failed to create ApiCallerJob: %v", err)
	}

	// Convert to hash
	hash := apiCallerJobToRedisHash(originalJob)

	// Convert string map for fromRedisHash
	stringHash := make(map[string]string)
	for k, v := range hash {
		stringHash[k] = v.(string)
	}

	// Convert back to job
	reconstructedJob, err := apiCallerJobFromRedisHash(originalJob.ID, stringHash)
	if err != nil {
		t.Fatalf("Failed to reconstruct job: %v", err)
	}

	// Verify round trip
	if reconstructedJob.ID != originalJob.ID {
		t.Errorf("Round trip ID mismatch: got %v, want %v", reconstructedJob.ID, originalJob.ID)
	}
	if reconstructedJob.Schedule != originalJob.Schedule {
		t.Errorf("Round trip Schedule mismatch: got %v, want %v", reconstructedJob.Schedule, originalJob.Schedule)
	}
	if reconstructedJob.Type != originalJob.Type {
		t.Errorf("Round trip Type mismatch: got %v, want %v", reconstructedJob.Type, originalJob.Type)
	}
	if reconstructedJob.API != originalJob.API {
		t.Errorf("Round trip API mismatch: got %v, want %v", reconstructedJob.API, originalJob.API)
	}
}

// Helper function for substring matching
func contains(s, substr string) bool {
	return len(s) >= len(substr) && findSubstring(s, substr)
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
