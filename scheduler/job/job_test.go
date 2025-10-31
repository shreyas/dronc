package job

import (
	"testing"
	"time"
)

func TestNewJob(t *testing.T) {
	tests := []struct {
		name     string
		schedule string
		jobType  JobRunGuarantee
		wantErr  bool
		errMsg   string
	}{
		{
			name:     "valid job with AtMostOnce guarantee",
			schedule: "0 0 * * *",
			jobType:  AtMostOnce,
			wantErr:  false,
		},
		{
			name:     "valid job with AtLeastOnce guarantee",
			schedule: "*/5 * * * *",
			jobType:  AtLeastOnce,
			wantErr:  false,
		},
		{
			name:     "invalid cron expression - malformed day-of-week",
			schedule: "0 0 * * 8",
			jobType:  AtMostOnce,
			wantErr:  true,
			errMsg:   "invalid cron expression",
		},
		{
			name:     "invalid cron expression - empty",
			schedule: "",
			jobType:  AtMostOnce,
			wantErr:  true,
			errMsg:   "invalid cron expression",
		},
		{
			name:     "invalid cron expression - malformed",
			schedule: "not a cron",
			jobType:  AtMostOnce,
			wantErr:  true,
			errMsg:   "invalid cron expression",
		},
		{
			name:     "valid cron - every minute",
			schedule: "* * * * *",
			jobType:  AtMostOnce,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job, err := newJob(tt.schedule, tt.jobType)
			if tt.wantErr {
				if err == nil {
					t.Errorf("newJob() expected error but got none")
					return
				}
				if tt.errMsg != "" && len(err.Error()) > 0 {
					// Just check if error message contains the expected substring
					if !contains(err.Error(), tt.errMsg) {
						t.Errorf("newJob() error = %v, want error containing %v", err, tt.errMsg)
					}
				}
				return
			}
			if err != nil {
				t.Errorf("newJob() unexpected error = %v", err)
				return
			}
			if job == nil {
				t.Errorf("newJob() returned nil job")
				return
			}
			if job.Schedule != tt.schedule {
				t.Errorf("newJob() schedule = %v, want %v", job.Schedule, tt.schedule)
			}
			if job.Type != tt.jobType {
				t.Errorf("newJob() type = %v, want %v", job.Type, tt.jobType)
			}
			if job.ID != "" {
				t.Errorf("newJob() ID should be empty, got %v", job.ID)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestJob_NextOccurance(t *testing.T) {
	tests := []struct {
		name     string
		schedule string
		after    int64
		wantErr  bool
	}{
		{
			name:     "daily at midnight - next occurrence",
			schedule: "0 0 * * *",
			after:    time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC).Unix(),
			wantErr:  false,
		},
		{
			name:     "every 5 minutes - next occurrence",
			schedule: "*/5 * * * *",
			after:    time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC).Unix(),
			wantErr:  false,
		},
		{
			name:     "hourly - next occurrence",
			schedule: "0 * * * *",
			after:    time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC).Unix(),
			wantErr:  false,
		},

		// TODO: add tests for jobs that run every second
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job, err := newJob(tt.schedule, AtMostOnce)
			if err != nil {
				t.Fatalf("newJob() failed to create job: %v", err)
			}

			nextOccurrence, err := job.NextOccurance(tt.after)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NextOccurance() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("NextOccurance() unexpected error = %v", err)
				return
			}

			// Verify the next occurrence is after the given time
			if nextOccurrence <= tt.after {
				t.Errorf("NextOccurance() returned %v which is not after %v", nextOccurrence, tt.after)
			}
		})
	}
}

func TestJob_NextOccurance_Sequential(t *testing.T) {
	// Test that we can generate multiple sequential occurrences
	job, err := newJob("*/5 * * * *", AtMostOnce)
	if err != nil {
		t.Fatalf("newJob() failed: %v", err)
	}

	// Start with a known time: 2024-01-01 12:00:00 UTC
	currentTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC).Unix()

	// Generate 5 occurrences
	occurrences := make([]int64, 0, 5)
	for i := 0; i < 5; i++ {
		next, err := job.NextOccurance(currentTime)
		if err != nil {
			t.Fatalf("NextOccurance() iteration %d failed: %v", i, err)
		}

		// Verify it's after current time
		if next <= currentTime {
			t.Errorf("Occurrence %d: got %v, expected > %v", i, next, currentTime)
		}

		occurrences = append(occurrences, next)
		currentTime = next
	}

	// Verify all occurrences are unique and increasing
	for i := 1; i < len(occurrences); i++ {
		if occurrences[i] <= occurrences[i-1] {
			t.Errorf("Occurrences not strictly increasing: %v <= %v", occurrences[i], occurrences[i-1])
		}
	}

	// For a "*/5 * * * *" schedule, occurrences should be ~5 minutes apart
	expectedDiff := int64(5 * 60) // 5 minutes in seconds
	for i := 1; i < len(occurrences); i++ {
		diff := occurrences[i] - occurrences[i-1]
		if diff != expectedDiff {
			t.Errorf("Occurrence %d: difference = %v seconds, want %v seconds", i, diff, expectedDiff)
		}
	}
}
