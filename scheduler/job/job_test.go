package job

import (
	"testing"
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
