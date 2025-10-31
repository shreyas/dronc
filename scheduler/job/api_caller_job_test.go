package job

import (
	"testing"
)

func TestNewApiCallerJob(t *testing.T) {
	tests := []struct {
		name    string
		req     ApiCallerJobRequest
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid http api caller job",
			req: ApiCallerJobRequest{
				Schedule: "0 0 * * *",
				Type:     AtMostOnce,
				API:      "http://example.com/api/endpoint",
			},
			wantErr: false,
		},
		{
			name: "valid https api caller job",
			req: ApiCallerJobRequest{
				Schedule: "*/5 * * * *",
				Type:     AtLeastOnce,
				API:      "https://api.example.com/webhook",
			},
			wantErr: false,
		},
		{
			name: "valid api with port",
			req: ApiCallerJobRequest{
				Schedule: "0 12 * * *",
				Type:     AtMostOnce,
				API:      "https://example.com:8080/api",
			},
			wantErr: false,
		},
		{
			name: "valid api with query params",
			req: ApiCallerJobRequest{
				Schedule: "0 0 * * *",
				Type:     AtMostOnce,
				API:      "https://example.com/api?foo=bar&baz=qux",
			},
			wantErr: false,
		},
		{
			name: "invalid api - empty",
			req: ApiCallerJobRequest{
				Schedule: "0 0 * * *",
				Type:     AtMostOnce,
				API:      "",
			},
			wantErr: true,
			errMsg:  "invalid api url",
		},
		{
			name: "invalid api - not a url",
			req: ApiCallerJobRequest{
				Schedule: "0 0 * * *",
				Type:     AtMostOnce,
				API:      "not a url",
			},
			wantErr: true,
			errMsg:  "invalid api url",
		},
		{
			name: "invalid api - no scheme",
			req: ApiCallerJobRequest{
				Schedule: "0 0 * * *",
				Type:     AtMostOnce,
				API:      "example.com/api",
			},
			wantErr: true,
			errMsg:  "invalid api url",
		},
		{
			name: "invalid api - ftp scheme",
			req: ApiCallerJobRequest{
				Schedule: "0 0 * * *",
				Type:     AtMostOnce,
				API:      "ftp://example.com/file",
			},
			wantErr: true,
			errMsg:  "must use http or https scheme",
		},
		{
			name: "invalid api - file scheme",
			req: ApiCallerJobRequest{
				Schedule: "0 0 * * *",
				Type:     AtMostOnce,
				API:      "file:///etc/passwd",
			},
			wantErr: true,
			errMsg:  "must use http or https scheme",
		},
		{
			name: "invalid schedule in base job",
			req: ApiCallerJobRequest{
				Schedule: "invalid cron",
				Type:     AtMostOnce,
				API:      "https://example.com/api",
			},
			wantErr: true,
			errMsg:  "invalid cron expression",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job, err := NewApiCallerJob(tt.req)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewApiCallerJob() expected error but got none")
					return
				}
				if tt.errMsg != "" && !contains(err.Error(), tt.errMsg) {
					t.Errorf("NewApiCallerJob() error = %v, want error containing %v", err, tt.errMsg)
				}
				return
			}
			if err != nil {
				t.Errorf("NewApiCallerJob() unexpected error = %v", err)
				return
			}
			if job == nil {
				t.Errorf("NewApiCallerJob() returned nil job")
				return
			}
			if job.Schedule != tt.req.Schedule {
				t.Errorf("NewApiCallerJob() schedule = %v, want %v", job.Schedule, tt.req.Schedule)
			}
			if job.Type != tt.req.Type {
				t.Errorf("NewApiCallerJob() type = %v, want %v", job.Type, tt.req.Type)
			}
			if job.API == "" {
				t.Errorf("NewApiCallerJob() API should not be empty")
			}
			if job.ID == "" {
				t.Errorf("NewApiCallerJob() ID should not be empty")
			}
			// Verify ID has the "capi:" namespace prefix
			if len(job.ID) < 5 || job.ID[:5] != "capi:" {
				t.Errorf("NewApiCallerJob() ID should start with 'capi:' prefix, got %v", job.ID)
			}
		})
	}
}

func TestApiCallerJobIDConsistency(t *testing.T) {
	req := ApiCallerJobRequest{
		Schedule: "0 0 * * *",
		Type:     AtMostOnce,
		API:      "https://example.com/api",
	}

	job1, err := NewApiCallerJob(req)
	if err != nil {
		t.Fatalf("NewApiCallerJob() unexpected error = %v", err)
	}

	job2, err := NewApiCallerJob(req)
	if err != nil {
		t.Fatalf("NewApiCallerJob() unexpected error = %v", err)
	}

	if job1.ID != job2.ID {
		t.Errorf("NewApiCallerJob() IDs should be consistent for same parameters, got %v and %v", job1.ID, job2.ID)
	}
}

func TestApiCallerJobIDUniqueness(t *testing.T) {
	baseReq := ApiCallerJobRequest{
		Schedule: "0 0 * * *",
		Type:     AtMostOnce,
		API:      "https://example.com/api",
	}

	job1, err := NewApiCallerJob(baseReq)
	if err != nil {
		t.Fatalf("NewApiCallerJob() unexpected error = %v", err)
	}

	// Different schedule
	req2 := baseReq
	req2.Schedule = "0 12 * * *"
	job2, err := NewApiCallerJob(req2)
	if err != nil {
		t.Fatalf("NewApiCallerJob() unexpected error = %v", err)
	}
	if job1.ID == job2.ID {
		t.Errorf("NewApiCallerJob() IDs should be different for different schedules")
	}

	// Different type
	req3 := baseReq
	req3.Type = AtLeastOnce
	job3, err := NewApiCallerJob(req3)
	if err != nil {
		t.Fatalf("NewApiCallerJob() unexpected error = %v", err)
	}
	if job1.ID == job3.ID {
		t.Errorf("NewApiCallerJob() IDs should be different for different types")
	}

	// Different API
	req4 := baseReq
	req4.API = "https://different.com/api"
	job4, err := NewApiCallerJob(req4)
	if err != nil {
		t.Fatalf("NewApiCallerJob() unexpected error = %v", err)
	}
	if job1.ID == job4.ID {
		t.Errorf("NewApiCallerJob() IDs should be different for different APIs")
	}
}

func TestApiCallerJobURLNormalization(t *testing.T) {
	tests := []struct {
		name        string
		url1        string
		url2        string
		shouldMatch bool
	}{
		{
			name:        "identical urls",
			url1:        "https://example.com/api",
			url2:        "https://example.com/api",
			shouldMatch: true,
		},
		{
			name:        "trailing slash difference",
			url1:        "https://example.com/api",
			url2:        "https://example.com/api/",
			shouldMatch: false, // URL normalization keeps trailing slash difference
		},
		{
			name:        "different paths",
			url1:        "https://example.com/api1",
			url2:        "https://example.com/api2",
			shouldMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req1 := ApiCallerJobRequest{
				Schedule: "0 0 * * *",
				Type:     AtMostOnce,
				API:      tt.url1,
			}
			req2 := ApiCallerJobRequest{
				Schedule: "0 0 * * *",
				Type:     AtMostOnce,
				API:      tt.url2,
			}

			job1, err := NewApiCallerJob(req1)
			if err != nil {
				t.Fatalf("NewApiCallerJob() unexpected error = %v", err)
			}
			job2, err := NewApiCallerJob(req2)
			if err != nil {
				t.Fatalf("NewApiCallerJob() unexpected error = %v", err)
			}

			if tt.shouldMatch && job1.ID != job2.ID {
				t.Errorf("Expected IDs to match for normalized URLs, got %v and %v", job1.ID, job2.ID)
			}
			if !tt.shouldMatch && job1.ID == job2.ID {
				t.Errorf("Expected IDs to differ for different URLs, both got %v", job1.ID)
			}
		})
	}
}
