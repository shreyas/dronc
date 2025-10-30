package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestScheduleApiCaller(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name           string
		requestBody    map[string]interface{}
		expectedStatus int
		expectedError  string
	}{
		{
			name: "valid request with ATMOST_ONCE",
			requestBody: map[string]interface{}{
				"schedule": "0 0 * * *",
				"api":      "https://example.com/webhook",
				"type":     "ATMOST_ONCE",
			},
			expectedStatus: http.StatusCreated,
		},
		{
			name: "valid request with ATLEAST_ONCE",
			requestBody: map[string]interface{}{
				"schedule": "*/5 * * * *",
				"api":      "https://api.example.com/callback",
				"type":     "ATLEAST_ONCE",
			},
			expectedStatus: http.StatusCreated,
		},
		{
			name: "missing schedule field",
			requestBody: map[string]interface{}{
				"api":  "https://example.com/webhook",
				"type": "ATMOST_ONCE",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "INVALID_REQUEST",
		},
		{
			name: "missing api field",
			requestBody: map[string]interface{}{
				"schedule": "0 0 * * *",
				"type":     "ATMOST_ONCE",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "INVALID_REQUEST",
		},
		{
			name: "missing type field",
			requestBody: map[string]interface{}{
				"schedule": "0 0 * * *",
				"api":      "https://example.com/webhook",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "INVALID_REQUEST",
		},
		{
			name: "invalid type value",
			requestBody: map[string]interface{}{
				"schedule": "0 0 * * *",
				"api":      "https://example.com/webhook",
				"type":     "INVALID_TYPE",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "VALIDATION_ERROR",
		},
		{
			name: "invalid URL format - no scheme",
			requestBody: map[string]interface{}{
				"schedule": "0 0 * * *",
				"api":      "example.com/webhook",
				"type":     "ATMOST_ONCE",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "VALIDATION_ERROR",
		},
		{
			name: "invalid URL format - empty (caught by binding)",
			requestBody: map[string]interface{}{
				"schedule": "0 0 * * *",
				"api":      "",
				"type":     "ATMOST_ONCE",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "INVALID_REQUEST",
		},
		{
			name: "invalid cron expression",
			requestBody: map[string]interface{}{
				"schedule": "invalid cron",
				"api":      "https://example.com/webhook",
				"type":     "ATMOST_ONCE",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "JOB_CREATION_FAILED",
		},
		{
			name: "invalid URL scheme - ftp (passes regex, caught by job validator)",
			requestBody: map[string]interface{}{
				"schedule": "0 0 * * *",
				"api":      "ftp://example.com/file",
				"type":     "ATMOST_ONCE",
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "VALIDATION_ERROR",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request
			body, _ := json.Marshal(tt.requestBody)
			req := httptest.NewRequest(http.MethodPost, "/v1/schedule/api-caller", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")

			// Create response recorder
			w := httptest.NewRecorder()

			// Create Gin context
			c, _ := gin.CreateTestContext(w)
			c.Request = req

			// Call handler
			ScheduleApiCaller(c)

			// Check status code
			if w.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, w.Code)
			}

			// Check error code if expecting error
			if tt.expectedError != "" {
				var errResp errorResponse
				if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
					t.Fatalf("failed to parse error response: %v", err)
				}
				if errResp.Error.Code != tt.expectedError {
					t.Errorf("expected error code %s, got %s", tt.expectedError, errResp.Error.Code)
				}
			}

			// Check success response
			if tt.expectedStatus == http.StatusCreated {
				var resp scheduleApiCallerResponse
				if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
					t.Fatalf("failed to parse success response: %v", err)
				}
				if resp.JobID == "" {
					t.Error("expected job_id to be set")
				}
				if resp.Schedule != tt.requestBody["schedule"] {
					t.Errorf("expected schedule %v, got %v", tt.requestBody["schedule"], resp.Schedule)
				}
				if resp.API != tt.requestBody["api"] {
					t.Errorf("expected api %v, got %v", tt.requestBody["api"], resp.API)
				}
				if resp.Type != tt.requestBody["type"] {
					t.Errorf("expected type %v, got %v", tt.requestBody["type"], resp.Type)
				}
			}
		})
	}
}
