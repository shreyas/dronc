package job

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
)

// ApiCallerJob represents a job that calls an HTTP API on execution
type ApiCallerJob struct {
	Job
	API string
}

// NewApiCallerJob creates a new ApiCallerJob from an ApiCallerJobRequest
func NewApiCallerJob(req ApiCallerJobRequest) (*ApiCallerJob, error) {
	// Create base job (validates schedule)
	job, err := newJob(req.Schedule, "capi", req.Type)
	if err != nil {
		return nil, err
	}

	// Validate and parse URL
	parsedURL, err := url.ParseRequestURI(req.API)
	if err != nil {
		return nil, fmt.Errorf("invalid api url: %w", err)
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return nil, fmt.Errorf("api url must use http or https scheme")
	}

	// Use normalized URL string for consistency
	normalizedURL := parsedURL.String()

	// Generate unique ID based on normalized values
	id := generateApiCallerJobID(job.namespace, job.Schedule, job.Type, normalizedURL)
	job.ID = id

	return &ApiCallerJob{
		Job: *job,
		API: normalizedURL,
	}, nil
}

// generateApiCallerJobID creates a unique ID for the ApiCallerJob
func generateApiCallerJobID(namespace, schedule string, jobType JobRunGuarantee, api string) string {
	hashInput := fmt.Sprintf("api_caller:%s:%d:%s", schedule, jobType, api)
	hash := sha256.Sum256([]byte(hashInput))
	return fmt.Sprintf("%s:%s", namespace, hex.EncodeToString(hash[:]))
}
