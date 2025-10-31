package api

import (
	"fmt"
	"net/http"
	"regexp"

	"github.com/gin-gonic/gin"
	"github.com/shreyas/dronc/scheduler"
	"github.com/shreyas/dronc/scheduler/job"
)

var urlRegex = regexp.MustCompile(`^https?://.+$`)

// apiCallerScheduleRequest represents the request to schedule an API caller job
type apiCallerScheduleRequest struct {
	Schedule string `json:"schedule" binding:"required"`
	API      string `json:"api" binding:"required"`
	Type     string `json:"type" binding:"required"`
}

// validate performs validation on the request fields
func (r *apiCallerScheduleRequest) validate() error {
	// Validate URL format
	if !urlRegex.MatchString(r.API) {
		return fmt.Errorf("invalid URL format")
	}

	// Validate type
	if r.Type != "ATMOST_ONCE" && r.Type != "ATLEAST_ONCE" {
		return fmt.Errorf("invalid type: must be ATMOST_ONCE or ATLEAST_ONCE")
	}

	return nil
}

// toJobRequest converts API request to job.ApiCallerJobRequest
func (r *apiCallerScheduleRequest) toJobRequest() (job.ApiCallerJobRequest, error) {
	var jobType job.JobRunGuarantee
	switch r.Type {
	case "ATMOST_ONCE":
		jobType = job.AtMostOnce
	case "ATLEAST_ONCE":
		jobType = job.AtLeastOnce
	default:
		return job.ApiCallerJobRequest{}, fmt.Errorf("invalid type")
	}

	return job.ApiCallerJobRequest{
		Schedule: r.Schedule,
		Type:     jobType,
		API:      r.API,
	}, nil
}

// scheduleApiCallerResponse represents the response after scheduling
type scheduleApiCallerResponse struct {
	JobID    string `json:"job_id"`
	Schedule string `json:"schedule"`
	API      string `json:"api"`
	Type     string `json:"type"`
}

// errorResponse represents a standardized error response
type errorResponse struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

// ScheduleApiCaller handles POST /v1/schedule/api-caller
func ScheduleApiCaller(c *gin.Context) {
	var req apiCallerScheduleRequest

	// Bind JSON request
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{
			Error: errorDetail{
				Code:    "INVALID_REQUEST",
				Message: "Invalid request body: " + err.Error(),
			},
		})
		return
	}

	// Validate request
	if err := req.validate(); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{
			Error: errorDetail{
				Code:    "VALIDATION_ERROR",
				Message: err.Error(),
			},
		})
		return
	}

	// Convert to job request
	jobReq, err := req.toJobRequest()
	if err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{
			Error: errorDetail{
				Code:    "INVALID_TYPE",
				Message: err.Error(),
				Field:   "type",
			},
		})
		return
	}

	// Create job
	apiCallerJob, err := job.NewApiCallerJob(jobReq)
	if err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{
			Error: errorDetail{
				Code:    "JOB_CREATION_FAILED",
				Message: err.Error(),
			},
		})
		return
	}

	// Store the job in Redis and schedule its next occurrences
	// todo: target call should look like this - scheduler.NewJobsManager()
	jobsManager := scheduler.NewJobsManager(nil, nil)
	// todo: pass the jobsManager from main.go (either via ctx, or via gin in someway) instead of creating a new one here
	if err := jobsManager.SetupNewJob(c.Request.Context(), apiCallerJob); err != nil {
		c.JSON(http.StatusInternalServerError, errorResponse{
			Error: errorDetail{
				Code:    "STORAGE_ERROR",
				Message: "Failed to store job: " + err.Error(),
			},
		})
		return
	}

	// Return success response
	c.JSON(http.StatusCreated, scheduleApiCallerResponse{
		JobID:    apiCallerJob.ID,
		Schedule: apiCallerJob.Schedule,
		API:      apiCallerJob.API,
		Type:     req.Type,
	})
}
