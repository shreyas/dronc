package api

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	redisClient "github.com/shreyas/dronc/lib/redis"
	"github.com/shreyas/dronc/scheduler/repository"
)

const (
	defaultExecutionsLimit = 50
	maxExecutionsLimit     = 200
)

// executionEventResponse represents a single execution event in the API response.
type executionEventResponse struct {
	ID              string `json:"id"`
	JobID           string `json:"job_id"`
	ScheduledTime   int64  `json:"scheduled_time"`
	ExecutionTime   int64  `json:"execution_time"`
	StatusCode      int    `json:"status_code"`
	Success         bool   `json:"success"`
	TimeTakenMillis int64  `json:"time_taken_ms"`
}

// executionEventsResponse represents the payload returned by the executions tracking endpoint.
type executionEventsResponse struct {
	Count      int                      `json:"count"`
	Executions []executionEventResponse `json:"executions"`
}

// TrackExecutions handles GET /v1/track/executions and returns recent execution events.
func TrackExecutions(c *gin.Context) {
	if redisClient.Client == nil {
		c.JSON(http.StatusInternalServerError, errorResponse{
			Error: errorDetail{
				Code:    "REDIS_NOT_INITIALIZED",
				Message: "Redis client not initialized",
			},
		})
		return
	}

	limit, err := parseLimit(c.Query("limit"))
	if err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{
			Error: errorDetail{
				Code:    "INVALID_REQUEST",
				Message: err.Error(),
				Field:   "limit",
			},
		})
		return
	}

	jobID := c.Query("job_id")

	repo := repository.NewExecEventsRepository(redisClient.Client)
	events, err := repo.ListExecutionEvents(c.Request.Context(), repository.ExecutionEventsQuery{
		JobID: jobID,
		Limit: limit,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, errorResponse{
			Error: errorDetail{
				Code:    "EXECUTION_EVENTS_QUERY_FAILED",
				Message: err.Error(),
			},
		})
		return
	}

	responses := make([]executionEventResponse, len(events))
	for i, event := range events {
		responses[i] = executionEventResponse{
			ID:              event.ID,
			JobID:           event.JobID,
			ScheduledTime:   event.ScheduledTime,
			ExecutionTime:   event.ExecutionTime,
			StatusCode:      event.StatusCode,
			Success:         event.Success,
			TimeTakenMillis: event.TimeTakenMillis,
		}
	}

	c.JSON(http.StatusOK, executionEventsResponse{
		Count:      len(responses),
		Executions: responses,
	})
}

func parseLimit(raw string) (int64, error) {
	if raw == "" {
		return defaultExecutionsLimit, nil
	}

	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, err
	}

	if value <= 0 {
		return 0, fmt.Errorf("limit must be a positive integer")
	}

	if value > maxExecutionsLimit {
		return maxExecutionsLimit, nil
	}

	return value, nil
}
