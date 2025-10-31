package repository

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setupExecEventsRepo(t *testing.T) (*ExecEventsRepository, func()) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	repo := NewExecEventsRepository(client)

	cleanup := func() {
		_ = client.Close()
		mr.Close()
	}

	return repo, cleanup
}

func TestExecEventsRepository_ListExecutionEvents_ReturnsEventsInReverseChronologicalOrder(t *testing.T) {
	repo, cleanup := setupExecEventsRepo(t)
	defer cleanup()

	ctx := context.Background()

	events := []ExecutionEvent{
		{
			JobID:           "job-1",
			ScheduledTime:   100,
			ExecutionTime:   150,
			StatusCode:      200,
			Success:         true,
			TimeTakenMillis: 25,
		},
		{
			JobID:           "job-2",
			ScheduledTime:   200,
			ExecutionTime:   250,
			StatusCode:      500,
			Success:         false,
			TimeTakenMillis: 40,
		},
		{
			JobID:           "job-3",
			ScheduledTime:   300,
			ExecutionTime:   350,
			StatusCode:      201,
			Success:         true,
			TimeTakenMillis: 30,
		},
	}

	for _, event := range events {
		if err := repo.SaveExecutionEvent(ctx, event); err != nil {
			t.Fatalf("failed to save execution event: %v", err)
		}
	}

	records, err := repo.ListExecutionEvents(ctx, ExecutionEventsQuery{})
	if err != nil {
		t.Fatalf("ListExecutionEvents returned error: %v", err)
	}

	if len(records) != len(events) {
		t.Fatalf("expected %d records, got %d", len(events), len(records))
	}

	for i, record := range records {
		expected := events[len(events)-1-i]
		if record.JobID != expected.JobID {
			t.Errorf("record %d job_id = %s, want %s", i, record.JobID, expected.JobID)
		}
		if record.ScheduledTime != expected.ScheduledTime {
			t.Errorf("record %d scheduled_time = %d, want %d", i, record.ScheduledTime, expected.ScheduledTime)
		}
		if record.ExecutionTime != expected.ExecutionTime {
			t.Errorf("record %d execution_time = %d, want %d", i, record.ExecutionTime, expected.ExecutionTime)
		}
		if record.StatusCode != expected.StatusCode {
			t.Errorf("record %d status_code = %d, want %d", i, record.StatusCode, expected.StatusCode)
		}
		if record.Success != expected.Success {
			t.Errorf("record %d success = %v, want %v", i, record.Success, expected.Success)
		}
		if record.TimeTakenMillis != expected.TimeTakenMillis {
			t.Errorf("record %d time_taken_ms = %d, want %d", i, record.TimeTakenMillis, expected.TimeTakenMillis)
		}
		if record.ID == "" {
			t.Errorf("record %d should include stream id", i)
		}
	}
}

func TestExecEventsRepository_ListExecutionEvents_AppliesLimitAndFilters(t *testing.T) {
	repo, cleanup := setupExecEventsRepo(t)
	defer cleanup()

	ctx := context.Background()

	// Insert multiple events with overlapping job IDs
	for i := 0; i < 5; i++ {
		event := ExecutionEvent{
			JobID:           "job-1",
			ScheduledTime:   int64(100 + i),
			ExecutionTime:   int64(150 + i),
			StatusCode:      200,
			Success:         true,
			TimeTakenMillis: int64(20 + i),
		}
		if err := repo.SaveExecutionEvent(ctx, event); err != nil {
			t.Fatalf("failed to save execution event: %v", err)
		}
	}

	for i := 0; i < 3; i++ {
		event := ExecutionEvent{
			JobID:           "job-2",
			ScheduledTime:   int64(200 + i),
			ExecutionTime:   int64(250 + i),
			StatusCode:      500,
			Success:         false,
			TimeTakenMillis: int64(30 + i),
		}
		if err := repo.SaveExecutionEvent(ctx, event); err != nil {
			t.Fatalf("failed to save execution event: %v", err)
		}
	}

	records, err := repo.ListExecutionEvents(ctx, ExecutionEventsQuery{JobID: "job-2", Limit: 2})
	if err != nil {
		t.Fatalf("ListExecutionEvents returned error: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	for _, record := range records {
		if record.JobID != "job-2" {
			t.Fatalf("expected only job-2 records, got %s", record.JobID)
		}
	}

	if records[0].ExecutionTime <= records[1].ExecutionTime {
		t.Errorf("expected records to be ordered by recency")
	}
}

func TestExecEventsRepository_ListExecutionEvents_EmptyResults(t *testing.T) {
	repo, cleanup := setupExecEventsRepo(t)
	defer cleanup()

	ctx := context.Background()

	records, err := repo.ListExecutionEvents(ctx, ExecutionEventsQuery{})
	if err != nil {
		t.Fatalf("ListExecutionEvents returned error: %v", err)
	}

	if len(records) != 0 {
		t.Fatalf("expected empty results, got %d", len(records))
	}
}
