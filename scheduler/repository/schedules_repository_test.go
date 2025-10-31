package repository

import (
	"context"
	"fmt"
	"testing"
)

func TestSchedulesRepository_AddSchedules_Single(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewSchedulesRepository(client)
	ctx := context.Background()

	jobID := "capi:test123"
	timestamp := int64(1704110400) // 2024-01-01 12:00:00 UTC

	// Add single schedule
	err := repo.AddSchedules(ctx, jobID, timestamp)
	if err != nil {
		t.Errorf("AddSchedules() error = %v, want nil", err)
	}

	// Verify schedule was added to Redis sorted set
	members, err := client.ZRangeWithScores(ctx, schedulesKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("Failed to read sorted set from Redis: %v", err)
	}

	if len(members) != 1 {
		t.Fatalf("Expected 1 member in sorted set, got %d", len(members))
	}

	expectedMember := "capi:test123:1704110400"
	if members[0].Member != expectedMember {
		t.Errorf("Member = %v, want %v", members[0].Member, expectedMember)
	}
	if int64(members[0].Score) != timestamp {
		t.Errorf("Score = %v, want %v", int64(members[0].Score), timestamp)
	}
}

func TestSchedulesRepository_AddSchedules_Multiple(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewSchedulesRepository(client)
	ctx := context.Background()

	jobID := "capi:test456"
	timestamps := []int64{
		1704110400, // 2024-01-01 12:00:00 UTC
		1704110700, // 2024-01-01 12:05:00 UTC
		1704111000, // 2024-01-01 12:10:00 UTC
		1704111300, // 2024-01-01 12:15:00 UTC
		1704111600, // 2024-01-01 12:20:00 UTC
	}

	// Add multiple schedules
	err := repo.AddSchedules(ctx, jobID, timestamps...)
	if err != nil {
		t.Errorf("AddSchedules() error = %v, want nil", err)
	}

	// Verify all schedules were added to Redis sorted set
	members, err := client.ZRangeWithScores(ctx, schedulesKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("Failed to read sorted set from Redis: %v", err)
	}

	if len(members) != len(timestamps) {
		t.Fatalf("Expected %d members in sorted set, got %d", len(timestamps), len(members))
	}

	// Verify each timestamp was added correctly
	for i, member := range members {
		expectedMember := fmt.Sprintf("%s:%d", jobID, timestamps[i])
		if member.Member != expectedMember {
			t.Errorf("Member %d = %v, want %v", i, member.Member, expectedMember)
		}
		if int64(member.Score) != timestamps[i] {
			t.Errorf("Score %d = %v, want %v", i, int64(member.Score), timestamps[i])
		}
	}
}

func TestSchedulesRepository_AddSchedules_Empty(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewSchedulesRepository(client)
	ctx := context.Background()

	jobID := "capi:test789"

	// Add no schedules (empty variadic)
	err := repo.AddSchedules(ctx, jobID)
	if err != nil {
		t.Errorf("AddSchedules() with no timestamps error = %v, want nil", err)
	}

	// Verify sorted set is empty
	members, err := client.ZRangeWithScores(ctx, schedulesKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("Failed to read sorted set from Redis: %v", err)
	}

	if len(members) != 0 {
		t.Errorf("Expected 0 members in sorted set, got %d", len(members))
	}
}

func TestSchedulesRepository_AddSchedules_MultipleJobs(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewSchedulesRepository(client)
	ctx := context.Background()

	// Add schedules for multiple jobs
	job1ID := "capi:job1"
	job1Timestamps := []int64{1704110400, 1704110700}

	job2ID := "capi:job2"
	job2Timestamps := []int64{1704111000, 1704111300}

	// Add schedules for job 1
	err := repo.AddSchedules(ctx, job1ID, job1Timestamps...)
	if err != nil {
		t.Errorf("AddSchedules() for job1 error = %v, want nil", err)
	}

	// Add schedules for job 2
	err = repo.AddSchedules(ctx, job2ID, job2Timestamps...)
	if err != nil {
		t.Errorf("AddSchedules() for job2 error = %v, want nil", err)
	}

	// Verify all schedules were added
	members, err := client.ZRangeWithScores(ctx, schedulesKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("Failed to read sorted set from Redis: %v", err)
	}

	expectedTotal := len(job1Timestamps) + len(job2Timestamps)
	if len(members) != expectedTotal {
		t.Fatalf("Expected %d total members in sorted set, got %d", expectedTotal, len(members))
	}

	// Verify members are sorted by score (timestamp)
	for i := 1; i < len(members); i++ {
		if members[i].Score < members[i-1].Score {
			t.Errorf("Members not sorted by score: %v < %v", members[i].Score, members[i-1].Score)
		}
	}
}

func TestSchedulesRepository_AddSchedules_KeyName(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewSchedulesRepository(client)
	ctx := context.Background()

	jobID := "capi:keytest"
	timestamp := int64(1704110400)

	// Add schedule
	err := repo.AddSchedules(ctx, jobID, timestamp)
	if err != nil {
		t.Errorf("AddSchedules() error = %v, want nil", err)
	}

	// Verify key name is "dronc:schedules"
	expectedKey := "dronc:schedules"
	exists, err := client.Exists(ctx, expectedKey).Result()
	if err != nil {
		t.Fatalf("Failed to check key existence: %v", err)
	}

	if exists != 1 {
		t.Errorf("Expected key %v to exist in Redis", expectedKey)
	}

	// Verify it's a sorted set
	keyType, err := client.Type(ctx, expectedKey).Result()
	if err != nil {
		t.Fatalf("Failed to get key type: %v", err)
	}

	if keyType != "zset" {
		t.Errorf("Key type = %v, want zset", keyType)
	}
}
