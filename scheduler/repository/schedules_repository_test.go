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

func TestSchedulesRepository_FindDueJobs_NoDueJobs(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewSchedulesRepository(client)
	ctx := context.Background()

	// Add schedules in the future
	jobID := "capi:future"
	timestamps := []int64{1704120000, 1704130000}
	err := repo.AddSchedules(ctx, jobID, timestamps...)
	if err != nil {
		t.Fatalf("AddSchedules() error = %v", err)
	}

	// Find due jobs with earlier timestamp
	jobIDChan := make(chan string, 10)
	err = repo.FindDueJobs(ctx, 1704110000, jobIDChan)
	close(jobIDChan)

	if err != nil {
		t.Errorf("FindDueJobs() error = %v, want nil", err)
	}

	// Verify no jobs were found
	var receivedJobIDs []string
	for jobID := range jobIDChan {
		receivedJobIDs = append(receivedJobIDs, jobID)
	}

	if len(receivedJobIDs) != 0 {
		t.Errorf("Expected 0 due jobs, got %d: %v", len(receivedJobIDs), receivedJobIDs)
	}

	// Verify schedules still exist in sorted set
	members, err := client.ZRangeWithScores(ctx, schedulesKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("Failed to read sorted set: %v", err)
	}

	if len(members) != 2 {
		t.Errorf("Expected 2 members in schedules, got %d", len(members))
	}
}

func TestSchedulesRepository_FindDueJobs_SingleDueJob(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewSchedulesRepository(client)
	ctx := context.Background()

	// Add a single schedule
	jobID := "capi:single"
	timestamp := int64(1704110400)
	err := repo.AddSchedules(ctx, jobID, timestamp)
	if err != nil {
		t.Fatalf("AddSchedules() error = %v", err)
	}

	// Find due jobs
	jobsChan := make(chan string, 10)
	err = repo.FindDueJobs(ctx, 1704110400, jobsChan)
	close(jobsChan)

	if err != nil {
		t.Errorf("FindDueJobs() error = %v, want nil", err)
	}

	// Verify job was found (as job-spec in format "jobID:timestamp")
	var receivedJobSpecs []string
	for jobSpec := range jobsChan {
		receivedJobSpecs = append(receivedJobSpecs, jobSpec)
	}

	if len(receivedJobSpecs) != 1 {
		t.Fatalf("Expected 1 due job, got %d", len(receivedJobSpecs))
	}

	expectedJobSpec := fmt.Sprintf("%s:%d", jobID, timestamp)
	if receivedJobSpecs[0] != expectedJobSpec {
		t.Errorf("Job spec = %v, want %v", receivedJobSpecs[0], expectedJobSpec)
	}

	// Verify job was removed from schedules
	members, err := client.ZRangeWithScores(ctx, schedulesKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("Failed to read sorted set: %v", err)
	}

	if len(members) != 0 {
		t.Errorf("Expected 0 members in schedules, got %d", len(members))
	}

	// Verify job was added to due_jobs set
	dueJobs, err := client.SMembers(ctx, dueJobsKey).Result()
	if err != nil {
		t.Fatalf("Failed to read due_jobs set: %v", err)
	}

	if len(dueJobs) != 1 {
		t.Fatalf("Expected 1 member in due_jobs, got %d", len(dueJobs))
	}

	if dueJobs[0] != expectedJobSpec {
		t.Errorf("Due job member = %v, want %v", dueJobs[0], expectedJobSpec)
	}
}

func TestSchedulesRepository_FindDueJobs_MultipleDueJobs(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewSchedulesRepository(client)
	ctx := context.Background()

	// Add multiple schedules
	job1ID := "capi:job1"
	job1Timestamps := []int64{1704110400, 1704110700}

	job2ID := "capi:job2"
	job2Timestamps := []int64{1704111000, 1704111300}

	err := repo.AddSchedules(ctx, job1ID, job1Timestamps...)
	if err != nil {
		t.Fatalf("AddSchedules() for job1 error = %v", err)
	}

	err = repo.AddSchedules(ctx, job2ID, job2Timestamps...)
	if err != nil {
		t.Fatalf("AddSchedules() for job2 error = %v", err)
	}

	// Find due jobs up to 1704111000
	jobsChan := make(chan string, 10)
	err = repo.FindDueJobs(ctx, 1704111000, jobsChan)
	close(jobsChan)

	if err != nil {
		t.Errorf("FindDueJobs() error = %v, want nil", err)
	}

	// Verify 3 job-specs were found (2 from job1, 1 from job2)
	var receivedJobSpecs []string
	for jobSpec := range jobsChan {
		receivedJobSpecs = append(receivedJobSpecs, jobSpec)
	}

	if len(receivedJobSpecs) != 3 {
		t.Fatalf("Expected 3 job-specs, got %d: %v", len(receivedJobSpecs), receivedJobSpecs)
	}

	// Verify expected job-specs
	expectedJobSpecs := []string{
		fmt.Sprintf("%s:%d", job1ID, job1Timestamps[0]),
		fmt.Sprintf("%s:%d", job1ID, job1Timestamps[1]),
		fmt.Sprintf("%s:%d", job2ID, job2Timestamps[0]),
	}

	for _, expected := range expectedJobSpecs {
		found := false
		for _, received := range receivedJobSpecs {
			if received == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected job-spec %v not found in received job-specs", expected)
		}
	}

	// Verify only 1 job remains in schedules
	members, err := client.ZRangeWithScores(ctx, schedulesKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("Failed to read sorted set: %v", err)
	}

	if len(members) != 1 {
		t.Errorf("Expected 1 member in schedules, got %d", len(members))
	}

	// Verify 3 jobs were added to due_jobs set
	dueJobs, err := client.SMembers(ctx, dueJobsKey).Result()
	if err != nil {
		t.Fatalf("Failed to read due_jobs set: %v", err)
	}

	if len(dueJobs) != 3 {
		t.Errorf("Expected 3 members in due_jobs, got %d", len(dueJobs))
	}
}

func TestSchedulesRepository_FindDueJobs_MixedDueAndFuture(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewSchedulesRepository(client)
	ctx := context.Background()

	// Add schedules with mix of past and future
	jobID := "capi:mixed"
	allTimestamps := []int64{
		1704110400, // past
		1704110700, // past
		1704120000, // future
		1704130000, // future
	}
	err := repo.AddSchedules(ctx, jobID, allTimestamps...)
	if err != nil {
		t.Fatalf("AddSchedules() error = %v", err)
	}

	// Find due jobs up to 1704110700
	jobsChan := make(chan string, 10)
	err = repo.FindDueJobs(ctx, 1704110700, jobsChan)
	close(jobsChan)

	if err != nil {
		t.Errorf("FindDueJobs() error = %v, want nil", err)
	}

	// Verify 2 job-specs were found
	var receivedJobSpecs []string
	for jobSpec := range jobsChan {
		receivedJobSpecs = append(receivedJobSpecs, jobSpec)
	}

	if len(receivedJobSpecs) != 2 {
		t.Fatalf("Expected 2 due jobs, got %d", len(receivedJobSpecs))
	}

	// Verify expected job-specs with correct timestamps
	expectedJobSpecs := []string{
		fmt.Sprintf("%s:%d", jobID, allTimestamps[0]),
		fmt.Sprintf("%s:%d", jobID, allTimestamps[1]),
	}

	for _, expected := range expectedJobSpecs {
		found := false
		for _, received := range receivedJobSpecs {
			if received == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected job-spec %v not found in received job-specs", expected)
		}
	}

	// Verify 2 jobs remain in schedules (future ones)
	members, err := client.ZRangeWithScores(ctx, schedulesKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("Failed to read sorted set: %v", err)
	}

	if len(members) != 2 {
		t.Errorf("Expected 2 members in schedules, got %d", len(members))
	}

	// Verify future timestamps are correct
	for _, member := range members {
		score := int64(member.Score)
		if score != 1704120000 && score != 1704130000 {
			t.Errorf("Unexpected timestamp in schedules: %d", score)
		}
	}
}

func TestSchedulesRepository_FindDueJobs_EmptySchedules(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewSchedulesRepository(client)
	ctx := context.Background()

	// Find due jobs without adding any schedules
	jobIDChan := make(chan string, 10)
	err := repo.FindDueJobs(ctx, 1704110400, jobIDChan)
	close(jobIDChan)

	if err != nil {
		t.Errorf("FindDueJobs() error = %v, want nil", err)
	}

	// Verify no jobs were found
	var receivedJobIDs []string
	for id := range jobIDChan {
		receivedJobIDs = append(receivedJobIDs, id)
	}

	if len(receivedJobIDs) != 0 {
		t.Errorf("Expected 0 due jobs, got %d", len(receivedJobIDs))
	}

	// Verify due_jobs set doesn't exist or is empty
	exists, err := client.Exists(ctx, dueJobsKey).Result()
	if err != nil {
		t.Fatalf("Failed to check key existence: %v", err)
	}

	if exists != 0 {
		t.Errorf("Expected due_jobs key to not exist")
	}
}

func TestSchedulesRepository_FindDueJobs_KeyNames(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	repo := NewSchedulesRepository(client)
	ctx := context.Background()

	// Add a schedule and find it
	jobID := "capi:keytest"
	timestamp := int64(1704110400)
	err := repo.AddSchedules(ctx, jobID, timestamp)
	if err != nil {
		t.Fatalf("AddSchedules() error = %v", err)
	}

	jobIDChan := make(chan string, 10)
	err = repo.FindDueJobs(ctx, timestamp, jobIDChan)
	close(jobIDChan)

	if err != nil {
		t.Errorf("FindDueJobs() error = %v", err)
	}

	// Verify due_jobs key name and type
	expectedKey := "dronc:due_jobs"
	exists, err := client.Exists(ctx, expectedKey).Result()
	if err != nil {
		t.Fatalf("Failed to check key existence: %v", err)
	}

	if exists != 1 {
		t.Errorf("Expected key %v to exist in Redis", expectedKey)
	}

	// Verify it's a set
	keyType, err := client.Type(ctx, expectedKey).Result()
	if err != nil {
		t.Fatalf("Failed to get key type: %v", err)
	}

	if keyType != "set" {
		t.Errorf("Key type = %v, want set", keyType)
	}
}
