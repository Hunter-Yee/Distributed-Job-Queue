package worker

import (
	"context"
	"testing"
	"time"

	"distributed-job-queue/internal/models"
	"distributed-job-queue/internal/queue"
)

func TestSimulatedExecutor_Success(t *testing.T) {
	exec := NewSimulatedExecutor()

	msg := &queue.QueueJobMessage{
		JobID:    "test-job-1",
		Type:     models.JobTypeCompute,
		Priority: models.PriorityMedium,
		Payload: models.JobPayload{
			DurationMS:         20,
			FailureProbability: 0.0,
		},
	}

	start := time.Now()
	err := exec.Execute(context.Background(), msg)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}

	if elapsed < 15*time.Millisecond {
		t.Errorf("expected execution duration >= 15ms, took: %v", elapsed)
	}
}

func TestSimulatedExecutor_Failure(t *testing.T) {
	exec := NewSimulatedExecutor()

	msg := &queue.QueueJobMessage{
		JobID:    "test-job-2",
		Type:     models.JobTypeCompute,
		Priority: models.PriorityMedium,
		Payload: models.JobPayload{
			DurationMS:         10,
			FailureProbability: 1.0, // 100% chance of failure
		},
	}

	err := exec.Execute(context.Background(), msg)
	if err == nil {
		t.Fatalf("expected execution failure, got nil")
	}
}

func TestSimulatedExecutor_ContextCancellation(t *testing.T) {
	exec := NewSimulatedExecutor()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	msg := &queue.QueueJobMessage{
		JobID:    "test-job-3",
		Type:     models.JobTypeCompute,
		Priority: models.PriorityMedium,
		Payload: models.JobPayload{
			DurationMS:         500, // Longer than timeout
			FailureProbability: 0.0,
		},
	}

	err := exec.Execute(ctx, msg)
	if err == nil {
		t.Fatalf("expected context cancellation error, got nil")
	}
}
