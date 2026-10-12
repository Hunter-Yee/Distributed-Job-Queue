package queue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"distributed-job-queue/internal/models"
	"distributed-job-queue/internal/queue"
)

func TestRabbitMQPublisher_InterfaceCompliance(t *testing.T) {
	var _ queue.JobPublisher = (*queue.RabbitMQPublisher)(nil)
}

func TestRabbitMQPublisher_ClosedState(t *testing.T) {
	// A publisher that has been closed should reject operations with ErrPublisherClosed
	var p queue.RabbitMQPublisher
	_ = p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	job := &models.Job{
		ID:        "job-test-1",
		Type:      models.JobTypeReport,
		Priority:  models.PriorityHigh,
		Payload:   models.JobPayload{DurationMS: 500, FailureProbability: 0},
		Status:    models.StatusQueued,
		CreatedAt: time.Now().UTC(),
	}

	err := p.Publish(ctx, job)
	if !errors.Is(err, queue.ErrPublisherClosed) {
		t.Errorf("expected ErrPublisherClosed when publishing on closed publisher, got %v", err)
	}

	if err := p.Ping(); !errors.Is(err, queue.ErrPublisherClosed) {
		t.Errorf("expected ErrPublisherClosed when pinging closed publisher, got %v", err)
	}
}

func TestRabbitMQPublisher_UnconnectedState(t *testing.T) {
	// An uninitialized publisher should report ErrNotConnected
	var p queue.RabbitMQPublisher

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	job := &models.Job{
		ID:        "job-test-2",
		Type:      models.JobTypeEmail,
		Priority:  models.PriorityLow,
		Payload:   models.JobPayload{DurationMS: 200, FailureProbability: 0},
		Status:    models.StatusQueued,
		CreatedAt: time.Now().UTC(),
	}

	err := p.Publish(ctx, job)
	if !errors.Is(err, queue.ErrNotConnected) {
		t.Errorf("expected ErrNotConnected, got %v", err)
	}

	if err := p.Ping(); !errors.Is(err, queue.ErrNotConnected) {
		t.Errorf("expected ErrNotConnected on ping, got %v", err)
	}
}

func TestRabbitMQPublisher_ConnectInvalidURL(t *testing.T) {
	// Using a guaranteed invalid host with 1 attempt to quickly verify connection failure handling
	_, err := queue.NewRabbitMQPublisherWithRetries("amqp://invalid-host-that-does-not-exist:12345/", 1, 1*time.Millisecond)
	if err == nil {
		t.Errorf("expected error connecting to non-existent host, got nil")
	}
}
