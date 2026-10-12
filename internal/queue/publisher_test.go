package queue_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"distributed-job-queue/internal/models"
	"distributed-job-queue/internal/queue"
)

// MockJobPublisher provides an in-memory implementation of queue.JobPublisher for testing.
type MockJobPublisher struct {
	mu        sync.Mutex
	Published []*models.Job
	Closed    bool
	Err       error
}

func (m *MockJobPublisher) Publish(ctx context.Context, job *models.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	m.Published = append(m.Published, job)
	return nil
}

func (m *MockJobPublisher) Ping() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	if m.Closed {
		return queue.ErrPublisherClosed
	}
	return nil
}

func (m *MockJobPublisher) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Closed = true
	return nil
}

func TestJobPublisher_InterfaceCompliance(t *testing.T) {
	var _ queue.JobPublisher = (*MockJobPublisher)(nil)
}

func TestNewQueueJobMessage_Serialization(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	job := &models.Job{
		ID:       "job-uuid-1234",
		Type:     models.JobTypeCompute,
		Priority: models.PriorityCritical,
		Payload: models.JobPayload{
			DurationMS:         1500,
			FailureProbability: 0.12,
		},
		Status:    models.StatusQueued,
		Attempts:  0,
		CreatedAt: now,
		UpdatedAt: now,
	}

	msg := queue.NewQueueJobMessage(job)
	if msg.JobID != job.ID {
		t.Errorf("expected JobID %s, got %s", job.ID, msg.JobID)
	}
	if msg.Type != job.Type {
		t.Errorf("expected Type %s, got %s", job.Type, msg.Type)
	}
	if msg.Priority != job.Priority {
		t.Errorf("expected Priority %s, got %s", job.Priority, msg.Priority)
	}
	if msg.Payload.DurationMS != 1500 {
		t.Errorf("expected DurationMS 1500, got %d", msg.Payload.DurationMS)
	}
	if msg.Payload.FailureProbability != 0.12 {
		t.Errorf("expected FailureProbability 0.12, got %f", msg.Payload.FailureProbability)
	}
	if !msg.CreatedAt.Equal(now) {
		t.Errorf("expected CreatedAt %v, got %v", now, msg.CreatedAt)
	}

	// Test JSON serialization & round-trip
	bytes, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal QueueJobMessage: %v", err)
	}

	var decoded queue.QueueJobMessage
	if err := json.Unmarshal(bytes, &decoded); err != nil {
		t.Fatalf("failed to unmarshal QueueJobMessage: %v", err)
	}

	if decoded.JobID != msg.JobID {
		t.Errorf("expected decoded JobID %s, got %s", msg.JobID, decoded.JobID)
	}
	if decoded.Type != msg.Type {
		t.Errorf("expected decoded Type %s, got %s", msg.Type, decoded.Type)
	}
	if decoded.Priority != msg.Priority {
		t.Errorf("expected decoded Priority %s, got %s", msg.Priority, decoded.Priority)
	}
	if decoded.Payload.DurationMS != 1500 {
		t.Errorf("expected decoded DurationMS 1500, got %d", decoded.Payload.DurationMS)
	}
}

func TestPriorityToAMQP(t *testing.T) {
	tests := []struct {
		name     string
		priority models.JobPriority
		expected uint8
	}{
		{
			name:     "low priority",
			priority: models.PriorityLow,
			expected: 1,
		},
		{
			name:     "medium priority",
			priority: models.PriorityMedium,
			expected: 4,
		},
		{
			name:     "high priority",
			priority: models.PriorityHigh,
			expected: 7,
		},
		{
			name:     "critical priority",
			priority: models.PriorityCritical,
			expected: 9,
		},
		{
			name:     "unknown priority fallback",
			priority: models.JobPriority("unknown"),
			expected: 4,
		},
		{
			name:     "empty priority fallback",
			priority: models.JobPriority(""),
			expected: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queue.PriorityToAMQP(tt.priority)
			if got != tt.expected {
				t.Errorf("PriorityToAMQP(%s) = %d, expected %d", tt.priority, got, tt.expected)
			}
		})
	}
}
