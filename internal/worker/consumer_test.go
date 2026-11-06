package worker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"distributed-job-queue/internal/models"
	"distributed-job-queue/internal/queue"
	"distributed-job-queue/internal/repository"
	"distributed-job-queue/internal/worker"
)

type MockRepository struct {
	mu       sync.Mutex
	jobs     map[string]*models.Job
	attempts map[string]int
	statuses map[string]models.JobStatus
}

func NewMockRepository() *MockRepository {
	return &MockRepository{
		jobs:     make(map[string]*models.Job),
		attempts: make(map[string]int),
		statuses: make(map[string]models.JobStatus),
	}
}

func (m *MockRepository) Create(ctx context.Context, job *models.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[job.ID] = job
	m.statuses[job.ID] = job.Status
	m.attempts[job.ID] = job.Attempts
	return nil
}

func (m *MockRepository) GetByID(ctx context.Context, id string) (*models.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, ok := m.jobs[id]
	if !ok {
		return nil, repository.ErrJobNotFound
	}
	return job, nil
}

func (m *MockRepository) List(ctx context.Context, limit, offset int) ([]*models.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var list []*models.Job
	for _, j := range m.jobs {
		list = append(list, j)
	}
	return list, nil
}

func (m *MockRepository) UpdateStatus(ctx context.Context, id string, status models.JobStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statuses[id] = status
	if j, ok := m.jobs[id]; ok {
		j.Status = status
	}
	return nil
}

func (m *MockRepository) RecordAttempt(ctx context.Context, id string, status models.JobStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.attempts[id]++
	m.statuses[id] = status
	if j, ok := m.jobs[id]; ok {
		j.Attempts = m.attempts[id]
		j.Status = status
	}
	return nil
}

type MockPublisher struct {
	mu           sync.Mutex
	Published    []*models.Job
	Retried      []*queue.QueueJobMessage
	RetryDelays  []time.Duration
	DeadLettered []*queue.QueueJobMessage
	DLQReasons   []string
}

func (m *MockPublisher) Publish(ctx context.Context, job *models.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Published = append(m.Published, job)
	return nil
}

func (m *MockPublisher) PublishRetry(ctx context.Context, msg *queue.QueueJobMessage, delay time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Retried = append(m.Retried, msg)
	m.RetryDelays = append(m.RetryDelays, delay)
	return nil
}

func (m *MockPublisher) PublishDLQ(ctx context.Context, msg *queue.QueueJobMessage, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DeadLettered = append(m.DeadLettered, msg)
	m.DLQReasons = append(m.DLQReasons, reason)
	return nil
}

func (m *MockPublisher) Ping() error  { return nil }
func (m *MockPublisher) Close() error { return nil }

type MockExecutor struct {
	execErr error
	calls   int
	mu      sync.Mutex
}

func (m *MockExecutor) Execute(ctx context.Context, msg *queue.QueueJobMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	return m.execErr
}

func TestBackoffCalculation(t *testing.T) {
	// Base: 1000ms.
	// Attempt 1 -> 1s
	// Attempt 2 -> 2s
	// Attempt 3 -> 4s
	baseDelay := 1000
	expected := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
	}

	for attempt := 1; attempt <= 3; attempt++ {
		multiplier := 1 << (attempt - 1)
		delay := time.Duration(baseDelay*multiplier) * time.Millisecond
		if delay != expected[attempt-1] {
			t.Errorf("attempt %d: expected delay %v, got %v", attempt, expected[attempt-1], delay)
		}
	}
}

func TestSimulatedExecutor_ZeroDuration(t *testing.T) {
	exec := worker.NewSimulatedExecutor()
	msg := &queue.QueueJobMessage{
		JobID: "test-fast",
		Payload: models.JobPayload{
			DurationMS:         0,
			FailureProbability: 0,
		},
	}
	// With 0 duration, defaults to 500ms
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := exec.Execute(ctx, msg)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded due to default 500ms duration, got: %v", err)
	}
}
