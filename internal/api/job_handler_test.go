package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"distributed-job-queue/internal/api"
	"distributed-job-queue/internal/models"
	"distributed-job-queue/internal/queue"
	"distributed-job-queue/internal/repository"
)

type MockJobRepository struct {
	mu   sync.RWMutex
	jobs map[string]*models.Job
}

func NewMockJobRepository() *MockJobRepository {
	return &MockJobRepository{
		jobs: make(map[string]*models.Job),
	}
}

func (m *MockJobRepository) Create(ctx context.Context, job *models.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[job.ID] = job
	return nil
}

func (m *MockJobRepository) GetByID(ctx context.Context, id string) (*models.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	job, exists := m.jobs[id]
	if !exists {
		return nil, repository.ErrJobNotFound
	}
	return job, nil
}

func (m *MockJobRepository) List(ctx context.Context, limit, offset int) ([]*models.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*models.Job
	for _, job := range m.jobs {
		result = append(result, job)
	}
	return result, nil
}

func (m *MockJobRepository) UpdateStatus(ctx context.Context, id string, status models.JobStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, exists := m.jobs[id]
	if !exists {
		return repository.ErrJobNotFound
	}
	job.Status = status
	return nil
}

type MockJobPublisher struct {
	mu        sync.Mutex
	Published []*models.Job
	PublishErr error
	PingErr    error
	Closed     bool
}

func (m *MockJobPublisher) Publish(ctx context.Context, job *models.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.PublishErr != nil {
		return m.PublishErr
	}
	m.Published = append(m.Published, job)
	return nil
}

func (m *MockJobPublisher) Ping() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.PingErr != nil {
		return m.PingErr
	}
	return nil
}

func (m *MockJobPublisher) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Closed = true
	return nil
}

var _ queue.JobPublisher = (*MockJobPublisher)(nil)

func TestJobHandler_CreateJob_Phase3Format(t *testing.T) {
	mockRepo := NewMockJobRepository()
	mockPublisher := &MockJobPublisher{}
	handler := api.NewJobHandler(mockRepo, mockPublisher)

	reqBody := []byte(`{
		"type": "report",
		"priority": "high",
		"duration_ms": 750,
		"failure_probability": 0.05
	}`)

	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBuffer(reqBody))
	w := httptest.NewRecorder()

	handler.CreateJob(w, req)

	res := w.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusCreated {
		t.Fatalf("expected status 201 Created, got %d", res.StatusCode)
	}

	var createdJob models.Job
	if err := json.NewDecoder(res.Body).Decode(&createdJob); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if createdJob.ID == "" {
		t.Errorf("expected non-empty job ID")
	}
	if createdJob.Type != models.JobTypeReport {
		t.Errorf("expected type report, got %s", createdJob.Type)
	}
	if createdJob.Priority != models.PriorityHigh {
		t.Errorf("expected priority high, got %s", createdJob.Priority)
	}
	if createdJob.Status != models.StatusQueued {
		t.Errorf("expected status queued, got %s", createdJob.Status)
	}
	if createdJob.Attempts != 0 {
		t.Errorf("expected attempts 0, got %d", createdJob.Attempts)
	}
	if createdJob.Payload.DurationMS != 750 {
		t.Errorf("expected duration_ms 750, got %d", createdJob.Payload.DurationMS)
	}
	if createdJob.Payload.FailureProbability != 0.05 {
		t.Errorf("expected failure_probability 0.05, got %f", createdJob.Payload.FailureProbability)
	}
}

func TestJobHandler_CreateJob_Phase4Format(t *testing.T) {
	mockRepo := NewMockJobRepository()
	mockPublisher := &MockJobPublisher{}
	handler := api.NewJobHandler(mockRepo, mockPublisher)

	reqBody := []byte(`{
		"type": "image_processing",
		"priority": "low",
		"payload": {
			"duration_ms": 2000,
			"failure_probability": 0.15
		}
	}`)

	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBuffer(reqBody))
	w := httptest.NewRecorder()

	handler.CreateJob(w, req)

	res := w.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusCreated {
		t.Fatalf("expected status 201 Created, got %d", res.StatusCode)
	}

	var createdJob models.Job
	if err := json.NewDecoder(res.Body).Decode(&createdJob); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if createdJob.Type != models.JobTypeImageProcessing {
		t.Errorf("expected type image_processing, got %s", createdJob.Type)
	}
	if createdJob.Payload.DurationMS != 2000 {
		t.Errorf("expected duration_ms 2000, got %d", createdJob.Payload.DurationMS)
	}

	if len(mockPublisher.Published) != 1 {
		t.Errorf("expected 1 published job, got %d", len(mockPublisher.Published))
	}
}

func TestJobHandler_CreateJob_PublishFailureCompensation(t *testing.T) {
	mockRepo := NewMockJobRepository()
	mockPublisher := &MockJobPublisher{
		PublishErr: errors.New("rabbitmq connection failure"),
	}
	handler := api.NewJobHandler(mockRepo, mockPublisher)

	reqBody := []byte(`{
		"type": "email",
		"priority": "high"
	}`)

	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBuffer(reqBody))
	w := httptest.NewRecorder()

	handler.CreateJob(w, req)

	res := w.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected status 500 InternalServerError, got %d", res.StatusCode)
	}

	// Verify compensating transaction: job in repository should have status 'failed'
	jobs, err := mockRepo.List(context.Background(), 10, 0)
	if err != nil {
		t.Fatalf("failed to list jobs from mock repo: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job in repository, got %d", len(jobs))
	}
	if jobs[0].Status != models.StatusFailed {
		t.Errorf("expected job status to be compensated to 'failed', got %s", jobs[0].Status)
	}
}

func TestJobHandler_GetJobByID_NotFound(t *testing.T) {
	mockRepo := NewMockJobRepository()
	mockPublisher := &MockJobPublisher{}
	handler := api.NewJobHandler(mockRepo, mockPublisher)

	req := httptest.NewRequest(http.MethodGet, "/jobs/job-nonexistent", nil)
	req.SetPathValue("id", "job-nonexistent")
	w := httptest.NewRecorder()

	handler.GetJobByID(w, req)

	res := w.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 Not Found, got %d", res.StatusCode)
	}
}

func TestJobHandler_GetJobByID_Found(t *testing.T) {
	mockRepo := NewMockJobRepository()
	mockPublisher := &MockJobPublisher{}
	handler := api.NewJobHandler(mockRepo, mockPublisher)

	existingJob := &models.Job{
		ID:        "job-123",
		Type:      models.JobTypeEmail,
		Priority:  models.PriorityMedium,
		Payload:   models.JobPayload{DurationMS: 300, FailureProbability: 0.0},
		Status:    models.StatusQueued,
		Attempts:  0,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	_ = mockRepo.Create(context.Background(), existingJob)

	req := httptest.NewRequest(http.MethodGet, "/jobs/job-123", nil)
	req.SetPathValue("id", "job-123")
	w := httptest.NewRecorder()

	handler.GetJobByID(w, req)

	res := w.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", res.StatusCode)
	}

	var fetchedJob models.Job
	if err := json.NewDecoder(res.Body).Decode(&fetchedJob); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if fetchedJob.ID != "job-123" {
		t.Errorf("expected job-123, got %s", fetchedJob.ID)
	}
}

func TestJobHandler_ListJobs(t *testing.T) {
	mockRepo := NewMockJobRepository()
	mockPublisher := &MockJobPublisher{}
	handler := api.NewJobHandler(mockRepo, mockPublisher)

	job1 := &models.Job{ID: "job-1", Type: models.JobTypeEmail, Priority: models.PriorityLow, Status: models.StatusQueued}
	job2 := &models.Job{ID: "job-2", Type: models.JobTypeReport, Priority: models.PriorityHigh, Status: models.StatusQueued}
	_ = mockRepo.Create(context.Background(), job1)
	_ = mockRepo.Create(context.Background(), job2)

	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	w := httptest.NewRecorder()

	handler.ListJobs(w, req)

	res := w.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", res.StatusCode)
	}

	var jobs []*models.Job
	if err := json.NewDecoder(res.Body).Decode(&jobs); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(jobs) != 2 {
		t.Errorf("expected 2 jobs, got %d", len(jobs))
	}
}

