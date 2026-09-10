package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"distributed-job-queue/internal/api"
	"distributed-job-queue/internal/models"
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

func TestJobHandler_CreateJob_Phase3Format(t *testing.T) {
	mockRepo := NewMockJobRepository()
	handler := api.NewJobHandler(mockRepo)

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
	handler := api.NewJobHandler(mockRepo)

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
}

func TestJobHandler_GetJobByID_NotFound(t *testing.T) {
	mockRepo := NewMockJobRepository()
	handler := api.NewJobHandler(mockRepo)

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
	handler := api.NewJobHandler(mockRepo)

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
