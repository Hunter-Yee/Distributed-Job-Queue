package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"distributed-job-queue/internal/models"
	"distributed-job-queue/internal/repository"

	"github.com/google/uuid"
)

type JobHandler struct {
	repo repository.JobRepository
}

func NewJobHandler(repo repository.JobRepository) *JobHandler {
	return &JobHandler{repo: repo}
}

func (h *JobHandler) CreateJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req models.CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON payload: %v", err))
		return
	}

	jobType, priority, payload, err := req.Validate()
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	now := time.Now().UTC()
	job := &models.Job{
		ID:        fmt.Sprintf("job-%s", uuid.New().String()),
		Type:      jobType,
		Priority:  priority,
		Payload:   payload,
		Status:    models.StatusQueued,
		Attempts:  0,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := h.repo.Create(r.Context(), job); err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to save job: %v", err))
		return
	}

	writeJSON(w, http.StatusCreated, job)
}

func (h *JobHandler) GetJobByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Extract ID from path using standard Go 1.22 r.PathValue or query/path parsing
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "job ID is required")
		return
	}

	job, err := h.repo.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrJobNotFound) {
			writeJSONError(w, http.StatusNotFound, "job not found")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get job: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, job)
}

func (h *JobHandler) ListJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	query := r.URL.Query()
	limit, _ := strconv.Atoi(query.Get("limit"))
	offset, _ := strconv.Atoi(query.Get("offset"))

	jobs, err := h.repo.List(r.Context(), limit, offset)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to list jobs: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, jobs)
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
