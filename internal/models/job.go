package models

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// JobType represents the type of task to perform.
type JobType string

const (
	JobTypeCompute         JobType = "compute"
	JobTypeEmail           JobType = "email"
	JobTypeReport          JobType = "report"
	JobTypeImageProcessing JobType = "image_processing"
)

// JobPriority represents the priority level of a job.
type JobPriority string

const (
	PriorityLow      JobPriority = "low"
	PriorityMedium   JobPriority = "medium"
	PriorityHigh     JobPriority = "high"
	PriorityCritical JobPriority = "critical"
)

// JobStatus represents the current state of a job in the system.
type JobStatus string

const (
	StatusQueued     JobStatus = "queued"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
)

// JobPayload contains execution parameters for simulated workloads.
type JobPayload struct {
	DurationMS         int     `json:"duration_ms"`
	FailureProbability float64 `json:"failure_probability"`
}

// Job is the primary model representing a job in the queue platform.
type Job struct {
	ID        string      `json:"id"`
	Type      JobType     `json:"type"`
	Priority  JobPriority `json:"priority"`
	Payload   JobPayload  `json:"payload"`
	Status    JobStatus   `json:"status"`
	Attempts  int         `json:"attempts"`
	CreatedAt time.Time   `json:"created_at"`
	UpdatedAt time.Time   `json:"updated_at"`
}

// CreateJobRequest defines the DTO for creating a job.
// It supports both flat format (Phase 3) and nested payload format (Phase 4).
type CreateJobRequest struct {
	Type               string      `json:"type"`
	Priority           string      `json:"priority"`
	Payload            *JobPayload `json:"payload,omitempty"`
	DurationMS         *int        `json:"duration_ms,omitempty"`
	FailureProbability *float64    `json:"failure_probability,omitempty"`
}

// UnmarshalJSON custom unmarshaler to gracefully handle both flat and nested JSON requests.
func (r *CreateJobRequest) UnmarshalJSON(data []byte) error {
	type Alias CreateJobRequest
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(r),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	return nil
}

// Validate checks if the job creation request has valid fields and returns a populated JobPayload.
func (r *CreateJobRequest) Validate() (JobType, JobPriority, JobPayload, error) {
	jobType := JobType(strings.ToLower(strings.TrimSpace(r.Type)))
	switch jobType {
	case JobTypeCompute, JobTypeEmail, JobTypeReport, JobTypeImageProcessing:
	case "":
		return "", "", JobPayload{}, fmt.Errorf("job 'type' is required")
	default:
		return "", "", JobPayload{}, fmt.Errorf("invalid job type '%s', allowed types: compute, email, report, image_processing", r.Type)
	}

	priorityStr := strings.ToLower(strings.TrimSpace(r.Priority))
	if priorityStr == "" {
		priorityStr = string(PriorityMedium)
	}
	priority := JobPriority(priorityStr)
	switch priority {
	case PriorityLow, PriorityMedium, PriorityHigh, PriorityCritical:
	default:
		return "", "", JobPayload{}, fmt.Errorf("invalid priority '%s', allowed priorities: low, medium, high, critical", r.Priority)
	}

	payload := JobPayload{
		DurationMS:         500,
		FailureProbability: 0.0,
	}

	// If explicit payload is provided, use its values
	if r.Payload != nil {
		if r.Payload.DurationMS > 0 {
			payload.DurationMS = r.Payload.DurationMS
		}
		if r.Payload.FailureProbability >= 0 && r.Payload.FailureProbability <= 1.0 {
			payload.FailureProbability = r.Payload.FailureProbability
		}
	}

	// Override with flat fields if specified (Phase 3 compatibility)
	if r.DurationMS != nil && *r.DurationMS > 0 {
		payload.DurationMS = *r.DurationMS
	}
	if r.FailureProbability != nil && *r.FailureProbability >= 0 && *r.FailureProbability <= 1.0 {
		payload.FailureProbability = *r.FailureProbability
	}

	if payload.FailureProbability < 0 || payload.FailureProbability > 1.0 {
		return "", "", JobPayload{}, fmt.Errorf("failure_probability must be between 0.0 and 1.0")
	}

	return jobType, priority, payload, nil
}
