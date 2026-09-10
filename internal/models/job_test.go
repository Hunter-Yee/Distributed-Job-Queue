package models_test

import (
	"encoding/json"
	"testing"

	"distributed-job-queue/internal/models"
)

func TestCreateJobRequestValidation_Phase3Format(t *testing.T) {
	jsonPayload := `{
		"type": "report",
		"priority": "high",
		"duration_ms": 750,
		"failure_probability": 0.05
	}`

	var req models.CreateJobRequest
	err := json.Unmarshal([]byte(jsonPayload), &req)
	if err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}

	jobType, priority, payload, err := req.Validate()
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}

	if jobType != models.JobTypeReport {
		t.Errorf("expected jobType %s, got %s", models.JobTypeReport, jobType)
	}
	if priority != models.PriorityHigh {
		t.Errorf("expected priority %s, got %s", models.PriorityHigh, priority)
	}
	if payload.DurationMS != 750 {
		t.Errorf("expected duration_ms 750, got %d", payload.DurationMS)
	}
	if payload.FailureProbability != 0.05 {
		t.Errorf("expected failure_probability 0.05, got %f", payload.FailureProbability)
	}
}

func TestCreateJobRequestValidation_Phase4Format(t *testing.T) {
	jsonPayload := `{
		"type": "compute",
		"priority": "critical",
		"payload": {
			"duration_ms": 1200,
			"failure_probability": 0.1
		}
	}`

	var req models.CreateJobRequest
	err := json.Unmarshal([]byte(jsonPayload), &req)
	if err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}

	jobType, priority, payload, err := req.Validate()
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}

	if jobType != models.JobTypeCompute {
		t.Errorf("expected jobType %s, got %s", models.JobTypeCompute, jobType)
	}
	if priority != models.PriorityCritical {
		t.Errorf("expected priority %s, got %s", models.PriorityCritical, priority)
	}
	if payload.DurationMS != 1200 {
		t.Errorf("expected duration_ms 1200, got %d", payload.DurationMS)
	}
	if payload.FailureProbability != 0.1 {
		t.Errorf("expected failure_probability 0.1, got %f", payload.FailureProbability)
	}
}

func TestCreateJobRequestValidation_InvalidType(t *testing.T) {
	jsonPayload := `{
		"type": "invalid_type",
		"priority": "low"
	}`

	var req models.CreateJobRequest
	_ = json.Unmarshal([]byte(jsonPayload), &req)

	_, _, _, err := req.Validate()
	if err == nil {
		t.Errorf("expected error for invalid job type, got nil")
	}
}

func TestCreateJobRequestValidation_Defaults(t *testing.T) {
	jsonPayload := `{
		"type": "email"
	}`

	var req models.CreateJobRequest
	_ = json.Unmarshal([]byte(jsonPayload), &req)

	jobType, priority, payload, err := req.Validate()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if jobType != models.JobTypeEmail {
		t.Errorf("expected jobType email, got %s", jobType)
	}
	if priority != models.PriorityMedium {
		t.Errorf("expected default priority medium, got %s", priority)
	}
	if payload.DurationMS != 500 {
		t.Errorf("expected default duration 500, got %d", payload.DurationMS)
	}
	if payload.FailureProbability != 0.0 {
		t.Errorf("expected default failure_probability 0.0, got %f", payload.FailureProbability)
	}
}
