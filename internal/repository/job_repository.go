package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"distributed-job-queue/internal/models"
)

var ErrJobNotFound = errors.New("job not found")

type JobRepository interface {
	Create(ctx context.Context, job *models.Job) error
	GetByID(ctx context.Context, id string) (*models.Job, error)
	List(ctx context.Context, limit, offset int) ([]*models.Job, error)
}

type PostgresJobRepository struct {
	db *sql.DB
}

func NewPostgresJobRepository(db *sql.DB) *PostgresJobRepository {
	return &PostgresJobRepository{db: db}
}

func (r *PostgresJobRepository) Create(ctx context.Context, job *models.Job) error {
	payloadJSON, err := json.Marshal(job.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal job payload: %w", err)
	}

	query := `
		INSERT INTO jobs (id, type, priority, payload, status, attempts, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	_, err = r.db.ExecContext(
		ctx,
		query,
		job.ID,
		string(job.Type),
		string(job.Priority),
		payloadJSON,
		string(job.Status),
		job.Attempts,
		job.CreatedAt,
		job.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to insert job into database: %w", err)
	}

	return nil
}

func (r *PostgresJobRepository) GetByID(ctx context.Context, id string) (*models.Job, error) {
	query := `
		SELECT id, type, priority, payload, status, attempts, created_at, updated_at
		FROM jobs
		WHERE id = $1
	`

	var job models.Job
	var typeStr, priorityStr, statusStr string
	var payloadBytes []byte

	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&job.ID,
		&typeStr,
		&priorityStr,
		&payloadBytes,
		&statusStr,
		&job.Attempts,
		&job.CreatedAt,
		&job.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrJobNotFound
		}
		return nil, fmt.Errorf("failed to query job by id: %w", err)
	}

	job.Type = models.JobType(typeStr)
	job.Priority = models.JobPriority(priorityStr)
	job.Status = models.JobStatus(statusStr)

	if err := json.Unmarshal(payloadBytes, &job.Payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job payload: %w", err)
	}

	return &job, nil
}

func (r *PostgresJobRepository) List(ctx context.Context, limit, offset int) ([]*models.Job, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	query := `
		SELECT id, type, priority, payload, status, attempts, created_at, updated_at
		FROM jobs
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`

	rows, err := r.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to query jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*models.Job
	for rows.Next() {
		var job models.Job
		var typeStr, priorityStr, statusStr string
		var payloadBytes []byte

		if err := rows.Scan(
			&job.ID,
			&typeStr,
			&priorityStr,
			&payloadBytes,
			&statusStr,
			&job.Attempts,
			&job.CreatedAt,
			&job.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan job row: %w", err)
		}

		job.Type = models.JobType(typeStr)
		job.Priority = models.JobPriority(priorityStr)
		job.Status = models.JobStatus(statusStr)

		if err := json.Unmarshal(payloadBytes, &job.Payload); err != nil {
			return nil, fmt.Errorf("failed to unmarshal job payload row: %w", err)
		}

		jobs = append(jobs, &job)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating job rows: %w", err)
	}

	if jobs == nil {
		jobs = []*models.Job{}
	}

	return jobs, nil
}
