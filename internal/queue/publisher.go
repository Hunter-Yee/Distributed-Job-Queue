package queue

import (
	"context"
	"time"

	"distributed-job-queue/internal/models"
)

const (
	// DefaultJobsQueue is the durable priority queue name for standard job execution.
	DefaultJobsQueue = "jobs.queue"

	// DefaultJobsExchange is the direct exchange used to route job messages.
	DefaultJobsExchange = "jobs.direct"

	// DefaultJobsRoutingKey is the routing key for processing standard jobs.
	DefaultJobsRoutingKey = "jobs.process"

	// DefaultRetryExchange is the direct exchange used for delayed retries.
	DefaultRetryExchange = "jobs.retry.direct"

	// DefaultRetryQueue is the TTL retry queue that dead-letters back to jobs.exchange.
	DefaultRetryQueue = "jobs.retry.queue"

	// DefaultRetryRoutingKey is the routing key for sending jobs to retry queue.
	DefaultRetryRoutingKey = "jobs.retry"

	// DefaultDLQExchange is the direct exchange for dead-lettered jobs.
	DefaultDLQExchange = "jobs.dlx"

	// DefaultDLQQueue is the durable queue holding permanently failed/exhausted jobs.
	DefaultDLQQueue = "jobs.dlq"

	// DefaultDLQRoutingKey is the routing key for dead-letter queue.
	DefaultDLQRoutingKey = "jobs.dead"

	// MaxPriorityLevel defines the maximum priority level supported by the queue (x-max-priority).
	MaxPriorityLevel uint8 = 10
)

// JobPublisher defines the contract for dispatching jobs to a message broker.
type JobPublisher interface {
	Publish(ctx context.Context, job *models.Job) error
	PublishRetry(ctx context.Context, msg *QueueJobMessage, delay time.Duration) error
	PublishDLQ(ctx context.Context, msg *QueueJobMessage, reason string) error
	Ping() error
	Close() error
}

// QueueJobMessage represents the structured JSON payload placed on the message queue.
type QueueJobMessage struct {
	JobID     string             `json:"job_id"`
	Type      models.JobType     `json:"type"`
	Priority  models.JobPriority `json:"priority"`
	Payload   models.JobPayload  `json:"payload"`
	Attempts  int                `json:"attempts"`
	LastError string             `json:"last_error,omitempty"`
	CreatedAt time.Time          `json:"created_at"`
}

// NewQueueJobMessage constructs a QueueJobMessage from a domain Job model.
func NewQueueJobMessage(job *models.Job) *QueueJobMessage {
	return &QueueJobMessage{
		JobID:     job.ID,
		Type:      job.Type,
		Priority:  job.Priority,
		Payload:   job.Payload,
		Attempts:  job.Attempts,
		CreatedAt: job.CreatedAt,
	}
}

// PriorityToAMQP maps domain JobPriority levels to an AMQP priority (0 - 10).
// Higher numbers receive preferential scheduling in RabbitMQ priority queues.
func PriorityToAMQP(priority models.JobPriority) uint8 {
	switch priority {
	case models.PriorityLow:
		return 1
	case models.PriorityMedium:
		return 4
	case models.PriorityHigh:
		return 7
	case models.PriorityCritical:
		return 9
	default:
		return 4
	}
}
