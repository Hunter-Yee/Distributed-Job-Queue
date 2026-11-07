package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// JobsCreated tracks total jobs submitted through the API.
	JobsCreated = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "jobqueue",
			Subsystem: "api",
			Name:      "jobs_created_total",
			Help:      "Total number of jobs ingested via API.",
		},
		[]string{"type", "priority"},
	)

	// IngestionDuration tracks latency of the job creation endpoint.
	IngestionDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "jobqueue",
			Subsystem: "api",
			Name:      "ingestion_duration_seconds",
			Help:      "Duration of HTTP job ingestion requests in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"type", "status"},
	)

	// JobsProcessed tracks total job executions by worker.
	JobsProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "jobqueue",
			Subsystem: "worker",
			Name:      "jobs_processed_total",
			Help:      "Total number of jobs processed by workers.",
		},
		[]string{"type", "status"},
	)

	// JobsRetried tracks the number of times jobs are requeued for retry.
	JobsRetried = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "jobqueue",
			Subsystem: "worker",
			Name:      "jobs_retried_total",
			Help:      "Total number of job retry attempts scheduled.",
		},
		[]string{"type", "priority"},
	)

	// JobsDeadLettered tracks jobs sent to the Dead Letter Queue.
	JobsDeadLettered = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "jobqueue",
			Subsystem: "worker",
			Name:      "jobs_deadletter_total",
			Help:      "Total number of jobs sent to dead-letter queue after exhausting retries.",
		},
		[]string{"type"},
	)

	// JobExecutionDuration tracks worker job processing duration.
	JobExecutionDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "jobqueue",
			Subsystem: "worker",
			Name:      "job_execution_duration_seconds",
			Help:      "Execution duration of individual jobs in seconds.",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
		},
		[]string{"type", "status"},
	)

	// ActiveWorkers tracks the number of worker goroutines currently executing a job.
	ActiveWorkers = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "jobqueue",
			Subsystem: "worker",
			Name:      "active_workers",
			Help:      "Current number of busy worker threads executing jobs.",
		},
	)
)
