package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"distributed-job-queue/internal/models"
	"distributed-job-queue/internal/queue"
	"distributed-job-queue/internal/repository"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ConsumerConfig holds configuration options for a RabbitMQ worker consumer.
type ConsumerConfig struct {
	RabbitMQURL string
	QueueName   string
	Prefetch    int
	Concurrency int
}

// Consumer consumes jobs from RabbitMQ, coordinates a concurrent worker pool,
// executes tasks, and updates job states in the database.
type Consumer struct {
	cfg      ConsumerConfig
	repo     repository.JobRepository
	executor Executor

	conn *amqp.Connection
	ch   *amqp.Channel

	wg sync.WaitGroup
}

// NewConsumer creates a new worker Consumer.
func NewConsumer(cfg ConsumerConfig, repo repository.JobRepository, executor Executor) *Consumer {
	if cfg.QueueName == "" {
		cfg.QueueName = queue.DefaultJobsQueue
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 5
	}
	if cfg.Prefetch <= 0 {
		cfg.Prefetch = cfg.Concurrency * 2
	}
	if executor == nil {
		executor = NewSimulatedExecutor()
	}

	return &Consumer{
		cfg:      cfg,
		repo:     repo,
		executor: executor,
	}
}

// Start connects to RabbitMQ, starts workers, and consumes until context cancellation.
func (c *Consumer) Start(ctx context.Context) error {
	conn, err := c.connectWithRetry(ctx, 15, 2*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}
	c.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}
	c.ch = ch

	// Set QoS Prefetch: fair dispatch across workers
	if err := ch.Qos(c.cfg.Prefetch, 0, false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to set channel QoS prefetch: %w", err)
	}

	deliveries, err := ch.ConsumeWithContext(
		ctx,
		c.cfg.QueueName,
		"",    // consumer tag (auto-generated)
		false, // auto-ack (explicit manual ack required)
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to start consuming from queue: %w", err)
	}

	log.Printf("Worker pool started: concurrency=%d, prefetch=%d, queue=%s", c.cfg.Concurrency, c.cfg.Prefetch, c.cfg.QueueName)

	jobsChan := make(chan amqp.Delivery)

	// Launch worker goroutines
	for i := 1; i <= c.cfg.Concurrency; i++ {
		c.wg.Add(1)
		go c.workerLoop(ctx, i, jobsChan)
	}

	// Dispatch deliveries to workers channel
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(jobsChan)
				return
			case d, ok := <-deliveries:
				if !ok {
					close(jobsChan)
					return
				}
				select {
				case jobsChan <- d:
				case <-ctx.Done():
					close(jobsChan)
					return
				}
			}
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	log.Println("Stopping consumer, waiting for workers to complete in-flight tasks...")
	c.wg.Wait()

	_ = ch.Close()
	_ = conn.Close()
	log.Println("All workers finished. Consumer stopped.")
	return nil
}

func (c *Consumer) workerLoop(ctx context.Context, workerID int, jobs <-chan amqp.Delivery) {
	defer c.wg.Done()

	for delivery := range jobs {
		c.processDelivery(ctx, workerID, delivery)
	}
}

func (c *Consumer) processDelivery(ctx context.Context, workerID int, d amqp.Delivery) {
	var jobMsg queue.QueueJobMessage
	if err := json.Unmarshal(d.Body, &jobMsg); err != nil {
		log.Printf("[Worker %d] Failed to unmarshal message payload: %v. Rejecting without requeue.", workerID, err)
		_ = d.Reject(false)
		return
	}

	log.Printf("[Worker %d] Received job: %s (type=%s, priority=%s)", workerID, jobMsg.JobID, jobMsg.Type, jobMsg.Priority)

	// 1. Mark status as processing in database
	dbCtx, cancelDB := context.WithTimeout(context.Background(), 5*time.Second)
	if err := c.repo.RecordAttempt(dbCtx, jobMsg.JobID, models.StatusProcessing); err != nil {
		log.Printf("[Worker %d] Failed to update job %s to processing: %v", workerID, jobMsg.JobID, err)
	}
	cancelDB()

	// 2. Execute job
	execErr := c.executor.Execute(ctx, &jobMsg)

	// 3. Update database status and acknowledge/reject delivery
	finalizeCtx, cancelFinalize := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFinalize()

	if execErr != nil {
		log.Printf("[Worker %d] Job %s execution failed: %v", workerID, jobMsg.JobID, execErr)
		_ = c.repo.UpdateStatus(finalizeCtx, jobMsg.JobID, models.StatusFailed)
		// Acknowledge the delivery from the main queue for now (in Step 2 we add retry/DLQ logic)
		_ = d.Ack(false)
		return
	}

	log.Printf("[Worker %d] Job %s completed successfully", workerID, jobMsg.JobID)
	_ = c.repo.UpdateStatus(finalizeCtx, jobMsg.JobID, models.StatusCompleted)
	_ = d.Ack(false)
}

func (c *Consumer) connectWithRetry(ctx context.Context, maxRetries int, delay time.Duration) (*amqp.Connection, error) {
	for attempt := 1; attempt <= maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		conn, err := amqp.Dial(c.cfg.RabbitMQURL)
		if err == nil {
			return conn, nil
		}

		log.Printf("Worker connection to RabbitMQ failed (attempt %d/%d): %v. Retrying in %v...", attempt, maxRetries, err, delay)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, errors.New("exhausted retry attempts connecting to rabbitmq")
}
