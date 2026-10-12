package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"distributed-job-queue/internal/models"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	// ErrPublisherClosed is returned when attempting to publish on a closed publisher.
	ErrPublisherClosed = errors.New("rabbitmq publisher is closed")
	// ErrNotConnected is returned when the publisher is temporarily disconnected.
	ErrNotConnected = errors.New("rabbitmq publisher is not connected")
)

// RabbitMQPublisher implements JobPublisher for RabbitMQ brokers.
type RabbitMQPublisher struct {
	url       string
	exchange  string
	queueName string

	mu       sync.RWMutex
	conn     *amqp.Connection
	ch       *amqp.Channel
	isClosed atomic.Bool

	reconnectMu sync.Mutex
}

// NewRabbitMQPublisher connects to RabbitMQ with default retry logic (15 attempts, 2s backoff),
// sets up queue topology, and starts connection monitoring for automatic recovery.
func NewRabbitMQPublisher(url string) (*RabbitMQPublisher, error) {
	return NewRabbitMQPublisherWithRetries(url, 15, 2*time.Second)
}

// NewRabbitMQPublisherWithRetries connects to RabbitMQ with specified retry parameters.
func NewRabbitMQPublisherWithRetries(url string, maxRetries int, retryDelay time.Duration) (*RabbitMQPublisher, error) {
	if maxRetries <= 0 {
		maxRetries = 1
	}

	p := &RabbitMQPublisher{
		url:       url,
		exchange:  DefaultJobsExchange,
		queueName: DefaultJobsQueue,
	}

	var err error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = p.connect()
		if err == nil {
			log.Printf("Successfully connected to RabbitMQ broker and declared topology")
			break
		}
		if attempt < maxRetries {
			log.Printf("RabbitMQ connection attempt %d failed: %v. Retrying in %v...", attempt, err, retryDelay)
			time.Sleep(retryDelay)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ after %d retries: %w", maxRetries, err)
	}

	return p, nil
}

// connect establishes the connection, opens a channel, declares the exchange and queue,
// and starts monitoring for connection drops.
func (p *RabbitMQPublisher) connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isClosed.Load() {
		return ErrPublisherClosed
	}

	conn, err := amqp.Dial(p.url)
	if err != nil {
		return fmt.Errorf("failed to dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	// 1. Declare direct exchange
	err = ch.ExchangeDeclare(
		p.exchange,
		"direct",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	// 2. Declare durable queue with priority support
	queueArgs := amqp.Table{
		"x-max-priority": int(MaxPriorityLevel),
	}
	_, err = ch.QueueDeclare(
		p.queueName,
		true,  // durable
		false, // auto-deleted
		false, // exclusive
		false, // no-wait
		queueArgs,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// 3. Bind queue to exchange
	err = ch.QueueBind(
		p.queueName,
		DefaultJobsRoutingKey,
		p.exchange,
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to bind queue to exchange: %w", err)
	}

	p.conn = conn
	p.ch = ch

	// Start background watcher for connection loss
	go p.monitorConnection(conn)

	return nil
}

// monitorConnection listens for connection closure and initiates automatic reconnection.
func (p *RabbitMQPublisher) monitorConnection(conn *amqp.Connection) {
	closeChan := conn.NotifyClose(make(chan *amqp.Error, 1))
	err, ok := <-closeChan
	if !ok || p.isClosed.Load() {
		// Clean close or publisher was explicitly shut down
		return
	}

	log.Printf("RabbitMQ connection lost: %v. Initiating reconnection loop...", err)
	p.reconnect()
}

// reconnect executes an exponential backoff loop to re-establish the connection.
func (p *RabbitMQPublisher) reconnect() {
	p.reconnectMu.Lock()
	defer p.reconnectMu.Unlock()

	if p.isClosed.Load() {
		return
	}

	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	for {
		if p.isClosed.Load() {
			return
		}

		log.Printf("Attempting to reconnect to RabbitMQ in %v...", backoff)
		time.Sleep(backoff)

		if err := p.connect(); err == nil {
			log.Println("Successfully reconnected to RabbitMQ and restored channel topology")
			return
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// Publish serializes and publishes a job message with persistent delivery mode and priority.
func (p *RabbitMQPublisher) Publish(ctx context.Context, job *models.Job) error {
	if p.isClosed.Load() {
		return ErrPublisherClosed
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.ch == nil || p.conn == nil || p.conn.IsClosed() {
		return ErrNotConnected
	}

	msgDTO := NewQueueJobMessage(job)
	body, err := json.Marshal(msgDTO)
	if err != nil {
		return fmt.Errorf("failed to marshal job queue message: %w", err)
	}

	publishing := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent, // Value 2: Flushes message to disk
		MessageId:    job.ID,
		Timestamp:    job.CreatedAt,
		Priority:     PriorityToAMQP(job.Priority),
		Body:         body,
	}

	err = p.ch.PublishWithContext(
		ctx,
		p.exchange,
		DefaultJobsRoutingKey,
		false, // mandatory
		false, // immediate
		publishing,
	)
	if err != nil {
		return fmt.Errorf("failed to publish message to rabbitmq: %w", err)
	}

	return nil
}

// Ping verifies whether the connection and channel are active and open.
func (p *RabbitMQPublisher) Ping() error {
	if p.isClosed.Load() {
		return ErrPublisherClosed
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.conn == nil || p.conn.IsClosed() {
		return ErrNotConnected
	}
	if p.ch == nil || p.ch.IsClosed() {
		return ErrNotConnected
	}

	return nil
}

// Close gracefully closes the channel and connection.
func (p *RabbitMQPublisher) Close() error {
	if !p.isClosed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	var errs []error
	if p.ch != nil && !p.ch.IsClosed() {
		if err := p.ch.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if p.conn != nil && !p.conn.IsClosed() {
		if err := p.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing rabbitmq publisher: %v", errs)
	}
	return nil
}
