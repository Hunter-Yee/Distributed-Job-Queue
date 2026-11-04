package worker

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"distributed-job-queue/internal/queue"
)

// Executor defines the interface for running a job.
type Executor interface {
	Execute(ctx context.Context, msg *queue.QueueJobMessage) error
}

// SimulatedExecutor executes jobs based on their simulated duration and failure probability.
type SimulatedExecutor struct {
	rng *rand.Rand
}

// NewSimulatedExecutor creates a new job executor with a local random generator.
func NewSimulatedExecutor() *SimulatedExecutor {
	return &SimulatedExecutor{
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Execute performs simulated processing:
// 1. Sleeps for msg.Payload.DurationMS (respecting ctx cancellation).
// 2. Evaluates msg.Payload.FailureProbability to simulate failure.
func (e *SimulatedExecutor) Execute(ctx context.Context, msg *queue.QueueJobMessage) error {
	duration := time.Duration(msg.Payload.DurationMS) * time.Millisecond
	if duration <= 0 {
		duration = 500 * time.Millisecond
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}

	if msg.Payload.FailureProbability > 0 {
		prob := e.rng.Float64()
		if prob < msg.Payload.FailureProbability {
			return fmt.Errorf("simulated execution failure (prob=%.2f, roll=%.2f)", msg.Payload.FailureProbability, prob)
		}
	}

	return nil
}
