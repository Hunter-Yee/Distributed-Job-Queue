package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"distributed-job-queue/internal/config"
	"distributed-job-queue/internal/db"
	"distributed-job-queue/internal/repository"
	"distributed-job-queue/internal/worker"
)

func main() {
	cfg := config.LoadConfig()

	log.Printf("Starting Distributed Job Queue Worker (concurrency: %d, prefetch: %d)...",
		cfg.WorkerConcurrency, cfg.WorkerPrefetch)

	// Initialize database connection
	database, err := db.InitDB(cfg.DBConnString())
	if err != nil {
		log.Fatalf("Worker database initialization failed: %v", err)
	}
	defer database.Close()

	repo := repository.NewPostgresJobRepository(database)
	executor := worker.NewSimulatedExecutor()

	consumerCfg := worker.ConsumerConfig{
		RabbitMQURL: cfg.RabbitMQURL(),
		Concurrency: cfg.WorkerConcurrency,
		Prefetch:    cfg.WorkerPrefetch,
	}

	consumer := worker.NewConsumer(consumerCfg, repo, executor)

	// Context with cancellation on SIGINT/SIGTERM
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received termination signal (%v), initiating graceful shutdown...", sig)
		cancel()
	}()

	log.Println("Worker is consuming jobs from RabbitMQ...")
	if err := consumer.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Worker consumer encountered an error: %v", err)
	}

	log.Println("Worker service shut down cleanly.")
}
