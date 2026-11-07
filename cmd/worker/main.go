package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-job-queue/internal/config"
	"distributed-job-queue/internal/db"
	"distributed-job-queue/internal/queue"
	"distributed-job-queue/internal/repository"
	"distributed-job-queue/internal/worker"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
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

	// Initialize RabbitMQ publisher for retries and DLQ routing
	publisher, err := queue.NewRabbitMQPublisher(cfg.RabbitMQURL())
	if err != nil {
		log.Fatalf("Worker RabbitMQ publisher initialization failed: %v", err)
	}
	defer publisher.Close()

	consumerCfg := worker.ConsumerConfig{
		RabbitMQURL:      cfg.RabbitMQURL(),
		Concurrency:      cfg.WorkerConcurrency,
		Prefetch:         cfg.WorkerPrefetch,
		MaxRetries:       cfg.MaxRetries,
		BaseRetryDelayMS: cfg.BaseRetryDelayMS,
	}

	consumer := worker.NewConsumer(consumerCfg, repo, publisher, executor)

	// Expose Prometheus metrics endpoint on a dedicated HTTP server
	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", promhttp.Handler())
	metricsServer := &http.Server{
		Addr:    ":" + cfg.WorkerMetricsPort,
		Handler: metricsMux,
	}

	go func() {
		log.Printf("Worker metrics listening on http://localhost:%s/metrics", cfg.WorkerMetricsPort)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Worker metrics HTTP server error: %v", err)
		}
	}()

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

	shutdownMetricsCtx, cancelMetrics := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelMetrics()
	_ = metricsServer.Shutdown(shutdownMetricsCtx)

	log.Println("Worker service shut down cleanly.")
}
