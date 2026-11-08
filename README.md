# Distributed Job Queue & Worker Platform
A fault-tolerant distributed system that accepts asynchronous jobs through a REST API, places them into a message queue, and distributes them across horizontally scalable workers. Jobs are represented as structured messages with attributes such as type, priority, processing duration, and simulated failure probability. The system supports concurrent workers, retries with exponential backoff, job prioritization, dead-letter queues, worker-failure recovery, and monitoring.

## Tools
- **Backend**: Go, REST API  
- **Message broker**: RabbitMQ  
- **Database**: PostgreSQL  
- **Infrastructure**: Docker + Docker Compose  
- **Testing & Benchmarking**: Go unit tests, Go load generator (`cmd/loadgen`)
- **Observability**: Prometheus & Grafana  

---

## Architecture & Components

1. **API Ingestion Service (`cmd/api`)**:
   - `POST /jobs`: Asynchronous job creation with priority, simulated duration, and failure probability.
   - `GET /jobs/{id}` & `GET /jobs`: Query status, attempt count, and payload.
   - `GET /health`: Readiness and liveness checks for PostgreSQL and RabbitMQ.
   - `GET /metrics`: Prometheus telemetry endpoint.

2. **Message Queue & Routing (`internal/queue`)**:
   - RabbitMQ with direct exchange (`jobs.direct`) and priority queue (`jobs.queue`) with `x-max-priority: 10`.
   - Dead-letter exchange (`jobs.dlx`) and DLQ (`jobs.dlq`).
   - Delay retry exchange (`jobs.retry.direct`) and TTL queue (`jobs.retry.queue`) for exponential backoff redelivery.

3. **Worker Processing Engine (`cmd/worker`, `internal/worker`)**:
   - Elastic worker pool with configurable concurrency and prefetch QoS (`ch.Qos`).
   - Automatic exponential backoff retries on failure (`base * 2^(attempts-1)`).
   - Routes permanently failing jobs to the DLQ after exhausting maximum retries.
   - Prometheus metrics server on port `9090`.

4. **Observability (`monitoring/`)**:
   - Prometheus scraping API (`api:8080`) and Worker (`worker:9090`).
   - Grafana dashboard service pre-configured in `docker-compose.yml`.

5. **Load Generator & Benchmarking Tool (`cmd/loadgen`)**:
   - High-throughput client dispatches thousands of jobs with configurable concurrency, failure rates, and priorities.
   - Reports throughput (RPS) and latency percentiles (Min, p50, p95, p99, Max).

---

## Getting Started

### 1. Start Platform with Docker Compose
```bash
docker-compose up -d --build
```

Scale worker nodes dynamically:
```bash
docker-compose up --scale worker=3 -d
```

### 2. Run the Benchmark / Load Generator
```bash
go run ./cmd/loadgen/main.go -jobs 1000 -concurrency 50 -failure-rate 0.05 -duration-ms 100
```

### 3. Access Observability
- **API**: `http://localhost:8080/jobs`
- **Worker Metrics**: `http://localhost:9090/metrics`
- **RabbitMQ Management**: `http://localhost:15672` (guest/guest)
- **Prometheus**: `http://localhost:9091`
- **Grafana**: `http://localhost:3000` (admin/admin)
