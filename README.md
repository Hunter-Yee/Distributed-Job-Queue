## Distributed Job Queue & Worker Platform
# A fault-tolerant distributed system that accepts asynchronous jobs through a REST API, places them into a message queue, and distributes them across horizontally scalable workers. Jobs will be represented as structured messages with attributes such as type, priority, processing duration, and simulated failure probability. The system will support concurrent workers, retries with exponential backoff, job prioritization, idempotent execution, dead-letter queues, worker-failure recovery, and monitoring. A load generator will create thousands of jobs and simulate failures and overload so you can benchmark throughput, latency, queue depth, failure/retry rates, and scalability as workers are added.

# Backend: Go, REST API
# Message broker: RabbitMQ
# Database: PostgreSQL
# Infrastructure: Docker + Docker Compose
# Testing: Go testing, Integration tests, Load testing with either k6/Locust or own Go load generator
# Observability: Prometheus/Grafana
# CI/CD: GitHub Actions