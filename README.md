# TaskFlow — Distributed Job Processing Platform

TaskFlow is a robust, distributed background-job processing platform built with Python, FastAPI, PostgreSQL, and Redis. 

Designed for high reliability and horizontal scalability, TaskFlow demonstrates real-world backend engineering patterns including priority queueing, exponential backoff, dead-letter queues, worker heartbeat tracking, and automated failure recovery.

---

## Key Features

* **Distributed Compute:** Horizontally scalable, stateless workers consuming from shared Redis queues.
* **Strict Priority Queuing:** Separate queues for HIGH, NORMAL, and LOW priority jobs.
* **Resilient Execution:** Configurable automatic retries with exponential backoff via Redis sorted sets.
* **Failure Recovery:** Independent recovery supervisor detects crashed workers via missing heartbeats and expired leases, safely recovering and requeuing stuck jobs.
* **Observability:** Prometheus metrics middleware and a pre-provisioned Grafana dashboard tracking throughput, latency, and queue depths.
* **Security:** JWT-based authentication, role-based access control (RBAC), payload validation, and rate limiting.
* **Deployment-Ready:** Fully containerized architecture orchestrated with Docker Compose, complete with automated Alembic database migrations.

---

## Architecture

At its core, TaskFlow separates the web layer (FastAPI), the data layer (PostgreSQL + Redis), and the compute layer (Worker processes).

* **FastAPI:** Handles HTTP requests, authentication, and job enqueuing.
* **Redis:** Acts as an in-memory priority queue, retry scheduler, and rate limiter.
* **PostgreSQL:** Acts as the source of truth for job states, worker registry, and attempt history.
* **Workers:** Consume jobs via `BLPOP`, executing handlers, maintaining heartbeats, and renewing leases.
* **Recovery Supervisor:** Sweeps the worker registry to detect dead nodes and recovers orphaned jobs using row-level locking (`SELECT FOR UPDATE SKIP LOCKED`).

For a detailed visual breakdown of the component roles, see [Architecture](docs/architecture.md).

---

## Tech Stack

| Layer          | Technology               |
|----------------|--------------------------|
| API Framework  | FastAPI + Uvicorn        |
| ORM            | SQLAlchemy 2.x           |
| Validation     | Pydantic v2              |
| Database       | PostgreSQL 16            |
| Queue          | Redis 7                  |
| Worker         | Custom Python consumer   |
| Migrations     | Alembic                  |
| Infrastructure | Docker + Docker Compose  |
| Observability  | Prometheus + Grafana     |
| Testing        | Pytest + FastAPI TestClient |

---

## Performance Benchmarks

The platform has been benchmarked for horizontal scaling efficiency. Below are the results for a lightweight CPU-bound workload (`SUM_NUMBERS`, concurrency 20, 100 jobs total).

| Workers | Throughput (jobs/s) | P50 Latency (s) | P99 Latency (s) | Scaling Efficiency |
|---------|---------------------|-----------------|-----------------|--------------------|
| 1       | 32.43               | 1.68            | 2.49            | 100.0%             |
| 2       | 24.75               | 2.63            | 3.35            | 38.2%              |
| 4       | 40.73               | 1.60            | 2.13            | 31.4%              |
| 8       | 34.40               | 1.65            | 2.56            | 13.3%              |

*(Note: Efficiency drops at higher worker counts in a local Docker Compose environment due to CPU contention and SQLite testing overhead. In a distributed multi-node deployment, scaling efficiency improves significantly.)*

For detailed metrics and other workload profiles, see the [Benchmark Report](benchmarks/REPORT.md).

---

## Quick Start

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose v2+

### 1. Configure Environment

```bash
cp .env.example .env
# Update JWT_SECRET and other variables if needed
```

### 2. Start the Stack

```bash
# Start the API, Postgres, Redis, 3 Workers, Recovery Supervisor, and Observability
docker compose up --build -d --scale worker=3
```

### 3. Access Services

- **API:** http://localhost:8000
- **Swagger Docs:** http://localhost:8000/docs
- **Grafana Dashboards:** http://localhost:3000 (admin / admin)
- **Prometheus:** http://localhost:9090

---

## API Usage Example

All job and worker endpoints require a valid JWT token. 

### 1. Register and Login

```bash
# Register a new user
curl -s -X POST http://localhost:8000/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email": "user@example.com", "password": "securepassword123"}'

# Login to get access token
TOKEN=$(curl -s -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email": "user@example.com", "password": "securepassword123"}' | jq -r .access_token)
```

### 2. Submit a Job

```bash
# Submit a HIGH priority job
curl -s -X POST http://localhost:8000/api/jobs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "SUM_NUMBERS",
    "priority": "HIGH",
    "max_retries": 3,
    "payload": { "numbers": [5, 10, 15] }
  }' | jq .
```

### 3. Check Job Status

```bash
# Replace <JOB_ID> with the ID returned from the previous command
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8000/api/jobs/<JOB_ID> | jq .
```

---

## Supported Job Types

The system includes several built-in handlers for demonstration and testing:

* `SUM_NUMBERS`: Sums a list of numbers (CPU bound).
* `TEXT_LENGTH`: Returns the length of a provided string.
* `SLEEP`: Sleeps for a specified duration (simulates long-running IO-bound tasks).
* `CRASH_WORKER`: Intentionally blocks the worker thread indefinitely to test dead-worker detection and recovery.
* `FAIL_N_TIMES`: Fails a configured number of times before succeeding (tests exponential backoff and retries).

---

## Deployment

TaskFlow is **deployment-ready**. The repository contains the necessary Dockerfiles, Compose configurations, and Alembic migrations to deploy to a production environment.

For detailed deployment instructions and considerations for managed cloud services, see the [Deployment Guide](docs/DEPLOYMENT.md).

---

## Project Structure

```
taskflow/
├── backend/
│   ├── app/
│   │   ├── api/             # HTTP endpoints (FastAPI routers)
│   │   ├── core/            # Config, Security, and Metrics
│   │   ├── database/        # SQLAlchemy Models & Connection
│   │   ├── queue/           # Redis Queue Abstraction
│   │   ├── services/        # Business Logic Layer
│   │   └── workers/         # Worker Consumer & Recovery Supervisor
│   ├── alembic/             # Database Migrations
│   ├── tests/               # Pytest Suite (120+ tests)
│   └── main.py              # Application Entrypoint
├── benchmarks/              # Load Testing Scripts & Results
├── docs/                    # Architecture & Deployment Documentation
├── monitoring/              # Grafana Dashboards & Prometheus Config
└── docker-compose.yml       # Infrastructure Orchestration
```

---

## License

This project is intended for educational purposes, portfolio demonstration, and as a reference architecture for distributed task processing.
