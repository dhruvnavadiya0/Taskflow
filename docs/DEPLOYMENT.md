# TaskFlow Deployment Guide

TaskFlow is designed to be deployment-ready using containerized infrastructure. This document explains how to deploy the platform to a production or staging environment.

> **Note:** TaskFlow is currently *deployment-ready*, meaning it is fully containerized and configured for a production-like environment, but it does not claim to be actively deployed to a public cloud URL.

## Prerequisites

- **Docker:** v24.0+ recommended
- **Docker Compose:** v2.0+ recommended
- A host machine with at least 2 CPU cores and 4GB RAM (depending on worker scaling).

## Environment Variables

Before deploying, you must configure the environment variables. Copy `.env.example` to `.env` and modify the values for your environment.

```bash
cp .env.example .env
```

**Critical Production Variables:**
- `JWT_SECRET`: Must be changed from the default to a secure, random string (e.g., `openssl rand -hex 32`).
- `DATABASE_URL`: By default, this points to the Docker Compose PostgreSQL instance. If using a managed database (like AWS RDS or GCP Cloud SQL), update this URL.
- `REDIS_URL`: By default, this points to the Docker Compose Redis instance. If using a managed Redis (like AWS ElastiCache), update this URL.

## Deployment Using Docker Compose

The simplest way to deploy the entire stack on a single instance (or a swarm) is using the provided `docker-compose.yml` file.

### 1. Build and Start the Stack

```bash
docker compose up --build -d
```

This command will start all required services:
- **api:** FastAPI backend exposed on port `8000`.
- **postgres:** Relational database on port `5432`.
- **redis:** In-memory queue on port `6379`.
- **worker:** Background job processor.
- **recovery:** Background recovery supervisor.
- **migrate:** Ephemeral container that runs Alembic database migrations and exits.
- **prometheus & grafana:** Observability stack (Grafana exposed on port `3000`).

### 2. Scaling Workers

TaskFlow's compute layer is fully stateless. You can scale the number of job-processing workers horizontally:

```bash
docker compose up -d --scale worker=4
```

This will run 4 independent worker containers consuming from the same Redis priority queues.

## Production Considerations

When moving to a public cloud or enterprise environment, consider the following architectural adjustments:

1. **Managed Services:** Replace the `postgres` and `redis` containers in `docker-compose.yml` with managed offerings (e.g., Amazon RDS and ElastiCache) for automated backups, high availability, and easier scaling.
2. **Reverse Proxy / API Gateway:** Do not expose the FastAPI backend directly to the public internet. Use a reverse proxy like Nginx, Traefik, or an AWS Application Load Balancer to terminate SSL/TLS and handle DDoS protection.
3. **Container Orchestration:** While Docker Compose is excellent for single-node deployments, consider migrating the container definitions to Kubernetes (using Helm or Kustomize) or Amazon ECS for automated health checks, rolling updates, and multi-node scaling.

## Observability

The observability stack starts automatically.
- **Grafana:** Accessible at `http://<your-server-ip>:3000` (Default login: admin / admin). The "TaskFlow Performance" dashboard is pre-provisioned and provides insights into job throughput, queue depths, and worker statuses.
- **Prometheus:** Accessible at `http://<your-server-ip>:9090`.

## Updating the Deployment

To deploy a new version of the code:

```bash
git pull origin main
docker compose up --build -d
```

The `migrate` container will automatically run any new database migrations on startup.
