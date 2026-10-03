"""TaskFlow Observability — Phase 4 Prometheus Metrics.

Defines Prometheus metrics for jobs, queues, workers, and database connection pool.
Provides helpers for recording metrics from API, worker, and recovery processes,
using Redis for multi-process metric synchronization.
"""

import logging
from typing import Any
from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    generate_latest,
    CONTENT_TYPE_LATEST,
    REGISTRY,
)
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database.connection import engine
from app.database.models import Job, JobStatus, Worker, WorkerStatus
from app.queue.redis_queue import RedisQueue

logger = logging.getLogger("taskflow.metrics")

# --- Prometheus Metric Definitions ---

active_requests_gauge = Gauge(
    "taskflow_http_requests_active",
    "Active HTTP requests",
    labelnames=["method", "endpoint"],
)

http_requests_total = Counter(
    "taskflow_http_requests_total",
    "Total HTTP requests",
    labelnames=["method", "endpoint", "status"],
)

http_request_duration_seconds = Histogram(
    "taskflow_http_request_duration_seconds",
    "HTTP request duration in seconds",
    labelnames=["method", "endpoint"],
)

JOBS_SUBMITTED = Counter(
    "taskflow_jobs_submitted_total",
    "Total number of jobs submitted to TaskFlow",
)

JOBS_COMPLETED = Counter(
    "taskflow_jobs_completed_total",
    "Total number of jobs successfully completed",
)

JOBS_FAILED = Counter(
    "taskflow_jobs_failed_total",
    "Total number of jobs that failed permanently (moved to dead-letter)",
)

JOBS_RETRIED = Counter(
    "taskflow_jobs_retried_total",
    "Total number of job retries scheduled",
)

JOBS_RECOVERED = Counter(
    "taskflow_jobs_recovered_total",
    "Total number of stuck jobs recovered from dead workers",
)

JOB_PROCESSING_DURATION = Histogram(
    "taskflow_job_processing_duration_seconds",
    "Time spent by worker executing the job (seconds)",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

JOB_QUEUE_WAIT = Histogram(
    "taskflow_job_queue_wait_seconds",
    "Time between job creation and worker execution start (seconds)",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

QUEUE_DEPTH = Gauge(
    "taskflow_queue_depth",
    "Current number of jobs waiting in the queue",
    labelnames=["priority"],
)

WORKERS_ONLINE = Gauge(
    "taskflow_workers_online",
    "Number of registered workers currently online",
)

WORKERS_DEAD = Gauge(
    "taskflow_workers_dead",
    "Number of registered workers marked dead",
)

WORKER_JOBS_ACTIVE = Gauge(
    "taskflow_worker_jobs_active",
    "Number of jobs currently in RUNNING status",
)

DB_POOL_CONNECTIONS = Gauge(
    "taskflow_db_pool_connections",
    "Total active connections in the database connection pool",
)

DB_POOL_AVAILABLE = Gauge(
    "taskflow_db_pool_available",
    "Available (idle) connections in the database connection pool",
)


# --- Redis Keys for Cross-Process Metrics ---
REDIS_METRICS_COUNTS = "taskflow:metrics:counts"
REDIS_METRICS_DURATIONS = "taskflow:metrics:durations"
REDIS_METRICS_WAITS = "taskflow:metrics:waits"


# --- Recording Helpers ---

def record_job_submitted(queue: RedisQueue | None = None) -> None:
    """Record a newly submitted job."""
    JOBS_SUBMITTED.inc()
    if queue is not None and hasattr(queue, "client") and queue.client is not None:
        try:
            queue.client.hincrby(REDIS_METRICS_COUNTS, "submitted", 1)
        except Exception:
            pass


def record_job_completed(
    duration_seconds: float,
    queue_wait_seconds: float,
    queue: RedisQueue | None = None,
) -> None:
    """Record successful job execution."""
    JOBS_COMPLETED.inc()
    if duration_seconds >= 0:
        JOB_PROCESSING_DURATION.observe(duration_seconds)
    if queue_wait_seconds >= 0:
        JOB_QUEUE_WAIT.observe(queue_wait_seconds)

    if queue is not None and hasattr(queue, "client") and queue.client is not None:
        try:
            pipe = queue.client.pipeline()
            pipe.hincrby(REDIS_METRICS_COUNTS, "completed", 1)
            pipe.rpush(REDIS_METRICS_DURATIONS, str(duration_seconds))
            pipe.rpush(REDIS_METRICS_WAITS, str(queue_wait_seconds))
            pipe.execute()
        except Exception:
            pass


def record_job_failed(
    duration_seconds: float,
    queue_wait_seconds: float,
    queue: RedisQueue | None = None,
) -> None:
    """Record job failure (moved to dead-letter)."""
    JOBS_FAILED.inc()
    if duration_seconds >= 0:
        JOB_PROCESSING_DURATION.observe(duration_seconds)
    if queue_wait_seconds >= 0:
        JOB_QUEUE_WAIT.observe(queue_wait_seconds)

    if queue is not None and hasattr(queue, "client") and queue.client is not None:
        try:
            pipe = queue.client.pipeline()
            pipe.hincrby(REDIS_METRICS_COUNTS, "failed", 1)
            pipe.rpush(REDIS_METRICS_DURATIONS, str(duration_seconds))
            pipe.rpush(REDIS_METRICS_WAITS, str(queue_wait_seconds))
            pipe.execute()
        except Exception:
            pass


def record_job_retried(queue: RedisQueue | None = None) -> None:
    """Record a scheduled job retry."""
    JOBS_RETRIED.inc()
    if queue is not None and hasattr(queue, "client") and queue.client is not None:
        try:
            queue.client.hincrby(REDIS_METRICS_COUNTS, "retried", 1)
        except Exception:
            pass


def record_job_recovered(count: int = 1, queue: RedisQueue | None = None) -> None:
    """Record recovered jobs from dead workers."""
    if count <= 0:
        return
    JOBS_RECOVERED.inc(count)
    if queue is not None and hasattr(queue, "client") and queue.client is not None:
        try:
            queue.client.hincrby(REDIS_METRICS_COUNTS, "recovered", count)
        except Exception:
            pass


# --- Scrape-Time Metric Collector ---

def collect_metrics(db: Session, queue: RedisQueue) -> None:
    """Synchronize Gauges and cross-process Counters before metrics are exported."""
    # 1. Update queue depth gauges
    try:
        lengths = queue.priority_lengths()
        QUEUE_DEPTH.labels(priority="high").set(lengths.get("high", 0))
        QUEUE_DEPTH.labels(priority="normal").set(lengths.get("normal", 0))
        QUEUE_DEPTH.labels(priority="low").set(lengths.get("low", 0))
        QUEUE_DEPTH.labels(priority="dead").set(queue.dead_letter_length())
    except Exception as exc:
        logger.debug("Failed to collect queue depth metrics: %s", exc)

    # 2. Update worker status gauges and active jobs
    try:
        online_count = (
            db.query(func.count(Worker.id))
            .filter(Worker.status == WorkerStatus.ONLINE)
            .scalar()
            or 0
        )
        dead_count = (
            db.query(func.count(Worker.id))
            .filter(Worker.status == WorkerStatus.DEAD)
            .scalar()
            or 0
        )
        active_count = (
            db.query(func.count(Job.id))
            .filter(Job.status == JobStatus.RUNNING)
            .scalar()
            or 0
        )
        WORKERS_ONLINE.set(online_count)
        WORKERS_DEAD.set(dead_count)
        WORKER_JOBS_ACTIVE.set(active_count)
    except Exception as exc:
        logger.debug("Failed to collect worker/job gauges: %s", exc)

    # 3. Update database pool gauges if pool provides them
    try:
        pool = engine.pool
        if hasattr(pool, "checkedin") and hasattr(pool, "checkedout"):
            checked_out = pool.checkedout()
            checked_in = pool.checkedin()
            DB_POOL_CONNECTIONS.set(checked_out + checked_in)
            DB_POOL_AVAILABLE.set(checked_in)
    except Exception as exc:
        logger.debug("Failed to collect DB pool metrics: %s", exc)

    # 4. Sync cross-process counters and histograms from Redis
    if hasattr(queue, "client") and queue.client is not None:
        try:
            pipe = queue.client.pipeline()
            pipe.hgetall(REDIS_METRICS_COUNTS)
            pipe.lrange(REDIS_METRICS_DURATIONS, 0, 4999)
            pipe.ltrim(REDIS_METRICS_DURATIONS, 5000, -1)
            pipe.lrange(REDIS_METRICS_WAITS, 0, 4999)
            pipe.ltrim(REDIS_METRICS_WAITS, 5000, -1)
            res = pipe.execute()

            counts = res[0] or {}
            durations = res[1] or []
            waits = res[3] or []

            # Sync monotonic counters
            counter_map = {
                "submitted": JOBS_SUBMITTED,
                "completed": JOBS_COMPLETED,
                "failed": JOBS_FAILED,
                "retried": JOBS_RETRIED,
                "recovered": JOBS_RECOVERED,
            }
            for key, prom_counter in counter_map.items():
                if key in counts:
                    try:
                        redis_val = float(counts[key])
                        curr_val = prom_counter._value.get()
                        if redis_val > curr_val:
                            prom_counter.inc(redis_val - curr_val)
                    except (ValueError, TypeError):
                        pass

            # Ingest histogram samples
            for d_str in durations:
                try:
                    d_val = float(d_str)
                    if d_val >= 0:
                        JOB_PROCESSING_DURATION.observe(d_val)
                except (ValueError, TypeError):
                    pass

            for w_str in waits:
                try:
                    w_val = float(w_str)
                    if w_val >= 0:
                        JOB_QUEUE_WAIT.observe(w_val)
                except (ValueError, TypeError):
                    pass

        except Exception as exc:
            logger.debug("Failed to sync cross-process Redis metrics: %s", exc)
