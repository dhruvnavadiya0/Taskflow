"""
TaskFlow Recovery Supervisor — Phase 3

Runs as a standalone process that periodically:
  1. Inspects all registered workers for stale heartbeats.
  2. Transitions ONLINE → SUSPECTED → DEAD based on timeout + grace period.
  3. Recovers stuck RUNNING jobs whose leases have expired from DEAD workers.

Run with: python -m app.workers.recovery

Dead-worker detection policy:
  - A worker whose last_heartbeat is older than WORKER_HEARTBEAT_TIMEOUT seconds
    is marked SUSPECTED.
  - A SUSPECTED worker whose last_heartbeat is older than
    WORKER_HEARTBEAT_TIMEOUT + WORKER_FAILURE_GRACE_PERIOD seconds is marked DEAD.
  - This two-step approach avoids immediately declaring a worker dead from one
    missed heartbeat (e.g. due to a brief network hiccup).

Job recovery policy:
  - Only jobs in RUNNING status, assigned to a DEAD worker, whose lease has
    expired (lease_expires_at < now) are recovered.
  - Recovery uses SELECT ... FOR UPDATE to prevent two recovery processes
    from requeueing the same job simultaneously.
  - The job's status is verified BEFORE recovery — if it's already COMPLETED,
    it is not recovered.
  - recovery_count is incremented on the job.
  - The in-progress attempt is marked RECOVERED.
"""

import logging
import signal
import sys
import time
from datetime import datetime, timezone, timedelta

from sqlalchemy import and_
from sqlalchemy.orm import Session

from app.core.config import settings
from app.database.connection import SessionLocal, Base, engine
from app.database.models import (
    Job, JobAttempt, Worker,
    JobStatus, AttemptStatus, WorkerStatus,
)
from app.queue.redis_queue import RedisQueue

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("taskflow.recovery")

_shutdown = False


def _handle_signal(signum: int, frame: object) -> None:
    global _shutdown
    logger.info("Received signal %s, shutting down recovery supervisor...", signum)
    _shutdown = True


def detect_stale_workers(db: Session) -> None:
    """Inspect workers and transition ONLINE → SUSPECTED → DEAD."""
    now = datetime.now(timezone.utc)
    timeout = timedelta(seconds=settings.WORKER_HEARTBEAT_TIMEOUT)
    grace = timedelta(seconds=settings.WORKER_FAILURE_GRACE_PERIOD)

    # ONLINE workers whose heartbeat has expired → SUSPECTED
    stale_online = (
        db.query(Worker)
        .filter(
            Worker.status == WorkerStatus.ONLINE,
            Worker.last_heartbeat < now - timeout,
        )
        .all()
    )
    for w in stale_online:
        w.status = WorkerStatus.SUSPECTED
        w.updated_at = now
        logger.warning("[supervisor] Worker %s heartbeat expired, marked SUSPECTED", w.id)

    # SUSPECTED workers whose heartbeat has expired beyond grace → DEAD
    dead_threshold = now - timeout - grace
    stale_suspected = (
        db.query(Worker)
        .filter(
            Worker.status == WorkerStatus.SUSPECTED,
            Worker.last_heartbeat < dead_threshold,
        )
        .all()
    )
    for w in stale_suspected:
        w.status = WorkerStatus.DEAD
        w.updated_at = now
        logger.warning("[supervisor] Worker %s confirmed DEAD", w.id)

    if stale_online or stale_suspected:
        db.commit()


def recover_stuck_jobs(db: Session, queue: RedisQueue) -> int:
    """Find and recover RUNNING jobs owned by DEAD workers with expired leases.

    Returns the number of jobs recovered.

    Uses SELECT ... FOR UPDATE (row-level locking) to prevent duplicate
    recovery by concurrent supervisor processes.
    """
    now = datetime.now(timezone.utc)

    # Get IDs of all DEAD workers
    dead_worker_ids = [
        w.id for w in
        db.query(Worker.id).filter(Worker.status == WorkerStatus.DEAD).all()
    ]
    if not dead_worker_ids:
        return 0

    # Find stuck jobs: RUNNING, owned by a dead worker, lease expired
    # Use with_for_update() for row-level locking
    stuck_jobs = (
        db.query(Job)
        .filter(
            Job.status == JobStatus.RUNNING,
            Job.worker_id.in_(dead_worker_ids),
            Job.lease_expires_at < now,
        )
        .with_for_update(skip_locked=True)
        .all()
    )

    recovered = 0
    for job in stuck_jobs:
        # Double-check status (belt and suspenders)
        if job.status != JobStatus.RUNNING:
            continue

        logger.info("[supervisor] Recovering job %s from dead worker %s",
                     job.id, job.worker_id)

        # Mark the in-progress attempt as RECOVERED
        running_attempt = (
            db.query(JobAttempt)
            .filter(
                JobAttempt.job_id == job.id,
                JobAttempt.status == AttemptStatus.RUNNING,
            )
            .first()
        )
        if running_attempt:
            running_attempt.status = AttemptStatus.RECOVERED
            running_attempt.finished_at = now
            running_attempt.error_message = f"Worker {job.worker_id} died — job recovered"

        # Reset job for reprocessing
        old_worker = job.worker_id
        job.status = JobStatus.PENDING
        job.worker_id = None
        job.lease_expires_at = None
        job.recovery_count += 1
        job.started_at = None
        job.completed_at = None

        db.commit()

        # Re-enqueue into the correct priority queue
        queue.enqueue_priority(job.id, job.priority.value)
        logger.info("[supervisor] Job %s returned to %s queue (recovery_count=%d)",
                     job.id, job.priority.value, job.recovery_count)
        recovered += 1

    if recovered > 0:
        from app.core.metrics import record_job_recovered
        record_job_recovered(recovered, queue)

    return recovered


def run_recovery() -> None:
    """Main recovery supervisor loop."""
    logger.info("Starting TaskFlow recovery supervisor")
    logger.info("Config: heartbeat_timeout=%ds, grace_period=%ds, "
                "lease_duration=%ds, recovery_interval=%ds",
                settings.WORKER_HEARTBEAT_TIMEOUT,
                settings.WORKER_FAILURE_GRACE_PERIOD,
                settings.JOB_LEASE_DURATION,
                settings.RECOVERY_INTERVAL)

    # Ensure tables exist
    Base.metadata.create_all(bind=engine)

    queue = RedisQueue(url=settings.REDIS_URL)

    if not queue.ping():
        logger.error("Cannot connect to Redis at %s", settings.REDIS_URL)
        sys.exit(1)
    logger.info("[supervisor] Connected to Redis")

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    while not _shutdown:
        db = SessionLocal()
        try:
            detect_stale_workers(db)
            recovered = recover_stuck_jobs(db, queue)
            if recovered:
                logger.info("[supervisor] Recovered %d stuck job(s)", recovered)
        except Exception as exc:
            logger.exception("[supervisor] Error in recovery cycle: %s", exc)
        finally:
            db.close()

        # Sleep for the recovery interval, checking for shutdown
        for _ in range(settings.RECOVERY_INTERVAL):
            if _shutdown:
                break
            time.sleep(1)

    logger.info("[supervisor] Recovery supervisor shut down.")


if __name__ == "__main__":
    run_recovery()
