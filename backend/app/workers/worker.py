"""
TaskFlow Worker — Phase 3

Continuously consumes jobs from Redis priority queues and executes them.
Features: multiple workers, retries with exponential backoff, dead-letter queue,
worker heartbeats, job leases, and graceful shutdown.

Run with: python -m app.workers.worker

Semantics:
    retry_count = number of retries already performed (starts at 0).
    max_retries = maximum number of retries allowed.

    Total execution attempts = retry_count + 1 (initial + retries).
    After a failure with retry_count >= max_retries the job moves to DEAD_LETTER.

    Delivery guarantee: at-least-once.
    Job handlers should be designed to be idempotent where possible.
"""

import logging
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta

from sqlalchemy.orm import Session

from app.core.config import settings
from app.database.connection import SessionLocal, Base, engine
from app.database.models import Job, JobAttempt, JobStatus, AttemptStatus
from app.queue.redis_queue import RedisQueue
from app.services.worker_service import WorkerService
from app.workers.handlers import registry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("taskflow.worker")

# Graceful shutdown flag
_shutdown = False


def _handle_signal(signum: int, frame: object) -> None:
    global _shutdown
    logger.info("Received signal %s, shutting down after current job...", signum)
    _shutdown = True


def generate_worker_id() -> str:
    """Generate a short, unique worker identifier."""
    short_id = uuid.uuid4().hex[:5]
    return f"worker-{short_id}"


def calculate_backoff(retry_count: int, base_delay: int | None = None) -> float:
    """Calculate exponential backoff delay in seconds.

    delay = base_delay × 2^retry_count
    """
    if base_delay is None:
        base_delay = settings.RETRY_BASE_DELAY
    return base_delay * (2 ** retry_count)


# ---------------------------------------------------------------------------
# Heartbeat thread
# ---------------------------------------------------------------------------

class HeartbeatThread(threading.Thread):
    """Background thread that sends worker heartbeats independently of job execution.

    This ensures heartbeats continue even during long-running jobs.
    """

    def __init__(self, worker_id: str, interval: int):
        super().__init__(daemon=True, name=f"heartbeat-{worker_id}")
        self.worker_id = worker_id
        self.interval = interval
        self._stop_event = threading.Event()

    def run(self) -> None:
        logger.info("[%s] Heartbeat thread started (interval=%ds)", self.worker_id, self.interval)
        while not self._stop_event.is_set():
            try:
                db = SessionLocal()
                try:
                    svc = WorkerService(db)
                    svc.heartbeat(self.worker_id)
                finally:
                    db.close()
            except Exception as exc:
                logger.error("[%s] Heartbeat error: %s", self.worker_id, exc)

            self._stop_event.wait(timeout=self.interval)

        logger.info("[%s] Heartbeat thread stopped", self.worker_id)

    def stop(self) -> None:
        self._stop_event.set()


# ---------------------------------------------------------------------------
# Lease renewal thread
# ---------------------------------------------------------------------------

class LeaseRenewalThread(threading.Thread):
    """Background thread that renews the lease on a currently-running job.

    Started when a job begins execution, stopped when it completes.
    """

    def __init__(self, worker_id: str, job_id: str, interval: int, duration: int):
        super().__init__(daemon=True, name=f"lease-{job_id[:8]}")
        self.worker_id = worker_id
        self.job_id = job_id
        self.interval = interval
        self.duration = duration
        self._stop_event = threading.Event()

    def run(self) -> None:
        logger.info("[%s] Lease renewal started for job %s (interval=%ds)",
                     self.worker_id, self.job_id, self.interval)
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self.interval)
            if self._stop_event.is_set():
                break
            try:
                db = SessionLocal()
                try:
                    job = db.query(Job).filter(Job.id == self.job_id).first()
                    if job and job.status == JobStatus.RUNNING and job.worker_id == self.worker_id:
                        job.lease_expires_at = datetime.now(timezone.utc) + timedelta(seconds=self.duration)
                        db.commit()
                        logger.info("[%s] Renewed lease for job %s until %s",
                                     self.worker_id, self.job_id, job.lease_expires_at)
                finally:
                    db.close()
            except Exception as exc:
                logger.error("[%s] Lease renewal error for job %s: %s",
                              self.worker_id, self.job_id, exc)

    def stop(self) -> None:
        self._stop_event.set()


# ---------------------------------------------------------------------------
# Job processing
# ---------------------------------------------------------------------------

def process_job(db: Session, job: Job, worker_id: str, queue: RedisQueue) -> None:
    """Execute a single job: look up the handler, run it, handle retries.

    Flow:
        1. Mark RUNNING, set ownership + lease, create attempt record.
        2. Start lease renewal thread.
        3. Execute handler.
        4. Stop lease renewal.
        5a. On success → COMPLETED, clear ownership.
        5b. On failure → check retry budget:
            - retries remaining → schedule retry.
            - no retries left   → DEAD_LETTER queue.
    """
    now = datetime.now(timezone.utc)
    attempt_number = job.retry_count + 1  # 1-based for humans

    # Mark RUNNING with ownership
    job.status = JobStatus.RUNNING
    job.started_at = now
    job.worker_id = worker_id
    job.lease_expires_at = now + timedelta(seconds=settings.JOB_LEASE_DURATION)
    db.commit()

    # Update worker's current job
    worker_svc = WorkerService(db)
    worker_svc.set_current_job(worker_id, job.id)

    # Create attempt record
    attempt = JobAttempt(
        job_id=job.id,
        worker_id=worker_id,
        attempt_number=attempt_number,
        started_at=now,
        status=AttemptStatus.RUNNING,
    )
    db.add(attempt)
    db.commit()

    logger.info("[%s] Starting attempt %d for job %s (lease until %s)",
                worker_id, attempt_number, job.id, job.lease_expires_at)

    # Start lease renewal
    lease_thread = LeaseRenewalThread(
        worker_id=worker_id,
        job_id=job.id,
        interval=settings.JOB_LEASE_RENEW_INTERVAL,
        duration=settings.JOB_LEASE_DURATION,
    )
    lease_thread.start()

    try:
        handler = registry.get_handler(job.job_type)
        result = handler(job.payload or {})

        # SUCCESS
        finished_at = datetime.now(timezone.utc)
        job.status = JobStatus.COMPLETED
        job.result = result
        job.completed_at = finished_at
        job.worker_id = None
        job.lease_expires_at = None

        attempt.status = AttemptStatus.COMPLETED
        attempt.finished_at = finished_at

        # Phase 4: metrics
        def _to_utc(dt: datetime | None) -> datetime | None:
            if dt is None:
                return None
            return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)

        created_utc = _to_utc(job.created_at)
        queue_wait = max(0.0, (now - created_utc).total_seconds()) if created_utc else 0.0
        duration = max(0.0, (finished_at - now).total_seconds())
        from app.core.metrics import record_job_completed, record_job_failed, record_job_retried
        record_job_completed(duration, queue_wait, queue)

        logger.info("[%s] Job %s completed in %.3fs (wait: %.3fs)",
                    worker_id, job.id, duration, queue_wait)

    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        error_msg = str(exc)

        attempt.status = AttemptStatus.FAILED
        attempt.finished_at = finished_at
        attempt.error_message = error_msg

        job.last_error = error_msg
        job.error_message = error_msg
        job.completed_at = finished_at

        logger.error("[%s] Job %s failed: %s", worker_id, job.id, error_msg)

        from app.core.metrics import record_job_completed, record_job_failed, record_job_retried
        if job.retry_count < job.max_retries:
            job.retry_count += 1
            delay = calculate_backoff(job.retry_count)
            retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
            job.next_retry_at = retry_at
            job.status = JobStatus.PENDING
            job.worker_id = None
            job.lease_expires_at = None

            queue.schedule_retry(job.id, time.time() + delay)
            record_job_retried(queue)

            logger.info(
                "[%s] Scheduling retry %d/%d for job %s in %.1f seconds",
                worker_id, job.retry_count, job.max_retries, job.id, delay,
            )
        else:
            job.status = JobStatus.DEAD_LETTER
            job.worker_id = None
            job.lease_expires_at = None
            queue.move_to_dead_letter(job.id)

            def _to_utc(dt: datetime | None) -> datetime | None:
                if dt is None:
                    return None
                return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)

            created_utc = _to_utc(job.created_at)
            queue_wait = max(0.0, (now - created_utc).total_seconds()) if created_utc else 0.0
            duration = max(0.0, (finished_at - now).total_seconds())
            record_job_failed(duration, queue_wait, queue)
            logger.warning(
                "[%s] Job %s moved to dead-letter queue after %d retries",
                worker_id, job.id, job.max_retries,
            )

    finally:
        # Always stop lease renewal
        lease_thread.stop()
        lease_thread.join(timeout=2)

    # Clear worker's current job
    worker_svc.set_current_job(worker_id, None)
    db.commit()


def promote_due_retries(queue: RedisQueue, db: Session) -> None:
    """Move any retry jobs whose delay has elapsed back into their priority queue."""
    due_ids = queue.get_due_retries()
    for job_id in due_ids:
        job = db.query(Job).filter(Job.id == job_id).first()
        if job is None:
            logger.warning("Retry job %s not found in database, skipping", job_id)
            continue
        if job.status != JobStatus.PENDING:
            continue

        queue.enqueue_priority(job_id, job.priority.value)
        logger.info("Promoted retry job %s to %s queue", job_id, job.priority.value)


def run_worker() -> None:
    """Main worker loop."""
    worker_id = generate_worker_id()
    logger.info("Starting TaskFlow worker [%s]", worker_id)
    logger.info("Registered job types: %s", registry.registered_types)

    # Ensure tables exist
    Base.metadata.create_all(bind=engine)

    queue = RedisQueue(url=settings.REDIS_URL)

    if not queue.ping():
        logger.error("Cannot connect to Redis at %s", settings.REDIS_URL)
        sys.exit(1)
    logger.info("[%s] Connected to Redis", worker_id)

    # Register worker in the database
    db = SessionLocal()
    try:
        worker_svc = WorkerService(db)
        worker_svc.register_worker(worker_id)
    finally:
        db.close()

    # Start heartbeat thread
    heartbeat = HeartbeatThread(worker_id, settings.WORKER_HEARTBEAT_INTERVAL)
    heartbeat.start()

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        while not _shutdown:
            # 1. Promote due retries
            db = SessionLocal()
            try:
                promote_due_retries(queue, db)
            except Exception as exc:
                logger.error("[%s] Error promoting retries: %s", worker_id, exc)
            finally:
                db.close()

            # 2. Block-pop from priority queues
            job_id = queue.dequeue_priority(timeout=settings.QUEUE_TIMEOUT)
            if job_id is None:
                continue

            logger.info("[%s] Received job: %s", worker_id, job_id)
            db = SessionLocal()
            try:
                job = db.query(Job).filter(Job.id == job_id).first()
                if job is None:
                    logger.warning("[%s] Job %s not found in database, skipping",
                                   worker_id, job_id)
                    continue

                process_job(db, job, worker_id, queue)
                logger.info("[%s] Job %s finished with status: %s",
                            worker_id, job.id, job.status.value)
            except Exception as exc:
                logger.exception("[%s] Unexpected error processing job %s: %s",
                                  worker_id, job_id, exc)
            finally:
                db.close()

    finally:
        # Graceful shutdown
        logger.info("[%s] Shutting down...", worker_id)
        heartbeat.stop()
        heartbeat.join(timeout=5)

        # Mark worker as DEAD
        db = SessionLocal()
        try:
            worker_svc = WorkerService(db)
            worker_svc.deregister_worker(worker_id)
        finally:
            db.close()

        logger.info("[%s] Worker shut down.", worker_id)


if __name__ == "__main__":
    run_worker()
