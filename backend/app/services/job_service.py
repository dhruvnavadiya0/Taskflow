import logging

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database.models import Job, JobStatus
from app.queue.redis_queue import RedisQueue
from app.schemas.job import JobCreate

logger = logging.getLogger(__name__)


class JobService:
    def __init__(self, db: Session, queue: RedisQueue):
        self.db = db
        self.queue = queue

    def create_job(self, data: JobCreate, user_id: str | None = None) -> Job:
        """Create a job in the database and enqueue it for processing."""
        job = Job(
            job_type=data.job_type,
            priority=data.priority,
            payload=data.payload,
            user_id=user_id,
            status=JobStatus.PENDING,
            max_retries=data.max_retries,
        )
        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)

        try:
            # Phase 2: use priority queue
            self.queue.enqueue_priority(job.id, job.priority.value)
            from app.core.metrics import record_job_submitted
            record_job_submitted(self.queue)
        except Exception as e:
            if not isinstance(e, RuntimeError):
                logger.error("Failed to enqueue job %s: %s", job.id, e)
                # Mark the job as failed since it won't be picked up by a worker
                job.status = JobStatus.FAILED
                job.error_message = f"Failed to enqueue: {e}"
                self.db.commit()
            raise RuntimeError(f"Job created but failed to enqueue: {e}") from e

        return job

    def get_job(self, job_id: str) -> Job | None:
        """Retrieve a single job by its ID."""
        return self.db.query(Job).filter(Job.id == job_id).first()

    def list_jobs(self, page: int = 1, limit: int = 20, user_id: str | None = None) -> tuple[list[Job], int]:
        """Return a paginated list of jobs and the total count."""
        query = self.db.query(Job)
        if user_id:
            query = query.filter(Job.user_id == user_id)
            
        total = query.count()
        offset = (page - 1) * limit
        jobs = (
            query.order_by(Job.created_at.desc())
            .offset(offset)
            .limit(limit)
            .all()
        )
        return jobs, total

    # ------------------------------------------------------------------
    # Phase 2: dead-letter operations
    # ------------------------------------------------------------------

    def list_dead_letter_jobs(
        self, page: int = 1, limit: int = 20
    ) -> tuple[list[Job], int]:
        """Return a paginated list of jobs in DEAD_LETTER status."""
        query = self.db.query(Job).filter(Job.status == JobStatus.DEAD_LETTER)
        total = query.count()
        offset = (page - 1) * limit
        jobs = query.order_by(Job.created_at.desc()).offset(offset).limit(limit).all()
        return jobs, total

    def retry_dead_letter_job(self, job_id: str) -> Job:
        """Re-enqueue a DEAD_LETTER job for another round of processing.

        Resets retry state and pushes the job back into its priority queue.
        Raises ValueError if the job is not in DEAD_LETTER status.
        """
        job = self.db.query(Job).filter(Job.id == job_id).first()
        if job is None:
            raise LookupError(f"Job {job_id} not found")
        if job.status != JobStatus.DEAD_LETTER:
            raise ValueError(
                f"Job {job_id} is not in DEAD_LETTER status (current: {job.status.value})"
            )

        # Reset execution state
        job.status = JobStatus.PENDING
        job.retry_count = 0
        job.next_retry_at = None
        job.last_error = None
        job.error_message = None
        job.result = None
        job.started_at = None
        job.completed_at = None
        self.db.commit()

        # Re-enqueue
        self.queue.enqueue_priority(job.id, job.priority.value)
        return job
