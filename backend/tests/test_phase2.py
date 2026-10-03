"""Phase 2 tests: worker behavior, retries, backoff, dead-letter queue, and priority."""

import time
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.orm import Session
from fastapi.testclient import TestClient

from app.database.models import Job, JobAttempt, JobStatus, JobPriority, AttemptStatus
from app.workers.worker import process_job, calculate_backoff, generate_worker_id, promote_due_retries
from app.queue.redis_queue import RedisQueue


# ---------------------------------------------------------------------------
# Worker ID
# ---------------------------------------------------------------------------

class TestWorkerID:
    def test_generate_worker_id_format(self):
        wid = generate_worker_id()
        assert wid.startswith("worker-")
        assert len(wid) == len("worker-") + 5  # 5 hex chars

    def test_generate_worker_id_unique(self):
        ids = {generate_worker_id() for _ in range(100)}
        assert len(ids) == 100  # all unique


# ---------------------------------------------------------------------------
# Exponential backoff
# ---------------------------------------------------------------------------

class TestExponentialBackoff:
    def test_backoff_retry_1(self):
        """delay = 2 * 2^1 = 4"""
        assert calculate_backoff(1, base_delay=2) == 4

    def test_backoff_retry_2(self):
        """delay = 2 * 2^2 = 8"""
        assert calculate_backoff(2, base_delay=2) == 8

    def test_backoff_retry_3(self):
        """delay = 2 * 2^3 = 16"""
        assert calculate_backoff(3, base_delay=2) == 16

    def test_backoff_increases(self):
        """Verify delay_1 < delay_2 < delay_3."""
        d1 = calculate_backoff(1, base_delay=2)
        d2 = calculate_backoff(2, base_delay=2)
        d3 = calculate_backoff(3, base_delay=2)
        assert d1 < d2 < d3

    def test_backoff_with_zero_base(self):
        """With base_delay=0, all backoffs should be 0 (used in tests)."""
        assert calculate_backoff(1, base_delay=0) == 0
        assert calculate_backoff(2, base_delay=0) == 0
        assert calculate_backoff(3, base_delay=0) == 0


# ---------------------------------------------------------------------------
# Process job — success
# ---------------------------------------------------------------------------

class TestProcessJobSuccess:
    def test_successful_job(self, db_session: Session):
        """A valid SUM_NUMBERS job should complete successfully."""
        job = Job(
            job_type="SUM_NUMBERS",
            payload={"numbers": [1, 2, 3]},
            status=JobStatus.PENDING,
            priority=JobPriority.NORMAL,
        )
        db_session.add(job)
        db_session.commit()

        mock_queue = MagicMock(spec=RedisQueue)
        process_job(db_session, job, "worker-test1", mock_queue)

        db_session.refresh(job)
        assert job.status == JobStatus.COMPLETED
        assert job.result == {"sum": 6}
        assert job.completed_at is not None

        # Check attempt record
        attempts = db_session.query(JobAttempt).filter(JobAttempt.job_id == job.id).all()
        assert len(attempts) == 1
        assert attempts[0].status == AttemptStatus.COMPLETED
        assert attempts[0].worker_id == "worker-test1"
        assert attempts[0].attempt_number == 1


# ---------------------------------------------------------------------------
# Process job — failure and retry
# ---------------------------------------------------------------------------

class TestProcessJobRetry:
    def test_failed_job_schedules_retry(self, db_session: Session):
        """A failing job with retries remaining should schedule a retry."""
        job = Job(
            job_type="ALWAYS_FAIL",
            payload={},
            status=JobStatus.PENDING,
            priority=JobPriority.NORMAL,
            max_retries=3,
            retry_count=0,
        )
        db_session.add(job)
        db_session.commit()

        mock_queue = MagicMock(spec=RedisQueue)
        process_job(db_session, job, "worker-retry1", mock_queue)

        db_session.refresh(job)
        assert job.status == JobStatus.PENDING
        assert job.retry_count == 1
        assert job.last_error is not None
        assert "ALWAYS_FAIL" in job.last_error
        mock_queue.schedule_retry.assert_called_once()
        mock_queue.move_to_dead_letter.assert_not_called()

    def test_retry_count_increments(self, db_session: Session):
        """Each failure should increment retry_count."""
        job = Job(
            job_type="ALWAYS_FAIL",
            payload={},
            status=JobStatus.PENDING,
            priority=JobPriority.NORMAL,
            max_retries=3,
            retry_count=0,
        )
        db_session.add(job)
        db_session.commit()

        mock_queue = MagicMock(spec=RedisQueue)

        # Attempt 1 (initial)
        process_job(db_session, job, "worker-r", mock_queue)
        db_session.refresh(job)
        assert job.retry_count == 1

        # Attempt 2 (retry 1)
        job.status = JobStatus.PENDING
        db_session.commit()
        process_job(db_session, job, "worker-r", mock_queue)
        db_session.refresh(job)
        assert job.retry_count == 2

        # Attempt 3 (retry 2)
        job.status = JobStatus.PENDING
        db_session.commit()
        process_job(db_session, job, "worker-r", mock_queue)
        db_session.refresh(job)
        assert job.retry_count == 3

    def test_max_retries_enforced_moves_to_dlq(self, db_session: Session):
        """When retry_count reaches max_retries, job goes to DEAD_LETTER."""
        job = Job(
            job_type="ALWAYS_FAIL",
            payload={},
            status=JobStatus.PENDING,
            priority=JobPriority.NORMAL,
            max_retries=3,
            retry_count=3,  # already exhausted
        )
        db_session.add(job)
        db_session.commit()

        mock_queue = MagicMock(spec=RedisQueue)
        process_job(db_session, job, "worker-dlq", mock_queue)

        db_session.refresh(job)
        assert job.status == JobStatus.DEAD_LETTER
        mock_queue.move_to_dead_letter.assert_called_once_with(job.id)
        mock_queue.schedule_retry.assert_not_called()

    def test_full_retry_cycle_to_dead_letter(self, db_session: Session):
        """Simulate a full retry cycle: initial + 3 retries = DEAD_LETTER.

        max_retries = 3 means:
            Attempt 1 (retry_count=0) → fail → retry_count=1, schedule retry
            Attempt 2 (retry_count=1) → fail → retry_count=2, schedule retry
            Attempt 3 (retry_count=2) → fail → retry_count=3, schedule retry
            Attempt 4 (retry_count=3) → fail → DEAD_LETTER
        """
        job = Job(
            job_type="ALWAYS_FAIL",
            payload={},
            status=JobStatus.PENDING,
            priority=JobPriority.HIGH,
            max_retries=3,
            retry_count=0,
        )
        db_session.add(job)
        db_session.commit()

        mock_queue = MagicMock(spec=RedisQueue)

        for i in range(4):
            job.status = JobStatus.PENDING
            db_session.commit()
            process_job(db_session, job, f"worker-cycle", mock_queue)
            db_session.refresh(job)

        assert job.status == JobStatus.DEAD_LETTER
        assert job.retry_count == 3

        # Should have 4 attempt records
        attempts = db_session.query(JobAttempt).filter(JobAttempt.job_id == job.id).all()
        assert len(attempts) == 4
        assert all(a.status == AttemptStatus.FAILED for a in attempts)


# ---------------------------------------------------------------------------
# Success after retry
# ---------------------------------------------------------------------------

class TestSuccessAfterRetry:
    def test_fail_then_succeed(self, db_session: Session):
        """FAIL_N_TIMES with failures_before_success=2 should:
        - Attempt 1: FAIL (retry_count 0→1)
        - Attempt 2: FAIL (retry_count 1→2)
        - Attempt 3: SUCCESS
        """
        from app.workers.handlers import _fail_counter
        _fail_counter.clear()

        job = Job(
            job_type="FAIL_N_TIMES",
            payload={"failures_before_success": 2},
            status=JobStatus.PENDING,
            priority=JobPriority.NORMAL,
            max_retries=3,
            retry_count=0,
        )
        db_session.add(job)
        db_session.commit()

        # Inject job_id into payload so the handler can track attempts
        job.payload["job_id"] = job.id
        db_session.commit()

        mock_queue = MagicMock(spec=RedisQueue)

        # Attempt 1: should fail
        process_job(db_session, job, "worker-fn", mock_queue)
        db_session.refresh(job)
        assert job.status == JobStatus.PENDING
        assert job.retry_count == 1

        # Attempt 2: should fail
        job.status = JobStatus.PENDING
        db_session.commit()
        process_job(db_session, job, "worker-fn", mock_queue)
        db_session.refresh(job)
        assert job.status == JobStatus.PENDING
        assert job.retry_count == 2

        # Attempt 3: should succeed
        job.status = JobStatus.PENDING
        db_session.commit()
        process_job(db_session, job, "worker-fn", mock_queue)
        db_session.refresh(job)
        assert job.status == JobStatus.COMPLETED
        assert job.result is not None
        assert "succeeded after failures" in job.result.get("message", "")

        # 3 attempt records
        attempts = db_session.query(JobAttempt).filter(JobAttempt.job_id == job.id).all()
        assert len(attempts) == 3
        assert attempts[0].status == AttemptStatus.FAILED
        assert attempts[1].status == AttemptStatus.FAILED
        assert attempts[2].status == AttemptStatus.COMPLETED


# ---------------------------------------------------------------------------
# Dead letter API
# ---------------------------------------------------------------------------

class TestDeadLetterAPI:
    def test_dead_letter_list_empty(self, admin_client: TestClient):
        response = admin_client.get("/api/jobs/dead-letter")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    def test_dead_letter_list_with_jobs(self, admin_client: TestClient, db_session: Session):
        """Create DLQ jobs directly and verify the API lists them."""
        for i in range(3):
            job = Job(
                job_type="ALWAYS_FAIL",
                payload={},
                status=JobStatus.DEAD_LETTER,
                priority=JobPriority.NORMAL,
                retry_count=3,
                max_retries=3,
                last_error="test error",
            )
            db_session.add(job)
        db_session.commit()

        response = admin_client.get("/api/jobs/dead-letter")
        data = response.json()
        assert data["total"] == 3
        assert len(data["items"]) == 3
        assert all(j["status"] == "DEAD_LETTER" for j in data["items"])

    def test_dead_letter_pagination(self, admin_client: TestClient, db_session: Session):
        for i in range(5):
            job = Job(
                job_type="ALWAYS_FAIL",
                payload={},
                status=JobStatus.DEAD_LETTER,
                priority=JobPriority.NORMAL,
                retry_count=3,
                max_retries=3,
            )
            db_session.add(job)
        db_session.commit()

        response = admin_client.get("/api/jobs/dead-letter?page=1&limit=2")
        data = response.json()
        assert data["total"] == 5
        assert len(data["items"]) == 2

    def test_manual_retry_from_dead_letter(self, admin_client: TestClient, db_session: Session):
        """POST /api/jobs/{id}/retry should reset a DEAD_LETTER job to PENDING."""
        job = Job(
            job_type="SUM_NUMBERS",
            payload={"numbers": [1, 2]},
            status=JobStatus.DEAD_LETTER,
            priority=JobPriority.HIGH,
            retry_count=3,
            max_retries=3,
            last_error="old error",
            error_message="old error",
        )
        db_session.add(job)
        db_session.commit()
        job_id = job.id

        with patch("app.api.jobs.job_queue") as mock_queue:
            response = admin_client.post(f"/api/jobs/{job_id}/retry")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "PENDING"
        assert data["retry_count"] == 0
        assert data["last_error"] is None
        assert data["error_message"] is None
        mock_queue.enqueue_priority.assert_called_once_with(job_id, "HIGH")

    def test_retry_non_dead_letter_returns_400(self, admin_client: TestClient, db_session: Session):
        job = Job(
            job_type="SUM_NUMBERS",
            payload={"numbers": [1]},
            status=JobStatus.PENDING,
            priority=JobPriority.NORMAL,
        )
        db_session.add(job)
        db_session.commit()

        response = admin_client.post(f"/api/jobs/{job.id}/retry")
        assert response.status_code == 400

    def test_retry_nonexistent_returns_404(self, admin_client: TestClient):
        response = admin_client.post("/api/jobs/nonexistent-id/retry")
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# Job attempt history API
# ---------------------------------------------------------------------------

class TestAttemptHistoryAPI:
    def test_get_attempts(self, admin_client: TestClient, db_session: Session):
        job = Job(
            job_type="SUM_NUMBERS",
            payload={"numbers": [1]},
            status=JobStatus.COMPLETED,
            priority=JobPriority.NORMAL,
        )
        db_session.add(job)
        db_session.commit()

        attempt = JobAttempt(
            job_id=job.id,
            worker_id="worker-test",
            attempt_number=1,
            status=AttemptStatus.COMPLETED,
        )
        db_session.add(attempt)
        db_session.commit()

        response = admin_client.get(f"/api/jobs/{job.id}/attempts")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["worker_id"] == "worker-test"
        assert data[0]["attempt_number"] == 1


# ---------------------------------------------------------------------------
# Retry delay calculation (verifying backoff grows)
# ---------------------------------------------------------------------------

class TestRetryDelayCalculation:
    def test_retry_next_retry_at_is_set(self, db_session: Session):
        """When a job fails and is retried, next_retry_at should be set."""
        job = Job(
            job_type="ALWAYS_FAIL",
            payload={},
            status=JobStatus.PENDING,
            priority=JobPriority.NORMAL,
            max_retries=3,
            retry_count=0,
        )
        db_session.add(job)
        db_session.commit()

        mock_queue = MagicMock(spec=RedisQueue)
        process_job(db_session, job, "worker-delay", mock_queue)

        db_session.refresh(job)
        # RETRY_BASE_DELAY=0 in test env, so next_retry_at should be very close to now
        assert job.next_retry_at is not None


# ---------------------------------------------------------------------------
# Promote due retries
# ---------------------------------------------------------------------------

class TestPromoteDueRetries:
    def test_promote_due_retries(self, db_session: Session):
        """Jobs in the retry sorted set that are due should be promoted to their priority queue."""
        job = Job(
            job_type="SUM_NUMBERS",
            payload={"numbers": [1]},
            status=JobStatus.PENDING,
            priority=JobPriority.HIGH,
            retry_count=1,
            max_retries=3,
        )
        db_session.add(job)
        db_session.commit()

        mock_queue = MagicMock(spec=RedisQueue)
        mock_queue.get_due_retries.return_value = [job.id]

        promote_due_retries(mock_queue, db_session)

        mock_queue.enqueue_priority.assert_called_once_with(job.id, "HIGH")

    def test_promote_skips_non_pending_jobs(self, db_session: Session):
        """If a job's status changed (e.g. manually retried), it should be skipped."""
        job = Job(
            job_type="SUM_NUMBERS",
            payload={"numbers": [1]},
            status=JobStatus.COMPLETED,  # not PENDING
            priority=JobPriority.NORMAL,
            retry_count=1,
            max_retries=3,
        )
        db_session.add(job)
        db_session.commit()

        mock_queue = MagicMock(spec=RedisQueue)
        mock_queue.get_due_retries.return_value = [job.id]

        promote_due_retries(mock_queue, db_session)

        mock_queue.enqueue_priority.assert_not_called()
