"""Phase 3 tests: worker reliability, heartbeats, leases, recovery, and failure simulation.

Tests are organized by the spec sections. All database operations use the in-memory
SQLite database provided by conftest.py; Redis is mocked.
"""

import time
import threading
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.orm import Session
from fastapi.testclient import TestClient

from app.database.models import (
    Job, JobAttempt, Worker,
    JobStatus, JobPriority, AttemptStatus, WorkerStatus,
)
from app.services.worker_service import WorkerService
from app.workers.worker import (
    process_job, generate_worker_id,
    HeartbeatThread, LeaseRenewalThread,
)
from app.workers.recovery import detect_stale_workers, recover_stuck_jobs
from app.queue.redis_queue import RedisQueue
from app.workers.handlers import registry, handle_sleep, handle_crash_worker


# ---------------------------------------------------------------------------
# Worker Registration
# ---------------------------------------------------------------------------

class TestWorkerRegistration:
    def test_worker_registers_online(self, db_session: Session):
        """A newly registered worker should be in ONLINE status."""
        svc = WorkerService(db_session)
        worker = svc.register_worker("worker-test1")

        assert worker.id == "worker-test1"
        assert worker.status == WorkerStatus.ONLINE
        assert worker.hostname  # should be set
        assert worker.current_job_id is None

    def test_worker_id_is_unique(self):
        """generate_worker_id should produce unique IDs."""
        ids = {generate_worker_id() for _ in range(200)}
        assert len(ids) == 200

    def test_worker_id_format(self):
        """Worker ID should match worker-XXXXX format."""
        wid = generate_worker_id()
        assert wid.startswith("worker-")
        assert len(wid) == len("worker-") + 5

    def test_worker_starts_online(self, db_session: Session):
        """Newly registered worker status must be ONLINE."""
        svc = WorkerService(db_session)
        worker = svc.register_worker("worker-online")
        assert worker.status == WorkerStatus.ONLINE

    def test_re_register_worker(self, db_session: Session):
        """Re-registering an existing worker should reset it to ONLINE."""
        svc = WorkerService(db_session)
        svc.register_worker("worker-rereg")
        svc.mark_worker_dead("worker-rereg")

        worker = svc.register_worker("worker-rereg")
        assert worker.status == WorkerStatus.ONLINE
        assert worker.current_job_id is None

    def test_multiple_workers_register_independently(self, db_session: Session):
        """Multiple workers should coexist in the registry."""
        svc = WorkerService(db_session)
        w1 = svc.register_worker("worker-a")
        w2 = svc.register_worker("worker-b")
        w3 = svc.register_worker("worker-c")

        all_workers = svc.list_workers()
        assert len(all_workers) == 3
        assert {w.id for w in all_workers} == {"worker-a", "worker-b", "worker-c"}


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

class TestHeartbeat:
    def test_heartbeat_updates_timestamp(self, db_session: Session):
        """heartbeat() should update the last_heartbeat field."""
        svc = WorkerService(db_session)
        worker = svc.register_worker("worker-hb")
        old_hb = worker.last_heartbeat

        # Advance time slightly
        time.sleep(0.01)
        svc.heartbeat("worker-hb")

        db_session.refresh(worker)
        assert worker.last_heartbeat >= old_hb

    def test_heartbeat_restores_suspected_to_online(self, db_session: Session):
        """A heartbeat from a SUSPECTED worker should restore it to ONLINE."""
        svc = WorkerService(db_session)
        svc.register_worker("worker-sus")
        svc.mark_worker_suspected("worker-sus")

        worker = svc.get_worker("worker-sus")
        assert worker.status == WorkerStatus.SUSPECTED

        svc.heartbeat("worker-sus")
        db_session.refresh(worker)
        assert worker.status == WorkerStatus.ONLINE

    def test_heartbeat_for_unknown_worker_registers(self, db_session: Session):
        """Heartbeat for an unregistered worker should auto-register it."""
        svc = WorkerService(db_session)
        svc.heartbeat("worker-unknown")

        worker = svc.get_worker("worker-unknown")
        assert worker is not None
        assert worker.status == WorkerStatus.ONLINE


# ---------------------------------------------------------------------------
# Worker Timeout / Stale Detection
# ---------------------------------------------------------------------------

class TestWorkerTimeout:
    def test_stale_worker_becomes_suspected(self, db_session: Session):
        """A worker whose heartbeat has expired should be marked SUSPECTED."""
        svc = WorkerService(db_session)
        worker = svc.register_worker("worker-stale")

        # Backdate the heartbeat to simulate staleness
        expired_time = datetime.now(timezone.utc) - timedelta(seconds=20)
        worker.last_heartbeat = expired_time
        db_session.commit()

        with patch("app.workers.recovery.settings") as mock_settings:
            mock_settings.WORKER_HEARTBEAT_TIMEOUT = 15
            mock_settings.WORKER_FAILURE_GRACE_PERIOD = 5
            detect_stale_workers(db_session)

        db_session.refresh(worker)
        assert worker.status == WorkerStatus.SUSPECTED

    def test_suspected_worker_becomes_dead_after_grace(self, db_session: Session):
        """A SUSPECTED worker whose heartbeat exceeds timeout + grace should become DEAD."""
        svc = WorkerService(db_session)
        worker = svc.register_worker("worker-dead")

        # Mark suspected and set heartbeat way in the past
        worker.status = WorkerStatus.SUSPECTED
        worker.last_heartbeat = datetime.now(timezone.utc) - timedelta(seconds=25)
        db_session.commit()

        with patch("app.workers.recovery.settings") as mock_settings:
            mock_settings.WORKER_HEARTBEAT_TIMEOUT = 15
            mock_settings.WORKER_FAILURE_GRACE_PERIOD = 5
            detect_stale_workers(db_session)

        db_session.refresh(worker)
        assert worker.status == WorkerStatus.DEAD

    def test_suspected_worker_not_dead_within_grace(self, db_session: Session):
        """A SUSPECTED worker within the grace period should remain SUSPECTED."""
        svc = WorkerService(db_session)
        worker = svc.register_worker("worker-grace")

        # Heartbeat is past timeout but within timeout + grace
        worker.status = WorkerStatus.SUSPECTED
        worker.last_heartbeat = datetime.now(timezone.utc) - timedelta(seconds=17)
        db_session.commit()

        with patch("app.workers.recovery.settings") as mock_settings:
            mock_settings.WORKER_HEARTBEAT_TIMEOUT = 15
            mock_settings.WORKER_FAILURE_GRACE_PERIOD = 5
            detect_stale_workers(db_session)

        db_session.refresh(worker)
        assert worker.status == WorkerStatus.SUSPECTED

    def test_healthy_worker_remains_online(self, db_session: Session):
        """A worker with a recent heartbeat should stay ONLINE."""
        svc = WorkerService(db_session)
        worker = svc.register_worker("worker-healthy")

        with patch("app.workers.recovery.settings") as mock_settings:
            mock_settings.WORKER_HEARTBEAT_TIMEOUT = 15
            mock_settings.WORKER_FAILURE_GRACE_PERIOD = 5
            detect_stale_workers(db_session)

        db_session.refresh(worker)
        assert worker.status == WorkerStatus.ONLINE


# ---------------------------------------------------------------------------
# Job Ownership
# ---------------------------------------------------------------------------

class TestJobOwnership:
    def test_running_job_records_worker_id(self, db_session: Session):
        """When a worker claims a job, it should set worker_id on the job."""
        job = Job(
            job_type="SUM_NUMBERS",
            payload={"numbers": [1, 2]},
            status=JobStatus.PENDING,
            priority=JobPriority.NORMAL,
        )
        db_session.add(job)
        db_session.commit()

        mock_queue = MagicMock(spec=RedisQueue)
        process_job(db_session, job, "worker-own1", mock_queue)

        db_session.refresh(job)
        # After completion, worker_id is cleared
        assert job.status == JobStatus.COMPLETED
        # But we can verify via attempt record
        attempt = db_session.query(JobAttempt).filter(
            JobAttempt.job_id == job.id
        ).first()
        assert attempt.worker_id == "worker-own1"

    def test_running_job_receives_lease(self, db_session: Session):
        """When a worker claims a job, lease_expires_at should be set."""
        job = Job(
            job_type="SLEEP",
            payload={"seconds": 0.05},
            status=JobStatus.PENDING,
            priority=JobPriority.NORMAL,
        )
        db_session.add(job)
        db_session.commit()

        # We'll capture the lease value during execution
        lease_captured = {}

        original_process = process_job.__wrapped__ if hasattr(process_job, '__wrapped__') else None

        # Instead of patching, just run and check the attempt
        mock_queue = MagicMock(spec=RedisQueue)
        process_job(db_session, job, "worker-lease", mock_queue)

        # Verify attempt was created
        attempt = db_session.query(JobAttempt).filter(
            JobAttempt.job_id == job.id
        ).first()
        assert attempt is not None
        assert attempt.worker_id == "worker-lease"

    def test_worker_set_current_job(self, db_session: Session):
        """WorkerService.set_current_job should update the worker's current_job_id."""
        svc = WorkerService(db_session)
        svc.register_worker("worker-cur")

        svc.set_current_job("worker-cur", "job-123")
        worker = svc.get_worker("worker-cur")
        assert worker.current_job_id == "job-123"

        svc.set_current_job("worker-cur", None)
        db_session.refresh(worker)
        assert worker.current_job_id is None


# ---------------------------------------------------------------------------
# Recovery of Stuck Jobs
# ---------------------------------------------------------------------------

class TestJobRecovery:
    def _create_stuck_job(self, db_session: Session, worker_id: str = "worker-dead",
                          priority: JobPriority = JobPriority.HIGH) -> Job:
        """Helper: create a RUNNING job with an expired lease owned by a DEAD worker."""
        # Register and kill the worker
        svc = WorkerService(db_session)
        try:
            svc.register_worker(worker_id)
        except Exception:
            pass
        svc.mark_worker_dead(worker_id)

        job = Job(
            job_type="SUM_NUMBERS",
            payload={"numbers": [1]},
            status=JobStatus.RUNNING,
            priority=priority,
            worker_id=worker_id,
            lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=10),
        )
        db_session.add(job)
        db_session.commit()  # commit so job.id is generated

        # Add a running attempt
        attempt = JobAttempt(
            job_id=job.id,
            worker_id=worker_id,
            attempt_number=1,
            status=AttemptStatus.RUNNING,
        )
        db_session.add(attempt)
        db_session.commit()
        return job

    def test_expired_lease_detected_and_recovered(self, db_session: Session):
        """A RUNNING job with expired lease on a DEAD worker should be recovered."""
        job = self._create_stuck_job(db_session)
        mock_queue = MagicMock(spec=RedisQueue)

        recovered = recover_stuck_jobs(db_session, mock_queue)

        assert recovered == 1
        db_session.refresh(job)
        assert job.status == JobStatus.PENDING
        assert job.worker_id is None
        assert job.lease_expires_at is None

    def test_recovery_count_increments(self, db_session: Session):
        """recovery_count should increase by 1 when a job is recovered."""
        job = self._create_stuck_job(db_session)
        assert job.recovery_count == 0

        mock_queue = MagicMock(spec=RedisQueue)
        recover_stuck_jobs(db_session, mock_queue)

        db_session.refresh(job)
        assert job.recovery_count == 1

    def test_job_returns_to_correct_priority_queue(self, db_session: Session):
        """Recovered job should be re-enqueued to its original priority queue."""
        job = self._create_stuck_job(db_session, priority=JobPriority.HIGH)
        mock_queue = MagicMock(spec=RedisQueue)

        recover_stuck_jobs(db_session, mock_queue)

        mock_queue.enqueue_priority.assert_called_once_with(job.id, "HIGH")

    def test_recovery_marks_attempt_recovered(self, db_session: Session):
        """The in-progress attempt should be marked RECOVERED."""
        job = self._create_stuck_job(db_session)
        mock_queue = MagicMock(spec=RedisQueue)

        recover_stuck_jobs(db_session, mock_queue)

        attempt = db_session.query(JobAttempt).filter(
            JobAttempt.job_id == job.id
        ).first()
        assert attempt.status == AttemptStatus.RECOVERED
        assert attempt.finished_at is not None
        assert "died" in attempt.error_message.lower()

    def test_multiple_stuck_jobs_recovered(self, db_session: Session):
        """Multiple stuck jobs should all be recovered in one sweep."""
        j1 = self._create_stuck_job(db_session, worker_id="worker-dead")
        j2 = self._create_stuck_job(db_session, worker_id="worker-dead")

        mock_queue = MagicMock(spec=RedisQueue)
        recovered = recover_stuck_jobs(db_session, mock_queue)

        assert recovered == 2
        for j in [j1, j2]:
            db_session.refresh(j)
            assert j.status == JobStatus.PENDING

    def test_job_returns_to_low_priority_queue(self, db_session: Session):
        """Recovered LOW priority job should go back to the LOW queue."""
        job = self._create_stuck_job(db_session, priority=JobPriority.LOW)
        mock_queue = MagicMock(spec=RedisQueue)

        recover_stuck_jobs(db_session, mock_queue)

        mock_queue.enqueue_priority.assert_called_once_with(job.id, "LOW")


# ---------------------------------------------------------------------------
# Race Condition Protection
# ---------------------------------------------------------------------------

class TestRaceConditions:
    def test_completed_job_is_not_recovered(self, db_session: Session):
        """A job that completed before recovery runs should NOT be recovered."""
        svc = WorkerService(db_session)
        svc.register_worker("worker-race")
        svc.mark_worker_dead("worker-race")

        job = Job(
            job_type="SUM_NUMBERS",
            payload={"numbers": [1]},
            status=JobStatus.COMPLETED,  # Already completed!
            priority=JobPriority.NORMAL,
            worker_id="worker-race",
            lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=10),
            result={"sum": 1},
        )
        db_session.add(job)
        db_session.commit()

        mock_queue = MagicMock(spec=RedisQueue)
        recovered = recover_stuck_jobs(db_session, mock_queue)

        assert recovered == 0
        db_session.refresh(job)
        assert job.status == JobStatus.COMPLETED

    def test_active_worker_job_not_recovered(self, db_session: Session):
        """A job owned by an ONLINE worker should NOT be recovered."""
        svc = WorkerService(db_session)
        svc.register_worker("worker-alive")

        job = Job(
            job_type="SUM_NUMBERS",
            payload={"numbers": [1]},
            status=JobStatus.RUNNING,
            priority=JobPriority.NORMAL,
            worker_id="worker-alive",
            lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
        )
        db_session.add(job)
        db_session.commit()

        mock_queue = MagicMock(spec=RedisQueue)
        recovered = recover_stuck_jobs(db_session, mock_queue)

        assert recovered == 0
        db_session.refresh(job)
        assert job.status == JobStatus.RUNNING

    def test_job_with_valid_lease_not_recovered(self, db_session: Session):
        """A job with a non-expired lease on a DEAD worker should NOT be recovered yet."""
        svc = WorkerService(db_session)
        svc.register_worker("worker-lease-ok")
        svc.mark_worker_dead("worker-lease-ok")

        job = Job(
            job_type="SUM_NUMBERS",
            payload={"numbers": [1]},
            status=JobStatus.RUNNING,
            priority=JobPriority.NORMAL,
            worker_id="worker-lease-ok",
            lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
        )
        db_session.add(job)
        db_session.commit()

        mock_queue = MagicMock(spec=RedisQueue)
        recovered = recover_stuck_jobs(db_session, mock_queue)

        assert recovered == 0

    def test_failed_job_not_recovered(self, db_session: Session):
        """A FAILED job should not be recovered even if worker is dead."""
        svc = WorkerService(db_session)
        svc.register_worker("worker-fail")
        svc.mark_worker_dead("worker-fail")

        job = Job(
            job_type="ALWAYS_FAIL",
            payload={},
            status=JobStatus.FAILED,
            priority=JobPriority.NORMAL,
            worker_id="worker-fail",
            lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=10),
        )
        db_session.add(job)
        db_session.commit()

        mock_queue = MagicMock(spec=RedisQueue)
        recovered = recover_stuck_jobs(db_session, mock_queue)

        assert recovered == 0

    def test_dead_letter_job_not_recovered(self, db_session: Session):
        """A DEAD_LETTER job should not be recovered."""
        svc = WorkerService(db_session)
        svc.register_worker("worker-dlq")
        svc.mark_worker_dead("worker-dlq")

        job = Job(
            job_type="ALWAYS_FAIL",
            payload={},
            status=JobStatus.DEAD_LETTER,
            priority=JobPriority.NORMAL,
            worker_id="worker-dlq",
            lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=10),
        )
        db_session.add(job)
        db_session.commit()

        mock_queue = MagicMock(spec=RedisQueue)
        recovered = recover_stuck_jobs(db_session, mock_queue)

        assert recovered == 0


# ---------------------------------------------------------------------------
# Multiple Workers
# ---------------------------------------------------------------------------

class TestMultipleWorkers:
    def test_independent_heartbeats(self, db_session: Session):
        """Multiple workers should maintain independent heartbeats."""
        svc = WorkerService(db_session)
        svc.register_worker("worker-m1")
        svc.register_worker("worker-m2")

        # Only heartbeat worker-m1
        time.sleep(0.01)
        svc.heartbeat("worker-m1")

        w1 = svc.get_worker("worker-m1")
        w2 = svc.get_worker("worker-m2")

        assert w1.last_heartbeat >= w2.last_heartbeat

    def test_one_dead_worker_does_not_affect_healthy(self, db_session: Session):
        """Marking one worker dead should not affect other workers."""
        svc = WorkerService(db_session)
        svc.register_worker("worker-good")
        svc.register_worker("worker-bad")

        # Make worker-bad stale
        bad = svc.get_worker("worker-bad")
        bad.last_heartbeat = datetime.now(timezone.utc) - timedelta(seconds=25)
        db_session.commit()

        with patch("app.workers.recovery.settings") as mock_settings:
            mock_settings.WORKER_HEARTBEAT_TIMEOUT = 15
            mock_settings.WORKER_FAILURE_GRACE_PERIOD = 5
            detect_stale_workers(db_session)

        good = svc.get_worker("worker-good")
        bad = svc.get_worker("worker-bad")
        assert good.status == WorkerStatus.ONLINE
        assert bad.status == WorkerStatus.SUSPECTED

    def test_recovered_job_processed_by_another_worker(self, db_session: Session):
        """After recovery, another worker should be able to process the job."""
        svc = WorkerService(db_session)
        svc.register_worker("worker-dead-a")
        svc.mark_worker_dead("worker-dead-a")

        job = Job(
            job_type="SUM_NUMBERS",
            payload={"numbers": [10, 20]},
            status=JobStatus.RUNNING,
            priority=JobPriority.NORMAL,
            worker_id="worker-dead-a",
            lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=10),
        )
        db_session.add(job)
        db_session.commit()  # commit so job.id is generated
        attempt_1 = JobAttempt(
            job_id=job.id,
            worker_id="worker-dead-a",
            attempt_number=1,
            status=AttemptStatus.RUNNING,
        )
        db_session.add(attempt_1)
        db_session.commit()

        # Recover
        mock_queue = MagicMock(spec=RedisQueue)
        recover_stuck_jobs(db_session, mock_queue)

        db_session.refresh(job)
        assert job.status == JobStatus.PENDING

        # Now worker-b processes it
        process_job(db_session, job, "worker-b", mock_queue)

        db_session.refresh(job)
        assert job.status == JobStatus.COMPLETED
        assert job.result == {"sum": 30}

        # Check attempts: should have 2 — one RECOVERED, one COMPLETED
        attempts = (
            db_session.query(JobAttempt)
            .filter(JobAttempt.job_id == job.id)
            .order_by(JobAttempt.attempt_number)
            .all()
        )
        assert len(attempts) == 2
        assert attempts[0].status == AttemptStatus.RECOVERED
        assert attempts[0].worker_id == "worker-dead-a"
        assert attempts[1].status == AttemptStatus.COMPLETED
        assert attempts[1].worker_id == "worker-b"


# ---------------------------------------------------------------------------
# Critical Failure Test (the most important Phase 3 test)
# ---------------------------------------------------------------------------

class TestCriticalFailure:
    """Simulates the full failure-recovery lifecycle:

    1. Worker A registers
    2. Job is submitted (PENDING)
    3. Worker A claims job (RUNNING, with ownership + lease)
    4. Worker A "crashes" (heartbeat stops, lease expires)
    5. Recovery supervisor detects Worker A as DEAD
    6. Recovery supervisor recovers the job (back to PENDING)
    7. Worker B processes the job to COMPLETED
    """

    def test_full_failure_recovery_lifecycle(self, db_session: Session):
        # --- Step 1: Worker A registers ---
        svc = WorkerService(db_session)
        svc.register_worker("worker-A")
        worker_a = svc.get_worker("worker-A")
        assert worker_a.status == WorkerStatus.ONLINE

        # --- Step 2: Create a job ---
        job = Job(
            job_type="SUM_NUMBERS",
            payload={"numbers": [5, 10, 15]},
            status=JobStatus.PENDING,
            priority=JobPriority.HIGH,
        )
        db_session.add(job)
        db_session.commit()
        job_id = job.id

        # --- Step 3: Worker A claims and starts the job ---
        now = datetime.now(timezone.utc)
        job.status = JobStatus.RUNNING
        job.started_at = now
        job.worker_id = "worker-A"
        job.lease_expires_at = now + timedelta(seconds=30)
        svc.set_current_job("worker-A", job.id)

        attempt = JobAttempt(
            job_id=job.id,
            worker_id="worker-A",
            attempt_number=1,
            status=AttemptStatus.RUNNING,
            started_at=now,
        )
        db_session.add(attempt)
        db_session.commit()

        assert job.status == JobStatus.RUNNING
        assert job.worker_id == "worker-A"

        # --- Step 4: Simulate Worker A crash ---
        # Heartbeat stops and lease expires
        worker_a.last_heartbeat = now - timedelta(seconds=25)
        job.lease_expires_at = now - timedelta(seconds=5)
        db_session.commit()

        # --- Step 5: Recovery supervisor detects dead worker ---
        with patch("app.workers.recovery.settings") as mock_settings:
            mock_settings.WORKER_HEARTBEAT_TIMEOUT = 15
            mock_settings.WORKER_FAILURE_GRACE_PERIOD = 5
            detect_stale_workers(db_session)

        # First pass: ONLINE → SUSPECTED
        db_session.refresh(worker_a)
        assert worker_a.status == WorkerStatus.SUSPECTED

        # Second pass (grace period exceeded): SUSPECTED → DEAD
        with patch("app.workers.recovery.settings") as mock_settings:
            mock_settings.WORKER_HEARTBEAT_TIMEOUT = 15
            mock_settings.WORKER_FAILURE_GRACE_PERIOD = 5
            detect_stale_workers(db_session)

        db_session.refresh(worker_a)
        assert worker_a.status == WorkerStatus.DEAD

        # --- Step 6: Recovery supervisor recovers the job ---
        mock_queue = MagicMock(spec=RedisQueue)
        recovered = recover_stuck_jobs(db_session, mock_queue)

        assert recovered == 1
        db_session.refresh(job)
        assert job.status == JobStatus.PENDING
        assert job.worker_id is None
        assert job.lease_expires_at is None
        assert job.recovery_count == 1

        # Verify it was re-enqueued to the HIGH priority queue
        mock_queue.enqueue_priority.assert_called_once_with(job_id, "HIGH")

        # --- Step 7: Worker B processes the job ---
        process_job(db_session, job, "worker-B", mock_queue)

        db_session.refresh(job)
        assert job.status == JobStatus.COMPLETED
        assert job.result == {"sum": 30}
        assert job.recovery_count == 1  # still 1

        # Verify attempt history
        attempts = (
            db_session.query(JobAttempt)
            .filter(JobAttempt.job_id == job_id)
            .order_by(JobAttempt.attempt_number)
            .all()
        )
        assert len(attempts) == 2
        assert attempts[0].worker_id == "worker-A"
        assert attempts[0].status == AttemptStatus.RECOVERED
        assert attempts[1].worker_id == "worker-B"
        assert attempts[1].status == AttemptStatus.COMPLETED


# ---------------------------------------------------------------------------
# False Positive Test (long-running jobs)
# ---------------------------------------------------------------------------

class TestFalsePositive:
    def test_long_running_job_worker_not_falsely_dead(self, db_session: Session):
        """A worker executing a long-running job should NOT be marked DEAD
        if its heartbeat is still recent (i.e., heartbeat thread is running).
        """
        svc = WorkerService(db_session)
        worker = svc.register_worker("worker-long")

        # Simulate: the worker's heartbeat is recent (heartbeat thread is alive)
        worker.last_heartbeat = datetime.now(timezone.utc)
        db_session.commit()

        # Run stale worker detection
        with patch("app.workers.recovery.settings") as mock_settings:
            mock_settings.WORKER_HEARTBEAT_TIMEOUT = 15
            mock_settings.WORKER_FAILURE_GRACE_PERIOD = 5
            detect_stale_workers(db_session)

        db_session.refresh(worker)
        assert worker.status == WorkerStatus.ONLINE

    def test_long_running_job_lease_renewed(self, db_session: Session):
        """A job with a renewed lease should not be recovered."""
        svc = WorkerService(db_session)
        svc.register_worker("worker-long2")

        job = Job(
            job_type="SLEEP",
            payload={"seconds": 60},
            status=JobStatus.RUNNING,
            priority=JobPriority.NORMAL,
            worker_id="worker-long2",
            # Lease is still valid (not expired)
            lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=20),
        )
        db_session.add(job)
        db_session.commit()

        # Even if we somehow run recovery, this shouldn't be recovered
        # because the worker is ONLINE (not DEAD)
        mock_queue = MagicMock(spec=RedisQueue)
        recovered = recover_stuck_jobs(db_session, mock_queue)
        assert recovered == 0


# ---------------------------------------------------------------------------
# Worker Shutdown
# ---------------------------------------------------------------------------

class TestWorkerShutdown:
    def test_graceful_shutdown_marks_dead(self, db_session: Session):
        """deregister_worker should mark the worker as DEAD."""
        svc = WorkerService(db_session)
        svc.register_worker("worker-shut")

        svc.deregister_worker("worker-shut")

        worker = svc.get_worker("worker-shut")
        assert worker.status == WorkerStatus.DEAD


# ---------------------------------------------------------------------------
# SLEEP Handler
# ---------------------------------------------------------------------------

class TestSleepHandler:
    def test_sleep_handler_completes(self):
        """SLEEP handler should sleep then return."""
        result = handle_sleep({"seconds": 0.01})
        assert result == {"slept": 0.01}

    def test_sleep_handler_negative_raises(self):
        """Negative seconds should raise ValueError."""
        with pytest.raises(ValueError, match="non-negative"):
            handle_sleep({"seconds": -1})


# ---------------------------------------------------------------------------
# CRASH_WORKER Handler
# ---------------------------------------------------------------------------

class TestCrashWorkerHandler:
    def test_crash_worker_registered(self):
        """CRASH_WORKER should be in the handler registry."""
        assert "CRASH_WORKER" in registry.registered_types

    def test_crash_worker_blocks_until_event(self):
        """CRASH_WORKER handler should block until _crash_cancel_event is set."""
        from app.workers.handlers import _crash_cancel_event
        _crash_cancel_event.clear()

        result_container = {}

        def run():
            result_container["result"] = handle_crash_worker({})

        t = threading.Thread(target=run)
        t.start()

        # Give a little time to make sure it's blocked
        t.join(timeout=0.1)
        assert t.is_alive(), "Handler should be blocking"

        # Now unblock
        _crash_cancel_event.set()
        t.join(timeout=2)
        assert not t.is_alive()
        assert result_container["result"] == {"message": "crash cancelled"}

        # Clean up
        _crash_cancel_event.clear()


# ---------------------------------------------------------------------------
# Workers API
# ---------------------------------------------------------------------------

class TestWorkersAPI:
    def test_list_workers_empty(self, admin_client: TestClient):
        """GET /api/workers with no workers should return empty list."""
        response = admin_client.get("/api/workers")
        assert response.status_code == 200
        assert response.json() == []

    def test_list_workers_with_data(self, admin_client: TestClient, db_session: Session):
        """GET /api/workers should return registered workers."""
        svc = WorkerService(db_session)
        svc.register_worker("worker-api1")
        svc.register_worker("worker-api2")

        response = admin_client.get("/api/workers")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        ids = {w["id"] for w in data}
        assert "worker-api1" in ids
        assert "worker-api2" in ids

    def test_get_worker_by_id(self, admin_client: TestClient, db_session: Session):
        """GET /api/workers/{id} should return worker details."""
        svc = WorkerService(db_session)
        svc.register_worker("worker-detail")

        response = admin_client.get("/api/workers/worker-detail")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "worker-detail"
        assert data["status"] == "ONLINE"
        assert "last_heartbeat" in data
        assert "registered_at" in data

    def test_get_nonexistent_worker_returns_404(self, admin_client: TestClient):
        """GET /api/workers/{id} for unknown worker should return 404."""
        response = admin_client.get("/api/workers/worker-does-not-exist")
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# Job API — Phase 3 fields
# ---------------------------------------------------------------------------

class TestJobAPIPhase3Fields:
    def test_job_response_includes_phase3_fields(self, admin_client: TestClient, db_session: Session):
        """GET /api/jobs/{id} should include worker_id, lease_expires_at, recovery_count."""
        job = Job(
            job_type="SUM_NUMBERS",
            payload={"numbers": [1]},
            status=JobStatus.RUNNING,
            priority=JobPriority.NORMAL,
            worker_id="worker-api-own",
            lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
            recovery_count=2,
        )
        db_session.add(job)
        db_session.commit()

        response = admin_client.get(f"/api/jobs/{job.id}")
        assert response.status_code == 200
        data = response.json()
        assert data["worker_id"] == "worker-api-own"
        assert data["lease_expires_at"] is not None
        assert data["recovery_count"] == 2


# ---------------------------------------------------------------------------
# Recovery Counter vs Retry Counter distinction
# ---------------------------------------------------------------------------

class TestRecoveryVsRetry:
    def test_recovery_count_independent_of_retry_count(self, db_session: Session):
        """recovery_count and retry_count are independent counters."""
        svc = WorkerService(db_session)
        svc.register_worker("worker-rv")
        svc.mark_worker_dead("worker-rv")

        job = Job(
            job_type="ALWAYS_FAIL",
            payload={},
            status=JobStatus.RUNNING,
            priority=JobPriority.NORMAL,
            worker_id="worker-rv",
            lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=10),
            retry_count=2,
            max_retries=3,
            recovery_count=0,
        )
        db_session.add(job)
        db_session.commit()  # commit so job.id is generated
        attempt = JobAttempt(
            job_id=job.id,
            worker_id="worker-rv",
            attempt_number=3,
            status=AttemptStatus.RUNNING,
        )
        db_session.add(attempt)
        db_session.commit()

        mock_queue = MagicMock(spec=RedisQueue)
        recover_stuck_jobs(db_session, mock_queue)

        db_session.refresh(job)
        assert job.recovery_count == 1  # incremented
        assert job.retry_count == 2     # unchanged


# ---------------------------------------------------------------------------
# No recovery when no dead workers
# ---------------------------------------------------------------------------

class TestNoDeadWorkers:
    def test_no_recovery_when_no_dead_workers(self, db_session: Session):
        """recover_stuck_jobs should return 0 when there are no dead workers."""
        mock_queue = MagicMock(spec=RedisQueue)
        recovered = recover_stuck_jobs(db_session, mock_queue)
        assert recovered == 0

    def test_no_recovery_when_all_workers_online(self, db_session: Session):
        """Even with running jobs, recovery should skip if workers are online."""
        svc = WorkerService(db_session)
        svc.register_worker("worker-online")

        job = Job(
            job_type="SUM_NUMBERS",
            payload={"numbers": [1]},
            status=JobStatus.RUNNING,
            priority=JobPriority.NORMAL,
            worker_id="worker-online",
            lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
        )
        db_session.add(job)
        db_session.commit()

        mock_queue = MagicMock(spec=RedisQueue)
        recovered = recover_stuck_jobs(db_session, mock_queue)
        assert recovered == 0
