"""Worker lifecycle service — registration, heartbeat, and status management.

Keeps worker lifecycle logic separate from job-processing logic.
"""

import logging
import socket
from datetime import datetime, timezone

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database.models import Worker, WorkerStatus

logger = logging.getLogger(__name__)


class WorkerService:
    def __init__(self, db: Session):
        self.db = db

    def register_worker(self, worker_id: str) -> Worker:
        """Register a new worker or re-register an existing one."""
        now = datetime.now(timezone.utc)
        hostname = socket.gethostname()

        worker = self.db.query(Worker).filter(Worker.id == worker_id).first()
        if worker is not None:
            # Re-register (e.g. after restart with same ID — unlikely but safe)
            worker.status = WorkerStatus.ONLINE
            worker.hostname = hostname
            worker.last_heartbeat = now
            worker.current_job_id = None
            worker.updated_at = now
        else:
            worker = Worker(
                id=worker_id,
                hostname=hostname,
                status=WorkerStatus.ONLINE,
                last_heartbeat=now,
                registered_at=now,
                updated_at=now,
            )
            self.db.add(worker)

        self.db.commit()
        logger.info("[%s] Registered on host %s", worker_id, hostname)
        return worker

    def heartbeat(self, worker_id: str) -> None:
        """Update the worker's heartbeat timestamp."""
        now = datetime.now(timezone.utc)
        worker = self.db.query(Worker).filter(Worker.id == worker_id).first()
        if worker is None:
            logger.warning("Heartbeat for unknown worker %s, registering", worker_id)
            self.register_worker(worker_id)
            return

        worker.last_heartbeat = now
        # If the worker was marked SUSPECTED, restore it to ONLINE
        if worker.status == WorkerStatus.SUSPECTED:
            worker.status = WorkerStatus.ONLINE
            logger.info("[%s] Restored from SUSPECTED to ONLINE", worker_id)
        worker.updated_at = now
        self.db.commit()

    def set_current_job(self, worker_id: str, job_id: str | None) -> None:
        """Update which job the worker is currently processing."""
        worker = self.db.query(Worker).filter(Worker.id == worker_id).first()
        if worker is not None:
            worker.current_job_id = job_id
            worker.updated_at = datetime.now(timezone.utc)
            self.db.commit()

    def mark_worker_dead(self, worker_id: str) -> None:
        """Mark a worker as DEAD."""
        worker = self.db.query(Worker).filter(Worker.id == worker_id).first()
        if worker is not None:
            worker.status = WorkerStatus.DEAD
            worker.updated_at = datetime.now(timezone.utc)
            self.db.commit()
            logger.warning("[supervisor] Worker %s marked DEAD", worker_id)

    def mark_worker_suspected(self, worker_id: str) -> None:
        """Mark a worker as SUSPECTED (heartbeat missed but not yet confirmed dead)."""
        worker = self.db.query(Worker).filter(Worker.id == worker_id).first()
        if worker is not None and worker.status == WorkerStatus.ONLINE:
            worker.status = WorkerStatus.SUSPECTED
            worker.updated_at = datetime.now(timezone.utc)
            self.db.commit()
            logger.warning("[supervisor] Worker %s marked SUSPECTED", worker_id)

    def deregister_worker(self, worker_id: str) -> None:
        """Mark a worker as DEAD during graceful shutdown."""
        self.mark_worker_dead(worker_id)

    def get_worker(self, worker_id: str) -> Worker | None:
        """Retrieve a single worker by ID."""
        return self.db.query(Worker).filter(Worker.id == worker_id).first()

    def list_workers(self) -> list[Worker]:
        """Return all registered workers."""
        return self.db.query(Worker).order_by(Worker.registered_at.desc()).all()
