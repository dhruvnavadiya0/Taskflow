from fastapi import APIRouter, HTTPException, Query

from app.core.security import AdminUser
from app.database.connection import DbSession
from app.schemas.job import WorkerResponse
from app.services.worker_service import WorkerService

router = APIRouter(prefix="/api/workers", tags=["workers"])


@router.get("", response_model=list[WorkerResponse])
def list_workers(db: DbSession, admin: AdminUser) -> list[WorkerResponse]:
    """Return all registered workers."""
    svc = WorkerService(db)
    workers = svc.list_workers()
    return [WorkerResponse.model_validate(w) for w in workers]


@router.get("/{worker_id}", response_model=WorkerResponse)
def get_worker(worker_id: str, db: DbSession, admin: AdminUser) -> WorkerResponse:
    """Return details for a specific worker."""
    svc = WorkerService(db)
    worker = svc.get_worker(worker_id)
    if worker is None:
        raise HTTPException(status_code=404, detail="Worker not found")
    return WorkerResponse.model_validate(worker)
