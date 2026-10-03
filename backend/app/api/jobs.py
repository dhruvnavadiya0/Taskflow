"""Jobs API — Phase 4 (authenticated, authorized, rate-limited).

All job endpoints require JWT authentication.
Normal users can only access their own jobs.
Admin users can access all jobs and the DLQ.
Rate limiting is enforced on job creation.
"""

import json
import logging
import sys

from fastapi import APIRouter, HTTPException, Query, Request, status

from app.core.rate_limit import check_rate_limit, rate_limit_key
from app.core.security import CurrentUser, AdminUser
from app.database.connection import DbSession
from app.database.models import UserRole
from app.queue.redis_queue import job_queue
from app.schemas.job import (
    JobCreate,
    JobCreateResponse,
    JobListResponse,
    JobResponse,
    DeadLetterListResponse,
    JobAttemptResponse,
)
from app.services.job_service import JobService

logger = logging.getLogger("taskflow.api.jobs")

router = APIRouter(prefix="/api/jobs", tags=["jobs"])


def _get_service(db: DbSession) -> JobService:
    return JobService(db=db, queue=job_queue)


@router.post("", response_model=JobCreateResponse, status_code=201)
def create_job(
    data: JobCreate,
    db: DbSession,
    user: CurrentUser,
    request: Request,
) -> JobCreateResponse:
    # Rate limit job creation per user
    try:
        check_rate_limit(
            job_queue.client,
            rate_limit_key("create_job", user.id),
        )
    except HTTPException:
        raise
    except Exception:
        pass  # fail open

    # Input validation: bound payload size
    if data.payload:
        try:
            payload_bytes = len(json.dumps(data.payload).encode("utf-8"))
            if payload_bytes > 65536:  # 64KB
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="Payload too large (max 64KB)",
                )
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Invalid payload format",
            )

    service = _get_service(db)
    try:
        job = service.create_job(data, user_id=user.id)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    return JobCreateResponse.model_validate(job)


@router.get("/dead-letter", response_model=DeadLetterListResponse)
def list_dead_letter_jobs(
    db: DbSession,
    user: CurrentUser,
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=20, ge=1, le=100),
) -> DeadLetterListResponse:
    """Return a paginated list of jobs in the dead-letter queue.
    Admin only.
    """
    if user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required to view dead-letter queue",
        )
    service = _get_service(db)
    jobs, total = service.list_dead_letter_jobs(page=page, limit=limit)
    return DeadLetterListResponse(
        items=[JobResponse.model_validate(j) for j in jobs],
        page=page,
        limit=limit,
        total=total,
    )


@router.get("/{job_id}", response_model=JobResponse)
def get_job(job_id: str, db: DbSession, user: CurrentUser) -> JobResponse:
    # Validate UUID-like format
    if len(job_id) > 36:
        raise HTTPException(status_code=404, detail="Job not found")

    service = _get_service(db)
    job = service.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    # Authorization: user can only view their own jobs unless admin
    if user.role != UserRole.ADMIN and job.user_id is not None and job.user_id != user.id:
        raise HTTPException(status_code=403, detail="Access denied")

    return JobResponse.model_validate(job)


@router.get("/{job_id}/attempts", response_model=list[JobAttemptResponse])
def get_job_attempts(job_id: str, db: DbSession, user: CurrentUser) -> list[JobAttemptResponse]:
    """Return the attempt history for a specific job."""
    service = _get_service(db)
    job = service.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    # Authorization
    if user.role != UserRole.ADMIN and job.user_id is not None and job.user_id != user.id:
        raise HTTPException(status_code=403, detail="Access denied")

    return [JobAttemptResponse.model_validate(a) for a in job.attempts]


@router.get("", response_model=JobListResponse)
def list_jobs(
    db: DbSession,
    user: CurrentUser,
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=20, ge=1, le=100),
) -> JobListResponse:
    service = _get_service(db)
    if user.role == UserRole.ADMIN:
        jobs, total = service.list_jobs(page=page, limit=limit)
    else:
        jobs, total = service.list_jobs(page=page, limit=limit, user_id=user.id)
    return JobListResponse(
        jobs=[JobResponse.model_validate(j) for j in jobs],
        page=page,
        limit=limit,
        total=total,
    )


@router.post("/{job_id}/retry", response_model=JobResponse)
def retry_dead_letter_job(job_id: str, db: DbSession, user: CurrentUser) -> JobResponse:
    """Manually retry a job that is in DEAD_LETTER status.
    Admin only.
    """
    if user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required to retry dead-letter jobs",
        )
    service = _get_service(db)
    try:
        job = service.retry_dead_letter_job(job_id)
    except LookupError:
        raise HTTPException(status_code=404, detail="Job not found")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return JobResponse.model_validate(job)
