from datetime import datetime

from pydantic import BaseModel, Field

from app.database.models import JobPriority, JobStatus, WorkerStatus


class JobCreate(BaseModel):
    job_type: str = Field(..., min_length=1, max_length=100)
    priority: JobPriority = JobPriority.NORMAL
    payload: dict = Field(default_factory=dict)
    # Phase 2: optional retry configuration
    max_retries: int = Field(default=3, ge=0, le=10)


class JobResponse(BaseModel):
    id: str
    job_type: str
    status: JobStatus
    priority: JobPriority
    payload: dict | None = None
    result: dict | None = None
    error_message: str | None = None
    # Phase 2 fields
    retry_count: int = 0
    max_retries: int = 3
    next_retry_at: datetime | None = None
    last_error: str | None = None
    # Phase 3 fields
    worker_id: str | None = None
    lease_expires_at: datetime | None = None
    recovery_count: int = 0

    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None

    model_config = {"from_attributes": True}


class JobCreateResponse(BaseModel):
    """Minimal response after job creation."""
    id: str
    job_type: str
    status: JobStatus
    priority: JobPriority

    model_config = {"from_attributes": True}


class JobListResponse(BaseModel):
    jobs: list[JobResponse]
    page: int
    limit: int
    total: int


# Phase 2: attempt history
class JobAttemptResponse(BaseModel):
    id: str
    job_id: str
    worker_id: str
    attempt_number: int
    started_at: datetime
    finished_at: datetime | None = None
    status: str
    error_message: str | None = None

    model_config = {"from_attributes": True}


# Phase 2: dead-letter list response
class DeadLetterListResponse(BaseModel):
    items: list[JobResponse]
    page: int
    limit: int
    total: int


# Phase 3: worker schemas
class WorkerResponse(BaseModel):
    id: str
    hostname: str
    status: WorkerStatus
    last_heartbeat: datetime
    current_job_id: str | None = None
    registered_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
