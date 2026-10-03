from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
import logging
import time

logger = logging.getLogger("taskflow.main")

from app.api.auth import router as auth_router
from app.api.jobs import router as jobs_router
from app.api.workers import router as workers_router
from app.core.metrics import (
    collect_metrics,
    active_requests_gauge,
    http_requests_total,
    http_request_duration_seconds,
)
from app.database.connection import Base, engine, DbSession
from app.queue.redis_queue import job_queue


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Create all tables on startup
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="TaskFlow",
    description="Distributed background-job processing platform",
    version="4.0.0",
    lifespan=lifespan,
)

app.include_router(auth_router)
app.include_router(jobs_router)
app.include_router(workers_router)

@app.middleware("http")
async def prometheus_metrics_middleware(request: Request, call_next):
    """Middleware to track HTTP request metrics for Prometheus."""
    path = request.url.path
    # Do not track metrics endpoint itself
    if path == "/metrics":
        return await call_next(request)

    method = request.method
    active_requests_gauge.labels(method=method, endpoint=path).inc()
    
    start_time = time.time()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        duration = time.time() - start_time
        active_requests_gauge.labels(method=method, endpoint=path).dec()
        http_requests_total.labels(method=method, endpoint=path, status=str(status_code)).inc()
        http_request_duration_seconds.labels(method=method, endpoint=path).observe(duration)

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Phase 4: sanitize unhandled exceptions to prevent leaking internal details."""
    logger.error("Unhandled exception: %s", exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )



@app.get("/")
def root() -> dict[str, str]:
    return {"message": "TaskFlow API", "docs": "/docs"}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "healthy"}


@app.get("/metrics")
def metrics(db: DbSession) -> Response:
    """Prometheus metrics endpoint for scraping TaskFlow system and job metrics."""
    collect_metrics(db, job_queue)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
