"""Phase 4: Redis-backed rate limiting.

Simple sliding-window rate limiter using Redis INCR + EXPIRE.
Configurable via RATE_LIMIT_REQUESTS and RATE_LIMIT_WINDOW environment variables.
"""

import logging
from typing import Any

from fastapi import HTTPException, Request, status

from app.core.config import settings

logger = logging.getLogger("taskflow.ratelimit")


def check_rate_limit(
    redis_client: Any,
    key: str,
    max_requests: int | None = None,
    window_seconds: int | None = None,
) -> None:
    """Check and enforce rate limit for the given key.

    Uses Redis INCR with TTL-based expiration for a simple fixed-window counter.
    Raises HTTPException 429 if the limit is exceeded.
    """
    if redis_client is None:
        return  # Skip if Redis is unavailable (e.g., in tests)

    if max_requests is None:
        max_requests = settings.RATE_LIMIT_REQUESTS
    if window_seconds is None:
        window_seconds = settings.RATE_LIMIT_WINDOW

    try:
        current = redis_client.incr(key)
        if current == 1:
            redis_client.expire(key, window_seconds)

        if current > max_requests:
            ttl = redis_client.ttl(key)
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded. Try again in {max(ttl, 1)} seconds.",
            )
    except HTTPException:
        raise
    except Exception as exc:
        # Don't block requests if Redis is down — fail open
        logger.warning("Rate limit check failed: %s", exc)


def rate_limit_key(endpoint: str, identifier: str) -> str:
    """Build a rate-limit key from an endpoint name and a client identifier (IP or user ID)."""
    return f"taskflow:ratelimit:{endpoint}:{identifier}"
