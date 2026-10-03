"""Workload C — Failure-heavy.

Controlled failures and retry loops to measure retry scheduling and DLQ behavior.
"""

from typing import Any
import uuid

WORKLOAD_NAME = "failure"
JOB_TYPE = "FAIL_N_TIMES"
DEFAULT_PRIORITY = "NORMAL"
DEFAULT_FAILURES_NEEDED = 2


def get_payload(job_index: int = 0, failures: int = DEFAULT_FAILURES_NEEDED) -> dict[str, Any]:
    """Return payload for a failure-heavy workload job with unique tracking ID."""
    return {
        "failures_before_success": failures,
        "job_id": f"bench-fail-{uuid.uuid4().hex[:8]}",
    }
