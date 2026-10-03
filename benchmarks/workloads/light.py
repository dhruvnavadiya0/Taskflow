"""Workload A — Light CPU.

Deterministic lightweight calculation to measure queue, API, and worker overhead.
"""

from typing import Any

WORKLOAD_NAME = "light"
JOB_TYPE = "SUM_NUMBERS"
DEFAULT_PRIORITY = "NORMAL"
DEFAULT_PAYLOAD = {"numbers": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}


def get_payload(job_index: int = 0) -> dict[str, Any]:
    """Return payload for a light workload job."""
    return {"numbers": [job_index + i for i in range(10)]}
