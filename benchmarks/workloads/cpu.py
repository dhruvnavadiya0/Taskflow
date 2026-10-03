"""Workload B — CPU-heavy.

Controlled, deterministic CPU-intensive calculations to measure worker CPU scaling.
"""

from typing import Any

WORKLOAD_NAME = "cpu"
JOB_TYPE = "CPU_TEST"
DEFAULT_PRIORITY = "NORMAL"
DEFAULT_ITERATIONS = 200_000
DEFAULT_PAYLOAD = {"iterations": DEFAULT_ITERATIONS}


def get_payload(job_index: int = 0, iterations: int = DEFAULT_ITERATIONS) -> dict[str, Any]:
    """Return payload for a CPU workload job."""
    return {"iterations": iterations}
