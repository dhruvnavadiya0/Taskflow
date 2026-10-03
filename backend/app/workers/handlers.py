import threading
import time
from typing import Any, Callable

# Type for a job handler function: takes a payload dict, returns a result dict
JobHandler = Callable[[dict[str, Any]], dict[str, Any]]


class JobRegistry:
    """Registry that maps job_type strings to handler functions."""

    def __init__(self) -> None:
        self._handlers: dict[str, JobHandler] = {}

    def register(self, job_type: str, handler: JobHandler) -> None:
        self._handlers[job_type] = handler

    def get_handler(self, job_type: str) -> JobHandler:
        handler = self._handlers.get(job_type)
        if handler is None:
            raise ValueError(f"Unknown job type: {job_type}")
        return handler

    @property
    def registered_types(self) -> list[str]:
        return list(self._handlers.keys())


# --- Handler implementations ---

def handle_sum_numbers(payload: dict[str, Any]) -> dict[str, Any]:
    numbers = payload.get("numbers")
    if not isinstance(numbers, list):
        raise ValueError("'numbers' must be a list")
    if not all(isinstance(n, (int, float)) for n in numbers):
        raise ValueError("'numbers' must contain only numeric values")
    return {"sum": sum(numbers)}


def handle_text_length(payload: dict[str, Any]) -> dict[str, Any]:
    text = payload.get("text")
    if not isinstance(text, str):
        raise ValueError("'text' must be a string")
    return {"length": len(text)}


# --- Phase 2: test-only handlers ---

def handle_always_fail(payload: dict[str, Any]) -> dict[str, Any]:
    """Always raises an exception — used for retry / DLQ testing."""
    raise RuntimeError("ALWAYS_FAIL: intentional failure for testing")


# In-process counter for FAIL_N_TIMES — keyed by job_id.
# This is intentionally a simple dict; it resets when the worker process restarts,
# which is acceptable for testing.
_fail_counter: dict[str, int] = {}


def handle_fail_n_times(payload: dict[str, Any]) -> dict[str, Any]:
    """Fails a configurable number of times, then succeeds.

    Payload:
        failures_before_success (int): how many times to fail before returning success.
        job_id (str): identifier used to track attempt count in-process.

    Example payload: {"failures_before_success": 2, "job_id": "abc123"}
    """
    failures_needed = payload.get("failures_before_success", 1)
    job_id = payload.get("job_id", "unknown")

    count = _fail_counter.get(job_id, 0)
    _fail_counter[job_id] = count + 1

    if count < failures_needed:
        raise RuntimeError(
            f"FAIL_N_TIMES: intentional failure {count + 1}/{failures_needed}"
        )

    # Clean up the counter and succeed
    _fail_counter.pop(job_id, None)
    return {"message": "succeeded after failures", "total_attempts": count + 1}


# --- Phase 3: test-only handlers ---

def handle_sleep(payload: dict[str, Any]) -> dict[str, Any]:
    """Sleeps for a configurable duration — used for testing long-running jobs.

    Payload:
        seconds (int|float): how long to sleep.

    Example: {"seconds": 20}
    """
    seconds = payload.get("seconds", 1)
    if not isinstance(seconds, (int, float)) or seconds < 0:
        raise ValueError("'seconds' must be a non-negative number")
    time.sleep(seconds)
    return {"slept": seconds}


# Thread-level event that tests can set to unblock a CRASH_WORKER handler.
# In production this is never set; the handler blocks until the thread is killed.
_crash_cancel_event = threading.Event()


def handle_crash_worker(payload: dict[str, Any]) -> dict[str, Any]:
    """Simulates a worker crash by blocking indefinitely.

    The idea: the worker thread executing this handler will hang, which means
    the heartbeat thread (a separate daemon thread) is what keeps the worker
    "alive".  When tests stop the heartbeat, the recovery supervisor will
    detect the stale worker and recover the job.

    In test scenarios, set ``_crash_cancel_event`` to unblock.
    """
    _crash_cancel_event.wait()  # blocks forever unless event is set
    return {"message": "crash cancelled"}


# --- Phase 4: benchmark handlers ---

def handle_cpu_test(payload: dict[str, Any]) -> dict[str, Any]:
    """Controlled CPU-intensive job handler for benchmarking worker scaling.

    Payload:
        iterations (int): number of deterministic computation iterations (default 100,000).

    Example: {"iterations": 500000}
    """
    iterations = payload.get("iterations", 100_000)
    if not isinstance(iterations, int) or iterations < 0:
        raise ValueError("'iterations' must be a non-negative integer")

    # Deterministic integer arithmetic to generate predictable CPU load
    total = 0
    for i in range(iterations):
        total = (total + i * 31) % 1_000_000_007
    return {"iterations": iterations, "checksum": total}


# --- Build the global registry ---

registry = JobRegistry()
registry.register("SUM_NUMBERS", handle_sum_numbers)
registry.register("TEXT_LENGTH", handle_text_length)
registry.register("ALWAYS_FAIL", handle_always_fail)
registry.register("FAIL_N_TIMES", handle_fail_n_times)
registry.register("SLEEP", handle_sleep)
registry.register("CRASH_WORKER", handle_crash_worker)
registry.register("CPU_TEST", handle_cpu_test)
