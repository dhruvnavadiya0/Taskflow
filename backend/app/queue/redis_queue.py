import json
import time

import redis

from app.core.config import settings


class RedisQueue:
    """Redis-backed job queue with priority support.

    Phase 2 introduces three priority queues (HIGH, NORMAL, LOW) and a
    dead-letter queue.  Workers use ``dequeue_priority`` to blocking-pop
    from all three queues in priority order.

    The legacy single-queue ``enqueue`` / ``dequeue`` methods are kept for
    backward compatibility but internal callers now use the priority-aware
    variants.
    """

    PRIORITY_QUEUES = (
        settings.QUEUE_HIGH,
        settings.QUEUE_NORMAL,
        settings.QUEUE_LOW,
    )

    def __init__(self, url: str = settings.REDIS_URL, queue_name: str = settings.QUEUE_NAME):
        self.client = redis.from_url(url, decode_responses=True)
        self.queue_name = queue_name

    # ------------------------------------------------------------------
    # Legacy helpers (Phase 1 compat)
    # ------------------------------------------------------------------

    def enqueue(self, job_id: str) -> None:
        """Push a job ID to the end of the default queue."""
        self.client.rpush(self.queue_name, job_id)

    def dequeue(self, timeout: int = settings.QUEUE_TIMEOUT) -> str | None:
        """Blocking pop from the front of the default queue."""
        result = self.client.blpop(self.queue_name, timeout=timeout)
        if result:
            return result[1]
        return None

    def length(self) -> int:
        """Return the current number of items in the default queue."""
        return self.client.llen(self.queue_name)

    # ------------------------------------------------------------------
    # Phase 2: priority queue operations
    # ------------------------------------------------------------------

    def enqueue_priority(self, job_id: str, priority: str) -> None:
        """Push a job ID to the appropriate priority queue.

        ``priority`` should be one of ``"HIGH"``, ``"NORMAL"``, ``"LOW"``.
        """
        queue_map = {
            "HIGH": settings.QUEUE_HIGH,
            "NORMAL": settings.QUEUE_NORMAL,
            "LOW": settings.QUEUE_LOW,
        }
        queue = queue_map.get(priority, settings.QUEUE_NORMAL)
        self.client.rpush(queue, job_id)

    def dequeue_priority(self, timeout: int = settings.QUEUE_TIMEOUT) -> str | None:
        """Blocking pop across all priority queues, preferring higher priority.

        Uses Redis ``BLPOP`` with multiple keys.  Redis ``BLPOP`` checks the
        keys in the order given and returns the first non-empty list, which
        gives us strict priority ordering: HIGH → NORMAL → LOW.
        """
        result = self.client.blpop(self.PRIORITY_QUEUES, timeout=timeout)
        if result:
            # blpop returns (queue_name, value)
            return result[1]
        return None

    def priority_lengths(self) -> dict[str, int]:
        """Return the length of each priority queue."""
        return {
            "high": self.client.llen(settings.QUEUE_HIGH),
            "normal": self.client.llen(settings.QUEUE_NORMAL),
            "low": self.client.llen(settings.QUEUE_LOW),
        }

    # ------------------------------------------------------------------
    # Phase 2: dead-letter queue operations
    # ------------------------------------------------------------------

    def move_to_dead_letter(self, job_id: str) -> None:
        """Push a job ID into the dead-letter queue."""
        self.client.rpush(settings.QUEUE_DEAD, job_id)

    def dead_letter_length(self) -> int:
        """Return the number of items in the dead-letter queue."""
        return self.client.llen(settings.QUEUE_DEAD)

    # ------------------------------------------------------------------
    # Phase 2: delayed retry via Redis sorted set
    # ------------------------------------------------------------------

    RETRY_ZSET = "taskflow:retry:scheduled"

    def schedule_retry(self, job_id: str, execute_at: float) -> None:
        """Schedule a job for retry at a future timestamp.

        Uses a Redis sorted set where the score is the Unix timestamp when
        the job should become eligible for retry.
        """
        self.client.zadd(self.RETRY_ZSET, {job_id: execute_at})

    def get_due_retries(self) -> list[str]:
        """Pop all retry entries whose scheduled time has passed.

        Returns a (possibly empty) list of job IDs.
        """
        now = time.time()
        # Atomically get and remove entries with score <= now
        pipe = self.client.pipeline()
        pipe.zrangebyscore(self.RETRY_ZSET, "-inf", now)
        pipe.zremrangebyscore(self.RETRY_ZSET, "-inf", now)
        results = pipe.execute()
        return results[0]  # list of job_ids

    # ------------------------------------------------------------------
    # General
    # ------------------------------------------------------------------

    def ping(self) -> bool:
        """Check if Redis is reachable."""
        try:
            return self.client.ping()
        except redis.ConnectionError:
            return False


# Shared queue instance for the API process
job_queue = RedisQueue()
