from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql+psycopg://taskflow:taskflow@localhost:5432/taskflow"
    REDIS_URL: str = "redis://localhost:6379/0"

    # Legacy single-queue name (kept for backward compat)
    QUEUE_NAME: str = "taskflow:jobs"

    # Phase 2: priority queues
    QUEUE_HIGH: str = "taskflow:queue:high"
    QUEUE_NORMAL: str = "taskflow:queue:normal"
    QUEUE_LOW: str = "taskflow:queue:low"
    QUEUE_DEAD: str = "taskflow:queue:dead"

    # Timeout in seconds for BLPOP (0 = block indefinitely)
    QUEUE_TIMEOUT: int = 5

    # Phase 2: retry configuration
    RETRY_BASE_DELAY: int = 2  # seconds
    RETRY_MAX_LIMIT: int = 10  # upper bound clients can request

    # Phase 3: worker heartbeat & lease configuration
    WORKER_HEARTBEAT_INTERVAL: int = 5   # seconds between heartbeats
    WORKER_HEARTBEAT_TIMEOUT: int = 15   # seconds before a worker is suspected stale
    JOB_LEASE_DURATION: int = 30         # seconds a worker holds a lease on a job
    JOB_LEASE_RENEW_INTERVAL: int = 10   # seconds between lease renewals
    WORKER_FAILURE_GRACE_PERIOD: int = 5 # extra seconds before SUSPECTED → DEAD
    RECOVERY_INTERVAL: int = 5           # seconds between recovery sweeps

    # Phase 4: JWT authentication
    JWT_SECRET: str = "taskflow-dev-secret-change-in-production"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_MINUTES: int = 60

    # Phase 4: rate limiting (Redis-backed)
    RATE_LIMIT_REQUESTS: int = 60
    RATE_LIMIT_WINDOW: int = 60  # seconds

    # Phase 4: input validation
    MAX_PAYLOAD_SIZE: int = 65536  # bytes (64KB)

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
