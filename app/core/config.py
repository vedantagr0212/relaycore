from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    PROJECT_NAME: str = os.getenv("PROJECT_NAME", "RelayCore Webhook & Event Delivery Engine")
    VERSION: str = os.getenv("VERSION", "1.0.0")
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://relaycore:relaycore_pass@localhost:5432/relaycore_db",
    )
    SQLITE_FALLBACK_PATH: str = os.getenv("SQLITE_FALLBACK_PATH", "relaycore_runtime.db")
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # Worker & Polling settings
    WORKER_BATCH_SIZE: int = int(os.getenv("WORKER_BATCH_SIZE", "25"))
    WORKER_POLL_INTERVAL_SEC: float = float(os.getenv("WORKER_POLL_INTERVAL_SEC", "0.5"))
    WORKER_HTTP_TIMEOUT_SEC: float = float(os.getenv("WORKER_HTTP_TIMEOUT_SEC", "5.0"))
    WORKER_CONCURRENCY: int = int(os.getenv("WORKER_CONCURRENCY", "4"))

    # Retry & Exponential Backoff settings
    DEFAULT_MAX_RETRIES: int = int(os.getenv("DEFAULT_MAX_RETRIES", "5"))
    BACKOFF_BASE_SECONDS: float = float(os.getenv("BACKOFF_BASE_SECONDS", "2.0"))
    BACKOFF_MAX_SECONDS: float = float(os.getenv("BACKOFF_MAX_SECONDS", "300.0"))
    BACKOFF_JITTER_RATIO: float = float(os.getenv("BACKOFF_JITTER_RATIO", "0.25"))

    # Circuit Breaker settings
    CB_FAILURE_THRESHOLD: int = int(os.getenv("CB_FAILURE_THRESHOLD", "5"))
    CB_RECOVERY_TIMEOUT_SEC: float = float(os.getenv("CB_RECOVERY_TIMEOUT_SEC", "30.0"))
    CB_HALF_OPEN_MAX_CALLS: int = int(os.getenv("CB_HALF_OPEN_MAX_CALLS", "2"))

    # Rate Limiter settings (Token Bucket per tenant)
    RL_DEFAULT_CAPACITY: int = int(os.getenv("RL_DEFAULT_CAPACITY", "20"))
    RL_DEFAULT_REFILL_RATE_PER_SEC: float = float(os.getenv("RL_DEFAULT_REFILL_RATE_PER_SEC", "10.0"))

    # Webhook Security
    SIGNATURE_TOLERANCE_SECONDS: int = int(os.getenv("SIGNATURE_TOLERANCE_SECONDS", "300"))


settings = Settings()
