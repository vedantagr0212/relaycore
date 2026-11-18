import concurrent.futures
import logging
import signal
import sqlite3
import time

from app.core.config import settings
from app.mocks.chaos_receiver import StatefulChaosTargetServer
from app.repositories.event_repository import TransactionalEventRepository
from app.services.circuit_breaker import EndpointCircuitBreaker
from app.services.dispatch_strategies import DeliveryStrategyRegistry
from app.services.rate_limiter import TokenBucketRateLimiter
from app.workers.dispatch_worker import RelayCoreDispatchWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("relaycore.worker_pool")

_running = True


def _handle_shutdown(signum: int, frame: object) -> None:
    global _running
    logger.info("Received signal %s; draining in-flight worker batches...", signum)
    _running = False


def run_daemon_pool() -> None:
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    conn = sqlite3.connect(settings.SQLITE_FALLBACK_PATH, check_same_thread=False)
    repo = TransactionalEventRepository(conn)
    rate_limiter = TokenBucketRateLimiter()
    circuit_breaker = EndpointCircuitBreaker()
    chaos_server = StatefulChaosTargetServer()
    registry = DeliveryStrategyRegistry(http_transport=chaos_server.handle_request)

    workers = [
        RelayCoreDispatchWorker(
            worker_id=f"relaycore-worker-{idx + 1}",
            repository=repo,
            rate_limiter=rate_limiter,
            circuit_breaker=circuit_breaker,
            strategy_registry=registry,
        )
        for idx in range(settings.WORKER_CONCURRENCY)
    ]

    logger.info(
        "Started RelayCore Worker Pool with %d concurrent workers (batch_size=%d, poll_interval=%.2fs)",
        len(workers),
        settings.WORKER_BATCH_SIZE,
        settings.WORKER_POLL_INTERVAL_SEC,
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(workers)) as pool:
        while _running:
            futures = [pool.submit(w.process_next_batch, settings.WORKER_BATCH_SIZE) for w in workers]
            total_claimed = 0
            for fut in concurrent.futures.as_completed(futures):
                summary = fut.result()
                total_claimed += summary.claimed_count
                if summary.claimed_count > 0:
                    logger.info(
                        "Worker=%s claimed=%d delivered=%d retried=%d dlq=%d",
                        summary.worker_id,
                        summary.claimed_count,
                        summary.delivered_count,
                        summary.retry_scheduled_count,
                        summary.dead_lettered_count,
                    )
            if total_claimed == 0:
                time.sleep(settings.WORKER_POLL_INTERVAL_SEC)


if __name__ == "__main__":
    run_daemon_pool()
