from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from app.core.config import settings
from app.repositories.event_repository import TransactionalEventRepository
from app.schemas.event_schemas import EventStatus, EventStatusResponse
from app.services.circuit_breaker import (
    CircuitBreakerOpenError,
    EndpointCircuitBreaker,
    extract_circuit_target_key,
)
from app.services.dispatch_strategies import DeliveryStrategyRegistry
from app.services.rate_limiter import TokenBucketRateLimiter


@dataclass
class WorkerBatchSummary:
    worker_id: str
    claimed_count: int
    delivered_count: int
    retry_scheduled_count: int
    dead_lettered_count: int
    deferred_by_rate_limiter: int
    deferred_by_circuit_breaker: int
    processed_event_ids: List[str]


class RelayCoreDispatchWorker:
    def __init__(
        self,
        worker_id: str,
        repository: TransactionalEventRepository,
        rate_limiter: TokenBucketRateLimiter,
        circuit_breaker: EndpointCircuitBreaker,
        strategy_registry: DeliveryStrategyRegistry,
    ) -> None:
        self.worker_id = worker_id
        self.repo = repository
        self.rate_limiter = rate_limiter
        self.circuit_breaker = circuit_breaker
        self.strategy_registry = strategy_registry
        self._known_circuit_targets: set[str] = set()

    @property
    def known_circuit_targets(self) -> set[str]:
        return set(self._known_circuit_targets)

    def process_next_batch(
        self,
        batch_size: int = settings.WORKER_BATCH_SIZE,
        now_override: Optional[datetime] = None,
    ) -> WorkerBatchSummary:
        claimed_rows = self.repo.claim_due_events_batch(
            worker_id=self.worker_id,
            batch_size=batch_size,
            now_override=now_override,
        )

        delivered = 0
        retried = 0
        dlq = 0
        deferred_rl = 0
        deferred_cb = 0
        processed_ids: List[str] = []

        for row in claimed_rows:
            event_id = row["event_id"]
            tenant_id = row["tenant_id"]
            channel = row["channel"]
            destination = row["destination"]
            processed_ids.append(event_id)

            cap = int(row.get("rate_limit_capacity") or settings.RL_DEFAULT_CAPACITY)
            refill = float(row.get("rate_limit_refill_per_sec") or settings.RL_DEFAULT_REFILL_RATE_PER_SEC)
            rl_decision = self.rate_limiter.acquire(
                tenant_id=tenant_id,
                capacity=cap,
                refill_rate_per_sec=refill,
            )
            if not rl_decision.allowed:
                self.repo.defer_event_without_penalty(
                    event_id=event_id,
                    worker_id=self.worker_id,
                    delay_seconds=rl_decision.retry_after_seconds,
                    reason_category="TENANT_RATE_LIMITED",
                    reason_detail=f"Tenant '{tenant_id}' token bucket exhausted; retry after {rl_decision.retry_after_seconds}s",
                    now_override=now_override,
                )
                deferred_rl += 1
                retried += 1
                continue

            target_key = extract_circuit_target_key(channel, destination)
            self._known_circuit_targets.add(target_key)
            try:
                self.circuit_breaker.before_call(target_key)
            except CircuitBreakerOpenError as cb_err:
                self.repo.defer_event_without_penalty(
                    event_id=event_id,
                    worker_id=self.worker_id,
                    delay_seconds=cb_err.retry_after_seconds,
                    reason_category=f"CIRCUIT_BREAKER_{cb_err.state.value}",
                    reason_detail=str(cb_err),
                    now_override=now_override,
                )
                deferred_cb += 1
                retried += 1
                continue

            strategy = self.strategy_registry.get_strategy(channel)
            result = strategy.dispatch(row)

            if result.success:
                self.circuit_breaker.record_success(target_key)
            elif result.status_code is None or result.status_code >= 500 or result.status_code == 408:
                self.circuit_breaker.record_failure(
                    target_key,
                    error_reason=f"{result.error_category} (HTTP {result.status_code})",
                )

            updated_event: EventStatusResponse = self.repo.record_delivery_outcome(
                event_id=event_id,
                worker_id=self.worker_id,
                success=result.success,
                status_code=result.status_code,
                latency_ms=result.latency_ms,
                error_category=result.error_category,
                response_excerpt=result.response_excerpt,
                retry_after_header=result.retry_after_header,
                force_dead_letter=(not result.is_transient_failure and not result.success),
                now_override=now_override,
            )

            if updated_event.status == EventStatus.DELIVERED:
                delivered += 1
            elif updated_event.status == EventStatus.DEAD_LETTER:
                dlq += 1
            else:
                retried += 1

        return WorkerBatchSummary(
            worker_id=self.worker_id,
            claimed_count=len(claimed_rows),
            delivered_count=delivered,
            retry_scheduled_count=retried,
            dead_lettered_count=dlq,
            deferred_by_rate_limiter=deferred_rl,
            deferred_by_circuit_breaker=deferred_cb,
            processed_event_ids=processed_ids,
        )
