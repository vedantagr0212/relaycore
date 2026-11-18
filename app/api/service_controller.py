from dataclasses import asdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app.mocks.chaos_receiver import ChaosBehaviorMode, StatefulChaosTargetServer
from app.repositories.event_repository import TransactionalEventRepository
from app.schemas.event_schemas import EventIngestRequest, EventStatusResponse
from app.services.circuit_breaker import EndpointCircuitBreaker
from app.services.dispatch_strategies import DeliveryStrategyRegistry
from app.services.rate_limiter import TokenBucketRateLimiter
from app.workers.dispatch_worker import RelayCoreDispatchWorker


class RelayCoreApplicationController:
    def __init__(
        self,
        repository: TransactionalEventRepository,
        rate_limiter: TokenBucketRateLimiter,
        circuit_breaker: EndpointCircuitBreaker,
        chaos_server: StatefulChaosTargetServer,
    ) -> None:
        self.repo = repository
        self.rate_limiter = rate_limiter
        self.circuit_breaker = circuit_breaker
        self.chaos_server = chaos_server
        self.strategy_registry = DeliveryStrategyRegistry(
            http_transport=self.chaos_server.handle_request
        )
        self.default_worker = RelayCoreDispatchWorker(
            worker_id="relaycore-worker-primary",
            repository=self.repo,
            rate_limiter=self.rate_limiter,
            circuit_breaker=self.circuit_breaker,
            strategy_registry=self.strategy_registry,
        )

    def ingest_event_endpoint(self, payload: Dict[str, Any] | EventIngestRequest) -> Tuple[int, EventStatusResponse]:
        req = payload if isinstance(payload, EventIngestRequest) else EventIngestRequest(**payload)
        resp = self.repo.ingest_event(req)
        http_status = 200 if resp.is_idempotent_replay else 202
        return http_status, resp

    def get_event_endpoint(self, event_id: str) -> Tuple[int, Dict[str, Any]]:
        try:
            ev = self.repo.get_event_by_id(event_id)
            return 200, ev.model_dump(mode="json")
        except KeyError:
            return 404, {"error": "event_not_found", "event_id": event_id}

    def list_dlq_endpoint(self, tenant_id: Optional[str] = None) -> Tuple[int, Dict[str, Any]]:
        dlq_events = self.repo.list_dead_letter_events(tenant_id=tenant_id)
        return 200, {
            "count": len(dlq_events),
            "tenant_filter": tenant_id,
            "items": [e.model_dump(mode="json") for e in dlq_events],
        }

    def replay_single_dlq_endpoint(self, event_id: str) -> Tuple[int, Dict[str, Any]]:
        try:
            replayed = self.repo.replay_dead_letter_event(event_id)
            return 200, replayed.model_dump(mode="json")
        except KeyError:
            return 404, {"error": "event_not_found", "event_id": event_id}
        except ValueError as exc:
            return 409, {"error": "invalid_dlq_replay_transition", "detail": str(exc)}

    def replay_bulk_dlq_endpoint(self, tenant_id: Optional[str] = None) -> Tuple[int, Dict[str, Any]]:
        dlq_events = self.repo.list_dead_letter_events(tenant_id=tenant_id)
        replayed_ids: List[str] = []
        for ev in dlq_events:
            self.repo.replay_dead_letter_event(ev.event_id)
            replayed_ids.append(ev.event_id)
        return 200, {
            "replayed_count": len(replayed_ids),
            "tenant_filter": tenant_id,
            "replayed_event_ids": replayed_ids,
        }

    def run_worker_tick_endpoint(
        self,
        batch_size: int = 25,
        now_override: Optional[datetime] = None,
    ) -> Tuple[int, Dict[str, Any]]:
        summary = self.default_worker.process_next_batch(
            batch_size=batch_size,
            now_override=now_override,
        )
        return 200, asdict(summary)

    def configure_chaos_route_endpoint(self, route_path: str, mode: str) -> Tuple[int, Dict[str, Any]]:
        chaos_mode = ChaosBehaviorMode(mode)
        self.chaos_server.set_route_mode(route_path, chaos_mode)
        return 200, {
            "route_path": route_path,
            "updated_mode": chaos_mode.value,
            "chaos_audit": self.chaos_server.get_audit_summary(),
        }

    def get_observability_dashboard(self) -> Tuple[int, Dict[str, Any]]:
        circuits = [
            self.circuit_breaker.get_snapshot(target)
            for target in sorted(self.default_worker.known_circuit_targets)
        ]
        return 200, {
            "metrics": self.repo.get_metrics_summary(),
            "circuit_breakers": circuits,
            "chaos_receiver_audit": self.chaos_server.get_audit_summary(),
            "smtp_spooled_emails_count": len(self.strategy_registry.email_strategy.spooled_messages),
        }
