from dataclasses import dataclass
from enum import Enum
import json
import random
import socket
import threading
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

from app.core.security import WebhookSignatureError, verify_webhook_signature


class ChaosBehaviorMode(str, Enum):
    HEALTHY_200 = "HEALTHY_200"
    FAIL_FIRST_N_THEN_200 = "FAIL_FIRST_N_THEN_200"
    RATE_LIMITED_429 = "RATE_LIMITED_429"
    ALWAYS_503_OUTAGE = "ALWAYS_503_OUTAGE"
    SOCKET_TIMEOUT = "SOCKET_TIMEOUT"
    FATAL_422_SCHEMA_ERROR = "FATAL_422_SCHEMA_ERROR"
    PROBABILISTIC_CHAOS = "PROBABILISTIC_CHAOS"


@dataclass
class RouteChaosConfig:
    mode: ChaosBehaviorMode = ChaosBehaviorMode.HEALTHY_200
    fail_count_before_success: int = 2
    retry_after_seconds: int = 4
    verify_hmac_signature: bool = True
    signing_secret: str = "whsec_relaycore_default_secret_key_2026"
    prob_503: float = 0.30
    prob_429: float = 0.20


@dataclass
class ReceivedWebhookRecord:
    route_path: str
    event_id: str
    tenant_id: str
    idempotency_key: str
    attempt_header: int
    signature_verified: bool
    payload: Dict[str, Any]
    status_returned: int


class StatefulChaosTargetServer:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._route_configs: Dict[str, RouteChaosConfig] = {
            "/mock-target/healthy": RouteChaosConfig(mode=ChaosBehaviorMode.HEALTHY_200),
            "/mock-target/flaky": RouteChaosConfig(
                mode=ChaosBehaviorMode.FAIL_FIRST_N_THEN_200,
                fail_count_before_success=2,
            ),
            "/mock-target/rate-limited": RouteChaosConfig(
                mode=ChaosBehaviorMode.RATE_LIMITED_429,
                fail_count_before_success=1,
                retry_after_seconds=3,
            ),
            "/mock-target/outage": RouteChaosConfig(mode=ChaosBehaviorMode.ALWAYS_503_OUTAGE),
            "/mock-target/timeout": RouteChaosConfig(mode=ChaosBehaviorMode.SOCKET_TIMEOUT),
            "/mock-target/fatal-422": RouteChaosConfig(mode=ChaosBehaviorMode.FATAL_422_SCHEMA_ERROR),
            "/mock-target/slack": RouteChaosConfig(
                mode=ChaosBehaviorMode.HEALTHY_200,
                verify_hmac_signature=False,
            ),
        }
        self._attempts_by_event: Dict[str, int] = {}
        self._delivered_by_idempotency_key: Dict[str, int] = {}
        self._duplicate_deliveries_detected: int = 0
        self._ledger: List[ReceivedWebhookRecord] = []

    def configure_route(self, route_path: str, config: RouteChaosConfig) -> None:
        with self._lock:
            self._route_configs[route_path] = config

    def set_route_mode(self, route_path: str, mode: ChaosBehaviorMode) -> None:
        with self._lock:
            cfg = self._route_configs.setdefault(route_path, RouteChaosConfig())
            cfg.mode = mode

    def handle_request(
        self,
        url: str,
        body_bytes: bytes,
        headers: Dict[str, str],
        timeout_sec: float = 5.0,
    ) -> Tuple[int, str, Dict[str, str]]:
        parsed = urlparse(url)
        route_path = parsed.path or "/mock-target/healthy"

        norm_headers = {k.lower(): v for k, v in headers.items()}
        event_id = norm_headers.get("x-relaycore-event-id", "slack_or_anonymous")
        tenant_id = norm_headers.get("x-relaycore-tenant-id", "unknown_tenant")
        idem_key = norm_headers.get("x-relaycore-idempotency-key", f"anon_{event_id}")
        attempt_hdr = int(norm_headers.get("x-relaycore-delivery-attempt", "1"))
        sig_header = norm_headers.get("x-relaycore-signature", "")

        with self._lock:
            cfg = self._route_configs.get(route_path, RouteChaosConfig())
            seen_failures = self._attempts_by_event.get(event_id, 0)
            self._attempts_by_event[event_id] = seen_failures + 1

        sig_verified = False
        if cfg.verify_hmac_signature:
            try:
                sig_verified = verify_webhook_signature(
                    payload_body=body_bytes,
                    signature_header=sig_header,
                    secret_key=cfg.signing_secret,
                )
            except WebhookSignatureError as exc:
                return (
                    401,
                    json.dumps({"error": "invalid_webhook_signature", "detail": str(exc)}),
                    {"content-type": "application/json"},
                )

        mode = cfg.mode
        if mode == ChaosBehaviorMode.SOCKET_TIMEOUT:
            raise socket.timeout(f"Simulated downstream socket hang on {route_path} (> {timeout_sec}s)")

        if mode == ChaosBehaviorMode.ALWAYS_503_OUTAGE:
            return (
                503,
                json.dumps({"error": "upstream_database_outage", "route": route_path}),
                {"content-type": "application/json", "retry-after": str(cfg.retry_after_seconds)},
            )

        if mode == ChaosBehaviorMode.FATAL_422_SCHEMA_ERROR:
            return (
                422,
                json.dumps({"error": "unprocessable_payload_schema", "detail": "Missing required customer field"}),
                {"content-type": "application/json"},
            )

        if mode == ChaosBehaviorMode.RATE_LIMITED_429:
            if seen_failures < cfg.fail_count_before_success:
                return (
                    429,
                    json.dumps({"error": "rate_limit_exceeded", "retry_after": cfg.retry_after_seconds}),
                    {"content-type": "application/json", "retry-after": str(cfg.retry_after_seconds)},
                )

        if mode == ChaosBehaviorMode.FAIL_FIRST_N_THEN_200:
            if seen_failures < cfg.fail_count_before_success:
                return (
                    503,
                    json.dumps(
                        {
                            "error": "transient_503_spike",
                            "failed_attempt": seen_failures + 1,
                            "will_recover_on_attempt": cfg.fail_count_before_success + 1,
                        }
                    ),
                    {"content-type": "application/json"},
                )

        if mode == ChaosBehaviorMode.PROBABILISTIC_CHAOS:
            roll = random.random()
            if roll < cfg.prob_503:
                return (
                    503,
                    json.dumps({"error": "probabilistic_503_fault"}),
                    {"content-type": "application/json"},
                )
            if roll < (cfg.prob_503 + cfg.prob_429):
                return (
                    429,
                    json.dumps({"error": "probabilistic_429_throttle"}),
                    {"content-type": "application/json", "retry-after": str(cfg.retry_after_seconds)},
                )

        payload_dict = json.loads(body_bytes.decode("utf-8")) if body_bytes else {}
        with self._lock:
            prev_deliveries = self._delivered_by_idempotency_key.get(idem_key, 0)
            if prev_deliveries > 0:
                self._duplicate_deliveries_detected += 1
            self._delivered_by_idempotency_key[idem_key] = prev_deliveries + 1

            self._ledger.append(
                ReceivedWebhookRecord(
                    route_path=route_path,
                    event_id=event_id,
                    tenant_id=tenant_id,
                    idempotency_key=idem_key,
                    attempt_header=attempt_hdr,
                    signature_verified=sig_verified,
                    payload=payload_dict,
                    status_returned=200,
                )
            )

        return (
            200,
            json.dumps(
                {
                    "status": "acknowledged",
                    "route": route_path,
                    "event_id": event_id,
                    "signature_verified": sig_verified,
                    "attempt_received": attempt_hdr,
                }
            ),
            {"content-type": "application/json"},
        )

    def get_audit_summary(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "total_successful_deliveries": len(self._ledger),
                "unique_idempotency_keys_delivered": len(self._delivered_by_idempotency_key),
                "duplicate_deliveries_detected": self._duplicate_deliveries_detected,
                "active_route_modes": {path: c.mode.value for path, c in self._route_configs.items()},
            }
