from dataclasses import dataclass
from enum import Enum
import threading
import time
from typing import Callable, Dict, Optional
from urllib.parse import urlparse
from app.core.config import settings


class CircuitState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreakerOpenError(RuntimeError):
    def __init__(self, target_key: str, state: CircuitState, retry_after_seconds: float) -> None:
        self.target_key = target_key
        self.state = state
        self.retry_after_seconds = round(max(0.1, retry_after_seconds), 3)
        super().__init__(
            f"Circuit Breaker [{state.value}] for target '{target_key}'. Fast-failing for {self.retry_after_seconds}s."
        )


@dataclass
class EndpointCircuitRecord:
    state: CircuitState = CircuitState.CLOSED
    consecutive_failures: int = 0
    opened_at: float = 0.0
    half_open_in_flight: int = 0
    half_open_successes: int = 0
    total_trips: int = 0
    last_error_reason: Optional[str] = None


def extract_circuit_target_key(channel: str, destination: str) -> str:
    if channel == "EMAIL" and "@" in destination:
        domain = destination.rsplit("@", 1)[-1].lower().strip()
        return f"smtp:{domain}"
    parsed = urlparse(destination)
    netloc = parsed.netloc.lower() or destination
    path_parts = [p for p in parsed.path.split("/") if p]
    first_seg = f"/{path_parts[0]}" if path_parts else ""
    return f"{parsed.scheme}://{netloc}{first_seg}"


class EndpointCircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = settings.CB_FAILURE_THRESHOLD,
        recovery_timeout_sec: float = settings.CB_RECOVERY_TIMEOUT_SEC,
        half_open_max_calls: int = settings.CB_HALF_OPEN_MAX_CALLS,
        time_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self._failure_threshold = failure_threshold
        self._recovery_timeout_sec = recovery_timeout_sec
        self._half_open_max_calls = half_open_max_calls
        self._time_fn = time_fn
        self._lock = threading.Lock()
        self._records: Dict[str, EndpointCircuitRecord] = {}

    def before_call(self, target_key: str) -> CircuitState:
        now = self._time_fn()
        with self._lock:
            rec = self._records.setdefault(target_key, EndpointCircuitRecord())
            if rec.state == CircuitState.CLOSED:
                return CircuitState.CLOSED
            if rec.state == CircuitState.OPEN:
                elapsed = now - rec.opened_at
                if elapsed >= self._recovery_timeout_sec:
                    rec.state = CircuitState.HALF_OPEN
                    rec.half_open_in_flight = 1
                    rec.half_open_successes = 0
                    return CircuitState.HALF_OPEN
                remaining = self._recovery_timeout_sec - elapsed
                raise CircuitBreakerOpenError(target_key, CircuitState.OPEN, remaining)
            if rec.half_open_in_flight >= self._half_open_max_calls:
                raise CircuitBreakerOpenError(
                    target_key, CircuitState.HALF_OPEN, self._recovery_timeout_sec * 0.5
                )
            rec.half_open_in_flight += 1
            return CircuitState.HALF_OPEN

    def record_success(self, target_key: str) -> CircuitState:
        with self._lock:
            rec = self._records.setdefault(target_key, EndpointCircuitRecord())
            if rec.state == CircuitState.HALF_OPEN:
                rec.half_open_in_flight = max(0, rec.half_open_in_flight - 1)
                rec.half_open_successes += 1
                if rec.half_open_successes >= self._half_open_max_calls:
                    rec.state = CircuitState.CLOSED
                    rec.consecutive_failures = 0
                    rec.half_open_in_flight = 0
                    rec.half_open_successes = 0
                    rec.last_error_reason = None
            else:
                rec.consecutive_failures = 0
                rec.state = CircuitState.CLOSED
            return rec.state

    def record_failure(self, target_key: str, error_reason: str = "Upstream 5xx/Timeout") -> CircuitState:
        now = self._time_fn()
        with self._lock:
            rec = self._records.setdefault(target_key, EndpointCircuitRecord())
            rec.last_error_reason = error_reason
            if rec.state == CircuitState.HALF_OPEN:
                rec.state = CircuitState.OPEN
                rec.opened_at = now
                rec.half_open_in_flight = 0
                rec.half_open_successes = 0
                rec.total_trips += 1
                return CircuitState.OPEN
            rec.consecutive_failures += 1
            if rec.consecutive_failures >= self._failure_threshold:
                rec.state = CircuitState.OPEN
                rec.opened_at = now
                rec.total_trips += 1
            return rec.state

    def get_snapshot(self, target_key: str) -> Dict[str, object]:
        with self._lock:
            rec = self._records.get(target_key, EndpointCircuitRecord())
            return {
                "target_key": target_key,
                "state": rec.state.value,
                "consecutive_failures": rec.consecutive_failures,
                "total_trips": rec.total_trips,
                "half_open_in_flight": rec.half_open_in_flight,
                "half_open_successes": rec.half_open_successes,
                "last_error_reason": rec.last_error_reason,
            }
