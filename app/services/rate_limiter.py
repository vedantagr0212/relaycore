from dataclasses import dataclass
import math
import threading
import time
from typing import Callable, Dict, Optional, Tuple
from app.core.config import settings


TOKEN_BUCKET_LUA_SCRIPT = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate_per_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'last_refill_ms')
local tokens = tonumber(bucket[1])
local last_refill_ms = tonumber(bucket[2])

if tokens == nil or last_refill_ms == nil then
    tokens = capacity
    last_refill_ms = now_ms
else
    local elapsed_ms = math.max(0, now_ms - last_refill_ms)
    local refilled = elapsed_ms * refill_rate_per_ms
    tokens = math.min(capacity, tokens + refilled)
    last_refill_ms = now_ms
end

local allowed = 0
local retry_after_ms = 0

if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
else
    local deficit = requested - tokens
    retry_after_ms = math.ceil(deficit / refill_rate_per_ms)
end

local ttl_sec = math.max(60, math.ceil((capacity / (refill_rate_per_ms * 1000.0)) * 2))
redis.call('HMSET', key, 'tokens', tokens, 'last_refill_ms', last_refill_ms)
redis.call('EXPIRE', key, ttl_sec)

return {allowed, tostring(tokens), retry_after_ms}
"""


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    tenant_id: str
    remaining_tokens: float
    retry_after_seconds: float
    capacity: int
    refill_rate_per_sec: float


class TokenBucketRateLimiter:
    def __init__(
        self,
        redis_client: Optional[object] = None,
        time_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self._redis = redis_client
        self._time_fn = time_fn
        self._lock = threading.Lock()
        self._buckets: Dict[str, Tuple[float, float]] = {}

    def acquire(
        self,
        tenant_id: str,
        capacity: int = settings.RL_DEFAULT_CAPACITY,
        refill_rate_per_sec: float = settings.RL_DEFAULT_REFILL_RATE_PER_SEC,
        requested_tokens: int = 1,
    ) -> RateLimitDecision:
        if capacity <= 0 or refill_rate_per_sec <= 0:
            raise ValueError("capacity and refill_rate_per_sec must be strictly positive.")

        key = f"relaycore:rl:tenant:{tenant_id}"
        now_ms = self._time_fn() * 1000.0
        refill_rate_per_ms = refill_rate_per_sec / 1000.0

        if self._redis is not None and hasattr(self._redis, "eval"):
            res = self._redis.eval(
                TOKEN_BUCKET_LUA_SCRIPT,
                1,
                key,
                capacity,
                refill_rate_per_ms,
                now_ms,
                requested_tokens,
            )
            allowed = bool(int(res[0]))
            remaining = round(float(res[1]), 4)
            retry_after_sec = round(float(res[2]) / 1000.0, 3)
            return RateLimitDecision(
                allowed=allowed,
                tenant_id=tenant_id,
                remaining_tokens=remaining,
                retry_after_seconds=retry_after_sec,
                capacity=capacity,
                refill_rate_per_sec=refill_rate_per_sec,
            )

        with self._lock:
            if key not in self._buckets:
                tokens = float(capacity)
                last_refill_ms = now_ms
            else:
                prev_tokens, prev_ms = self._buckets[key]
                elapsed_ms = max(0.0, now_ms - prev_ms)
                tokens = min(float(capacity), prev_tokens + (elapsed_ms * refill_rate_per_ms))
                last_refill_ms = now_ms

            if tokens >= requested_tokens:
                tokens -= requested_tokens
                allowed = True
                retry_after_sec = 0.0
            else:
                deficit = requested_tokens - tokens
                retry_after_ms = math.ceil(deficit / refill_rate_per_ms)
                allowed = False
                retry_after_sec = round(retry_after_ms / 1000.0, 3)

            self._buckets[key] = (tokens, last_refill_ms)
            return RateLimitDecision(
                allowed=allowed,
                tenant_id=tenant_id,
                remaining_tokens=round(tokens, 4),
                retry_after_seconds=retry_after_sec,
                capacity=capacity,
                refill_rate_per_sec=refill_rate_per_sec,
            )
