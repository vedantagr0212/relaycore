import json
import unittest
from typing import Dict, Tuple

from app.core.security import verify_webhook_signature
from app.schemas.event_schemas import ChannelType
from app.services.circuit_breaker import (
    CircuitBreakerOpenError,
    CircuitState,
    EndpointCircuitBreaker,
    extract_circuit_target_key,
)
from app.services.dispatch_strategies import DeliveryStrategyRegistry
from app.services.rate_limiter import TokenBucketRateLimiter


class FakeClock:
    def __init__(self, initial: float = 1000.0) -> None:
        self.now = initial

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class TestPhase2ResilienceAndStrategies(unittest.TestCase):
    def test_01_token_bucket_rate_limiter_burst_and_refill(self) -> None:
        clock = FakeClock()
        limiter = TokenBucketRateLimiter(time_fn=clock)

        for expected_rem in [2.0, 1.0, 0.0]:
            dec = limiter.acquire("tenant_finverse", capacity=3, refill_rate_per_sec=2.0)
            self.assertTrue(dec.allowed)
            self.assertAlmostEqual(dec.remaining_tokens, expected_rem)

        throttled = limiter.acquire("tenant_finverse", capacity=3, refill_rate_per_sec=2.0)
        self.assertFalse(throttled.allowed)
        self.assertAlmostEqual(throttled.retry_after_seconds, 0.5, places=2)

        other_tenant = limiter.acquire("tenant_beta", capacity=3, refill_rate_per_sec=2.0)
        self.assertTrue(other_tenant.allowed)

        clock.advance(0.5)
        recovered = limiter.acquire("tenant_finverse", capacity=3, refill_rate_per_sec=2.0)
        self.assertTrue(recovered.allowed)
        self.assertAlmostEqual(recovered.remaining_tokens, 0.0)

    def test_02_circuit_breaker_full_state_machine(self) -> None:
        clock = FakeClock()
        cb = EndpointCircuitBreaker(
            failure_threshold=3,
            recovery_timeout_sec=10.0,
            half_open_max_calls=2,
            time_fn=clock,
        )
        target = extract_circuit_target_key("WEBHOOK", "https://api.partner.io/v1/webhooks")
        self.assertEqual(target, "https://api.partner.io/v1")

        for _ in range(3):
            self.assertEqual(cb.before_call(target), CircuitState.CLOSED)
            state = cb.record_failure(target, "HTTP 503 Service Unavailable")
        self.assertEqual(state, CircuitState.OPEN)

        with self.assertRaises(CircuitBreakerOpenError) as ctx:
            cb.before_call(target)
        self.assertEqual(ctx.exception.state, CircuitState.OPEN)
        self.assertAlmostEqual(ctx.exception.retry_after_seconds, 10.0, places=1)

        clock.advance(10.1)
        self.assertEqual(cb.before_call(target), CircuitState.HALF_OPEN)
        self.assertEqual(cb.before_call(target), CircuitState.HALF_OPEN)

        with self.assertRaises(CircuitBreakerOpenError) as ctx_ho:
            cb.before_call(target)
        self.assertEqual(ctx_ho.exception.state, CircuitState.HALF_OPEN)

        self.assertEqual(cb.record_failure(target, "Still 503"), CircuitState.OPEN)

        clock.advance(10.1)
        self.assertEqual(cb.before_call(target), CircuitState.HALF_OPEN)
        self.assertEqual(cb.record_success(target), CircuitState.HALF_OPEN)
        self.assertEqual(cb.before_call(target), CircuitState.HALF_OPEN)
        self.assertEqual(cb.record_success(target), CircuitState.CLOSED)
        self.assertEqual(cb.get_snapshot(target)["consecutive_failures"], 0)

    def test_03_webhook_and_slack_strategies_with_signature_and_retry_after(self) -> None:
        captured_requests = []

        def mock_transport(url: str, body: bytes, headers: Dict[str, str], timeout: float) -> Tuple[int, str, Dict[str, str]]:
            captured_requests.append((url, body, headers))
            if "rate-limit" in url:
                return 429, '{"error":"rate_limited"}', {"retry-after": "12"}
            return 200, '{"ok":true}', {}

        registry = DeliveryStrategyRegistry(http_transport=mock_transport)
        secret = "whsec_acme_signing_key_999"
        event_row = {
            "event_id": "evt_1001",
            "tenant_id": "acme_commerce",
            "idempotency_key": "idem_991122",
            "event_type": "order.completed",
            "channel": "WEBHOOK",
            "destination": "https://client.example.com/webhooks/orders",
            "payload_json": json.dumps({"order_id": "ORD-55", "total": 1250.0}, sort_keys=True),
            "custom_headers_json": json.dumps({"X-Custom-Source": "AcmeCore"}),
            "signing_secret": secret,
            "retry_count": 0,
        }

        wh_res = registry.get_strategy(ChannelType.WEBHOOK).dispatch(event_row)
        self.assertTrue(wh_res.success)
        self.assertEqual(wh_res.status_code, 200)

        _, sent_body, sent_headers = captured_requests[-1]
        self.assertTrue(
            verify_webhook_signature(sent_body, sent_headers["X-RelayCore-Signature"], secret)
        )
        self.assertEqual(sent_headers["X-Custom-Source"], "AcmeCore")

        event_row["destination"] = "https://client.example.com/rate-limit"
        rl_res = registry.get_strategy(ChannelType.WEBHOOK).dispatch(event_row)
        self.assertFalse(rl_res.success)
        self.assertEqual(rl_res.status_code, 429)
        self.assertEqual(rl_res.retry_after_header, 12.0)
        self.assertTrue(rl_res.is_transient_failure)

        event_row["channel"] = "SLACK"
        event_row["destination"] = "https://hooks.slack.com/services/T00/B00/X00"
        slack_res = registry.get_strategy(ChannelType.SLACK).dispatch(event_row)
        self.assertTrue(slack_res.success)
        _, slack_body, _ = captured_requests[-1]
        parsed_slack = json.loads(slack_body.decode("utf-8"))
        self.assertEqual(parsed_slack["blocks"][0]["type"], "header")
        self.assertEqual(len(parsed_slack["blocks"]), 3)

    def test_04_mock_smtp_protocol_engine_all_bounce_modes(self) -> None:
        registry = DeliveryStrategyRegistry()
        smtp = registry.email_strategy

        base_event = {
            "event_id": "evt_email_01",
            "tenant_id": "nexus_crm",
            "idempotency_key": "idem_email_01",
            "event_type": "user.welcome",
            "channel": "EMAIL",
            "destination": "vedant@nitw.ac.in",
            "payload_json": json.dumps({"subject": "Welcome to RelayCore", "user": "Vedant"}),
            "retry_count": 0,
        }

        ok_res = smtp.dispatch(base_event)
        self.assertTrue(ok_res.success)
        self.assertEqual(ok_res.status_code, 250)
        self.assertEqual(len(smtp.spooled_messages), 1)
        self.assertIn("DKIM-Signature:", smtp.spooled_messages[0]["raw_mime"])

        greylist_event = dict(base_event, destination="ops@throttled-mx.example.com")
        gl_res = smtp.dispatch(greylist_event)
        self.assertFalse(gl_res.success)
        self.assertEqual(gl_res.status_code, 429)
        self.assertEqual(gl_res.retry_after_header, 5.0)
        self.assertTrue(gl_res.is_transient_failure)

        soft_event = dict(base_event, destination="user@mailbox-full.example.com")
        soft_res = smtp.dispatch(soft_event)
        self.assertFalse(soft_res.success)
        self.assertEqual(soft_res.status_code, 503)
        self.assertTrue(soft_res.is_transient_failure)

        hard_event = dict(base_event, destination="hardbounce@invalid-domain.example.com")
        hard_res = smtp.dispatch(hard_event)
        self.assertFalse(hard_res.success)
        self.assertEqual(hard_res.status_code, 422)
        self.assertFalse(hard_res.is_transient_failure)
