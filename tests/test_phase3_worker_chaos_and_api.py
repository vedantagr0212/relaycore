import concurrent.futures
from datetime import datetime, timedelta, timezone
import sqlite3
import unittest

from app.api.service_controller import RelayCoreApplicationController
from app.mocks.chaos_receiver import StatefulChaosTargetServer
from app.repositories.event_repository import TransactionalEventRepository
from app.schemas.event_schemas import EventStatus
from app.services.circuit_breaker import CircuitState, EndpointCircuitBreaker
from app.services.rate_limiter import TokenBucketRateLimiter
from app.workers.dispatch_worker import RelayCoreDispatchWorker


class FakeMonotonicClock:
    def __init__(self, initial: float = 5000.0) -> None:
        self.now = initial

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class TestPhase3EndToEndWorkerChaosAndAPI(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.clock = FakeMonotonicClock()
        self.repo = TransactionalEventRepository(self.conn)
        self.rate_limiter = TokenBucketRateLimiter(time_fn=self.clock)
        self.circuit_breaker = EndpointCircuitBreaker(
            failure_threshold=3,
            recovery_timeout_sec=15.0,
            half_open_max_calls=1,
            time_fn=self.clock,
        )
        self.chaos_server = StatefulChaosTargetServer()
        self.controller = RelayCoreApplicationController(
            repository=self.repo,
            rate_limiter=self.rate_limiter,
            circuit_breaker=self.circuit_breaker,
            chaos_server=self.chaos_server,
        )

    def tearDown(self) -> None:
        self.conn.close()

    def test_01_flaky_endpoint_auto_recovery_and_hmac_verification(self) -> None:
        status_code, ingested = self.controller.ingest_event_endpoint(
            {
                "tenant_id": "zeta_cdp_prod",
                "idempotency_key": "flaky_idem_001",
                "event_type": "customer.segment.entered",
                "channel": "WEBHOOK",
                "destination": "http://localhost:8000/mock-target/flaky",
                "payload": {"customer_id": "cust_881", "segment": "high_ltv"},
                "max_retries": 4,
            }
        )
        self.assertEqual(status_code, 202)
        eid = ingested.event_id

        dup_code, dup_resp = self.controller.ingest_event_endpoint(
            {
                "tenant_id": "zeta_cdp_prod",
                "idempotency_key": "flaky_idem_001",
                "event_type": "customer.segment.entered",
                "channel": "WEBHOOK",
                "destination": "http://localhost:8000/mock-target/flaky",
                "payload": {"customer_id": "cust_881", "segment": "high_ltv"},
                "max_retries": 4,
            }
        )
        self.assertEqual(dup_code, 200)
        self.assertTrue(dup_resp.is_idempotent_replay)

        base_dt = datetime.now(timezone.utc)

        _, tick1 = self.controller.run_worker_tick_endpoint(now_override=base_dt)
        self.assertEqual(tick1["retry_scheduled_count"], 1)

        _, tick2 = self.controller.run_worker_tick_endpoint(
            now_override=base_dt + timedelta(seconds=30)
        )
        self.assertEqual(tick2["retry_scheduled_count"], 1)

        _, tick3 = self.controller.run_worker_tick_endpoint(
            now_override=base_dt + timedelta(seconds=120)
        )
        self.assertEqual(tick3["delivered_count"], 1)

        _, ev_state = self.controller.get_event_endpoint(eid)
        self.assertEqual(ev_state["status"], EventStatus.DELIVERED.value)
        self.assertEqual(len(ev_state["attempts"]), 3)
        self.assertEqual(ev_state["attempts"][0]["status_code"], 503)
        self.assertEqual(ev_state["attempts"][1]["status_code"], 503)
        self.assertEqual(ev_state["attempts"][2]["status_code"], 200)

        audit = self.chaos_server.get_audit_summary()
        self.assertEqual(audit["total_successful_deliveries"], 1)
        self.assertEqual(audit["duplicate_deliveries_detected"], 0)

    def test_02_outage_circuit_breaker_fast_fail_dlq_and_bulk_replay(self) -> None:
        _, ev1 = self.controller.ingest_event_endpoint(
            {
                "tenant_id": "zeta_outage_tenant",
                "idempotency_key": "outage_evt_1",
                "event_type": "order.created",
                "channel": "WEBHOOK",
                "destination": "http://localhost:8000/mock-target/outage",
                "payload": {"order": 1},
                "max_retries": 2,
            }
        )
        base_dt = datetime.now(timezone.utc)

        self.controller.run_worker_tick_endpoint(now_override=base_dt)
        self.controller.run_worker_tick_endpoint(now_override=base_dt + timedelta(seconds=20))
        _, tick3 = self.controller.run_worker_tick_endpoint(now_override=base_dt + timedelta(seconds=90))
        self.assertEqual(tick3["dead_lettered_count"], 1)

        cb_snap = self.circuit_breaker.get_snapshot("http://localhost:8000/mock-target")
        self.assertEqual(cb_snap["state"], CircuitState.OPEN.value)

        _, ev2 = self.controller.ingest_event_endpoint(
            {
                "tenant_id": "zeta_outage_tenant",
                "idempotency_key": "outage_evt_2",
                "event_type": "order.created",
                "channel": "WEBHOOK",
                "destination": "http://localhost:8000/mock-target/outage",
                "payload": {"order": 2},
                "max_retries": 2,
            }
        )
        _, tick_fast_fail = self.controller.run_worker_tick_endpoint(
            now_override=base_dt + timedelta(seconds=95)
        )
        self.assertEqual(tick_fast_fail["deferred_by_circuit_breaker"], 1)
        _, ev2_state = self.controller.get_event_endpoint(ev2.event_id)
        self.assertEqual(ev2_state["retry_count"], 0)
        self.assertEqual(ev2_state["attempts"][-1]["error_category"], "CIRCUIT_BREAKER_OPEN")

        self.controller.configure_chaos_route_endpoint("/mock-target/outage", "HEALTHY_200")
        self.clock.advance(20.0)

        _, replay_res = self.controller.replay_bulk_dlq_endpoint(tenant_id="zeta_outage_tenant")
        self.assertEqual(replay_res["replayed_count"], 1)

        _, tick_recover = self.controller.run_worker_tick_endpoint(
            now_override=base_dt + timedelta(seconds=200)
        )
        self.assertEqual(tick_recover["delivered_count"], 2)

        _, dlq_after = self.controller.list_dlq_endpoint(tenant_id="zeta_outage_tenant")
        self.assertEqual(dlq_after["count"], 0)

    def test_03_fatal_422_routes_immediately_to_dlq_on_first_attempt(self) -> None:
        _, ev = self.controller.ingest_event_endpoint(
            {
                "tenant_id": "zeta_schema_test",
                "idempotency_key": "fatal_422_01",
                "event_type": "billing.failed",
                "channel": "WEBHOOK",
                "destination": "http://localhost:8000/mock-target/fatal-422",
                "payload": {"bad_field": True},
                "max_retries": 5,
            }
        )
        _, summary = self.controller.run_worker_tick_endpoint()
        self.assertEqual(summary["dead_lettered_count"], 1)

        _, ev_state = self.controller.get_event_endpoint(ev.event_id)
        self.assertEqual(ev_state["status"], EventStatus.DEAD_LETTER.value)
        self.assertEqual(ev_state["retry_count"], 1)
        self.assertEqual(len(ev_state["attempts"]), 1)

    def test_04_multi_worker_concurrent_pool_across_all_channels(self) -> None:
        self.repo.ensure_tenant(
            tenant_id="zeta_high_throughput",
            rate_limit_capacity=100,
            rate_limit_refill_per_sec=100.0,
        )

        for i in range(30):
            ch = ["WEBHOOK", "SLACK", "EMAIL"][i % 3]
            dest = {
                "WEBHOOK": "http://localhost:8000/mock-target/healthy",
                "SLACK": "http://localhost:8000/mock-target/slack",
                "EMAIL": f"user{i}@nitw.ac.in",
            }[ch]
            self.controller.ingest_event_endpoint(
                {
                    "tenant_id": "zeta_high_throughput",
                    "idempotency_key": f"multi_ch_{i:03d}",
                    "event_type": "notification.dispatched",
                    "channel": ch,
                    "destination": dest,
                    "payload": {"index": i, "subject": f"Alert #{i}"},
                }
            )

        workers = [
            RelayCoreDispatchWorker(
                worker_id=f"pool-worker-{w}",
                repository=self.repo,
                rate_limiter=self.rate_limiter,
                circuit_breaker=self.circuit_breaker,
                strategy_registry=self.controller.strategy_registry,
            )
            for w in range(4)
        ]

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(w.process_next_batch, 10) for w in workers]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        total_delivered = sum(r.delivered_count for r in results)
        self.assertEqual(total_delivered, 30)

        _, dash = self.controller.get_observability_dashboard()
        self.assertEqual(dash["metrics"]["events_by_status"]["DELIVERED"], 30)
        self.assertEqual(dash["chaos_receiver_audit"]["duplicate_deliveries_detected"], 0)
        self.assertEqual(dash["smtp_spooled_emails_count"], 10)
