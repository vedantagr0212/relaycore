import concurrent.futures
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone

from app.core.security import (
    WebhookSignatureError,
    generate_webhook_signature,
    verify_webhook_signature,
)
from app.repositories.event_repository import TransactionalEventRepository
from app.schemas.event_schemas import (
    ChannelType,
    EventIngestRequest,
    EventStatus,
)


class TestPhase1CoreFoundation(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.repo = TransactionalEventRepository(self.conn)

    def tearDown(self) -> None:
        self.conn.close()

    def test_01_hmac_sha256_signing_and_replay_protection(self) -> None:
        payload = b'{"event":"order.paid","amount":49900}'
        secret = "whsec_live_9876543210abcdef"
        sig_header, _ = generate_webhook_signature(payload, secret, timestamp=1700000000)

        self.assertTrue(
            verify_webhook_signature(
                payload, sig_header, secret, tolerance_seconds=300, current_timestamp=1700000120
            )
        )

        with self.assertRaises(WebhookSignatureError):
            verify_webhook_signature(
                b'{"event":"order.paid","amount":999999}',
                sig_header,
                secret,
                current_timestamp=1700000120,
            )

        with self.assertRaises(WebhookSignatureError):
            verify_webhook_signature(
                payload, sig_header, secret, tolerance_seconds=300, current_timestamp=1700000500
            )

    def test_02_idempotent_event_ingestion(self) -> None:
        req = EventIngestRequest(
            tenant_id="zeta_marketing_prod",
            idempotency_key="idem_key_2026_0001",
            event_type="campaign.webhook.triggered",
            channel=ChannelType.WEBHOOK,
            destination="https://api.customer-domain.com/webhooks/zeta",
            payload={"campaign_id": "cmp_901", "user_id": "usr_442"},
            max_retries=3,
        )
        first = self.repo.ingest_event(req)
        self.assertFalse(first.is_idempotent_replay)
        self.assertEqual(first.status, EventStatus.PENDING)

        second = self.repo.ingest_event(req)
        self.assertTrue(second.is_idempotent_replay)
        self.assertEqual(first.event_id, second.event_id)

    def test_03_concurrent_worker_polling_zero_duplicates(self) -> None:
        for i in range(40):
            self.repo.ingest_event(
                EventIngestRequest(
                    tenant_id="zeta_tenant_alpha",
                    idempotency_key=f"concurrent_idem_{i:04d}",
                    event_type="user.signup",
                    channel=ChannelType.WEBHOOK,
                    destination="http://localhost:8000/mock-target/webhook",
                    payload={"seq": i},
                )
            )

        claimed_by_workers = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = [
                pool.submit(self.repo.claim_due_events_batch, f"worker-{w}", 6)
                for w in range(8)
            ]
            for fut in concurrent.futures.as_completed(futures):
                claimed_by_workers.extend(fut.result())

        claimed_ids = [row["event_id"] for row in claimed_by_workers]
        self.assertEqual(len(claimed_ids), 40)
        self.assertEqual(len(set(claimed_ids)), 40)

    def test_04_exponential_backoff_dlq_transition_and_replay(self) -> None:
        req = EventIngestRequest(
            tenant_id="zeta_tenant_beta",
            idempotency_key="dlq_lifecycle_test_01",
            event_type="invoice.failed",
            channel=ChannelType.WEBHOOK,
            destination="http://localhost:8000/mock-target/unstable",
            payload={"invoice_id": "inv_77"},
            max_retries=2,
        )
        ingested = self.repo.ingest_event(req)
        eid = ingested.event_id

        batch1 = self.repo.claim_due_events_batch("worker-1", batch_size=1)
        self.assertEqual(len(batch1), 1)
        after_fail_1 = self.repo.record_delivery_outcome(
            event_id=eid,
            worker_id="worker-1",
            success=False,
            status_code=503,
            latency_ms=142.5,
            error_category="UPSTREAM_5XX",
            response_excerpt="503 Service Unavailable",
        )
        self.assertEqual(after_fail_1.status, EventStatus.RETRY_SCHEDULED)
        self.assertEqual(after_fail_1.retry_count, 1)

        future_time = datetime.now(timezone.utc) + timedelta(seconds=60)
        batch2 = self.repo.claim_due_events_batch("worker-2", batch_size=1, now_override=future_time)
        self.assertEqual(len(batch2), 1)
        after_fail_2 = self.repo.record_delivery_outcome(
            event_id=eid,
            worker_id="worker-2",
            success=False,
            status_code=504,
            latency_ms=5001.2,
            error_category="GATEWAY_TIMEOUT",
            response_excerpt="504 Gateway Timeout",
        )
        self.assertEqual(after_fail_2.status, EventStatus.RETRY_SCHEDULED)
        self.assertEqual(after_fail_2.retry_count, 2)

        future_time_2 = datetime.now(timezone.utc) + timedelta(seconds=300)
        batch3 = self.repo.claim_due_events_batch("worker-1", batch_size=1, now_override=future_time_2)
        self.assertEqual(len(batch3), 1)
        after_dlq = self.repo.record_delivery_outcome(
            event_id=eid,
            worker_id="worker-1",
            success=False,
            status_code=503,
            latency_ms=118.0,
            error_category="MAX_RETRIES_EXHAUSTED",
            response_excerpt="Upstream still down",
        )
        self.assertEqual(after_dlq.status, EventStatus.DEAD_LETTER)
        self.assertEqual(len(after_dlq.attempts), 3)

        replayed = self.repo.replay_dead_letter_event(eid)
        self.assertEqual(replayed.status, EventStatus.PENDING)
        self.assertEqual(replayed.retry_count, 0)

        batch4 = self.repo.claim_due_events_batch("worker-3", batch_size=1)
        self.assertEqual(len(batch4), 1)
        delivered = self.repo.record_delivery_outcome(
            event_id=eid,
            worker_id="worker-3",
            success=True,
            status_code=200,
            latency_ms=34.8,
            response_excerpt='{"status":"received"}',
        )
        self.assertEqual(delivered.status, EventStatus.DELIVERED)
        self.assertEqual(len(delivered.attempts), 4)
