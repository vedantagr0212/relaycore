import json
import random
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from app.core.config import settings
from app.schemas.event_schemas import (
    AttemptLogResponse,
    ChannelType,
    EventIngestRequest,
    EventStatus,
    EventStatusResponse,
    validate_state_transition,
)

NON_RETRYABLE_STATUS_CODES = {400, 401, 403, 404, 405, 406, 410, 422}


def compute_backoff_seconds(
    retry_count: int,
    base_seconds: float = settings.BACKOFF_BASE_SECONDS,
    max_seconds: float = settings.BACKOFF_MAX_SECONDS,
    jitter_ratio: float = settings.BACKOFF_JITTER_RATIO,
    retry_after_header: Optional[float] = None,
) -> float:
    exp_delay = min(max_seconds, base_seconds * (2 ** max(0, retry_count - 1)))
    if retry_after_header is not None and retry_after_header > 0:
        exp_delay = min(max_seconds, max(exp_delay, float(retry_after_header)))
    jitter_bound = exp_delay * jitter_ratio
    jitter = random.uniform(0.0, jitter_bound)
    return round(min(max_seconds, exp_delay + jitter), 3)


class TransactionalEventRepository:
    POSTGRES_CLAIM_BATCH_SQL = """
        WITH due_events AS (
            SELECT event_id
            FROM delivery_events
            WHERE status IN ('PENDING', 'RETRY_SCHEDULED')
              AND next_retry_at <= NOW()
            ORDER BY next_retry_at ASC
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
        )
        UPDATE delivery_events de
        SET status = 'PROCESSING',
            locked_by_worker = :worker_id,
            updated_at = NOW()
        FROM due_events
        WHERE de.event_id = due_events.event_id
        RETURNING de.event_id, de.tenant_id, de.idempotency_key, de.event_type,
                  de.channel, de.destination, de.payload_json, de.custom_headers_json,
                  de.status, de.retry_count, de.max_retries, de.next_retry_at;
    """

    def __init__(self, sqlite_conn: sqlite3.Connection) -> None:
        self._conn = sqlite_conn
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.executescript(
                """
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS tenants (
                    tenant_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    signing_secret TEXT NOT NULL,
                    rate_limit_capacity INTEGER NOT NULL DEFAULT 20,
                    rate_limit_refill_per_sec REAL NOT NULL DEFAULT 10.0,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS delivery_events (
                    event_id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
                    idempotency_key TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    custom_headers_json TEXT NOT NULL DEFAULT '{}',
                    status TEXT NOT NULL,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    max_retries INTEGER NOT NULL DEFAULT 5,
                    next_retry_at TEXT NOT NULL,
                    locked_by_worker TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE (tenant_id, idempotency_key)
                );

                CREATE INDEX IF NOT EXISTS ix_delivery_events_poll_queue
                    ON delivery_events (status, next_retry_at);

                CREATE INDEX IF NOT EXISTS ix_delivery_events_tenant_status
                    ON delivery_events (tenant_id, status, created_at);

                CREATE TABLE IF NOT EXISTS delivery_attempt_logs (
                    attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL REFERENCES delivery_events(event_id) ON DELETE CASCADE,
                    attempt_number INTEGER NOT NULL,
                    worker_id TEXT NOT NULL,
                    status_code INTEGER,
                    latency_ms REAL NOT NULL,
                    error_category TEXT,
                    response_excerpt TEXT,
                    attempted_at TEXT NOT NULL
                );
                """
            )
            self._conn.commit()

    def ensure_tenant(
        self,
        tenant_id: str,
        name: str = "Default Tenant",
        signing_secret: str = "whsec_relaycore_default_secret_key_2026",
        rate_limit_capacity: int = 20,
        rate_limit_refill_per_sec: float = 10.0,
    ) -> Dict[str, Any]:
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT OR IGNORE INTO tenants
                    (tenant_id, name, signing_secret, rate_limit_capacity, rate_limit_refill_per_sec, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (tenant_id, name, signing_secret, rate_limit_capacity, rate_limit_refill_per_sec, now_iso),
            )
            self._conn.commit()
            cur.execute("SELECT * FROM tenants WHERE tenant_id = ?", (tenant_id,))
            return dict(cur.fetchone())

    def ingest_event(self, req: EventIngestRequest) -> EventStatusResponse:
        self.ensure_tenant(req.tenant_id)
        now_iso = datetime.now(timezone.utc).isoformat()
        event_id = str(uuid.uuid4())

        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute(
                    """
                    INSERT INTO delivery_events (
                        event_id, tenant_id, idempotency_key, event_type, channel,
                        destination, payload_json, custom_headers_json, status,
                        retry_count, max_retries, next_retry_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
                    """,
                    (
                        event_id,
                        req.tenant_id,
                        req.idempotency_key,
                        req.event_type,
                        req.channel.value,
                        req.destination,
                        json.dumps(req.payload, sort_keys=True),
                        json.dumps(req.custom_headers or {}),
                        EventStatus.PENDING.value,
                        req.max_retries,
                        now_iso,
                        now_iso,
                        now_iso,
                    ),
                )
                self._conn.commit()
                is_replay = False
                target_event_id = event_id
            except sqlite3.IntegrityError:
                self._conn.rollback()
                cur.execute(
                    "SELECT event_id FROM delivery_events WHERE tenant_id = ? AND idempotency_key = ?",
                    (req.tenant_id, req.idempotency_key),
                )
                target_event_id = cur.fetchone()["event_id"]
                is_replay = True

        resp = self.get_event_by_id(target_event_id)
        resp.is_idempotent_replay = is_replay
        return resp

    def claim_due_events_batch(
        self,
        worker_id: str,
        batch_size: int = settings.WORKER_BATCH_SIZE,
        now_override: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        now_dt = now_override or datetime.now(timezone.utc)
        now_iso = now_dt.isoformat()

        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                cur.execute(
                    """
                    SELECT event_id, status
                    FROM delivery_events
                    WHERE status IN ('PENDING', 'RETRY_SCHEDULED')
                      AND next_retry_at <= ?
                    ORDER BY next_retry_at ASC
                    LIMIT ?
                    """,
                    (now_iso, batch_size),
                )
                candidates = cur.fetchall()
                if not candidates:
                    self._conn.commit()
                    return []

                claimed_ids = []
                for row in candidates:
                    validate_state_transition(EventStatus(row["status"]), EventStatus.PROCESSING)
                    claimed_ids.append(row["event_id"])

                placeholders = ",".join("?" for _ in claimed_ids)
                cur.execute(
                    f"""
                    UPDATE delivery_events
                    SET status = 'PROCESSING',
                        locked_by_worker = ?,
                        updated_at = ?
                    WHERE event_id IN ({placeholders})
                    """,
                    [worker_id, now_iso] + claimed_ids,
                )

                cur.execute(
                    f"""
                    SELECT de.*, t.signing_secret, t.rate_limit_capacity, t.rate_limit_refill_per_sec
                    FROM delivery_events de
                    JOIN tenants t ON de.tenant_id = t.tenant_id
                    WHERE de.event_id IN ({placeholders})
                    ORDER BY de.next_retry_at ASC
                    """,
                    claimed_ids,
                )
                claimed_rows = [dict(r) for r in cur.fetchall()]
                self._conn.commit()
                return claimed_rows
            except Exception:
                self._conn.rollback()
                raise

    def defer_event_without_penalty(
        self,
        event_id: str,
        worker_id: str,
        delay_seconds: float,
        reason_category: str,
        reason_detail: str,
        now_override: Optional[datetime] = None,
    ) -> EventStatusResponse:
        now = now_override or datetime.now(timezone.utc)
        now_iso = now.isoformat()
        next_retry_iso = (now + timedelta(seconds=max(0.05, delay_seconds))).isoformat()

        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                cur.execute("SELECT * FROM delivery_events WHERE event_id = ?", (event_id,))
                row = cur.fetchone()
                if not row:
                    raise KeyError(f"Event {event_id} not found.")

                validate_state_transition(EventStatus(row["status"]), EventStatus.RETRY_SCHEDULED)
                attempt_num = int(row["retry_count"]) + 1

                cur.execute(
                    """
                    UPDATE delivery_events
                    SET status = 'RETRY_SCHEDULED',
                        next_retry_at = ?,
                        locked_by_worker = NULL,
                        last_error = ?,
                        updated_at = ?
                    WHERE event_id = ?
                    """,
                    (
                        next_retry_iso,
                        f"[{reason_category}] {reason_detail}",
                        now_iso,
                        event_id,
                    ),
                )

                cur.execute(
                    """
                    INSERT INTO delivery_attempt_logs (
                        event_id, attempt_number, worker_id, status_code,
                        latency_ms, error_category, response_excerpt, attempted_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event_id,
                        attempt_num,
                        worker_id,
                        None,
                        0.1,
                        reason_category,
                        reason_detail[:500],
                        now_iso,
                    ),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

        return self.get_event_by_id(event_id)

    def record_delivery_outcome(
        self,
        event_id: str,
        worker_id: str,
        success: bool,
        status_code: Optional[int],
        latency_ms: float,
        error_category: Optional[str] = None,
        response_excerpt: Optional[str] = None,
        retry_after_header: Optional[float] = None,
        force_dead_letter: bool = False,
        now_override: Optional[datetime] = None,
    ) -> EventStatusResponse:
        now = now_override or datetime.now(timezone.utc)
        now_iso = now.isoformat()

        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                cur.execute("SELECT * FROM delivery_events WHERE event_id = ?", (event_id,))
                row = cur.fetchone()
                if not row:
                    raise KeyError(f"Event {event_id} not found.")

                current_status = EventStatus(row["status"])
                retry_count = int(row["retry_count"])
                max_retries = int(row["max_retries"])
                attempt_number = retry_count + 1

                if success:
                    target_status = EventStatus.DELIVERED
                    new_retry_count = retry_count
                    next_retry_iso = row["next_retry_at"]
                    last_error = None
                else:
                    new_retry_count = retry_count + 1
                    is_non_retryable_http = status_code in NON_RETRYABLE_STATUS_CODES
                    if force_dead_letter or is_non_retryable_http or new_retry_count > max_retries:
                        target_status = EventStatus.DEAD_LETTER
                        next_retry_iso = now_iso
                    else:
                        target_status = EventStatus.RETRY_SCHEDULED
                        delay_sec = compute_backoff_seconds(
                            retry_count=new_retry_count,
                            retry_after_header=retry_after_header,
                        )
                        next_retry_iso = (now + timedelta(seconds=delay_sec)).isoformat()
                    last_error = f"[{error_category or 'DELIVERY_ERROR'}] HTTP {status_code}: {response_excerpt or ''}".strip()

                validate_state_transition(current_status, target_status)

                cur.execute(
                    """
                    UPDATE delivery_events
                    SET status = ?,
                        retry_count = ?,
                        next_retry_at = ?,
                        locked_by_worker = NULL,
                        last_error = ?,
                        updated_at = ?
                    WHERE event_id = ?
                    """,
                    (
                        target_status.value,
                        new_retry_count,
                        next_retry_iso,
                        last_error,
                        now_iso,
                        event_id,
                    ),
                )

                cur.execute(
                    """
                    INSERT INTO delivery_attempt_logs (
                        event_id, attempt_number, worker_id, status_code,
                        latency_ms, error_category, response_excerpt, attempted_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event_id,
                        attempt_number,
                        worker_id,
                        status_code,
                        round(latency_ms, 2),
                        error_category,
                        (response_excerpt or "")[:500],
                        now_iso,
                    ),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

        return self.get_event_by_id(event_id)

    def replay_dead_letter_event(self, event_id: str) -> EventStatusResponse:
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("SELECT status FROM delivery_events WHERE event_id = ?", (event_id,))
            row = cur.fetchone()
            if not row:
                raise KeyError(f"Event {event_id} not found.")

            validate_state_transition(EventStatus(row["status"]), EventStatus.PENDING)
            cur.execute(
                """
                UPDATE delivery_events
                SET status = 'PENDING',
                    retry_count = 0,
                    next_retry_at = ?,
                    locked_by_worker = NULL,
                    last_error = NULL,
                    updated_at = ?
                WHERE event_id = ?
                """,
                (now_iso, now_iso, event_id),
            )
            self._conn.commit()

        return self.get_event_by_id(event_id)

    def list_dead_letter_events(self, tenant_id: Optional[str] = None) -> List[EventStatusResponse]:
        with self._lock:
            cur = self._conn.cursor()
            if tenant_id:
                cur.execute(
                    "SELECT event_id FROM delivery_events WHERE status = 'DEAD_LETTER' AND tenant_id = ? ORDER BY updated_at DESC",
                    (tenant_id,),
                )
            else:
                cur.execute(
                    "SELECT event_id FROM delivery_events WHERE status = 'DEAD_LETTER' ORDER BY updated_at DESC"
                )
            ids = [r["event_id"] for r in cur.fetchall()]
        return [self.get_event_by_id(eid) for eid in ids]

    def get_metrics_summary(self) -> Dict[str, Any]:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT status, COUNT(*) as cnt FROM delivery_events GROUP BY status"
            )
            counts = {row["status"]: int(row["cnt"]) for row in cur.fetchall()}
            cur.execute("SELECT COUNT(*) as total_attempts FROM delivery_attempt_logs")
            total_attempts = int(cur.fetchone()["total_attempts"])
            return {
                "events_by_status": {
                    s.value: counts.get(s.value, 0) for s in EventStatus
                },
                "total_delivery_attempts": total_attempts,
            }

    def get_event_by_id(self, event_id: str) -> EventStatusResponse:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM delivery_events WHERE event_id = ?", (event_id,))
            row = cur.fetchone()
            if not row:
                raise KeyError(f"Event {event_id} not found.")

            cur.execute(
                """
                SELECT attempt_number, status_code, latency_ms, error_category,
                       response_excerpt, attempted_at
                FROM delivery_attempt_logs
                WHERE event_id = ?
                ORDER BY attempt_number ASC
                """,
                (event_id,),
            )
            attempts = [
                AttemptLogResponse(
                    attempt_number=a["attempt_number"],
                    status_code=a["status_code"],
                    latency_ms=a["latency_ms"],
                    error_category=a["error_category"],
                    response_excerpt=a["response_excerpt"],
                    attempted_at=datetime.fromisoformat(a["attempted_at"]),
                )
                for a in cur.fetchall()
            ]

            return EventStatusResponse(
                event_id=row["event_id"],
                tenant_id=row["tenant_id"],
                idempotency_key=row["idempotency_key"],
                event_type=row["event_type"],
                channel=ChannelType(row["channel"]),
                destination=row["destination"],
                status=EventStatus(row["status"]),
                retry_count=row["retry_count"],
                max_retries=row["max_retries"],
                next_retry_at=datetime.fromisoformat(row["next_retry_at"]),
                last_error=row["last_error"],
                created_at=datetime.fromisoformat(row["created_at"]),
                updated_at=datetime.fromisoformat(row["updated_at"]),
                attempts=attempts,
            )
