# RelayCore — Fault-Tolerant Asynchronous Webhook & Event Delivery Engine

**Author:** Vedant Amit Agrawal ([GitHub](https://github.com/vedantagr0212/) | [LinkedIn](https://www.linkedin.com/in/vedant-agrawal-0ba2a7325))  
**Tech Stack:** Python 3.11, FastAPI, PostgreSQL 16, Redis 7 (Lua Scripting), AsyncIO / Multi-Worker Pools, SQLAlchemy, Docker Compose, PyTest, MyPy, Ruff

---

## 1. Problem Statement & Engineering Motivation

When enterprise applications (payment gateways, Customer Data Platforms, marketing orchestration pipelines) dispatch webhooks or notifications **synchronously** inside user-facing HTTP request threads, three failure modes occur under production load:
1. **Cascading Thread & Socket Starvation:** A slow or unresponsive third-party endpoint (`> 5s` latency or TCP hang) blocks upstream worker threads, exhausting connection pools across unrelated tenants.
2. **Duplicate Executions Under Concurrent Polling:** When multiple worker processes poll a database queue without row-level skip locking (`SELECT ... FOR UPDATE SKIP LOCKED`) or idempotency keys, race conditions cause the same payment or campaign event to be dispatched multiple times.
3. **Retry Storms (Thundering Herd):** Naive fixed-interval retries hammer recovering downstream servers, turning brief transient blips (`503 Service Unavailable` or `429 Too Many Requests`) into prolonged outages.

**RelayCore** solves these challenges by decoupling event ingestion from downstream HTTP/Slack/SMTP dispatch via an **ACID transactional outbox queue**, **PostgreSQL `FOR UPDATE SKIP LOCKED` concurrent worker pools**, **Redis Lua token-bucket rate limiting**, **3-state per-endpoint circuit breakers**, **exponential backoff with uniform jitter**, and **Dead-Letter Queue (DLQ) replay APIs**.

---

## 2. System Architecture

```text
+---------------------------------------------------------------------------------------------------+
|                                   CLIENT / UPSTREAM SERVICES                                      |
|                     POST /v1/events (Idempotency-Key, Tenant-ID, Channel, Payload)                |
+-------------------------------------------------+-------------------------------------------------+
                                                  |
                                                  v
+---------------------------------------------------------------------------------------------------+
|                             FASTAPI INGESTION & CONTROL PLANE (`app/main.py`)                     |
|  - Pydantic v2 Schema Validation (URL/Email format, payload rules, channel constraints)           |
|  - Idempotent Deduplication: UNIQUE (tenant_id, idempotency_key) -> 202 Accepted vs 200 Replay    |
|  - DLQ Inspection & Replay Endpoints (`POST /v1/dlq/{id}/replay`, `POST /v1/dlq/replay-batch`)    |
+-------------------------------------------------+-------------------------------------------------+
                                                  |
                                  Writes ACID Row | (status = 'PENDING')
                                                  v
+---------------------------------------------------------------------------------------------------+
|                        POSTGRESQL 16 TRANSACTIONAL QUEUE (`delivery_events`)                      |
|  - Partial Composite B-Tree Index: `ix_delivery_events_poll_queue ON (status, next_retry_at)`     |
|    WHERE `status IN ('PENDING', 'RETRY_SCHEDULED')`                                               |
|  - Immutable Attempt Audit Table: `delivery_attempt_logs` (status_code, latency_ms, error_cat)    |
+-------------------------------------------------+-------------------------------------------------+
                                                  |
                  Concurrent Non-Blocking Polling | `SELECT ... FOR UPDATE SKIP LOCKED LIMIT :batch`
                                                  v
+---------------------------------------------------------------------------------------------------+
|                     MULTI-WORKER DISPATCH POOL (`RelayCoreDispatchWorker`)                        |
|                                                                                                   |
|  +---------------------------------------------------------------------------------------------+  |
|  | Guard 1: Per-Tenant Token Bucket Rate Limiter (`app/services/rate_limiter.py`)              |  |
|  | - Atomic Redis Lua Script (`HMGET` -> Refill Math -> Deduct -> `HMSET`)                     |  |
|  | - If throttled: Defers event for exact token refill deficit without burning `retry_count`   |  |
|  +----------------------------------------------+----------------------------------------------+  |
|                                                 |                                                 |
|                                                 v                                                 |
|  +---------------------------------------------------------------------------------------------+  |
|  | Guard 2: 3-State Endpoint Circuit Breaker (`app/services/circuit_breaker.py`)               |  |
|  | - Isolated per destination host (`scheme://host/segment` or `smtp:domain`)                  |  |
|  | - States: CLOSED <-> OPEN (Fast-Fail) <-> HALF_OPEN (Bounded Probe Traffic)                 |  |
|  +----------------------------------------------+----------------------------------------------+  |
|                                                 |                                                 |
|                                                 v                                                 |
|  +---------------------------------------------------------------------------------------------+  |
|  | Pluggable Channel Dispatchers — Strategy Pattern (`app/services/dispatch_strategies.py`)    |  |
|  |  1. `WebhookHTTPDispatcher`     : HMAC-SHA256 Signing (`t=<ts>,v1=<hex>`), `Retry-After`    |  |
|  |  2. `SlackBlockKitDispatcher`   : Structured Slack Block Kit UI formatting                  |  |
|  |  3. `MockSMTPProtocolDispatcher`: RFC 5322 Multipart MIME + DKIM + 5-Step SMTP State Engine |  |
|  +---------------------------------------------------------------------------------------------+  |
+---------------------------------------------------------------------------------------------------+
```

---

## 3. Core Engineering Highlights

### 3.1 Lock-Free Concurrent Polling (`SELECT ... FOR UPDATE SKIP LOCKED`)
To prevent race conditions and head-of-line blocking across $N$ concurrent worker processes, workers claim batches atomically inside a single CTE transaction:

```sql
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
RETURNING de.*;
```

**Why Partial Composite Indexing Matters:**
Once 99% of historical events reach `DELIVERED`, a standard index on `(status, next_retry_at)` bloats with terminal rows. RelayCore defines a **partial B-Tree index**:
```sql
CREATE INDEX ix_delivery_events_poll_queue
    ON delivery_events (status, next_retry_at)
    WHERE status IN ('PENDING', 'RETRY_SCHEDULED');
```
This keeps the active polling index compact in RAM regardless of how many millions of `DELIVERED` rows accumulate.

---

### 3.2 Strict Lifecycle State Machine (LLD State Pattern)
Every event follows an explicit directed state transition graph enforced in `app/schemas/event_schemas.py`:

* `PENDING` $\rightarrow$ `PROCESSING` (when claimed by a worker) or `DEAD_LETTER`
* `PROCESSING` $\rightarrow$ `DELIVERED` (on `2xx` / `250 OK`), `RETRY_SCHEDULED` (on transient `408`/`429`/`5xx`/timeout), or `DEAD_LETTER` (on fatal `400`/`401`/`404`/`422` or when `retry_count > max_retries`)
* `RETRY_SCHEDULED` $\rightarrow$ `PROCESSING` (when `next_retry_at <= NOW()`)
* `DEAD_LETTER` $\rightarrow$ `PENDING` (exclusively via manual single or bulk DLQ replay API, which resets `retry_count = 0`)

---

### 3.3 Exponential Backoff with Uniform Jitter & `Retry-After` Respect
When a retryable failure occurs, the next execution timestamp is computed as:
$$t_{\text{delay}} = \min\left(T_{\max}, \max\left(\text{Retry-After}, T_{\text{base}} \cdot 2^{\text{attempt} - 1}\right)\right) + \mathcal{U}\left(0, \alpha \cdot t_{\text{exp}}\right)$$
* Honors downstream HTTP `Retry-After` headers on `429 Too Many Requests` or `503 Service Unavailable`.
* Adds uniform jitter ($\alpha = 0.25$) to spread out retry spikes across time and prevent synchronized thundering herds.

---

### 3.4 Cryptographic Webhook Security (`HMAC-SHA256` + Replay Window)
Every outbound webhook includes a Stripe/Razorpay-compatible signature header:
```text
X-RelayCore-Signature: t=1763450000,v1=5d8f62a94c...
```
* **Canonical Signed Message:** `f"{unix_timestamp}.{raw_utf8_json_body}"` signed with the tenant's `signing_secret` via `HMAC-SHA256`.
* **Replay Attack Defense:** `verify_webhook_signature()` rejects any request whose timestamp `t` diverges from current server time by more than `300 seconds` and compares digests using constant-time `hmac.compare_digest()`.

---

### 3.5 Built-In Stateful Chaos Target Server & Multi-Step Mock SMTP Engine
RelayCore ships with two realistic simulation engines for testing and live technical demonstrations:

1. **Stateful Partner Webhook Chaos Receiver (`app/mocks/chaos_receiver.py`):**
   * `/mock-target/healthy` (`HEALTHY_200`): Verifies `HMAC-SHA256` signature and records delivery in an idempotency audit ledger.
   * `/mock-target/flaky` (`FAIL_FIRST_N_THEN_200`): Deterministically returns `503 Service Unavailable` on attempts #1 and #2 for a given `event_id`, then recovers with `200 OK` on attempt #3.
   * `/mock-target/rate-limited` (`RATE_LIMITED_429`): Returns `429 Too Many Requests` with `Retry-After: 3` on the first attempt, then `200 OK` on retry.
   * `/mock-target/outage` (`ALWAYS_503_OUTAGE`): Simulates a complete downstream outage to trip the Circuit Breaker to `OPEN` and route events to `DEAD_LETTER` (can be toggled live to `HEALTHY_200` via `POST /v1/chaos/configure` to demonstrate DLQ bulk replay).
   * `/mock-target/fatal-422` (`FATAL_422_SCHEMA_ERROR`): Returns non-retryable `422 Unprocessable Entity`, routing the event immediately to `DEAD_LETTER` on attempt #1 without wasting retries.

2. **RFC 5321 / 5322 Mock SMTP Protocol Engine (`MockSMTPProtocolDispatcher`):**
   * Generates a real `multipart/alternative` MIME message (Plain Text + HTML) with `DKIM-Signature` and executes a 5-stage SMTP session transcript (`220` $\rightarrow$ `EHLO` $\rightarrow$ `STARTTLS` $\rightarrow$ `MAIL/RCPT` $\rightarrow$ `DATA 250 OK`).
   * Simulates MX greylisting (`*@throttled-mx.example.com` $\rightarrow$ `421 4.7.0`), mailbox-full soft bounces (`*@mailbox-full.example.com` $\rightarrow$ `452 4.2.2`), and user-unknown hard bounces (`hardbounce@*` $\rightarrow$ `550 5.1.1`).

---

## 4. Quick Start & Running Locally

### Option A: Docker Compose (PostgreSQL 16 + Redis 7 + FastAPI + Worker Pool)
```bash
docker compose up --build
```
* **Swagger / OpenAPI Interactive Docs:** `http://localhost:8000/docs`
* **Health Check:** `http://localhost:8000/health`

### Option B: Local Python Execution & Automated Test Suite
```bash
pip install -r requirements.txt

# Run the complete 12-test unit, concurrency, and chaos integration suite
pytest -v
# or using standard library unittest:
python -m unittest discover -s tests -v

# Start FastAPI server locally
uvicorn app.main:app --reload --port 8000
```

---

## 5. End-to-End Live Demo Walkthrough (`curl`)

### Step 1: Ingest an Event Targeting a Flaky Partner Endpoint
```bash
curl -X POST http://localhost:8000/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "acme_payments_prod",
    "idempotency_key": "evt_demo_2026_01",
    "event_type": "campaign.conversion.recorded",
    "channel": "WEBHOOK",
    "destination": "http://localhost:8000/mock-target/flaky",
    "payload": {"campaign_id": "cmp_77", "revenue_inr": 14500},
    "max_retries": 4
  }'
```
*Returns `202 Accepted` with `"status": "PENDING"`. Re-running the exact same `curl` command returns `200 OK` with `"is_idempotent_replay": true` and the identical `event_id`.*

### Step 2: Simulate Downstream Outage $\rightarrow$ Circuit Breaker Trip $\rightarrow$ DLQ Bulk Replay
```bash
# 1. Ingest event to /mock-target/outage (which returns 503 Service Unavailable)
curl -X POST http://localhost:8000/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "acme_payments_prod",
    "idempotency_key": "evt_outage_01",
    "event_type": "order.created",
    "channel": "WEBHOOK",
    "destination": "http://localhost:8000/mock-target/outage",
    "payload": {"order_id": "ORD-9001"},
    "max_retries": 1
  }'

# 2. Inspect Dead-Letter Queue (DLQ) after retries exhaust
curl http://localhost:8000/v1/dlq?tenant_id=acme_payments_prod

# 3. Recover downstream endpoint from ALWAYS_503_OUTAGE -> HEALTHY_200
curl -X POST http://localhost:8000/v1/chaos/configure \
  -H "Content-Type: application/json" \
  -d '{"route_path": "/mock-target/outage", "mode": "HEALTHY_200"}'

# 4. Trigger Bulk DLQ Replay for the tenant
curl -X POST "http://localhost:8000/v1/dlq/replay-batch?tenant_id=acme_payments_prod"

# 5. Trigger worker dispatch and view Observability Dashboard
curl -X POST http://localhost:8000/v1/workers/run-once
curl http://localhost:8000/v1/observability/dashboard
```

---

## 6. Repository Structure

```text
relaycore/
├── app/
│   ├── api/
│   │   └── service_controller.py    # Layered Application Controller (Ingestion, DLQ Replay, Metrics)
│   ├── core/
│   │   ├── config.py                # Environment-driven operational settings
│   │   └── security.py              # HMAC-SHA256 signing & constant-time replay-window verification
│   ├── db/
│   │   └── schema.sql               # Normalized PostgreSQL DDL & partial composite B-Tree indexes
│   ├── mocks/
│   │   └── chaos_receiver.py        # Stateful partner webhook receiver with 7 chaos modes
│   ├── repositories/
│   │   └── event_repository.py      # ACID repository, SKIP LOCKED polling, backoff & DLQ transitions
│   ├── schemas/
│   │   └── event_schemas.py         # Pydantic v2 models & State Machine transition validator
│   ├── services/
│   │   ├── circuit_breaker.py       # 3-State Endpoint Circuit Breaker (CLOSED / OPEN / HALF_OPEN)
│   │   ├── dispatch_strategies.py   # Strategy Pattern: WebhookHTTP, SlackBlockKit, MockSMTP
│   │   └── rate_limiter.py          # Atomic Redis Lua Token-Bucket Rate Limiter
│   ├── workers/
│   │   ├── dispatch_worker.py       # Fault-tolerant batch processor
│   │   └── run_worker_pool.py       # Multi-threaded daemon worker pool runner
│   └── main.py                      # FastAPI application & OpenAPI routes
├── tests/
│   ├── test_phase1_core.py                        # HMAC security, idempotency, 8-worker concurrency, DLQ
│   ├── test_phase2_resilience_and_strategies.py   # Token bucket, Circuit Breaker, Slack & SMTP bounce tests
│   └── test_phase3_worker_chaos_and_api.py        # End-to-end flaky recovery, outage fast-fail & bulk replay
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── requirements.txt
└── README.md
```
