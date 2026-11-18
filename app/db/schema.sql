-- RelayCore Normalized PostgreSQL Schema (DDL)
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    signing_secret VARCHAR(128) NOT NULL,
    rate_limit_capacity INTEGER NOT NULL DEFAULT 20,
    rate_limit_refill_per_sec DOUBLE PRECISION NOT NULL DEFAULT 10.0,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS delivery_events (
    event_id VARCHAR(36) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    idempotency_key VARCHAR(128) NOT NULL,
    event_type VARCHAR(96) NOT NULL,
    channel VARCHAR(16) NOT NULL CHECK (channel IN ('WEBHOOK', 'SLACK', 'EMAIL')),
    destination VARCHAR(512) NOT NULL,
    payload_json TEXT NOT NULL,
    custom_headers_json TEXT NOT NULL DEFAULT '{}',
    status VARCHAR(24) NOT NULL CHECK (status IN ('PENDING', 'PROCESSING', 'RETRY_SCHEDULED', 'DELIVERED', 'DEAD_LETTER')),
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 5,
    next_retry_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    locked_by_worker VARCHAR(64),
    last_error TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tenant_idempotency UNIQUE (tenant_id, idempotency_key)
);

-- Partial Composite B-Tree Index for High-Throughput Transactional Worker Polling
CREATE INDEX IF NOT EXISTS ix_delivery_events_poll_queue
    ON delivery_events (status, next_retry_at)
    WHERE status IN ('PENDING', 'RETRY_SCHEDULED');

-- Index for Tenant & DLQ operational queries
CREATE INDEX IF NOT EXISTS ix_delivery_events_tenant_status
    ON delivery_events (tenant_id, status, created_at DESC);

CREATE TABLE IF NOT EXISTS delivery_attempt_logs (
    attempt_id SERIAL PRIMARY KEY,
    event_id VARCHAR(36) NOT NULL REFERENCES delivery_events(event_id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    worker_id VARCHAR(64) NOT NULL,
    status_code INTEGER,
    latency_ms DOUBLE PRECISION NOT NULL,
    error_category VARCHAR(64),
    response_excerpt TEXT,
    attempted_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_attempt_logs_event_id
    ON delivery_attempt_logs (event_id, attempt_number ASC);
