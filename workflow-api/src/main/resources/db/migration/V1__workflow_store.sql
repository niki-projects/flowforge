CREATE TABLE workflow_definition (
    id UUID PRIMARY KEY,
    name VARCHAR(120) NOT NULL,
    description VARCHAR(2000),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    enabled BOOLEAN NOT NULL,
    steps JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lock_version BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE workflow_execution (
    id UUID PRIMARY KEY,
    definition_id UUID NOT NULL REFERENCES workflow_definition(id),
    workflow_version INTEGER NOT NULL,
    status VARCHAR(30) NOT NULL CHECK (status IN ('PENDING','RUNNING','COMPLETED','FAILED','CANCEL_REQUESTED','CANCELED')),
    input JSONB NOT NULL,
    output JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    failure_reason VARCHAR(500),
    correlation_id VARCHAR(100) NOT NULL,
    idempotency_key VARCHAR(100),
    UNIQUE (definition_id, idempotency_key)
);
CREATE INDEX executions_created_idx ON workflow_execution (created_at DESC, id);
CREATE INDEX executions_active_idx ON workflow_execution (status, created_at);
CREATE TABLE step_execution (
    id UUID PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES workflow_execution(id),
    definition_step_id VARCHAR(80) NOT NULL,
    execution_order INTEGER NOT NULL,
    definition JSONB NOT NULL,
    status VARCHAR(30) NOT NULL CHECK (status IN ('PENDING','DISPATCHED','RUNNING','RETRY_SCHEDULED','COMPLETED','FAILED','CANCELED','TIMED_OUT')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    assigned_worker VARCHAR(160),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    lease_until TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    error_code VARCHAR(80),
    error_message VARCHAR(500),
    output JSONB NOT NULL DEFAULT '{}',
    UNIQUE (execution_id, execution_order),
    UNIQUE (execution_id, definition_step_id)
);
CREATE INDEX steps_execution_idx ON step_execution (execution_id, execution_order);
CREATE TABLE execution_event (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES workflow_execution(id),
    event_type VARCHAR(80) NOT NULL,
    details JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX history_execution_idx ON execution_event (execution_id, id);
CREATE TABLE outbox_event (
    id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(80) NOT NULL,
    topic VARCHAR(120) NOT NULL,
    payload TEXT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','PUBLISHED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX outbox_pending_idx ON outbox_event (next_attempt_at) WHERE status='PENDING';
CREATE TABLE inbox_message (
    message_id UUID NOT NULL,
    consumer_name VARCHAR(80) NOT NULL,
    processing_status VARCHAR(20) NOT NULL DEFAULT 'PROCESSED',
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (message_id, consumer_name)
);
-- Workers own these records. A task attempt has a durable claim and cached outcome.
CREATE TABLE worker_job (
    step_id UUID NOT NULL,
    attempt INTEGER NOT NULL,
    owner VARCHAR(160) NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('RUNNING','COMPLETED')),
    result TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (step_id, attempt)
);
CREATE TABLE worker_outbox (
    id UUID PRIMARY KEY,
    step_id UUID NOT NULL,
    payload TEXT NOT NULL,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX worker_outbox_pending_idx ON worker_outbox (created_at) WHERE published_at IS NULL;
