ALTER TABLE worker_outbox ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_outbox ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX worker_outbox_due_idx ON worker_outbox (next_attempt_at) WHERE published_at IS NULL;
