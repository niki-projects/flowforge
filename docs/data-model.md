# Data model

Flyway SQL under `workflow-api/src/main/resources/db/migration` is the authoritative schema. Hibernate validates; it never creates or updates tables at normal startup. All timestamps are `TIMESTAMPTZ`, and service times use UTC `Instant`.

| Table | Invariants / access |
|---|---|
| workflow_definition | UUID, application revision, optimistic lock version, JSONB step definitions |
| workflow_execution | Definition FK and captured version; JSONB input/output; status check; unique definition/idempotency key |
| step_execution | Execution FK; unique execution/order and execution/step ID; JSONB immutable definition snapshot; latest attempt, result and lease |
| execution_event | Monotonic history ID, execution FK; type, timestamp and bounded metadata |
| outbox_event | Stable message UUID, topic, payload, pending/published state, publication attempts and due time |
| inbox_message | Composite message/consumer primary key makes result application idempotent |
| worker_job | Unique step/attempt, owner token, lease, durable cached outcome |
| worker_outbox | Outcome payload, publication timestamp, publication attempt count and next due time |

Definitions embed ordered step definitions in JSONB because they are a small, bounded aggregate (maximum 50 steps), always validated and replaced together. Each step includes its ID, name, type, configuration, retry policy, deadline, order and dependencies. Execution creation expands this into normalized step execution rows. No entity is shared across services. Definition updates never change an existing execution's snapshot. DELETE disables a definition and preserves foreign keys/history.

Indexes support execution status/time filtering, bounded history pagination, step ordering, and pending outbox scans. API pagination is limited to 100 rows. Execution listing uses a page of IDs and bounded detail lookups; this favors simple DTO mapping over one large entity graph. Very deep offset pagination and a history count can still be expensive; cursor pagination is future work.

All engine mutations lock the parent execution first. API start locks its definition, serializing concurrent idempotency-key checks and definition changes. JPA definition edits also use optimistic version checking. Worker claims use PostgreSQL conditional upsert and unique constraints. Claim ownership is fenced on completion, so an old owner cannot overwrite a reclaimed attempt.

History retains 90 days; published task/result outboxes retain seven days in bounded hourly batches. Inbox and worker-job keys have no automatic expiry because deleting them without a bounded replay horizon could re-enable an old task. Plan archival/partitioning before high-volume use.
