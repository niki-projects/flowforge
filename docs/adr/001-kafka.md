# ADR 001: Kafka for asynchronous task and outcome delivery

Status: Accepted

FlowForge needs durable asynchronous handoff, consumer groups, partition ordering and replay for operational diagnosis. Kafka is widely used in enterprise Java systems and has mature Spring integration. Use separate task topics for Java/Kotlin capability routing and one result topic, with versioned JSON records and DLTs.

Consequences: consumers and database state must tolerate duplicates. Partition count bounds parallel consumption. The transactional outbox bridges database state to Kafka; producer idempotence alone is insufficient. Kafka is more infrastructure than a small task queue requires, but demonstrates realistic distributed delivery. It is not used as the authoritative execution store or a general event-sourcing log.

Alternatives: a PostgreSQL-only queue would simplify local deployment but omit broker failure boundaries. RabbitMQ would fit task delivery; Kafka was selected for durable stream/group semantics and enterprise familiarity, not a claim of universal superiority.
