# ADR 002: PostgreSQL and transactional outboxes

Status: Accepted

Execution transitions, attempt counts and scheduling must remain correct under concurrent orchestrators and restarts. Use PostgreSQL constraints, parent row locking, transactions, JSONB payloads and Flyway. Business mutations and corresponding outbox inserts commit atomically. Publishers wait for Kafka acknowledgment before marking publication. Consumers deduplicate by message identity or task attempt.

Consequences: at-least-once publication and duplicate-safe consumers are explicit. A bounded broker send holds one outbox row lock and database connection; monitoring and small batches limit the cost. API/orchestrator share a schema to keep the workflow aggregate consistent without a distributed transaction. Worker claim/outbox tables are logically worker-owned, although local deployment uses the same database/account.

Alternatives: XA would increase coupling and recovery complexity. Direct database update followed by Kafka send can lose dispatch on crash. CDC-based outbox publishing is a future option if throughput justifies Debezium and its operational cost. Redis is limited to advisory rate limiting and never owns execution state.
