# Delivery, concurrency and failure handling

The durable guarantee is at-least-once delivery with idempotent state application. PostgreSQL and Kafka do not participate in a distributed transaction. Arbitrary external side effects cannot be made exactly once by this engine.

## Transaction boundaries

1. Start: definition lock, execution row, snapshotted steps and lifecycle history commit together.
2. Dispatch: parent execution lock, step attempt increment/deadline, history and task outbox commit together.
3. Worker claim: unique step/attempt ownership and STARTED outbox commit together. Handler runs outside a database transaction.
4. Worker finish: compare owner token, persist outcome and result outbox together. Kafka task offset commits after the listener returns.
5. Result: parent execution lock, inbox insert, attempt check, step state/history/retry or workflow failure commit together. Kafka offset commits after return.

The task outbox publisher holds one row lock during a broker send bounded to five seconds. Concurrent publishers skip locked rows. After acknowledgment it marks the row published; after failure it schedules capped exponential backoff. This simple approach consumes a connection during broker acknowledgment, an explicit tradeoff. Worker outbox rows also use skip-locked publishing; failure stops that batch and persists capped exponential backoff with jitter before the next attempt.

## Failure matrix

| Failure | Behavior |
|---|---|
| Worker crashes after receive, before claim | Offset remains uncommitted; Kafka redelivers |
| Worker crashes after claim | Claim remains; scheduler detects expired dispatch lease and creates a new attempt within policy |
| Worker completes but crashes before task acknowledgment | Result/outbox are durable; replay sees completed claim and does not invoke handler again |
| Crash after Kafka publish, before outbox update | Same stable message can be published again; inbox/claim constraints absorb duplicates |
| Duplicate tasks arrive at two workers | Unique step/attempt and conditional claim grant one owner; active duplicates return without executing |
| Original claim expires and is reclaimed | New owner token fences late completion by the old owner |
| Old result arrives after retry | Attempt mismatch is ignored; old result cannot advance the new attempt |
| Kafka unavailable | Pending outboxes remain durable; bounded sends fail and retry; consumers recover connection |
| PostgreSQL unavailable | Transaction rolls back; transient persistence failures retry in the Kafka listener without exhausting into DLT; API returns a sanitized problem |
| Redis unavailable | Advisory rate limiting fails open; no durable state depends on Redis |
| Task exceeds deadline | Worker interrupts its future, publishes TIMED_OUT, and engine applies retry policy; dispatch lease also recovers missing outcomes |
| Cancellation races with result | Parent lock serializes decisions; CANCEL_REQUESTED stops new work and all later outcomes are ignored |
| HTTP 503 or 429 | Retryable task failure, exponential backoff and bounded attempts |
| Invalid JSON / unsupported schema | Bounded listener retries then matching-partition DLT; a failed DLT send is not treated as successful recovery |
| Old queued task after dispatch deadline | Worker discards the stale attempt; scheduler has authority to recover or fail it |

`RUNNING` records actual worker acknowledgment and is separate from dispatch. A result can legally complete a DISPATCHED step if STARTED arrives late. STARTED after completion is ignored. Lease expiry is `timeoutMs + 30 seconds` from dispatch, not a heartbeat. Clocks should be synchronized. Worker claim lease is `timeoutMs + 10 seconds` from claim. Consumer session timeout is ten seconds with a three-second heartbeat so a crashed member does not delay reassignment beyond the normal recovery margin. Slow queues can exhaust attempts even when handlers are healthy, so monitor backlog and tune deployment capacity. Shutdown is bounded: work that cannot finish before the container grace period relies on the same lease recovery path.

## Retry and cancellation policy

Retry delay = `min(300000ms, initialDelay × 2^(attempt−1)) × uniform(0.75,1.25)`. Maximum attempts includes the first attempt and is limited to 10. Validation/configuration/HTTP-policy failures are permanent. HTTP I/O, HTTP 429/5xx, timeout and lease expiry are transient. History records each attempt's failure and retry delay; step rows expose latest state.

Cancellation is cooperative at the orchestration boundary: no new steps, pending/active rows become CANCELED, late outcomes cannot reopen the execution. A worker already running may finish or an HTTP request may reach its target. No compensation or remote interrupt protocol is implemented. Manual retry only accepts FAILED and creates a new execution with the current enabled definition and original input.

## Operations

Inspect `.DLT` payloads using Kafka tooling inside the Compose network. Fix the producer/schema/configuration before replay. For malformed task messages, a valid replay must retain step ID/attempt and satisfy the lease; otherwise the engine's recovery attempt is authoritative. Never manufacture a result for an arbitrary completed step. There is no public DLT replay endpoint.

Single-node local infrastructure has no HA guarantee. Worker interruption, timeouts, retry exhaustion, malformed delivery, cancellation and duplicate processing have automated full-service scenarios. Local broker interruption verified durable task/result backoff and recovery; Redis interruption and database API failure/recovery were also exercised. Transaction-boundary crash injection, sustained outages and long-duration retention still require further verification before production use.
