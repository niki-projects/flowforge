# ADR 003: Sequential workflow execution first

Status: Accepted

Retries, cancellation, recovery and idempotent completion are more valuable initial scope than a broad unverified DAG engine. Require contiguous ordered steps and immediate-predecessor dependencies; permit only references to earlier steps. Persist dependencies and definition snapshots for a later DAG extension.

Consequences: one active step per workflow execution; each step receives the preceding successful output. Cycles, forward references, duplicate IDs and unsupported branching are rejected. Independent workflow executions still run concurrently. Explicit parent execution locks simplify cancellation/result races. Parallel DAG execution will require join semantics, per-edge data flow, partial failure rules and corresponding tests; storing dependencies alone does not claim those features.
