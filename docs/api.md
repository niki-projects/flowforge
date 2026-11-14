# API reference

Base path: `/api/v1`. Send `X-API-Key` on every management request. JSON request bodies are limited to 262144 bytes, with no more than 50 workflow steps. Swagger is at `/swagger-ui/index.html`; OpenAPI JSON is at `/v3/api-docs`. Use the Authorize button to supply the key.

## Contracts

`WorkflowRequest`: `name` (1–120 characters), optional `description` (up to 2000), `enabled`, and `steps` (1–50). Each step has `id`, `name`, `type`, `configuration` object, `retryPolicy` (`maxAttempts` 1–10, `initialDelayMs` 100–60000), `timeoutMs` 100–120000, `executionOrder` (contiguous, zero-based), and `dependencies` array. Every step after the first must depend on its immediate predecessor; other dependencies must refer to earlier steps. Duplicate IDs and cycles are rejected.

`WorkflowView` adds UUID `id`, integer `version`, UTC `createdAt` and `updatedAt`. `StartRequest` is `{ "input": { ... } }`. `ExecutionView` includes UUID `id`, `workflowDefinitionId`, `workflowVersion`, `status`, JSON `input`/`output`, lifecycle timestamps, nullable `failureReason`, and `correlationId`.

`StepView` includes UUID `id`, `workflowExecutionId`, `stepDefinitionId`, latest `status`, `attempt`, `assignedWorker`, timestamps, `errorCode`, `errorMessage` and JSON `output`. Attempts start at zero (not dispatched). `HistoryView` includes monotonic `id`, `eventType`, `createdAt` and metadata `details`. Payload data is not copied into history.

Paged responses contain `content`, `page`, `size`, and `totalElements`. Default page is zero, size 20, maximum size 100. Execution and definition lists sort by creation descending; histories sort by event ID ascending. Lists do not load the full execution history into memory.

## Endpoints

| Method / path | Response / behavior |
|---|---|
| POST /workflows | 201 definition |
| GET /workflows?page=0&size=20 | 200 paged definitions |
| GET /workflows/{id} | 200 definition, 404 if missing |
| PUT /workflows/{id} | 200 updated definition; increments revision; optimistic conflicts 409 |
| DELETE /workflows/{id} | 204; disables, preserves history |
| POST /workflows/{id}/executions | 202 execution; disabled definition 409 |
| GET /executions?status=RUNNING&page=0&size=20 | 200 paged execution views; optional status filter |
| GET /executions/{id} | 200 execution |
| GET /executions/{id}/steps | 200 bounded list (maximum 50) |
| GET /executions/{id}/events?page=0&size=20 | 200 paged history |
| POST /executions/{id}/cancel | 202 request; terminal execution 409; repeated cancel idempotent |
| POST /executions/{id}/retry | 202 new execution; only FAILED eligible |

Start and retry accept optional `Idempotency-Key` (1–100 characters). Scope is the target workflow definition. An identical key/input returns the prior execution, including if it has completed; different input is 409. Supply a new key for an intentional new execution. Request deduplication is independent from Kafka deduplication. Manual retry uses the latest enabled definition, so inspect its version before retrying an older failure.

`X-Correlation-ID` accepts 1–100 letters, digits, dots, underscores or hyphens; otherwise a fresh UUID is generated. The response echoes it. Retry retains the previous execution's correlation ID.

## Handler configuration

| Type / worker | Configuration / result |
|---|---|
| VALIDATE_JSON / Java | `required: ["name"]`; rejects missing/null top-level fields, preserves input |
| JSON_TRANSFORM / Kotlin | Required `set: {"processed":true}`; optional `remove: ["temporary"]`, `uppercase: ["name"]`; shallow copy with set/remove/uppercase in that order |
| DELAY / Java | `durationMs: 0..60000`; preserves input; consumes one listener slot |
| RESULT_OUTPUT / Java | Optional `fields: ["name"]` projection; otherwise preserves all input |
| HTTP_REQUEST / Java | `url: "https://allowed.example/path"`; policy-controlled GET; merges `httpStatus` and UTF-8 `httpBody` into input |

No expression execution, arbitrary script language, nested JSON-path operations, arbitrary HTTP methods/headers, or JSON Schema engine is provided. Optional transform arrays should contain field names; entries of other types are ignored. Inputs and outputs must be JSON objects. Output is bounded to 256 KiB; HTTP body to 64 KiB.

## Errors

```json
{
  "type": "urn:flowforge:problem:workflow_disabled",
  "title": "Conflict",
  "status": 409,
  "detail": "Workflow is disabled",
  "code": "WORKFLOW_DISABLED",
  "correlationId": "customer-001"
}
```

Stable error codes include `INVALID_REQUEST`, `UNAUTHORIZED`, `RATE_LIMITED`, `PAYLOAD_TOO_LARGE`, `WORKFLOW_NOT_FOUND`, `EXECUTION_NOT_FOUND`, `WORKFLOW_DISABLED`, `IDEMPOTENCY_CONFLICT`, `INVALID_TRANSITION`, `CONCURRENT_MODIFICATION`, `RESOURCE_NOT_FOUND`, `METHOD_NOT_ALLOWED`, `UNSUPPORTED_MEDIA_TYPE`, and `SERVICE_UNAVAILABLE`. Exceptions do not expose stack traces, SQL, payloads or upstream response bodies. `429` includes `Retry-After: 60`.

Operational endpoints: health is public with no details; API info and Prometheus require the key. Other service management ports are internal to the Compose network and should stay private in any deployment.
