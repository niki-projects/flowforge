# Testing and verification

## Commands

```powershell
./gradlew.bat build
./gradlew.bat integrationTest
./gradlew.bat :workflow-api:nativeIntegrationTest
./gradlew.bat jacocoTestReport
```

Unit tests exclude the JUnit `integration` tag. The separate `integrationTest` task selects only that tag and starts PostgreSQL/Kafka through Testcontainers. Docker must be running; this task fails rather than silently skipping when Docker is unavailable. Build dependencies prepare all four executable JARs before the platform tests.

The native alternative uses Zonky PostgreSQL 17.7 platform binaries and Spring Kafka's `EmbeddedKafkaKraftBroker`. These are real processes/protocols: four service JARs run as child JVMs, PostgreSQL uses its real SQL engine, and task/result delivery crosses a real KRaft broker. Native tests do not validate Dockerfiles, networking, image availability, Redis or monitoring containers. They require binary execution support and may fail under root on Unix because PostgreSQL refuses root execution; use Docker-backed tests there.

Tests reserve local ports and start only test-owned processes. Startup uses bounded Awaitility polling, not arbitrary sleeps. Service readiness alone does not prove that consumers own partitions; end-to-end assertions wait for actual outcomes. Child processes and broker/database resources are closed in teardown. Logs are under `workflow-api/build/integration-logs`; reports under each module's `build/reports/tests`. No test runtime state belongs in Git.

## Coverage

Unit tests cover sequential dependency validation, cycles/duplicates/orders, explicit transition policy, attempt fencing, retry bounds/jitter, handler configuration, required-field validation, result projection, interruptible delay, HTTP policy denial, Kotlin transformation immutability, and bounded task contracts.

The full-service suite covers:

1. Create/start/poll a workflow across both workers, output/history, durable task publication and request idempotency/conflicts.
2. Invalid definitions, unauthorized access, and missing resources.
3. Permanent failures without automatic retry; manual retry creates a separate execution.
4. Timeout, retry scheduling and exhausted attempts.
5. Cancellation during active work and late result protection.
6. Duplicate task/results and durable worker claims.
7. Forced Java worker process termination and restart; persisted deadline is advanced to exercise lease recovery without waiting the full timeout.
8. Malformed task routing to DLT, followed by healthy task completion.
9. Eight concurrent idempotent starts produce one execution; duplicate active tasks across three partitions produce one completed worker outcome.
10. The checked-in sample, definition update/disable, OpenAPI, metrics and structured service names match documentation.
11. Oversized request bodies, invalid pagination/status and starting a nonexistent definition return appropriate problems.

JaCoCo is enabled for host unit tests; it does not instrument child service JVMs, so its report must not be presented as whole-system coverage. No arbitrary coverage target is claimed. Failure tests inspect persisted state and bounded asynchronous completion, not logs alone.

## Recorded local verification

The following results were recorded on 2026-11-14 during implementation. They are historical evidence, not checks rerun for repository cleanup or a statement of current infrastructure health.

Docker Desktop 4.99.0 and Linux engine 29.9.3 were running; Compose v5.7.0 was available. Docker reported approximately 3.76 GiB memory, below the README recommendation of 6 GB, but the local stack started successfully.

| Check | Actual result |
|---|---|
| Fresh baseline `gradlew test build integrationTest jacocoTestReport --rerun-tasks` | Passed: 19 unit tests, 11 Docker-backed full-service scenarios; 3m 42s |
| Fixed `gradlew test build integrationTest jacocoTestReport` | Passed: 21 unit tests, 11 Docker-backed scenarios; 3m 8s; zero failures/errors/skips in integration XML |
| `gradlew :workflow-api:nativeIntegrationTest` | Passed: all 11 real-service scenarios on native PostgreSQL/embedded KRaft; 3m 15s |
| Formatting | `gradlew spotlessApply` succeeded; fixed build includes Spotless checks |
| Full Compose build/start | All four application images built; `docker compose up --build -d --wait --wait-timeout 240` succeeded |
| Fixed image rerun | Rebuilt all four images (container Gradle build 7m 56s), restarted with retained volumes, verified HTTP 400 for null steps and successful sample completion; all targets up and both outboxes empty |
| Health/dependencies | Nine long-running services healthy; Kafka initialization exited zero; both Flyway migrations applied successfully |
| PowerShell demo | Completed; `ADA`, `ada@example.com`, `onboardingStatus=READY` |
| Bash demo | Completed with the same output in a Linux container with curl/jq on the Compose network; an earlier attempt before API readiness failed and was rerun successfully |
| Communication | Demo crossed API/database, orchestrator/Kafka and both workers; Redis counter and monitoring connections checked directly |
| Monitoring | All four Prometheus scrape targets up; Grafana authenticated API exposed FlowForge Operations; provisioned Prometheus datasource health returned OK |
| Configuration | `docker compose config --quiet` and `promtool check config --syntax-only` passed; sample/Postman/dashboard JSON parsed |
| CI | actionlint 1.7.12 passed; referenced checkout v6, setup-java v6, gradle/actions v5 and upload-artifact v7 tags resolved through GitHub API; Java 21, Gradle tasks and Compose commands match the project |

Temurin Java 21.0.13 launched host verification. The configured versions remain Gradle 9.6.0, Kotlin 2.3.20, Spring Boot 4.1.2, PostgreSQL 17.7, Kafka 4.2.2, Redis 7.4.7, Prometheus 3.8.0 and Grafana 12.3.0.

### Reliability verification

The automated real PostgreSQL/Kafka suite exercised cross-worker output/history; idempotent start/conflicts; invalid and unauthorized requests; permanent failure and manual retry; timeouts/backoff/exhaustion; active cancellation and late outcomes; duplicate tasks/results; forced worker termination/restart and lease recovery; malformed-message DLT handling; concurrent idempotent starts/worker claims; sample CRUD/OpenAPI/metrics; body limits and pagination. Crash recovery advances the persisted lease deadline in the test instead of waiting the full lease. Both task and worker result publishers have unit checks for acknowledgment and failure backoff.

Additional live Compose fault injection stopped Kafka during an active delay. The worker finished while Kafka was down, and its result remained unpublished with persisted retry attempts. Another execution retained its pending task outbox with failed publication attempts. After restarting Kafka, both executions completed and both outbox backlogs drained to zero. Stopping Redis preserved authenticated API availability. Stopping PostgreSQL produced HTTP 503; restarting it restored HTTP 200. All stopped infrastructure was restored.

Transaction annotations/templates, row locks, unique constraints and owner/attempt comparisons were reviewed. Delivery remains at least once: the review does not establish exactly-once external effects or exhaustive crash safety at every instruction/commit boundary.

### Security review

Unauthenticated management, API metrics and API info returned HTTP 401. Health, Swagger and OpenAPI returned HTTP 200 as documented. API-key minimum length, constant-time comparison, correlation validation, body bounds, sanitized API errors and internal-only worker management ports were reviewed. Only API 8080 and Grafana 3000 are published, both on 127.0.0.1. Application images run as UID 10001 with read-only root filesystems, dropped capabilities and no-new-privileges. Local Compose credentials and integration-test keys are intentional examples.

Live Redis limiting returned 120 HTTP 200 responses and then HTTP 429 on request 121. Retry-After was 60; the expiring counter reached 121. The verification counter was cleared afterward. Redis outage separately verified failure-open behavior.

A bounded source scan found no matching access-token, cloud credential or private-key patterns. This does not certify absence of every possible secret. HTTP integration remains disabled by default; exact-origin HTTPS restrictions, address checks, redirect denial and response bounds are present. Its documented DNS rebinding risk remains and requires external egress controls. Internal plaintext traffic, shared database credentials and one administration key remain local-development limitations. No dependency vulnerability certification is claimed.

### Verification boundaries

Hosted GitHub Actions was not triggered or observed. Local actionlint and command execution validate configuration and local compatibility, not hosted CI success. Public HTTPS integration, sustained outage/load/retention tests, production availability, every transactional crash boundary and external side-effect deduplication were not verified. The destructive README `docker compose down -v` command was not exercised; normal `docker compose down` was executed and named volumes retained. JaCoCo covers host unit JVMs, not child service processes.

Test reports are written to each module's ignored `build/test-results` and `build/reports/tests` directories.

A disposable Docker context export also confirmed exclusion of private environment files and inclusion of the example environment file during that recorded run.
