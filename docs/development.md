# Development and operations

Install JDK 21 and Docker Desktop with Linux containers. IntelliJ can import `settings.gradle.kts` directly; set Gradle JVM and project SDK to Java 21. Gradle toolchains can download Java 21 through Foojay if necessary. No preview features or locally installed Gradle are needed.

On Windows use `./gradlew.bat`; on Unix use `./gradlew` (or `bash gradlew` when executable bits are not preserved). The project requires Gradle 9.6.0 through its wrapper.

## Local stack

```powershell
Copy-Item .env.example .env   # Optional; defaults are local-only
docker compose config --quiet
docker compose up --build -d --wait --wait-timeout 240
docker compose ps
./scripts/demo.ps1
docker compose logs -f workflow-orchestrator
docker compose down
```

Unix demo: `bash scripts/demo.sh` (curl and jq required). Windows demo uses built-in PowerShell HTTP/JSON support. For curl use `curl.exe` because older Windows PowerShell aliases `curl` to Invoke-WebRequest.

Start infrastructure only with `docker compose up -d postgres kafka kafka-init redis`. Application containers build from the root Dockerfile; each runtime includes only its own executable JAR. API starts after PostgreSQL, applies Flyway, and becomes ready before workers/orchestrator start. Kafka init creates the three topics and three DLTs. Infra volumes survive normal shutdown. `docker compose down -v` resets only this project's named volumes and destroys local data; no global Docker prune is used.

Allocate at least 6 GB to Docker Desktop. Default per-service memory/CPU limits suit a local demo; startup builds may need additional memory. A single Kafka broker and PostgreSQL instance are intentional. Compose retries are health-based, not fixed sleeps.

## Host services

The default Compose infrastructure is deliberately not exposed on host ports. Use a private, explicit override for host debugging:

```yaml
# compose.host.yml (local file)
services:
  postgres:
    ports: ["127.0.0.1:5432:5432"]
  redis:
    ports: ["127.0.0.1:6379:6379"]
```

For Kafka host access configure a second advertised listener as described in Kafka's official documentation, or use the native integration suite instead. Merely publishing 9092 is insufficient because Kafka advertises its internal hostname. Do not alter the default secure port exposure for convenience.

With reachable infrastructure, set the following environment variables before `:workflow-api:bootRun`, `:workflow-orchestrator:bootRun`, `:workflow-worker-java:bootRun` or `:workflow-worker-kotlin:bootRun` in separate terminals:

| Variable | Purpose |
|---|---|
| DATABASE_URL | JDBC PostgreSQL URL |
| DATABASE_USER / DATABASE_PASSWORD | Database account; password required |
| FLOWFORGE_API_KEY | API key (minimum 16 characters; API only) |
| KAFKA_BOOTSTRAP_SERVERS | Host-reachable advertised broker listeners |
| REDIS_HOST | Redis host (API only) |
| SERVER_PORT | Override service port; Kotlin host default also 8082, use 8083 |
| WORKER_CONCURRENCY | Listener concurrency, default 3 |
| FLOWFORGE_HTTP_ENABLED / FLOWFORGE_HTTP_ALLOWED_ORIGINS | Optional Java HTTP policy |

Start API first so migrations complete. Default ports: API 8080, orchestrator 8081, workers 8082 (Compose sets Kotlin 8083). `docker compose up -d --scale workflow-worker-java=2` adds a replica with no host port conflicts; three partitions bound active consumers. Scale orchestrator similarly: skip-locked execution/outbox claims support concurrent instances.

## Monitoring and diagnosis

Open Grafana <http://localhost:3000>, sign in with the local credentials documented in README, and open FlowForge Operations. Prometheus is private; inspect its logs with `docker compose logs prometheus`. Metrics are on each service's `/actuator/prometheus`; API requires X-API-Key. Use `docker compose exec` on the service network for private endpoints.

Track execution/history IDs, then correlate service logs via correlation ID. Do not log request bodies to debug failures. A growing pending task outbox suggests a broker/publisher problem; retries and timed-out steps can indicate missing worker capacity or rebalance delays. DLTs require operator inspection; no automatic replay tool is provided.

Application configuration requires environment secrets outside Compose. Production-inspired hosting should use private networks, TLS, separate least-privilege database roles and broker ACLs, an externally managed migration step, backups, and egress controls. No production deployment automation is configured.
