# Security model

This is a local engineering demonstration with explicit boundaries, not a hardened public multi-tenant service. There is one administrator API key, no user identity or tenant isolation, and no encrypted internal transport in the local stack.

## API

`FLOWFORGE_API_KEY` is mandatory outside Compose and must have at least 16 characters. The filter compares its UTF-8 bytes using constant-time comparison. Provide it as `X-API-Key`, never in a URL. Health endpoints expose no details. Swagger assets and schemas are public, and actual management calls require the key. API info/metrics also require the key. Worker/orchestrator operational endpoints are accessible only on private deployment networks.

Request validation bounds names, definition count, attempts and deadlines. Streaming body reads stop after 256 KiB plus one byte, including chunked requests. UUID and enum parsing errors return sanitized problems. Rate limiting uses one Redis fixed-window counter for the shared management identity: 120 requests/minute. The increment and initial expiry are atomic Lua operations. Redis data is advisory and expires after 60 seconds; no cache invalidation is necessary. Redis unavailability bypasses this limiter, so edge rate limiting remains necessary for exposed hosting.

Data, API keys, HTTP URLs and exception details are not logged by application code. Correlation IDs are sanitized before MDC insertion. Structured service logs can include infrastructure connection metadata; protect operational logs. Input/output data remains in PostgreSQL; protect backups and define a retention policy for real personal data.

## HTTP task controls

Disabled by default. To enable in the Java worker set `FLOWFORGE_HTTP_ENABLED=true` and `FLOWFORGE_HTTP_ALLOWED_ORIGINS=https://api.example.com` (comma-separated, exact lower-case origins, optional explicit nonstandard port). No wildcard hostnames are accepted.

- HTTPS only, GET only; no userinfo, fragments, arbitrary authorization headers or request bodies.
- Exact origin allowlist before DNS lookup.
- Every resolved address is checked: loopback, unspecified, link-local, RFC1918/site-local, multicast, IPv6 ULA, carrier-grade NAT and selected special-use ranges are rejected.
- Redirect limit is zero; all 3xx responses fail permanently.
- Connect timeout is three seconds; HTTP request deadline at most ten seconds and worker deadline at most 120 seconds.
- Streaming response capped at 65536 bytes; stream closes on completion/failure.
- HTTP 429/5xx and I/O errors are transient; ordinary 4xx and policy errors are permanent.

The Java HTTP client resolves DNS separately from validation; DNS rebinding/TOCTOU cannot be eliminated by this code alone. Special-use address classification is conservative but not an exhaustive registry implementation. Use a controlled egress proxy, firewall and fixed approved destinations before enabling HTTP with untrusted definitions. Do not allow metadata service access. There is intentionally no private-network bypass for convenience.

Cancellation and timeouts do not guarantee remote rollback or termination of an already accepted request. An upstream requiring idempotency must provide its own idempotency protocol; the current integration intentionally supports read-only GET behavior only.

## Infrastructure and supply chain

Compose publishes API/Grafana to loopback, not infrastructure ports. Application containers run as UID 10001 with a read-only root filesystem, dropped capabilities, bounded memory and no-new-privileges. `/tmp` is writable through bounded tmpfs. Internal services use local plaintext networking and a shared database identity; separate roles, migration credentials, TLS, broker ACLs and secret management are deployment work.

`.env.example` and Compose defaults are labeled local-only. `.env` is ignored. Application YAML does not provide default passwords or API keys. Do not commit credentials or use the local defaults in hosted environments. Do not place secrets in workflow input/configuration: database state and legitimate API readers can access these values.

Dependencies are selected through the Boot BOM and pinned direct tools. Run dependency reports (`gradlew dependencies`), review current upstream advisories, enable GitHub Dependabot, and scan built images before deployment. A clean build does not prove absence of vulnerabilities. CI uses read-only repository permissions and does not publish images or deploy infrastructure.
