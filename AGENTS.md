# FlowForge engineering rules

Use Java 21, English documentation, and Flyway migrations. Keep transport contracts free of service internals. Never commit, push, or publish without the owner's instruction. Run unit tests with `gradlew test` and Docker-backed tests with `gradlew integrationTest`. Do not claim infrastructure checks passed without executing them. Read the relevant design documentation and docs/testing.md before changing behavior; distinguish recorded verification from checks executed in the current session.
