plugins { id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0" }
rootProject.name = "flowforge"
include("common-contracts", "workflow-api", "workflow-orchestrator", "worker-runtime", "workflow-worker-java", "workflow-worker-kotlin")
