plugins { id("org.springframework.boot") }
dependencies {
    implementation(project(":common-contracts"))
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    implementation("org.flywaydb:flyway-database-postgresql")
    implementation("org.springdoc:springdoc-openapi-starter-webmvc-ui:3.0.3")
    runtimeOnly("org.postgresql:postgresql")
    runtimeOnly("io.micrometer:micrometer-registry-prometheus")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.testcontainers:testcontainers-postgresql")
    testImplementation("org.testcontainers:testcontainers-kafka")
    testImplementation("org.awaitility:awaitility")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation("io.zonky.test:embedded-postgres:2.2.2")
    testImplementation(enforcedPlatform("io.zonky.test.postgres:embedded-postgres-binaries-bom:18.6.0"))
}
tasks.named("integrationTest") {
    dependsOn(":workflow-api:bootJar", ":workflow-orchestrator:bootJar", ":workflow-worker-java:bootJar", ":workflow-worker-kotlin:bootJar")
}
tasks.register<Test>("nativeIntegrationTest") {
    description = "Runs real PostgreSQL binaries and an embedded Kafka broker without Docker."
    dependsOn("bootJar", ":workflow-orchestrator:bootJar", ":workflow-worker-java:bootJar", ":workflow-worker-kotlin:bootJar")
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform { includeTags("integration") }
    systemProperty("flowforge.root", rootProject.projectDir.absolutePath)
    systemProperty("flowforge.test.backend", "native")
    maxHeapSize = "1g"
}
