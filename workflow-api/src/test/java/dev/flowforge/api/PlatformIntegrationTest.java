package dev.flowforge.api;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import dev.flowforge.contracts.Contracts;
import dev.flowforge.contracts.Contracts.*;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.junit.jupiter.api.*;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.ObjectMapper;

/** Same full-service scenarios run against Testcontainers or native PostgreSQL + KRaft. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PlatformIntegrationTest {
  private final Path root = Path.of(System.getProperty("flowforge.root"));
  private final ObjectMapper json = new ObjectMapper();
  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
  private final Map<String, Process> processes = new LinkedHashMap<>();
  private final Map<String, Integer> ports = new HashMap<>();
  private final String apiKey = "integration-test-api-key";
  private PostgreSQLContainer postgres;
  private KafkaContainer kafka;
  private EmbeddedPostgres nativePg;
  private EmbeddedKafkaKraftBroker nativeKafka;
  private String jdbcUrl, username, password, brokers;
  private KafkaProducer<String, String> producer;

  @BeforeAll
  void start() throws Exception {
    Files.createDirectories(root.resolve("workflow-api/build/integration-logs"));
    if ("native".equals(System.getProperty("flowforge.test.backend"))) {
      nativePg = EmbeddedPostgres.builder().setPort(0).start();
      jdbcUrl = nativePg.getJdbcUrl("postgres", "postgres");
      username = "postgres";
      password = "integration-password";
      nativeKafka = new EmbeddedKafkaKraftBroker(1, 3);
      nativeKafka.brokerProperties(
          Map.of("auto.create.topics.enable", "false", "offsets.topic.replication.factor", "1"));
      nativeKafka.afterPropertiesSet();
      brokers = nativeKafka.getBrokersAsString();
    } else {
      postgres =
          new PostgreSQLContainer("postgres:17.7-alpine")
              .withDatabaseName("flowforge")
              .withUsername("flowforge")
              .withPassword("integration-password");
      postgres.start();
      jdbcUrl = postgres.getJdbcUrl();
      username = postgres.getUsername();
      password = postgres.getPassword();
      kafka = new KafkaContainer("apache/kafka:4.2.2");
      kafka.start();
      brokers = kafka.getBootstrapServers();
    }
    try (var admin = AdminClient.create(Map.of("bootstrap.servers", brokers))) {
      var topics = new ArrayList<NewTopic>();
      for (String topic :
          List.of(Contracts.JAVA_TASKS, Contracts.KOTLIN_TASKS, Contracts.RESULTS)) {
        topics.add(new NewTopic(topic, 3, (short) 1));
        topics.add(new NewTopic(topic + ".DLT", 3, (short) 1));
      }
      admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
    }
    producer =
        new KafkaProducer<>(
            Map.of(
                "bootstrap.servers",
                brokers,
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer",
                "acks",
                "all"));
    launch("workflow-api");
    launch("workflow-orchestrator");
    launch("workflow-worker-java");
    launch("workflow-worker-kotlin");
  }

  private void launch(String module) throws Exception {
    int port =
        ports.computeIfAbsent(
            module,
            key -> {
              try (var socket = new java.net.ServerSocket(0)) {
                return socket.getLocalPort();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    var builder =
        new ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Xmx256m",
            "-jar",
            root.resolve(module + "/build/libs/" + module + "-1.0.0.jar").toString());
    builder
        .directory(root.toFile())
        .redirectErrorStream(true)
        .redirectOutput(
            root.resolve("workflow-api/build/integration-logs/" + module + ".log").toFile());
    builder
        .environment()
        .putAll(
            Map.of(
                "DATABASE_URL",
                jdbcUrl,
                "DATABASE_USER",
                username,
                "DATABASE_PASSWORD",
                password,
                "KAFKA_BOOTSTRAP_SERVERS",
                brokers,
                "FLOWFORGE_API_KEY",
                apiKey,
                "SERVER_PORT",
                Integer.toString(port),
                "SPRING_MAIN_BANNER_MODE",
                "off"));
    processes.put(module, builder.start());
    await()
        .atMost(Duration.ofSeconds(120))
        .pollInterval(Duration.ofMillis(300))
        .ignoreExceptions()
        .failFast(
            module + " terminated; see build/integration-logs",
            () -> !processes.get(module).isAlive())
        .untilAsserted(
            () -> {
              assertThat(processes.get(module).isAlive())
                  .as("%s process; see build/integration-logs", module)
                  .isTrue();
              assertThat(
                      http.send(
                              HttpRequest.newBuilder(
                                      URI.create(
                                          "http://localhost:"
                                              + port
                                              + "/actuator/health/readiness"))
                                  .timeout(Duration.ofSeconds(2))
                                  .build(),
                              HttpResponse.BodyHandlers.ofString())
                          .statusCode())
                  .isEqualTo(200);
            });
  }

  @AfterAll
  void stop() throws Exception {
    for (var process : processes.values()) process.destroy();
    for (var process : processes.values())
      if (!process.waitFor(15, TimeUnit.SECONDS))
        process.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
    if (producer != null) producer.close(Duration.ofSeconds(3));
    if (nativeKafka != null) nativeKafka.destroy();
    if (nativePg != null) nativePg.close();
    if (kafka != null) kafka.stop();
    if (postgres != null) postgres.stop();
  }

  @Test
  @Order(1)
  void fullWorkflowCrossesBothWorkersAndPersistsOutput() throws Exception {
    var workflow =
        create(
            List.of(
                step(
                    "validate",
                    "VALIDATE_JSON",
                    Map.of("required", List.of("name")),
                    0,
                    List.of(),
                    1,
                    1000),
                step(
                    "transform",
                    "JSON_TRANSFORM",
                    Map.of("set", Map.of("processed", true), "uppercase", List.of("name")),
                    1,
                    List.of("validate"),
                    1,
                    1000),
                step("output", "RESULT_OUTPUT", Map.of(), 2, List.of("transform"), 1, 1000)));
    var execution = start(workflow, Map.of("name", "Ada"), "happy-path");
    var done = terminal(execution, "COMPLETED");
    assertThat(((Map<?, ?>) done.get("output")).get("name")).isEqualTo("ADA");
    assertThat(((Map<?, ?>) done.get("output")).get("processed")).isEqualTo(true);
    assertThat(get("/executions/" + execution + "/steps").body()).contains("COMPLETED");
    assertThat(get("/executions/" + execution + "/events").body())
        .contains("StepExecutionStarted", "WorkflowCompleted");
    assertThat(start(workflow, Map.of("name", "Ada"), "happy-path")).isEqualTo(execution);
    assertThat(
            post(
                    "/workflows/" + workflow + "/executions",
                    Map.of("input", Map.of("name", "different")),
                    "happy-path")
                .statusCode())
        .isEqualTo(409);
    assertThat(
            scalar(
                "SELECT count(*) FROM outbox_event WHERE aggregate_id='"
                    + execution
                    + "' AND status='PUBLISHED'"))
        .isEqualTo(3);
  }

  @Test
  @Order(2)
  void invalidDefinitionAndUnauthorizedRequestsAreRejected() throws Exception {
    assertThat(
            post(
                    "/workflows",
                    Map.of(
                        "name",
                        "null-step",
                        "enabled",
                        true,
                        "steps",
                        Arrays.asList((Object) null)),
                    null)
                .statusCode())
        .isEqualTo(400);
    assertThat(
            post(
                    "/workflows",
                    Map.of(
                        "name",
                        "invalid",
                        "enabled",
                        true,
                        "steps",
                        List.of(
                            step(
                                "cycle", "RESULT_OUTPUT", Map.of(), 0, List.of("cycle"), 1, 1000))),
                    null)
                .statusCode())
        .isEqualTo(400);
    var response =
        http.send(
            HttpRequest.newBuilder(URI.create(base() + "/api/v1/workflows")).build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(401);
    assertThat(response.body()).contains("UNAUTHORIZED");
    assertThat(get("/workflows/" + UUID.randomUUID()).statusCode()).isEqualTo(404);
  }

  @Test
  @Order(3)
  void permanentValidationFailureDoesNotRetryAndManualRetryCreatesNewExecution() throws Exception {
    var workflow =
        create(
            List.of(
                step(
                    "required",
                    "VALIDATE_JSON",
                    Map.of("required", List.of("missing")),
                    0,
                    List.of(),
                    3,
                    1000)));
    var id = start(workflow, Map.of(), null);
    terminal(id, "FAILED");
    assertThat(scalar("SELECT attempt FROM step_execution WHERE execution_id='" + id + "'"))
        .isEqualTo(1);
    var response = post("/executions/" + id + "/retry", null, "manual-retry");
    assertThat(response.statusCode()).isEqualTo(202);
    String next = map(response.body()).get("id").toString();
    assertThat(next).isNotEqualTo(id);
    terminal(next, "FAILED");
  }

  @Test
  @Order(4)
  void timeoutRetriesThenExhaustsAttempts() throws Exception {
    var workflow =
        create(List.of(step("timeout", "DELAY", Map.of("durationMs", 1000), 0, List.of(), 2, 100)));
    var id = start(workflow, Map.of(), null);
    terminal(id, "FAILED");
    assertThat(scalar("SELECT attempt FROM step_execution WHERE execution_id='" + id + "'"))
        .isEqualTo(2);
    assertThat(get("/executions/" + id + "/events").body())
        .contains("StepRetryScheduled", "StepExecutionTimedOut");
  }

  @Test
  @Order(5)
  void cancellationWinsOverLateWorkerResult() throws Exception {
    var workflow =
        create(List.of(step("delay", "DELAY", Map.of("durationMs", 1500), 0, List.of(), 1, 5000)));
    var id = start(workflow, Map.of(), null);
    await()
        .atMost(Duration.ofSeconds(15))
        .until(
            () ->
                scalar(
                        "SELECT count(*) FROM step_execution WHERE execution_id='"
                            + id
                            + "' AND status='RUNNING'")
                    == 1);
    assertThat(post("/executions/" + id + "/cancel", null, null).statusCode()).isEqualTo(202);
    terminal(id, "CANCELED");
    await()
        .atMost(Duration.ofSeconds(15))
        .until(
            () ->
                scalar(
                        "SELECT count(*) FROM worker_job w JOIN step_execution s ON w.step_id=s.id WHERE s.execution_id='"
                            + id
                            + "' AND w.status='COMPLETED'")
                    == 1);
    assertThat(map(get("/executions/" + id).body()).get("status")).isEqualTo("CANCELED");
  }

  @Test
  @Order(6)
  void duplicateTasksHaveOneDurableClaimAndDuplicateResultsCannotReopenExecution()
      throws Exception {
    var workflow =
        create(List.of(step("result", "RESULT_OUTPUT", Map.of(), 0, List.of(), 1, 1000)));
    var id = start(workflow, Map.of("value", 42), null);
    terminal(id, "COMPLETED");
    String payload = string("SELECT payload FROM outbox_event WHERE aggregate_id='" + id + "'");
    for (int i = 0; i < 8; i++)
      producer
          .send(new ProducerRecord<>(Contracts.JAVA_TASKS, "duplicate-" + i, payload))
          .get(10, TimeUnit.SECONDS);
    String result =
        string(
            "SELECT result FROM worker_job WHERE step_id=(SELECT id FROM step_execution WHERE execution_id='"
                + id
                + "')");
    for (int i = 0; i < 3; i++)
      producer.send(new ProducerRecord<>(Contracts.RESULTS, id, result)).get(10, TimeUnit.SECONDS);
    awaitConsumed(Contracts.JAVA_TASKS, "workflow-worker-java-v1");
    awaitConsumed(Contracts.RESULTS, "flowforge-orchestrator-v1");
    assertThat(
            scalar(
                "SELECT count(*) FROM execution_event WHERE execution_id='"
                    + id
                    + "' AND event_type='StepExecutionCompleted'"))
        .isEqualTo(1);
    assertThat(
            scalar(
                "SELECT count(*) FROM inbox_message WHERE message_id='"
                    + map(result).get("messageId")
                    + "' AND consumer_name='orchestrator'"))
        .isEqualTo(1);
    assertThat(
            scalar(
                "SELECT count(*) FROM worker_job WHERE step_id=(SELECT id FROM step_execution WHERE execution_id='"
                    + id
                    + "')"))
        .isEqualTo(1);
    assertThat(map(get("/executions/" + id).body()).get("status")).isEqualTo("COMPLETED");
  }

  @Test
  @Order(7)
  void crashedWorkerIsRecoveredWithANewAttempt() throws Exception {
    var workflow =
        create(List.of(step("crash", "DELAY", Map.of("durationMs", 3000), 0, List.of(), 2, 10000)));
    var id = start(workflow, Map.of("recovered", true), null);
    await()
        .atMost(Duration.ofSeconds(15))
        .until(
            () ->
                scalar(
                        "SELECT count(*) FROM step_execution WHERE execution_id='"
                            + id
                            + "' AND status='RUNNING'")
                    == 1);
    processes.get("workflow-worker-java").destroyForcibly().waitFor(10, TimeUnit.SECONDS);
    try (var connection = connection();
        var statement = connection.createStatement()) {
      // Advance the persisted deadline instead of waiting 40 seconds; recovery path is unchanged.
      statement.executeUpdate(
          "UPDATE step_execution SET lease_until=now()-interval '1 second' WHERE execution_id='"
              + id
              + "'");
    }
    launch("workflow-worker-java");
    terminal(id, "COMPLETED");
    assertThat(scalar("SELECT attempt FROM step_execution WHERE execution_id='" + id + "'"))
        .isEqualTo(2);
    assertThat(get("/executions/" + id + "/events").body()).contains("LEASE_EXPIRED");
  }

  @Test
  @Order(8)
  void malformedMessagesAreDeadLetteredWithoutBlockingHealthyTasks() throws Exception {
    producer
        .send(new ProducerRecord<>(Contracts.JAVA_TASKS, "poison", "{broken-json"))
        .get(10, TimeUnit.SECONDS);
    try (var admin = AdminClient.create(Map.of("bootstrap.servers", brokers))) {
      var partitions = new HashMap<org.apache.kafka.common.TopicPartition, OffsetSpec>();
      for (int p = 0; p < 3; p++)
        partitions.put(
            new org.apache.kafka.common.TopicPartition(Contracts.JAVA_TASKS + ".DLT", p),
            OffsetSpec.latest());
      await()
          .atMost(Duration.ofSeconds(20))
          .until(
              () ->
                  admin.listOffsets(partitions).all().get().values().stream()
                          .mapToLong(ListOffsetsResult.ListOffsetsResultInfo::offset)
                          .sum()
                      > 0);
    }
    var workflow =
        create(List.of(step("healthy", "RESULT_OUTPUT", Map.of(), 0, List.of(), 1, 1000)));
    terminal(start(workflow, Map.of(), null), "COMPLETED");
  }

  @Test
  @Order(9)
  void concurrentStartsUseOneExecutionAndConcurrentWorkersUseOneClaim() throws Exception {
    String workflow =
        create(List.of(step("concurrent", "RESULT_OUTPUT", Map.of(), 0, List.of(), 1, 1000)));
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var futures = new ArrayList<Future<String>>();
      for (int i = 0; i < 8; i++)
        futures.add(executor.submit(() -> start(workflow, Map.of("same", true), "concurrent-key")));
      Set<String> ids = new HashSet<>();
      for (var future : futures) ids.add(future.get(20, TimeUnit.SECONDS));
      assertThat(ids).hasSize(1);
      terminal(ids.iterator().next(), "COMPLETED");
    }
    UUID stepId = UUID.randomUUID();
    var task =
        new Task(
            UUID.randomUUID(),
            1,
            Instant.now(),
            "concurrent-claim",
            UUID.randomUUID(),
            stepId,
            1,
            StepType.DELAY,
            Map.of("durationMs", 1200),
            Map.of("claimed", true),
            5000);
    for (int partition = 0; partition < 3; partition++)
      producer
          .send(
              new ProducerRecord<>(
                  Contracts.JAVA_TASKS, partition, "same-claim", json.writeValueAsString(task)))
          .get(10, TimeUnit.SECONDS);
    await()
        .atMost(Duration.ofSeconds(20))
        .until(
            () ->
                scalar(
                        "SELECT count(*) FROM worker_job WHERE step_id='"
                            + stepId
                            + "' AND status='COMPLETED'")
                    == 1);
    awaitConsumed(Contracts.JAVA_TASKS, "workflow-worker-java-v1");
    assertThat(
            scalar(
                "SELECT count(*) FROM worker_outbox WHERE step_id='"
                    + stepId
                    + "' AND payload::jsonb->>'outcome'='COMPLETED'"))
        .isEqualTo(1);
  }

  @Test
  @Order(10)
  void sampleWorkflowCrudOpenApiAndMetricsMatchDocumentation() throws Exception {
    var sample = map(Files.readString(root.resolve("samples/customer-workflow.json")));
    var created = post("/workflows", sample, null);
    assertThat(created.statusCode()).isEqualTo(201);
    String workflow = map(created.body()).get("id").toString();
    var execution = start(workflow, Map.of("name", "Ada", "email", "ada@example.com"), null);
    assertThat(((Map<?, ?>) terminal(execution, "COMPLETED").get("output")).get("onboardingStatus"))
        .isEqualTo("READY");
    var delete =
        http.send(
            HttpRequest.newBuilder(URI.create(base() + "/api/v1/workflows/" + workflow))
                .header("X-API-Key", apiKey)
                .DELETE()
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(delete.statusCode()).isEqualTo(204);
    assertThat(
            post("/workflows/" + workflow + "/executions", Map.of("input", Map.of()), null)
                .statusCode())
        .isEqualTo(409);
    var update =
        http.send(
            HttpRequest.newBuilder(URI.create(base() + "/api/v1/workflows/" + workflow))
                .header("X-API-Key", apiKey)
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(sample)))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(update.statusCode()).isEqualTo(200);
    assertThat(map(update.body()).get("version")).isEqualTo(2);
    var schema =
        http.send(
            HttpRequest.newBuilder(URI.create(base() + "/v3/api-docs")).build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(schema.statusCode()).isEqualTo(200);
    assertThat(schema.body()).contains("openapi", "X-API-Key", "WorkflowRequest");
    var metrics =
        http.send(
            HttpRequest.newBuilder(URI.create(base() + "/actuator/prometheus"))
                .header("X-API-Key", apiKey)
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(metrics.statusCode()).isEqualTo(200);
    assertThat(metrics.body()).contains("jvm_memory");
    assertThat(
            Files.readString(root.resolve("workflow-api/build/integration-logs/workflow-api.log")))
        .contains("\"service\":\"workflow-api\"");
  }

  @Test
  @Order(11)
  void oversizedPayloadAndInvalidPaginationReturnProblems() throws Exception {
    assertThat(post("/workflows", Map.of("large", "x".repeat(262145)), null).statusCode())
        .isEqualTo(413);
    assertThat(get("/executions?size=0").statusCode()).isEqualTo(400);
    assertThat(get("/executions?status=UNKNOWN").statusCode()).isEqualTo(400);
    assertThat(
            post("/workflows/" + UUID.randomUUID() + "/executions", Map.of("input", Map.of()), null)
                .statusCode())
        .isEqualTo(404);
  }

  private void awaitConsumed(String topic, String group) throws Exception {
    try (var admin = AdminClient.create(Map.of("bootstrap.servers", brokers))) {
      var latest = new HashMap<org.apache.kafka.common.TopicPartition, OffsetSpec>();
      for (int p = 0; p < 3; p++)
        latest.put(new org.apache.kafka.common.TopicPartition(topic, p), OffsetSpec.latest());
      var ends = admin.listOffsets(latest).all().get(10, TimeUnit.SECONDS);
      await()
          .atMost(Duration.ofSeconds(20))
          .until(
              () -> {
                var committed =
                    admin
                        .listConsumerGroupOffsets(group)
                        .partitionsToOffsetAndMetadata()
                        .get(10, TimeUnit.SECONDS);
                return ends.entrySet().stream()
                    .allMatch(
                        entry ->
                            committed.containsKey(entry.getKey())
                                && committed.get(entry.getKey()).offset()
                                    >= entry.getValue().offset());
              });
    }
  }

  private Map<String, Object> step(
      String id,
      String type,
      Map<String, Object> configuration,
      int order,
      List<String> dependencies,
      int attempts,
      long timeout) {
    return Map.of(
        "id",
        id,
        "name",
        id,
        "type",
        type,
        "configuration",
        configuration,
        "retryPolicy",
        Map.of("maxAttempts", attempts, "initialDelayMs", 100),
        "timeoutMs",
        timeout,
        "executionOrder",
        order,
        "dependencies",
        dependencies);
  }

  private String create(List<Map<String, Object>> steps) throws Exception {
    var response =
        post(
            "/workflows",
            Map.of(
                "name",
                "integration-" + UUID.randomUUID(),
                "description",
                "integration",
                "enabled",
                true,
                "steps",
                steps),
            null);
    assertThat(response.statusCode()).as(response.body()).isEqualTo(201);
    return map(response.body()).get("id").toString();
  }

  private String start(String workflow, Map<String, Object> input, String key) throws Exception {
    var response = post("/workflows/" + workflow + "/executions", Map.of("input", input), key);
    assertThat(response.statusCode()).as(response.body()).isEqualTo(202);
    return map(response.body()).get("id").toString();
  }

  private Map<String, Object> terminal(String id, String expected) {
    await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () ->
                assertThat(map(get("/executions/" + id).body()).get("status"))
                    .as(get("/executions/" + id + "/events").body())
                    .isEqualTo(expected));
    try {
      return map(get("/executions/" + id).body());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String base() {
    return "http://localhost:" + ports.get("workflow-api");
  }

  private HttpResponse<String> get(String path) throws Exception {
    return http.send(
        HttpRequest.newBuilder(URI.create(base() + "/api/v1" + path))
            .header("X-API-Key", apiKey)
            .timeout(Duration.ofSeconds(10))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> post(String path, Object body, String key) throws Exception {
    var request =
        HttpRequest.newBuilder(URI.create(base() + "/api/v1" + path))
            .header("X-API-Key", apiKey)
            .header("Content-Type", "application/json")
            .timeout(Duration.ofSeconds(10));
    if (key != null) request.header("Idempotency-Key", key);
    return http.send(
        request
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    body == null ? "" : json.writeValueAsString(body)))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> map(String body) {
    return json.readValue(body, Map.class);
  }

  private Connection connection() throws SQLException {
    return DriverManager.getConnection(jdbcUrl, username, password);
  }

  private long scalar(String sql) throws SQLException {
    try (var c = connection();
        var s = c.createStatement();
        var r = s.executeQuery(sql)) {
      r.next();
      return r.getLong(1);
    }
  }

  private String string(String sql) throws SQLException {
    try (var c = connection();
        var s = c.createStatement();
        var r = s.executeQuery(sql)) {
      r.next();
      return r.getString(1);
    }
  }
}
