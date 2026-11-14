package dev.flowforge.javaworker;

import static dev.flowforge.contracts.Contracts.*;
import static org.assertj.core.api.Assertions.*;

import dev.flowforge.worker.TaskFailure;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;

class JavaTaskHandlerTest {
  private final JavaTaskHandler handler = new JavaTaskHandler(new HttpTask(false, ""));

  private Task task(StepType type, Map<String, Object> config, Map<String, Object> input) {
    return new Task(
        UUID.randomUUID(),
        1,
        Instant.now(),
        "test",
        UUID.randomUUID(),
        UUID.randomUUID(),
        1,
        type,
        config,
        input,
        1000);
  }

  @Test
  void validationPassesAndRejectsMissingField() throws Exception {
    assertThat(
            handler.execute(
                task(
                    StepType.VALIDATE_JSON,
                    Map.of("required", List.of("name")),
                    Map.of("name", "Ada"))))
        .containsEntry("name", "Ada");
    assertThatThrownBy(
            () ->
                handler.execute(
                    task(StepType.VALIDATE_JSON, Map.of("required", List.of("name")), Map.of())))
        .isInstanceOf(TaskFailure.class)
        .satisfies(e -> assertThat(((TaskFailure) e).retryable()).isFalse());
  }

  @Test
  void outputProjectsFields() throws Exception {
    assertThat(
            handler.execute(
                task(
                    StepType.RESULT_OUTPUT,
                    Map.of("fields", List.of("name")),
                    Map.of("name", "Ada", "secret", "hidden"))))
        .containsExactlyEntriesOf(Map.of("name", "Ada"));
  }

  @Test
  void delayPreservesInput() throws Exception {
    assertThat(handler.execute(task(StepType.DELAY, Map.of("durationMs", 0), Map.of("ok", true))))
        .containsEntry("ok", true);
  }

  @Test
  void httpDisabledByDefault() {
    assertThatThrownBy(
            () ->
                handler.execute(
                    task(StepType.HTTP_REQUEST, Map.of("url", "https://example.com"), Map.of())))
        .isInstanceOf(TaskFailure.class)
        .hasMessage("HTTP tasks are disabled");
  }

  @Test
  void rejectsInternalAndUnlistedDestinations() {
    var policy = new HttpTask(true, "https://127.0.0.1,https://localhost");
    for (String url :
        List.of(
            "https://127.0.0.1",
            "https://localhost",
            "http://example.com",
            "https://example.com",
            "https://user@localhost"))
      assertThatThrownBy(() -> policy.validate(java.net.URI.create(url)))
          .isInstanceOf(TaskFailure.class);
  }

  @Test
  void delayCanBeInterrupted() {
    Thread.currentThread().interrupt();
    try {
      assertThatThrownBy(
              () -> handler.execute(task(StepType.DELAY, Map.of("durationMs", 100), Map.of())))
          .isInstanceOf(InterruptedException.class);
    } finally {
      Thread.interrupted();
    }
  }
}
