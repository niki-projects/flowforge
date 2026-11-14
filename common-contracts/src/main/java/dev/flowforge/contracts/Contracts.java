package dev.flowforge.contracts;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.time.Instant;
import java.util.*;

/** Public REST and versioned Kafka schemas; no persistence or service implementation types. */
public final class Contracts {
  private Contracts() {}

  public enum StepType {
    JSON_TRANSFORM,
    DELAY,
    HTTP_REQUEST,
    VALIDATE_JSON,
    RESULT_OUTPUT
  }

  public enum WorkflowStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCEL_REQUESTED,
    CANCELED
  }

  public enum StepStatus {
    PENDING,
    DISPATCHED,
    RUNNING,
    RETRY_SCHEDULED,
    COMPLETED,
    FAILED,
    CANCELED,
    TIMED_OUT
  }

  public record RetryPolicy(
      @Min(1) @Max(10) int maxAttempts, @Min(100) @Max(60000) long initialDelayMs) {}

  public record StepDefinition(
      @NotBlank @Size(max = 80) String id,
      @NotBlank @Size(max = 120) String name,
      @NotNull StepType type,
      @NotNull Map<String, Object> configuration,
      @NotNull @Valid RetryPolicy retryPolicy,
      @Min(100) @Max(120000) long timeoutMs,
      @Min(0) int executionOrder,
      @NotNull List<String> dependencies) {}

  public record WorkflowRequest(
      @NotBlank @Size(max = 120) String name,
      @Size(max = 2000) String description,
      boolean enabled,
      @NotEmpty @Size(max = 50) List<@NotNull @Valid StepDefinition> steps) {}

  public record WorkflowView(
      UUID id,
      String name,
      String description,
      int version,
      boolean enabled,
      List<StepDefinition> steps,
      Instant createdAt,
      Instant updatedAt) {}

  public record StartRequest(@NotNull Map<String, Object> input) {}

  public record ExecutionView(
      UUID id,
      UUID workflowDefinitionId,
      int workflowVersion,
      WorkflowStatus status,
      Map<String, Object> input,
      Map<String, Object> output,
      Instant createdAt,
      Instant startedAt,
      Instant completedAt,
      String failureReason,
      String correlationId) {}

  public record StepView(
      UUID id,
      UUID workflowExecutionId,
      String stepDefinitionId,
      StepStatus status,
      int attempt,
      String assignedWorker,
      Instant startedAt,
      Instant completedAt,
      String errorCode,
      String errorMessage,
      Map<String, Object> output) {}

  public record HistoryView(
      long id, String eventType, Instant createdAt, Map<String, Object> details) {}

  public record Page<T>(List<T> content, int page, int size, long totalElements) {}

  public record Task(
      UUID messageId,
      int schemaVersion,
      Instant timestamp,
      String correlationId,
      UUID executionId,
      UUID stepExecutionId,
      int attempt,
      StepType type,
      Map<String, Object> configuration,
      Map<String, Object> input,
      long timeoutMs) {}

  public record Result(
      UUID messageId,
      int schemaVersion,
      Instant timestamp,
      String correlationId,
      UUID executionId,
      UUID stepExecutionId,
      int attempt,
      String workerId,
      String outcome,
      Map<String, Object> output,
      String errorCode,
      String errorMessage,
      boolean retryable) {}

  public static final String JAVA_TASKS = "flowforge.tasks.java.v1";
  public static final String KOTLIN_TASKS = "flowforge.tasks.kotlin.v1";
  public static final String RESULTS = "flowforge.results.v1";
}
