package dev.flowforge.contracts;

import static dev.flowforge.contracts.Contracts.*;

import java.util.*;

public final class WorkflowRules {
  private WorkflowRules() {}

  public static void validate(WorkflowRequest request) {
    var steps = request.steps();
    if (steps == null || steps.isEmpty())
      throw new IllegalArgumentException("At least one step is required");
    Set<String> known = new HashSet<>();
    for (int i = 0; i < steps.size(); i++) {
      var step = steps.get(i);
      if (step == null) throw new IllegalArgumentException("Steps must not contain null");
      if (step.executionOrder() != i || !known.add(step.id()))
        throw new IllegalArgumentException(
            "Step IDs must be unique and orders contiguous starting at zero");
      Set<String> previous = new HashSet<>();
      for (int j = 0; j < i; j++) previous.add(steps.get(j).id());
      if (!previous.containsAll(step.dependencies())
          || new HashSet<>(step.dependencies()).size() != step.dependencies().size())
        throw new IllegalArgumentException(
            "Dependencies must refer to distinct preceding steps; cycles are unsupported");
      if (i > 0 && !step.dependencies().contains(steps.get(i - 1).id()))
        throw new IllegalArgumentException(
            "Sequential steps must depend on their immediate predecessor");
      var config = step.configuration();
      switch (step.type()) {
        case DELAY -> {
          Object delay = config.get("durationMs");
          if (!(delay instanceof Number n) || n.longValue() < 0 || n.longValue() > 60000)
            throw new IllegalArgumentException("DELAY requires durationMs in 0..60000");
        }
        case JSON_TRANSFORM -> {
          if (!(config.get("set") instanceof Map<?, ?>))
            throw new IllegalArgumentException("JSON_TRANSFORM requires a set object");
        }
        case VALIDATE_JSON -> {
          if (!(config.get("required") instanceof List<?> list)
              || list.stream().anyMatch(v -> !(v instanceof String)))
            throw new IllegalArgumentException(
                "VALIDATE_JSON requires an array of required field names");
        }
        case HTTP_REQUEST -> {
          if (!(config.get("url") instanceof String))
            throw new IllegalArgumentException("HTTP_REQUEST requires url");
        }
        case RESULT_OUTPUT -> {}
      }
    }
  }

  public static boolean workflowTransition(WorkflowStatus from, WorkflowStatus to) {
    return switch (from) {
      case PENDING -> Set.of(WorkflowStatus.RUNNING, WorkflowStatus.CANCEL_REQUESTED).contains(to);
      case RUNNING ->
          Set.of(WorkflowStatus.COMPLETED, WorkflowStatus.FAILED, WorkflowStatus.CANCEL_REQUESTED)
              .contains(to);
      case CANCEL_REQUESTED -> to == WorkflowStatus.CANCELED;
      default -> false;
    };
  }

  public static boolean stepTransition(StepStatus from, StepStatus to) {
    return switch (from) {
      case PENDING, RETRY_SCHEDULED ->
          Set.of(StepStatus.DISPATCHED, StepStatus.CANCELED).contains(to);
      case DISPATCHED ->
          Set.of(
                  StepStatus.RUNNING,
                  StepStatus.COMPLETED,
                  StepStatus.FAILED,
                  StepStatus.TIMED_OUT,
                  StepStatus.CANCELED)
              .contains(to);
      case RUNNING ->
          Set.of(StepStatus.COMPLETED, StepStatus.FAILED, StepStatus.TIMED_OUT, StepStatus.CANCELED)
              .contains(to);
      case FAILED, TIMED_OUT -> to == StepStatus.RETRY_SCHEDULED;
      default -> false;
    };
  }

  public static long backoff(long initial, int attempt, double jitter) {
    if (initial < 1 || attempt < 1 || attempt > 10 || jitter < 0 || jitter > 1)
      throw new IllegalArgumentException("Invalid retry parameters");
    return (long) (Math.min(300000, initial * (1L << (attempt - 1))) * (0.75 + 0.5 * jitter));
  }

  public static boolean acceptsResult(int currentAttempt, int incomingAttempt, StepStatus status) {
    return currentAttempt == incomingAttempt
        && (status == StepStatus.DISPATCHED || status == StepStatus.RUNNING);
  }
}
