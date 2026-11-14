package dev.flowforge.contracts;

import static dev.flowforge.contracts.Contracts.*;
import static org.assertj.core.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;

class WorkflowRulesTest {
  @Test
  void rejectsNullStepsAsInvalidDefinitions() {
    assertThatThrownBy(
            () ->
                WorkflowRules.validate(
                    new WorkflowRequest("invalid", "", true, Arrays.asList((StepDefinition) null))))
        .isInstanceOf(IllegalArgumentException.class);
  }

  static StepDefinition step(String id, int order, List<String> dependencies) {
    return new StepDefinition(
        id,
        id,
        StepType.RESULT_OUTPUT,
        Map.of(),
        new RetryPolicy(3, 100),
        1000,
        order,
        dependencies);
  }

  @Test
  void acceptsSequentialDefinitions() {
    assertThatCode(
            () ->
                WorkflowRules.validate(
                    new WorkflowRequest(
                        "valid",
                        "",
                        true,
                        List.of(step("one", 0, List.of()), step("two", 1, List.of("one"))))))
        .doesNotThrowAnyException();
  }

  @Test
  void rejectsCyclesAndMissingDependencies() {
    assertThatThrownBy(
            () ->
                WorkflowRules.validate(
                    new WorkflowRequest(
                        "cycle",
                        "",
                        true,
                        List.of(step("one", 0, List.of("two")), step("two", 1, List.of("one"))))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                WorkflowRules.validate(
                    new WorkflowRequest(
                        "gap",
                        "",
                        true,
                        List.of(step("one", 0, List.of()), step("two", 1, List.of())))))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsDuplicateIdsAndOrders() {
    assertThatThrownBy(
            () ->
                WorkflowRules.validate(
                    new WorkflowRequest(
                        "duplicate",
                        "",
                        true,
                        List.of(step("one", 0, List.of()), step("one", 1, List.of("one"))))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                WorkflowRules.validate(
                    new WorkflowRequest("order", "", true, List.of(step("one", 3, List.of())))))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void terminalStatesCannotBeReopened() {
    for (var state :
        List.of(WorkflowStatus.COMPLETED, WorkflowStatus.CANCELED, WorkflowStatus.FAILED))
      for (var target : WorkflowStatus.values())
        assertThat(WorkflowRules.workflowTransition(state, target)).isFalse();
    assertThat(
            WorkflowRules.workflowTransition(
                WorkflowStatus.CANCEL_REQUESTED, WorkflowStatus.CANCELED))
        .isTrue();
    assertThat(
            WorkflowRules.workflowTransition(
                WorkflowStatus.CANCEL_REQUESTED, WorkflowStatus.COMPLETED))
        .isFalse();
  }

  @Test
  void stepTransitionPolicySeparatesTimeoutFromRetry() {
    assertThat(WorkflowRules.stepTransition(StepStatus.RUNNING, StepStatus.TIMED_OUT)).isTrue();
    assertThat(WorkflowRules.stepTransition(StepStatus.TIMED_OUT, StepStatus.RETRY_SCHEDULED))
        .isTrue();
    assertThat(WorkflowRules.stepTransition(StepStatus.COMPLETED, StepStatus.DISPATCHED)).isFalse();
  }

  @Test
  void resultsAreFencedByAttemptAndActiveState() {
    assertThat(WorkflowRules.acceptsResult(2, 1, StepStatus.RUNNING)).isFalse();
    assertThat(WorkflowRules.acceptsResult(2, 2, StepStatus.COMPLETED)).isFalse();
    assertThat(WorkflowRules.acceptsResult(2, 2, StepStatus.DISPATCHED)).isTrue();
  }

  @Test
  void exponentialBackoffIsBoundedAndJittered() {
    assertThat(WorkflowRules.backoff(1000, 1, 0)).isEqualTo(750);
    assertThat(WorkflowRules.backoff(1000, 2, 1)).isEqualTo(2500);
    assertThat(WorkflowRules.backoff(60000, 10, 0.5)).isEqualTo(300000);
    assertThatThrownBy(() -> WorkflowRules.backoff(100, 11, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void validatesHandlerConfigurationsBeforeExecution() {
    var invalid =
        new StepDefinition(
            "delay",
            "delay",
            StepType.DELAY,
            Map.of("durationMs", -1),
            new RetryPolicy(1, 100),
            1000,
            0,
            List.of());
    assertThatThrownBy(
            () -> WorkflowRules.validate(new WorkflowRequest("bad", "", true, List.of(invalid))))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
