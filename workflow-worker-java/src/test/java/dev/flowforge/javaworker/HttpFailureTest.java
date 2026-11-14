package dev.flowforge.javaworker;

import static org.assertj.core.api.Assertions.*;

import dev.flowforge.worker.TaskFailure;
import org.junit.jupiter.api.Test;

class HttpFailureTest {
  @Test
  void transientStatusesAreRetryable() {
    for (int status : new int[] {429, 500, 503, 504})
      assertThatThrownBy(() -> HttpTask.classifyStatus(status))
          .isInstanceOf(TaskFailure.class)
          .satisfies(error -> assertThat(((TaskFailure) error).retryable()).isTrue());
  }

  @Test
  void redirectsAndClientRejectionArePermanent() {
    for (int status : new int[] {301, 302, 307, 400, 401, 403, 404})
      assertThatThrownBy(() -> HttpTask.classifyStatus(status))
          .isInstanceOf(TaskFailure.class)
          .satisfies(error -> assertThat(((TaskFailure) error).retryable()).isFalse());
    assertThatCode(() -> HttpTask.classifyStatus(200)).doesNotThrowAnyException();
  }
}
