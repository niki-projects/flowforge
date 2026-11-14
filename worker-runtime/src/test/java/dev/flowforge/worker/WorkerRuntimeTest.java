package dev.flowforge.worker;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import dev.flowforge.contracts.Contracts.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.*;
import tools.jackson.databind.ObjectMapper;

class WorkerRuntimeTest {
  @SuppressWarnings("unchecked")
  @Test
  void resultOutboxRequiresAcknowledgmentAndPersistsFailureBackoff() {
    for (boolean acknowledged : new boolean[] {true, false}) {
      JdbcTemplate db = mock(JdbcTemplate.class);
      TransactionTemplate tx = mock(TransactionTemplate.class);
      KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
      UUID id = UUID.randomUUID();
      var row =
          Map.<String, Object>of(
              "id", id, "step_id", UUID.randomUUID(), "payload", "{}", "attempt_count", 0);
      when(db.queryForList(anyString())).thenReturn(List.of(row), List.of());
      when(tx.execute(any()))
          .thenAnswer(
              invocation ->
                  ((TransactionCallback<Object>) invocation.getArgument(0))
                      .doInTransaction(new SimpleTransactionStatus()));
      when(kafka.send(anyString(), anyString(), anyString()))
          .thenReturn(
              acknowledged
                  ? CompletableFuture.completedFuture(null)
                  : CompletableFuture.failedFuture(new TimeoutException("broker unavailable")));
      var runtime =
          new WorkerRuntime(
              db,
              tx,
              kafka,
              new ObjectMapper(),
              task -> Map.of(),
              new SimpleMeterRegistry(),
              "test-worker");
      try {
        runtime.publish();
        if (acknowledged) {
          verify(db).update(contains("published_at=now()"), eq(id));
          verify(db, never())
              .update(contains("next_attempt_at=now()"), anyInt(), anyLong(), eq(id));
        } else {
          verify(db, never()).update(contains("published_at=now()"), eq(id));
          verify(db)
              .update(
                  contains("next_attempt_at=now()"),
                  eq(1),
                  longThat(delay -> delay >= 375 && delay <= 625),
                  eq(id));
        }
      } finally {
        runtime.shutdown();
      }
    }
  }

  @Test
  void rejectsUnboundedTaskDeadlinesAndUnknownSchemas() {
    for (int version : List.of(1, 2)) {
      var task =
          new Task(
              UUID.randomUUID(),
              version,
              Instant.now(),
              "test",
              UUID.randomUUID(),
              UUID.randomUUID(),
              1,
              StepType.DELAY,
              Map.of(),
              Map.of(),
              version == 1 ? 999999 : 1000);
      assertThatThrownBy(() -> WorkerRuntime.validate(task))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }
}
