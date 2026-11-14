package dev.flowforge.orchestrator;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.*;

class OutboxPublisherTest {
  @SuppressWarnings("unchecked")
  @Test
  void acknowledgmentMarksPublishedButBrokerFailureSchedulesBackoff() {
    for (boolean acknowledged : new boolean[] {true, false}) {
      JdbcTemplate db = mock(JdbcTemplate.class);
      TransactionTemplate tx = mock(TransactionTemplate.class);
      KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
      UUID id = UUID.randomUUID();
      Map<String, Object> row =
          Map.of(
              "id",
              id,
              "aggregate_id",
              UUID.randomUUID(),
              "topic",
              "tasks",
              "payload",
              "{}",
              "attempt_count",
              0);
      when(db.queryForList(anyString())).thenReturn(List.of(row), List.of());
      when(tx.execute(any()))
          .thenAnswer(
              invocation ->
                  ((TransactionCallback<Object>) invocation.getArgument(0))
                      .doInTransaction(new SimpleTransactionStatus()));
      if (acknowledged)
        when(kafka.send(anyString(), anyString(), anyString()))
            .thenReturn(CompletableFuture.completedFuture(null));
      else
        when(kafka.send(anyString(), anyString(), anyString()))
            .thenReturn(CompletableFuture.failedFuture(new TimeoutException("broker unavailable")));
      new OutboxPublisher(db, tx, kafka).publish();
      if (acknowledged) {
        verify(db).update(contains("status='PUBLISHED'"), eq(id));
        verify(db, never()).update(contains("next_attempt_at=now()"), anyInt(), anyLong(), eq(id));
      } else {
        verify(db).update(contains("next_attempt_at=now()"), eq(1), eq(500L), eq(id));
        verify(db, never()).update(contains("status='PUBLISHED'"), eq(id));
      }
    }
  }
}
