package dev.flowforge.orchestrator;

import dev.flowforge.contracts.WorkflowRules;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Component
class OutboxPublisher {
  private final JdbcTemplate db;
  private final TransactionTemplate tx;
  private final KafkaTemplate<String, String> kafka;

  OutboxPublisher(JdbcTemplate db, TransactionTemplate tx, KafkaTemplate<String, String> kafka) {
    this.db = db;
    this.tx = tx;
    this.kafka = kafka;
  }

  @Scheduled(fixedDelay = 100)
  void publish() {
    for (int i = 0; i < 30; i++) {
      Boolean found =
          tx.execute(
              s -> {
                var rows =
                    db.queryForList(
                        "SELECT * FROM outbox_event WHERE status='PENDING' AND next_attempt_at<=now() ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED");
                if (rows.isEmpty()) return false;
                var row = rows.getFirst();
                UUID id = (UUID) row.get("id");
                try {
                  // Bounded transaction holds one row lock. A crash after send permits duplicate
                  // delivery.
                  kafka
                      .send(
                          row.get("topic").toString(),
                          row.get("aggregate_id").toString(),
                          row.get("payload").toString())
                      .get(5, TimeUnit.SECONDS);
                  db.update(
                      "UPDATE outbox_event SET status='PUBLISHED',published_at=now(),attempt_count=attempt_count+1 WHERE id=?",
                      id);
                } catch (Exception e) {
                  if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                  int attempts = ((Number) row.get("attempt_count")).intValue() + 1;
                  long delay = WorkflowRules.backoff(500, Math.min(10, attempts), 0.5);
                  db.update(
                      "UPDATE outbox_event SET attempt_count=?,next_attempt_at=now()+(? * interval '1 millisecond') WHERE id=?",
                      attempts,
                      delay,
                      id);
                }
                return true;
              });
      if (!Boolean.TRUE.equals(found)) break;
    }
  }

  @Scheduled(fixedDelay = 3600000)
  void retention() {
    // Keep inbox keys and worker claims indefinitely until a replay horizon is formally bounded.
    db.update(
        "DELETE FROM outbox_event WHERE id IN (SELECT id FROM outbox_event WHERE status='PUBLISHED' AND published_at<now()-interval '7 days' LIMIT 1000)");
    db.update(
        "DELETE FROM execution_event WHERE id IN (SELECT id FROM execution_event WHERE created_at<now()-interval '90 days' LIMIT 1000)");
  }
}
