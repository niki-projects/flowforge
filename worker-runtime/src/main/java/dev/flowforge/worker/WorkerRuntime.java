package dev.flowforge.worker;

import dev.flowforge.contracts.Contracts;
import dev.flowforge.contracts.Contracts.*;
import dev.flowforge.contracts.WorkflowRules;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;

@Component
public class WorkerRuntime {
  private final JdbcTemplate db;
  private final TransactionTemplate tx;
  private final KafkaTemplate<String, String> kafka;
  private final ObjectMapper json;
  private final TaskHandler handler;
  private final MeterRegistry metrics;
  private final String workerId;
  private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

  public WorkerRuntime(
      JdbcTemplate db,
      TransactionTemplate tx,
      KafkaTemplate<String, String> kafka,
      ObjectMapper json,
      TaskHandler handler,
      MeterRegistry metrics,
      @Value("${spring.application.name}") String name) {
    this.db = db;
    this.tx = tx;
    this.kafka = kafka;
    this.json = json;
    this.handler = handler;
    this.metrics = metrics;
    this.workerId = name + "-" + UUID.randomUUID();
  }

  @KafkaListener(
      topics = "${flowforge.task-topic}",
      groupId = "${spring.application.name}-v1",
      concurrency = "${flowforge.worker-concurrency:3}")
  public void receive(String payload) {
    Task task = json.readValue(payload, Task.class);
    validate(task);
    // Do not execute a broker backlog after the orchestrator's dispatch lease expires.
    if (task.timestamp().plusMillis(task.timeoutMs() + 30000).isBefore(Instant.now())) return;
    String owner = workerId + "/" + UUID.randomUUID();
    Boolean claimed =
        tx.execute(
            s -> {
              var rows =
                  db.queryForList(
                      """
                INSERT INTO worker_job(step_id,attempt,owner,lease_until,status) VALUES (?,?,?,?,'RUNNING')
                ON CONFLICT (step_id,attempt) DO UPDATE SET owner=excluded.owner,lease_until=excluded.lease_until
                WHERE worker_job.status='RUNNING' AND worker_job.lease_until<now() RETURNING owner
                """,
                      task.stepExecutionId(),
                      task.attempt(),
                      owner,
                      Timestamp.from(Instant.now().plusMillis(task.timeoutMs() + 10000)));
              if (rows.isEmpty()) return false;
              emit(task, "STARTED", Map.of(), null, null, false);
              return true;
            });
    if (!Boolean.TRUE.equals(claimed)) return;
    MDC.put("correlationId", task.correlationId());
    MDC.put("executionId", task.executionId().toString());
    MDC.put("stepExecutionId", task.stepExecutionId().toString());
    long start = System.nanoTime();
    Future<Map<String, Object>> future = executor.submit(() -> handler.execute(task));
    String outcome = "COMPLETED", code = null, message = null;
    boolean retryable = false;
    Map<String, Object> output = Map.of();
    try {
      output = future.get(task.timeoutMs(), TimeUnit.MILLISECONDS);
      if (output == null || json.writeValueAsBytes(output).length > 262144)
        throw new TaskFailure("OUTPUT_TOO_LARGE", "Task output exceeded its limit", false);
    } catch (TimeoutException e) {
      future.cancel(true);
      outcome = "TIMED_OUT";
      code = "TASK_TIMEOUT";
      message = "Task deadline exceeded";
      retryable = true;
    } catch (InterruptedException e) {
      future.cancel(true);
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Worker interrupted; claim expires for recovery", e);
    } catch (Exception e) {
      output = Map.of();
      Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
      outcome = "FAILED";
      if (cause instanceof TaskFailure failure) {
        code = failure.code();
        message = failure.getMessage();
        retryable = failure.retryable();
      } else {
        code = "HANDLER_ERROR";
        message = "Task handler failed";
        retryable = false;
      }
    } finally {
      metrics
          .timer("step_execution_duration", "type", task.type().name())
          .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
      MDC.clear();
    }
    String resultOutcome = outcome, resultCode = code, resultMessage = message;
    boolean resultRetryable = retryable;
    var resultOutput = output;
    tx.executeWithoutResult(
        s -> {
          String result =
              json.writeValueAsString(
                  new Result(
                      UUID.randomUUID(),
                      1,
                      Instant.now(),
                      task.correlationId(),
                      task.executionId(),
                      task.stepExecutionId(),
                      task.attempt(),
                      workerId,
                      resultOutcome,
                      resultOutput,
                      resultCode,
                      resultMessage,
                      resultRetryable));
          int changed =
              db.update(
                  "UPDATE worker_job SET status='COMPLETED',result=? WHERE step_id=? AND attempt=? AND owner=? AND status='RUNNING'",
                  result,
                  task.stepExecutionId(),
                  task.attempt(),
                  owner);
          if (changed == 1)
            db.update(
                "INSERT INTO worker_outbox(id,step_id,payload) VALUES (?,?,?)",
                UUID.randomUUID(),
                task.stepExecutionId(),
                result);
        });
    if (!outcome.equals("COMPLETED"))
      metrics
          .counter("worker_task_failure_total", "code", Objects.toString(code, "HANDLER_ERROR"))
          .increment();
    LoggerFactory.getLogger(getClass())
        .info(
            "Task finished executionId={} stepExecutionId={} attempt={} outcome={}",
            task.executionId(),
            task.stepExecutionId(),
            task.attempt(),
            outcome);
  }

  static void validate(Task task) {
    if (task.schemaVersion() != 1
        || task.timestamp() == null
        || task.messageId() == null
        || task.executionId() == null
        || task.stepExecutionId() == null
        || task.type() == null
        || task.attempt() < 1
        || task.attempt() > 10
        || task.timeoutMs() < 100
        || task.timeoutMs() > 120000
        || task.input() == null
        || task.configuration() == null)
      throw new IllegalArgumentException("Invalid task contract");
  }

  private void emit(
      Task task,
      String outcome,
      Map<String, Object> output,
      String code,
      String message,
      boolean retryable) {
    var result =
        new Result(
            UUID.randomUUID(),
            1,
            Instant.now(),
            task.correlationId(),
            task.executionId(),
            task.stepExecutionId(),
            task.attempt(),
            workerId,
            outcome,
            output,
            code,
            message,
            retryable);
    db.update(
        "INSERT INTO worker_outbox(id,step_id,payload) VALUES (?,?,?)",
        result.messageId(),
        task.stepExecutionId(),
        json.writeValueAsString(result));
  }

  @Scheduled(fixedDelay = 100)
  public void publish() {
    for (int i = 0; i < 30; i++) {
      Boolean found =
          tx.execute(
              s -> {
                var rows =
                    db.queryForList(
                        "SELECT * FROM worker_outbox WHERE published_at IS NULL AND next_attempt_at<=now() ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED");
                if (rows.isEmpty()) return false;
                var row = rows.getFirst();
                try {
                  kafka
                      .send(
                          Contracts.RESULTS,
                          row.get("step_id").toString(),
                          row.get("payload").toString())
                      .get(5, TimeUnit.SECONDS);
                  db.update(
                      "UPDATE worker_outbox SET published_at=now() WHERE id=?", row.get("id"));
                } catch (Exception e) {
                  if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                  int attempts = ((Number) row.get("attempt_count")).intValue() + 1;
                  long delay =
                      WorkflowRules.backoff(
                          500, Math.min(10, attempts), ThreadLocalRandom.current().nextDouble());
                  db.update(
                      "UPDATE worker_outbox SET attempt_count=?,next_attempt_at=now()+(? * interval '1 millisecond') WHERE id=?",
                      attempts,
                      delay,
                      row.get("id"));
                  return false;
                }
                return true;
              });
      if (!Boolean.TRUE.equals(found)) break;
    }
  }

  @Scheduled(fixedDelay = 3600000)
  public void retention() {
    db.update(
        "DELETE FROM worker_outbox WHERE id IN (SELECT id FROM worker_outbox WHERE published_at<now()-interval '7 days' LIMIT 1000)");
  }

  @PreDestroy
  public void shutdown() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(10, TimeUnit.SECONDS)) executor.shutdownNow();
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
