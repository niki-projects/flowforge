package dev.flowforge.orchestrator;

import dev.flowforge.contracts.Contracts;
import dev.flowforge.contracts.Contracts.*;
import dev.flowforge.contracts.WorkflowRules;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;

@Service
class Engine {
  private final JdbcTemplate db;
  private final TransactionTemplate tx;
  private final ObjectMapper json;
  private final MeterRegistry metrics;

  Engine(JdbcTemplate db, TransactionTemplate tx, ObjectMapper json, MeterRegistry metrics) {
    this.db = db;
    this.tx = tx;
    this.json = json;
    this.metrics = metrics;
    metrics.gauge(
        "active_workflow_executions",
        this,
        e ->
            e.count(
                "SELECT count(*) FROM workflow_execution WHERE status IN ('PENDING','RUNNING','CANCEL_REQUESTED')"));
    metrics.gauge(
        "outbox_pending_events",
        this,
        e -> e.count("SELECT count(*) FROM outbox_event WHERE status='PENDING'"));
  }

  private double count(String sql) {
    try {
      return db.queryForObject(sql, Long.class);
    } catch (org.springframework.dao.DataAccessException e) {
      return Double.NaN;
    }
  }

  @Scheduled(fixedDelayString = "${flowforge.scheduler-delay-ms:200}")
  void tick() {
    for (int i = 0; i < 30; i++) {
      Boolean handled =
          tx.execute(
              s -> {
                var rows =
                    db.queryForList(
                        """
                    SELECT e.* FROM workflow_execution e
                    WHERE e.status IN ('PENDING','RUNNING','CANCEL_REQUESTED')
                      AND (e.status IN ('PENDING','CANCEL_REQUESTED') OR EXISTS (
                        SELECT 1 FROM step_execution s WHERE s.execution_id=e.id AND (
                          (s.status IN ('PENDING','RETRY_SCHEDULED') AND s.next_attempt_at<=now()
                            AND NOT EXISTS (SELECT 1 FROM step_execution p WHERE p.execution_id=e.id AND p.execution_order<s.execution_order AND p.status<>'COMPLETED'))
                          OR (s.status IN ('DISPATCHED','RUNNING') AND s.lease_until<now())))
                        OR NOT EXISTS (SELECT 1 FROM step_execution s WHERE s.execution_id=e.id AND s.status<>'COMPLETED'))
                    ORDER BY e.created_at LIMIT 1 FOR UPDATE OF e SKIP LOCKED
                    """);
                if (rows.isEmpty()) return false;
                advance(rows.getFirst());
                return true;
              });
      if (!Boolean.TRUE.equals(handled)) break;
    }
  }

  private void advance(Map<String, Object> execution) {
    UUID id = (UUID) execution.get("id");
    String correlation = (String) execution.get("correlation_id");
    MDC.put("correlationId", correlation);
    MDC.put("executionId", id.toString());
    try {
      if ("CANCEL_REQUESTED".equals(execution.get("status"))) {
        db.update(
            "UPDATE step_execution SET status='CANCELED',completed_at=now() WHERE execution_id=? AND status IN ('PENDING','DISPATCHED','RUNNING','RETRY_SCHEDULED')",
            id);
        transition(id, WorkflowStatus.CANCEL_REQUESTED, WorkflowStatus.CANCELED);
        history(id, "WorkflowCanceled", Map.of());
        return;
      }
      if ("PENDING".equals(execution.get("status"))) {
        transition(id, WorkflowStatus.PENDING, WorkflowStatus.RUNNING);
        db.update("UPDATE workflow_execution SET started_at=now() WHERE id=?", id);
        metrics.counter("workflows_started_total").increment();
      }
      var steps =
          db.queryForList(
              "SELECT * FROM step_execution WHERE execution_id=? ORDER BY execution_order", id);
      Map<String, Object> input = map(execution.get("input").toString());
      for (var step : steps) {
        String status = step.get("status").toString();
        if (status.equals("COMPLETED")) {
          input = map(step.get("output").toString());
          continue;
        }
        var definition = json.readValue(step.get("definition").toString(), StepDefinition.class);
        if (status.equals("DISPATCHED") || status.equals("RUNNING")) {
          if (((Timestamp) step.get("lease_until")).toInstant().isBefore(Instant.now()))
            fail(
                execution,
                step,
                definition,
                "LEASE_EXPIRED",
                "Worker did not report before its deadline",
                true,
                true);
          return;
        }
        if (status.equals("RETRY_SCHEDULED")
            && ((Timestamp) step.get("next_attempt_at")).toInstant().isAfter(Instant.now())) return;
        if (!status.equals("PENDING") && !status.equals("RETRY_SCHEDULED")) return;
        Set<String> completed = new HashSet<>();
        for (var previous : steps)
          if (previous.get("status").equals("COMPLETED"))
            completed.add(previous.get("definition_step_id").toString());
        if (!completed.containsAll(definition.dependencies())) return;
        UUID stepId = (UUID) step.get("id");
        int attempt = ((Number) step.get("attempt")).intValue() + 1;
        if (!WorkflowRules.stepTransition(StepStatus.valueOf(status), StepStatus.DISPATCHED))
          throw new IllegalStateException("Invalid dispatch");
        db.update(
            "UPDATE step_execution SET status='DISPATCHED',attempt=?,lease_until=?,assigned_worker=NULL,started_at=NULL,completed_at=NULL,error_code=NULL,error_message=NULL WHERE id=?",
            attempt,
            Timestamp.from(Instant.now().plusMillis(definition.timeoutMs() + 30000)),
            stepId);
        var task =
            new Task(
                UUID.randomUUID(),
                1,
                Instant.now(),
                correlation,
                id,
                stepId,
                attempt,
                definition.type(),
                definition.configuration(),
                input,
                definition.timeoutMs());
        String topic =
            definition.type() == StepType.JSON_TRANSFORM
                ? Contracts.KOTLIN_TASKS
                : Contracts.JAVA_TASKS;
        db.update(
            "INSERT INTO outbox_event(id,aggregate_id,event_type,topic,payload) VALUES (?,?,'StepExecutionRequested',?,?)",
            task.messageId(),
            id,
            topic,
            json.writeValueAsString(task));
        history(id, "StepExecutionRequested", Map.of("stepId", stepId, "attempt", attempt));
        return;
      }
      transition(id, WorkflowStatus.RUNNING, WorkflowStatus.COMPLETED);
      db.update(
          "UPDATE workflow_execution SET output=?::jsonb WHERE id=?",
          json.writeValueAsString(input),
          id);
      history(id, "WorkflowCompleted", Map.of());
      metrics.counter("workflows_completed_total").increment();
      duration(execution);
    } finally {
      MDC.clear();
    }
  }

  @KafkaListener(topics = Contracts.RESULTS, groupId = "flowforge-orchestrator-v1")
  void result(String payload) {
    var result = json.readValue(payload, Result.class);
    if (result.schemaVersion() != 1
        || result.messageId() == null
        || result.executionId() == null
        || result.stepExecutionId() == null
        || result.attempt() < 1
        || !Set.of("STARTED", "COMPLETED", "FAILED", "TIMED_OUT")
            .contains(Objects.toString(result.outcome(), "")))
      throw new IllegalArgumentException("Unsupported result contract");
    tx.executeWithoutResult(
        s -> {
          // Every engine mutation takes the execution lock first, including cancellation and
          // recovery.
          var executions =
              db.queryForList(
                  "SELECT * FROM workflow_execution WHERE id=? FOR UPDATE", result.executionId());
          if (executions.isEmpty()) return;
          int inserted =
              db.update(
                  "INSERT INTO inbox_message(message_id,consumer_name) VALUES (?,'orchestrator') ON CONFLICT DO NOTHING",
                  result.messageId());
          if (inserted == 0) return;
          var execution = executions.getFirst();
          if (!Set.of("PENDING", "RUNNING").contains(execution.get("status"))) return;
          var steps =
              db.queryForList(
                  "SELECT * FROM step_execution WHERE id=? AND execution_id=?",
                  result.stepExecutionId(),
                  result.executionId());
          if (steps.isEmpty()) return;
          var step = steps.getFirst();
          if (!WorkflowRules.acceptsResult(
              ((Number) step.get("attempt")).intValue(),
              result.attempt(),
              StepStatus.valueOf(step.get("status").toString()))) return;
          UUID stepId = result.stepExecutionId();
          if (result.outcome().equals("STARTED")) {
            if (step.get("status").equals("DISPATCHED")) {
              db.update(
                  "UPDATE step_execution SET status='RUNNING',assigned_worker=?,started_at=now() WHERE id=?",
                  result.workerId(),
                  stepId);
              history(
                  result.executionId(),
                  "StepExecutionStarted",
                  Map.of("stepId", stepId, "attempt", result.attempt()));
            }
            return;
          }
          var definition = json.readValue(step.get("definition").toString(), StepDefinition.class);
          if (result.outcome().equals("COMPLETED")) {
            db.update(
                "UPDATE step_execution SET status='COMPLETED',assigned_worker=?,output=?::jsonb,completed_at=now(),lease_until=NULL WHERE id=?",
                result.workerId(),
                json.writeValueAsString(result.output()),
                stepId);
            history(
                result.executionId(),
                "StepExecutionCompleted",
                Map.of("stepId", stepId, "attempt", result.attempt()));
            if (step.get("started_at") instanceof Timestamp started)
              metrics
                  .timer("step_execution_duration", "type", definition.type().name())
                  .record(Duration.between(started.toInstant(), Instant.now()));
          } else
            fail(
                execution,
                step,
                definition,
                clean(result.errorCode(), 80),
                clean(result.errorMessage(), 500),
                result.retryable(),
                result.outcome().equals("TIMED_OUT"));
        });
  }

  private void fail(
      Map<String, Object> execution,
      Map<String, Object> step,
      StepDefinition definition,
      String code,
      String message,
      boolean retryable,
      boolean timedOut) {
    UUID stepId = (UUID) step.get("id");
    UUID id = (UUID) execution.get("id");
    int attempt = ((Number) step.get("attempt")).intValue();
    db.update(
        "UPDATE step_execution SET status=?,error_code=?,error_message=?,completed_at=now(),lease_until=NULL WHERE id=?",
        timedOut ? "TIMED_OUT" : "FAILED",
        code,
        message,
        stepId);
    history(
        id,
        timedOut ? "StepExecutionTimedOut" : "StepExecutionFailed",
        Map.of("stepId", stepId, "attempt", attempt, "code", code));
    if (retryable && attempt < definition.retryPolicy().maxAttempts()) {
      long delay =
          WorkflowRules.backoff(
              definition.retryPolicy().initialDelayMs(),
              attempt,
              ThreadLocalRandom.current().nextDouble());
      db.update(
          "UPDATE step_execution SET status='RETRY_SCHEDULED',next_attempt_at=? WHERE id=?",
          Timestamp.from(Instant.now().plusMillis(delay)),
          stepId);
      history(id, "StepRetryScheduled", Map.of("stepId", stepId, "delayMs", delay));
      metrics.counter("task_retry_total").increment();
    } else {
      transition(id, WorkflowStatus.RUNNING, WorkflowStatus.FAILED);
      db.update("UPDATE workflow_execution SET failure_reason=? WHERE id=?", message, id);
      db.update(
          "UPDATE step_execution SET status='CANCELED',completed_at=now() WHERE execution_id=? AND status='PENDING'",
          id);
      history(id, "WorkflowFailed", Map.of("stepId", stepId, "code", code));
      metrics.counter("workflows_failed_total").increment();
      duration(execution);
    }
  }

  private void transition(UUID id, WorkflowStatus from, WorkflowStatus to) {
    if (!WorkflowRules.workflowTransition(from, to))
      throw new IllegalStateException("Invalid workflow transition");
    int changed =
        db.update(
            "UPDATE workflow_execution SET status=?,completed_at=CASE WHEN ? IN ('COMPLETED','FAILED','CANCELED') THEN now() ELSE completed_at END WHERE id=? AND status=?",
            to.name(),
            to.name(),
            id,
            from.name());
    if (changed != 1) throw new IllegalStateException("Conflicting workflow transition");
  }

  private void duration(Map<String, Object> execution) {
    if (execution.get("started_at") instanceof Timestamp start)
      metrics
          .timer("workflow_execution_duration")
          .record(Duration.between(start.toInstant(), Instant.now()));
  }

  private void history(UUID id, String type, Map<String, Object> details) {
    db.update(
        "INSERT INTO execution_event(execution_id,event_type,details) VALUES (?,?,?::jsonb)",
        id,
        type,
        json.writeValueAsString(details));
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> map(String value) {
    return json.readValue(value, Map.class);
  }

  private String clean(String value, int max) {
    return value == null ? "TASK_FAILED" : value.substring(0, Math.min(max, value.length()));
  }
}
