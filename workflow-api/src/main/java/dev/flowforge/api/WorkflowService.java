package dev.flowforge.api;

import dev.flowforge.contracts.Contracts.*;
import dev.flowforge.contracts.WorkflowRules;
import java.time.Instant;
import java.util.*;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

@Service
@Transactional
class WorkflowService {
  private final WorkflowRepository repository;
  private final JdbcTemplate jdbc;
  private final ObjectMapper json;

  WorkflowService(WorkflowRepository repository, JdbcTemplate jdbc, ObjectMapper json) {
    this.repository = repository;
    this.jdbc = jdbc;
    this.json = json;
  }

  WorkflowView create(WorkflowRequest request) {
    WorkflowRules.validate(request);
    var entity = new WorkflowEntity();
    entity.id = UUID.randomUUID();
    entity.createdAt = Instant.now();
    entity.revision = 1;
    assign(entity, request);
    return view(repository.saveAndFlush(entity));
  }

  WorkflowView update(UUID id, WorkflowRequest request) {
    WorkflowRules.validate(request);
    var entity = find(id);
    entity.revision++;
    assign(entity, request);
    return view(repository.saveAndFlush(entity));
  }

  void disable(UUID id) {
    var entity = find(id);
    entity.enabled = false;
    entity.updatedAt = Instant.now();
  }

  @Transactional(readOnly = true)
  WorkflowView get(UUID id) {
    return view(find(id));
  }

  @Transactional(readOnly = true)
  Page<WorkflowView> list(int page, int size) {
    var result =
        repository.findAll(PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "createdAt")));
    return new Page<>(
        result.getContent().stream().map(this::view).toList(),
        page,
        size,
        result.getTotalElements());
  }

  private void assign(WorkflowEntity entity, WorkflowRequest request) {
    entity.name = request.name();
    entity.description = request.description();
    entity.enabled = request.enabled();
    entity.steps = json.writeValueAsString(request.steps());
    entity.updatedAt = Instant.now();
  }

  private WorkflowEntity find(UUID id) {
    return repository
        .findById(id)
        .orElseThrow(() -> new ApiException(404, "WORKFLOW_NOT_FOUND", "Workflow not found"));
  }

  private WorkflowView view(WorkflowEntity entity) {
    var steps = Arrays.asList(json.readValue(entity.steps, StepDefinition[].class));
    return new WorkflowView(
        entity.id,
        entity.name,
        entity.description,
        entity.revision,
        entity.enabled,
        steps,
        entity.createdAt,
        entity.updatedAt);
  }

  ExecutionView start(UUID id, StartRequest request, String correlationId, String key) {
    // Serialize starts with definition edits and idempotent starts for this definition.
    if (jdbc.queryForList(
            "SELECT id FROM workflow_definition WHERE id=? FOR UPDATE", UUID.class, id)
        .isEmpty()) throw new ApiException(404, "WORKFLOW_NOT_FOUND", "Workflow not found");
    var workflow = get(id);
    if (key != null) {
      var existing =
          jdbc.queryForList(
              "SELECT id,input FROM workflow_execution WHERE definition_id=? AND idempotency_key=?",
              id,
              key);
      if (!existing.isEmpty()) {
        var execution = execution((UUID) existing.getFirst().get("id"));
        if (!execution.input().equals(request.input()))
          throw new ApiException(409, "IDEMPOTENCY_CONFLICT", "Key was used with different input");
        return execution;
      }
    }
    if (!workflow.enabled())
      throw new ApiException(409, "WORKFLOW_DISABLED", "Workflow is disabled");
    UUID executionId = UUID.randomUUID();
    jdbc.update(
        "INSERT INTO workflow_execution(id,definition_id,workflow_version,status,input,correlation_id,idempotency_key) VALUES (?,?,?,'PENDING',?::jsonb,?,?)",
        executionId,
        id,
        workflow.version(),
        json.writeValueAsString(request.input()),
        correlationId,
        key);
    for (var step : workflow.steps()) {
      jdbc.update(
          "INSERT INTO step_execution(id,execution_id,definition_step_id,execution_order,definition,status) VALUES (?,?,?,?,?::jsonb,'PENDING')",
          UUID.randomUUID(),
          executionId,
          step.id(),
          step.executionOrder(),
          json.writeValueAsString(step));
    }
    event(executionId, "WorkflowStarted", Map.of("version", workflow.version()));
    return execution(executionId);
  }

  @Transactional(readOnly = true)
  ExecutionView execution(UUID id) {
    var rows =
        jdbc.query(
            "SELECT * FROM workflow_execution WHERE id=?",
            (rs, n) ->
                new ExecutionView(
                    rs.getObject("id", UUID.class),
                    rs.getObject("definition_id", UUID.class),
                    rs.getInt("workflow_version"),
                    WorkflowStatus.valueOf(rs.getString("status")),
                    map(rs.getString("input")),
                    map(rs.getString("output")),
                    rs.getTimestamp("created_at").toInstant(),
                    instant(rs.getTimestamp("started_at")),
                    instant(rs.getTimestamp("completed_at")),
                    rs.getString("failure_reason"),
                    rs.getString("correlation_id")),
            id);
    if (rows.isEmpty()) throw new ApiException(404, "EXECUTION_NOT_FOUND", "Execution not found");
    return rows.getFirst();
  }

  @Transactional(readOnly = true)
  Page<ExecutionView> executions(int page, int size, WorkflowStatus status) {
    String where = status == null ? "" : " WHERE status=?";
    var args = new ArrayList<Object>();
    if (status != null) args.add(status.name());
    long count =
        jdbc.queryForObject(
            "SELECT count(*) FROM workflow_execution" + where, Long.class, args.toArray());
    args.add(size);
    args.add((long) page * size);
    var ids =
        jdbc.queryForList(
            "SELECT id FROM workflow_execution"
                + where
                + " ORDER BY created_at DESC,id LIMIT ? OFFSET ?",
            UUID.class,
            args.toArray());
    return new Page<>(ids.stream().map(this::execution).toList(), page, size, count);
  }

  @Transactional(readOnly = true)
  List<StepView> steps(UUID id) {
    execution(id);
    return jdbc.query(
        "SELECT * FROM step_execution WHERE execution_id=? ORDER BY execution_order",
        (rs, n) ->
            new StepView(
                rs.getObject("id", UUID.class),
                id,
                rs.getString("definition_step_id"),
                StepStatus.valueOf(rs.getString("status")),
                rs.getInt("attempt"),
                rs.getString("assigned_worker"),
                instant(rs.getTimestamp("started_at")),
                instant(rs.getTimestamp("completed_at")),
                rs.getString("error_code"),
                rs.getString("error_message"),
                map(rs.getString("output"))),
        id);
  }

  @Transactional(readOnly = true)
  Page<HistoryView> events(UUID id, int page, int size) {
    execution(id);
    var rows =
        jdbc.query(
            "SELECT * FROM execution_event WHERE execution_id=? ORDER BY id LIMIT ? OFFSET ?",
            (rs, n) ->
                new HistoryView(
                    rs.getLong("id"),
                    rs.getString("event_type"),
                    rs.getTimestamp("created_at").toInstant(),
                    map(rs.getString("details"))),
            id,
            size,
            (long) page * size);
    long count =
        jdbc.queryForObject(
            "SELECT count(*) FROM execution_event WHERE execution_id=?", Long.class, id);
    return new Page<>(rows, page, size, count);
  }

  ExecutionView cancel(UUID id) {
    lock(id);
    var current = execution(id);
    if (current.status() == WorkflowStatus.CANCEL_REQUESTED
        || current.status() == WorkflowStatus.CANCELED) return current;
    if (!WorkflowRules.workflowTransition(current.status(), WorkflowStatus.CANCEL_REQUESTED))
      throw new ApiException(409, "INVALID_TRANSITION", "Terminal executions cannot be canceled");
    jdbc.update("UPDATE workflow_execution SET status='CANCEL_REQUESTED' WHERE id=?", id);
    event(id, "WorkflowCancellationRequested", Map.of());
    return execution(id);
  }

  ExecutionView retry(UUID id, String key) {
    var previous = execution(id);
    if (previous.status() != WorkflowStatus.FAILED)
      throw new ApiException(409, "INVALID_TRANSITION", "Only failed executions can be retried");
    // Retry explicitly uses the current enabled definition, never mutates the old attempt.
    return start(
        previous.workflowDefinitionId(),
        new StartRequest(previous.input()),
        previous.correlationId(),
        key);
  }

  private void lock(UUID id) {
    execution(id);
    jdbc.queryForObject("SELECT id FROM workflow_execution WHERE id=? FOR UPDATE", UUID.class, id);
  }

  private void event(UUID id, String type, Map<String, Object> details) {
    jdbc.update(
        "INSERT INTO execution_event(execution_id,event_type,details) VALUES (?,?,?::jsonb)",
        id,
        type,
        json.writeValueAsString(details));
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> map(String value) {
    return json.readValue(value, Map.class);
  }

  private static Instant instant(java.sql.Timestamp value) {
    return value == null ? null : value.toInstant();
  }
}
