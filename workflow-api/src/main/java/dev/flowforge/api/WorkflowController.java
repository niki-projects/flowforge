package dev.flowforge.api;

import dev.flowforge.contracts.Contracts.*;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.*;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1")
@Validated
class WorkflowController {
  private final WorkflowService service;

  WorkflowController(WorkflowService service) {
    this.service = service;
  }

  @PostMapping("/workflows")
  @ResponseStatus(HttpStatus.CREATED)
  WorkflowView create(@Valid @RequestBody WorkflowRequest request) {
    return service.create(request);
  }

  @GetMapping("/workflows")
  Page<WorkflowView> workflows(
      @RequestParam(defaultValue = "0") @Min(0) @Max(100000) int page,
      @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
    return service.list(page, size);
  }

  @GetMapping("/workflows/{id}")
  WorkflowView workflow(@PathVariable UUID id) {
    return service.get(id);
  }

  @PutMapping("/workflows/{id}")
  WorkflowView update(@PathVariable UUID id, @Valid @RequestBody WorkflowRequest request) {
    return service.update(id, request);
  }

  @DeleteMapping("/workflows/{id}")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  void delete(@PathVariable UUID id) {
    service.disable(id);
  }

  @PostMapping("/workflows/{id}/executions")
  @ResponseStatus(HttpStatus.ACCEPTED)
  ExecutionView start(
      @PathVariable UUID id,
      @Valid @RequestBody StartRequest request,
      @RequestHeader(value = "Idempotency-Key", required = false) @Size(min = 1, max = 100)
          String key) {
    return service.start(id, request, MDC.get("correlationId"), key);
  }

  @GetMapping("/executions")
  Page<ExecutionView> executions(
      @RequestParam(defaultValue = "0") @Min(0) @Max(100000) int page,
      @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size,
      @RequestParam(required = false) WorkflowStatus status) {
    return service.executions(page, size, status);
  }

  @GetMapping("/executions/{id}")
  ExecutionView execution(@PathVariable UUID id) {
    return service.execution(id);
  }

  @GetMapping("/executions/{id}/steps")
  List<StepView> steps(@PathVariable UUID id) {
    return service.steps(id);
  }

  @GetMapping("/executions/{id}/events")
  Page<HistoryView> events(
      @PathVariable UUID id,
      @RequestParam(defaultValue = "0") @Min(0) int page,
      @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
    return service.events(id, page, size);
  }

  @PostMapping("/executions/{id}/cancel")
  @ResponseStatus(HttpStatus.ACCEPTED)
  ExecutionView cancel(@PathVariable UUID id) {
    return service.cancel(id);
  }

  @PostMapping("/executions/{id}/retry")
  @ResponseStatus(HttpStatus.ACCEPTED)
  ExecutionView retry(
      @PathVariable UUID id,
      @RequestHeader(value = "Idempotency-Key", required = false) @Size(min = 1, max = 100)
          String key) {
    return service.retry(id, key);
  }
}
