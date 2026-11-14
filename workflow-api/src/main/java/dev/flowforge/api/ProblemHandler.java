package dev.flowforge.api;

import java.net.URI;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.dao.*;
import org.springframework.http.*;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
class ProblemHandler {
  @ExceptionHandler(org.springframework.web.servlet.resource.NoResourceFoundException.class)
  ResponseEntity<ProblemDetail> missing(Exception e) {
    return problem(404, "RESOURCE_NOT_FOUND", "Resource not found");
  }

  @ExceptionHandler(org.springframework.web.HttpRequestMethodNotSupportedException.class)
  ResponseEntity<ProblemDetail> method(Exception e) {
    return problem(405, "METHOD_NOT_ALLOWED", "HTTP method is not supported for this resource");
  }

  @ExceptionHandler(org.springframework.web.HttpMediaTypeNotSupportedException.class)
  ResponseEntity<ProblemDetail> mediaType(Exception e) {
    return problem(415, "UNSUPPORTED_MEDIA_TYPE", "Use application/json");
  }

  @ExceptionHandler(ApiException.class)
  ResponseEntity<ProblemDetail> api(ApiException e) {
    return problem(e.status, e.code, e.getMessage());
  }

  @ExceptionHandler({
    IllegalArgumentException.class,
    MethodArgumentNotValidException.class,
    HandlerMethodValidationException.class,
    jakarta.validation.ConstraintViolationException.class,
    MethodArgumentTypeMismatchException.class,
    HttpMessageNotReadableException.class
  })
  ResponseEntity<ProblemDetail> invalid(Exception e) {
    return problem(
        400,
        "INVALID_REQUEST",
        "Request validation failed; check the documented schema and dependency rules");
  }

  @ExceptionHandler({
    OptimisticLockingFailureException.class,
    DataIntegrityViolationException.class
  })
  ResponseEntity<ProblemDetail> conflict(Exception e) {
    return problem(
        409, "CONCURRENT_MODIFICATION", "Resource changed concurrently; refresh and retry");
  }

  @ExceptionHandler(Exception.class)
  ResponseEntity<ProblemDetail> unexpected(Exception e) {
    LoggerFactory.getLogger(getClass())
        .error("Unhandled API failure type={}", e.getClass().getSimpleName());
    return problem(503, "SERVICE_UNAVAILABLE", "Operation could not be completed; retry later");
  }

  static ResponseEntity<ProblemDetail> problem(int status, String code, String detail) {
    var problem = ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(status), detail);
    problem.setType(URI.create("urn:flowforge:problem:" + code.toLowerCase(java.util.Locale.ROOT)));
    problem.setProperty("code", code);
    problem.setProperty("correlationId", MDC.get("correlationId"));
    return ResponseEntity.status(status).body(problem);
  }
}
