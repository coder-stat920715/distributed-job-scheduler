package com.example.scheduler.api;

import com.example.scheduler.common.Exceptions.InvalidScheduleException;
import com.example.scheduler.common.Exceptions.JobNotFoundException;
import java.net.URI;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.*;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/** RFC 7807 problem+json responses. */
@Slf4j
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

  private ProblemDetail problem(HttpStatus status, String title, String detail, String type) {
    ProblemDetail pd = ProblemDetail.forStatusAndDetail(status, detail);
    pd.setTitle(title);
    pd.setType(URI.create("https://scheduler.example.com/problems/" + type));
    return pd;
  }

  @ExceptionHandler(JobNotFoundException.class)
  ProblemDetail notFound(JobNotFoundException e) {
    return problem(HttpStatus.NOT_FOUND, "Job not found", e.getMessage(), "job-not-found");
  }

  @ExceptionHandler(InvalidScheduleException.class)
  ProblemDetail invalid(InvalidScheduleException e) {
    return problem(HttpStatus.UNPROCESSABLE_ENTITY, "Invalid schedule", e.getMessage(), "invalid-schedule");
  }

  @ExceptionHandler(ObjectOptimisticLockingFailureException.class)
  ProblemDetail conflict(ObjectOptimisticLockingFailureException e) {
    return problem(HttpStatus.CONFLICT, "Concurrent modification", "Reload the job and retry", "conflict");
  }

  @ExceptionHandler(DataIntegrityViolationException.class)
  ProblemDetail integrity(DataIntegrityViolationException e) {
    return problem(HttpStatus.CONFLICT, "Constraint violation",
        "A job with this tenantId and name already exists", "duplicate-job");
  }

  @ExceptionHandler(Exception.class)
  ProblemDetail unexpected(Exception e) {
    log.error("unhandled", e);
    return problem(HttpStatus.INTERNAL_SERVER_ERROR, "Internal error", "Unexpected error", "internal");
  }

  @Override
  protected ResponseEntity<Object> handleMethodArgumentNotValid(MethodArgumentNotValidException ex,
      HttpHeaders headers, HttpStatusCode status, WebRequest request) {
    ProblemDetail pd = problem(HttpStatus.BAD_REQUEST, "Validation failed", "Request body is invalid", "validation");
    Map<String, String> errors = ex.getBindingResult().getFieldErrors().stream()
        .collect(Collectors.toMap(FieldError::getField,
            fe -> String.valueOf(fe.getDefaultMessage()), (a, b) -> a));
    pd.setProperty("errors", errors);
    return ResponseEntity.badRequest().contentType(MediaType.APPLICATION_PROBLEM_JSON).body(pd);
  }
}
