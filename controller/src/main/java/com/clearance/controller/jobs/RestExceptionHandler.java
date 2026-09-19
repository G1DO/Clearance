package com.clearance.controller.jobs;

import jakarta.validation.ConstraintViolationException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.ServletRequestBindingException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/** Maps domain faults to stable HTTP statuses without leaking cross-project existence. */
@RestControllerAdvice
public class RestExceptionHandler {

  @ExceptionHandler(IdempotencyConflictException.class)
  public ResponseEntity<Map<String, Object>> conflict(IdempotencyConflictException e) {
    Job existing = e.getExisting();
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("error", "idempotency_conflict");
    body.put("message", "operation already used with a different payload");
    body.put("existingJobId", existing.jobId().toString());
    body.put("existingPayloadHash", existing.payloadHash());
    return ResponseEntity.status(409).body(body);
  }

  @ExceptionHandler(JobNotFoundException.class)
  public ResponseEntity<Map<String, Object>> notFound(JobNotFoundException e) {
    return ResponseEntity.status(404)
        .body(Map.of("error", "not_found", "message", "job not found"));
  }

  @ExceptionHandler(BadRequestException.class)
  public ResponseEntity<Map<String, Object>> badRequest(BadRequestException e) {
    return ResponseEntity.badRequest()
        .body(Map.of("error", "bad_request", "message", e.getMessage()));
  }

  @ExceptionHandler({
    MethodArgumentNotValidException.class,
    ConstraintViolationException.class,
    HttpMessageNotReadableException.class,
    MissingRequestHeaderException.class,
    MethodArgumentTypeMismatchException.class
  })
  public ResponseEntity<Map<String, Object>> validation(Exception e) {
    String message = "invalid request";
    if (e instanceof MethodArgumentNotValidException manv) {
      var fieldError = manv.getBindingResult().getFieldError();
      if (fieldError != null) {
        message = fieldError.getField() + " " + fieldError.getDefaultMessage();
      }
    } else if (e instanceof MissingRequestHeaderException mrh) {
      message = "missing header: " + mrh.getHeaderName();
    } else if (e instanceof MethodArgumentTypeMismatchException matm) {
      if (matm.getRequiredType() == UUID.class) {
        message = "invalid jobId";
      } else {
        message = "invalid parameter: " + matm.getName();
      }
    } else if (e instanceof HttpMessageNotReadableException) {
      message = "malformed JSON body";
    } else {
      message = e.getMessage() != null ? e.getMessage() : message;
    }
    return ResponseEntity.badRequest()
        .body(Map.of("error", "bad_request", "message", message));
  }

  @ExceptionHandler(ServletRequestBindingException.class)
  public ResponseEntity<Map<String, Object>> missingScope(ServletRequestBindingException e) {
    return ResponseEntity.status(401)
        .body(Map.of("error", "unauthorized", "message", "missing authentication scope"));
  }
}
