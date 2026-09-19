package com.clearance.controller.jobs;

/** Thrown when {@code (project_id, operation_id)} exists with a different payload hash. */
public class IdempotencyConflictException extends RuntimeException {

  private final Job existing;

  public IdempotencyConflictException(Job existing) {
    super("idempotency conflict: operation already used with a different payload");
    this.existing = existing;
  }

  public Job getExisting() {
    return existing;
  }
}
