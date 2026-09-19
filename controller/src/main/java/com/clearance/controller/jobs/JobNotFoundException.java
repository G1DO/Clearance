package com.clearance.controller.jobs;

/** Thrown when a job id is unknown or belongs to another project (both surface as 404). */
public class JobNotFoundException extends RuntimeException {
  public JobNotFoundException() {
    super("job not found");
  }
}
