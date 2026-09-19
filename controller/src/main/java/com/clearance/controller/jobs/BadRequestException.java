package com.clearance.controller.jobs;

/** Simple 400 marker for header/body faults detected in the controller layer. */
public class BadRequestException extends RuntimeException {
  public BadRequestException(String message) {
    super(message);
  }
}
