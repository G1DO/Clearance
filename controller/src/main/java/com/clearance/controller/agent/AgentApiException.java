package com.clearance.controller.agent;

/** An agent API rejection that occurs before any authoritative mutation. */
public class AgentApiException extends RuntimeException {
  private final int status;
  private final String code;

  public AgentApiException(int status, String code, String message) {
    super(message);
    this.status = status;
    this.code = code;
  }

  public int status() { return status; }
  public String code() { return code; }
}
