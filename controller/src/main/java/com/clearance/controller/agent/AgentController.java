package com.clearance.controller.agent;

import com.clearance.controller.auth.RunnerKeyAuthFilter;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.JsonNode;

@RestController
@RequestMapping(value = "/internal/v1/agents", produces = "application/json")
public class AgentController {
  private final AgentService service;
  private final ObjectProvider<AgentPollFaults> faults;

  public AgentController(AgentService service, ObjectProvider<AgentPollFaults> faults) {
    this.service = service;
    this.faults = faults;
  }

  @PostMapping(value = "/poll", consumes = "application/json")
  public ResponseEntity<String> poll(
      @RequestAttribute(RunnerKeyAuthFilter.RUNNER_ATTRIBUTE) UUID runnerId,
      @RequestBody JsonNode body) {
    var request = AgentProtocol.parsePollRequest(body);
    checkIdentity(runnerId, request.runnerId());
    long timeoutSeconds = request.timeoutSeconds() == null ? 20 : Math.min(request.timeoutSeconds(), 30);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
    while (true) {
      // The proxied service commits before returning. No transaction/connection is held
      // during the wait or fault injection, and this path never calls the scheduler.
      var assignment = service.poll(runnerId, request.agentIncarnation());
      long remaining = deadline - System.nanoTime();
      if (assignment.assigned() || remaining <= 0) {
        String response = AgentProtocol.encodePollResponse(assignment);
        AgentPollFaults testFaults = faults.getIfAvailable();
        if (assignment.assigned() && testFaults != null) {
          response = testFaults.deliverPoll(runnerId, response);
          if (response == null) {
            return ResponseEntity.status(503).build();
          }
        }
        return ResponseEntity.ok(response);
      }
      try {
        TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(100)));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AgentApiException(503, "internal", "poll interrupted");
      }
    }
  }

  @PostMapping(value = "/report", consumes = "application/json")
  public String report(
      @RequestAttribute(RunnerKeyAuthFilter.RUNNER_ATTRIBUTE) UUID runnerId,
      @RequestBody JsonNode body) {
    checkIdentity(runnerId, AgentProtocol.assertedRunnerId(body));
    var report = AgentProtocol.parseReportRequest(body);
    AgentPollFaults testFaults = faults.getIfAvailable();
    if (testFaults != null) {
      testFaults.beforeReport(runnerId);
    }
    return AgentProtocol.encodeReportResponse(service.report(runnerId, report));
  }

  private static void checkIdentity(UUID authenticated, UUID asserted) {
    if (asserted != null && !authenticated.equals(asserted)) {
      throw new AgentApiException(403, "fenced_rejected", "runner_id does not match machine identity");
    }
  }

  @ExceptionHandler(AgentApiException.class)
  public ResponseEntity<String> rejected(AgentApiException e) {
    return error(e.status(), e.code(), e.getMessage());
  }

  @ExceptionHandler({IllegalArgumentException.class, HttpMessageNotReadableException.class})
  public ResponseEntity<String> invalid(Exception e) {
    return error(400, "bad_request", e instanceof IllegalArgumentException
        ? e.getMessage() : "malformed JSON body");
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<String> internal(Exception e) {
    org.slf4j.LoggerFactory.getLogger(AgentController.class).error("Agent request failed", e);
    return error(500, "internal", "agent request failed");
  }

  private static ResponseEntity<String> error(int status, String code, String message) {
    return ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON).body(
        AgentProtocol.encodeErrorBody(new AgentProtocol.ErrorBody(code, message)));
  }
}
