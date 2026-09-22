package com.clearance.controller.agent;

import com.clearance.controller.auth.RunnerKeyAuthFilter;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** Configures only the calling runner's one-shot transport faults. No ownership writes. */
@RestController
@Profile("test")
public class AgentFaultController {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final AgentPollFaults faults;

  public AgentFaultController(AgentPollFaults faults) {
    this.faults = faults;
  }

  @PostMapping(value = RunnerKeyAuthFilter.FAULT_PATH,
      consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> configure(
      @RequestAttribute(RunnerKeyAuthFilter.RUNNER_ATTRIBUTE) UUID runnerId,
      @RequestBody String body) {
    try {
      JsonNode node = MAPPER.readTree(body);
      if (node == null || !node.isObject()) {
        throw new IllegalArgumentException("fault controls require a JSON object");
      }
      UUID asserted = AgentProtocol.assertedRunnerId(node);
      if (asserted != null && !runnerId.equals(asserted)) {
        return ResponseEntity.status(403).contentType(MediaType.APPLICATION_JSON)
            .body(AgentProtocol.encodeErrorBody(new AgentProtocol.ErrorBody(
                "fenced_rejected", "runner_id does not match machine identity")));
      }
      boolean dropPoll = optionalBoolean(node, "drop_next_poll");
      long pollDelay = optionalDelay(node, "delay_next_poll_ms");
      long reportDelay = optionalDelay(node, "delay_next_report_ms");
      faults.configure(runnerId, dropPoll, pollDelay, reportDelay);
      return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body("{}");
    } catch (IllegalArgumentException e) {
      return badRequest(e.getMessage());
    } catch (tools.jackson.core.JacksonException e) {
      return badRequest("malformed JSON body");
    }
  }

  private static boolean optionalBoolean(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || value.isNull()) {
      return false;
    }
    if (!value.isBoolean()) {
      throw new IllegalArgumentException(field + " must be a boolean");
    }
    return value.asBoolean();
  }

  private static long optionalDelay(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || value.isNull()) {
      return 0;
    }
    if (!value.isIntegralNumber() || !value.canConvertToLong()
        || value.asLong() < 0 || value.asLong() > 5000) {
      throw new IllegalArgumentException(field + " must be an integer between 0 and 5000");
    }
    return value.asLong();
  }

  private static ResponseEntity<String> badRequest(String message) {
    return ResponseEntity.badRequest().contentType(MediaType.APPLICATION_JSON)
        .body(AgentProtocol.encodeErrorBody(new AgentProtocol.ErrorBody("bad_request", message)));
  }
}
