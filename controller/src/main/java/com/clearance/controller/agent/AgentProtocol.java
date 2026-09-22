package com.clearance.controller.agent;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * Versioned Java codec for the agent wire contract v1
 * ({@code contracts/agent-v1/README.md}).
 *
 * <p>Single place where field names, enum values, timestamp format, optional/null handling,
 * unknown-field tolerance, and error shapes are enforced for the controller side. Unknown
 * fields — including the reserved {@code recoveryGeneration} — are ignored, never rejected,
 * and never alter ownership interpretation. The codec intentionally never reads
 * {@code recoveryGeneration}.
 *
 * <p>Report fencing fields {@code allocation_id}, {@code runner_epoch},
 * {@code agent_incarnation}, {@code seq} are required. {@code seq} is per
 * {@code allocation_id}, starts at 1, sender-increments by 1 (enforced by senders and
 * checked by receivers; this codec validates presence and range, sequencing across
 * reports is endpoint state owned by later work). Terminal vocabulary is
 * {@code STARTING}, {@code RUNNING}, {@code SUCCEEDED}, {@code FAILED} plus
 * {@code HEARTBEAT}; stickiness and no-release semantics are documented in the contract
 * and owned by later endpoint work, not re-decided here.
 */
public final class AgentProtocol {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Pattern RUNNER_CLASS_PATTERN = Pattern.compile("[A-Za-z0-9._-]{1,128}");

  private static final Pattern UUID_PATTERN = Pattern.compile(
      "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
  private static final Pattern CODE_PATTERN = Pattern.compile("[a-z][a-z0-9]*(?:_[a-z0-9]+)*");
  private static final Pattern TIMESTAMP_PATTERN = Pattern.compile(
      "[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt](?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]"
          + "(?:\\.[0-9]{1,9})?(?:[Zz]|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])");

  private AgentProtocol() {}

  public enum ReportStatus {
    STARTING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    HEARTBEAT
  }

  public record ReportRequest(
      UUID allocationId,
      long runnerEpoch,
      long agentIncarnation,
      long seq,
      ReportStatus status,
      Instant ts,
      String detail,
      String error) {}

  public record PollResponse(
      boolean assigned,
      UUID allocationId,
      UUID jobId,
      Long runnerEpoch,
      List<String> argv,
      String runnerClass,
      Long pollAfterMs) {}

  public record ReportResponse(boolean accepted, String reason, boolean terminal) {}

  public record ErrorBody(String error, String message) {}

  // ---- ReportRequest ----

  public static ReportRequest parseReportRequest(String json) {
    try {
      JsonNode node = MAPPER.readTree(json);
      return parseReportRequest(node);
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalArgumentException("malformed JSON body: " + e.getMessage(), e);
    }
  }

  public static ReportRequest parseReportRequest(JsonNode node) {
    if (node == null || node.isNull() || !node.isObject()) {
      throw new IllegalArgumentException("report: expected JSON object");
    }
    UUID allocationId = requiredUuid(node, "allocation_id");
    long runnerEpoch = requiredLongMin(node, "runner_epoch", 1);
    long agentIncarnation = requiredLongMin(node, "agent_incarnation", 0);
    long seq = requiredLongMin(node, "seq", 1);
    ReportStatus status = requiredStatus(node);
    Instant ts = requiredInstant(node, "ts");
    String detail = optionalString(node, "detail");
    String error = optionalString(node, "error");
    // Unknown fields (including reserved recoveryGeneration) are ignored by construction:
    // only the fields above are read.
    return new ReportRequest(allocationId, runnerEpoch, agentIncarnation, seq, status, ts, detail, error);
  }

  public static String encodeReportRequest(ReportRequest r) {
    ObjectNode o = MAPPER.createObjectNode();
    o.put("allocation_id", r.allocationId().toString());
    o.put("runner_epoch", r.runnerEpoch());
    o.put("agent_incarnation", r.agentIncarnation());
    o.put("seq", r.seq());
    o.put("status", r.status().name());
    o.put("ts", DateTimeFormatter.ISO_INSTANT.format(r.ts()));
    if (r.detail() != null) {
      o.put("detail", r.detail());
    }
    if (r.error() != null) {
      o.put("error", r.error());
    }
    parseReportRequest(o);
    try {
      return MAPPER.writeValueAsString(o);
    } catch (Exception e) {
      throw new IllegalStateException("failed to encode report request", e);
    }
  }

  public static JsonNode toJsonNode(ReportRequest r) {
    try {
      return MAPPER.readTree(encodeReportRequest(r));
    } catch (Exception e) {
      throw new IllegalStateException("failed to encode report request", e);
    }
  }

  // ---- PollResponse ----

  public static PollResponse parsePollResponse(String json) {
    try {
      return parsePollResponse(MAPPER.readTree(json));
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalArgumentException("malformed JSON body: " + e.getMessage(), e);
    }
  }

  public static PollResponse parsePollResponse(JsonNode node) {
    if (node == null || node.isNull() || !node.isObject()) {
      throw new IllegalArgumentException("poll: expected JSON object");
    }
    boolean assigned = requiredBool(node, "assigned");
    Long pollAfterMs = optionalLongMin(node, "poll_after_ms", 0);
    if (!assigned) {
      // Idle: allocation fields, if present, are ignored (tolerance, never validated).
      return new PollResponse(false, null, null, null, null, null, pollAfterMs);
    }
    UUID allocationId = requiredUuid(node, "allocation_id");
    UUID jobId = requiredUuid(node, "job_id");
    long runnerEpoch = requiredLongMin(node, "runner_epoch", 1);
    List<String> argv = requiredArgv(node);
    String runnerClass = requiredRunnerClass(node);
    return new PollResponse(true, allocationId, jobId, runnerEpoch, argv, runnerClass, pollAfterMs);
  }

  public static String encodePollResponse(PollResponse p) {
    ObjectNode o = MAPPER.createObjectNode();
    o.put("assigned", p.assigned());
    if (p.pollAfterMs() != null) {
      o.put("poll_after_ms", p.pollAfterMs());
    }
    if (p.assigned()) {
      o.put("allocation_id", p.allocationId().toString());
      o.put("job_id", p.jobId().toString());
      o.put("runner_epoch", p.runnerEpoch());
      ArrayNode arr = o.putArray("argv");
      for (String a : p.argv()) {
        arr.add(a);
      }
      o.put("runner_class", p.runnerClass());
    }
    parsePollResponse(o);
    try {
      return MAPPER.writeValueAsString(o);
    } catch (Exception e) {
      throw new IllegalStateException("failed to encode poll response", e);
    }
  }

  // ---- ReportResponse ----

  public static ReportResponse parseReportResponse(String json) {
    try {
      return parseReportResponse(MAPPER.readTree(json));
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalArgumentException("malformed JSON body: " + e.getMessage(), e);
    }
  }

  public static ReportResponse parseReportResponse(JsonNode node) {
    if (node == null || node.isNull() || !node.isObject()) {
      throw new IllegalArgumentException("ack: expected JSON object");
    }
    boolean accepted = requiredBool(node, "accepted");
    String reason = requiredCode(node, "reason");
    boolean terminal = requiredBool(node, "terminal");
    return new ReportResponse(accepted, reason, terminal);
  }

  public static String encodeReportResponse(ReportResponse r) {
    ObjectNode o = MAPPER.createObjectNode();
    o.put("accepted", r.accepted());
    o.put("reason", r.reason());
    o.put("terminal", r.terminal());
    parseReportResponse(o);
    try {
      return MAPPER.writeValueAsString(o);
    } catch (Exception e) {
      throw new IllegalStateException("failed to encode report response", e);
    }
  }

  // ---- ErrorBody ----

  public static ErrorBody parseErrorBody(String json) {
    try {
      return parseErrorBody(MAPPER.readTree(json));
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalArgumentException("malformed JSON body: " + e.getMessage(), e);
    }
  }

  public static ErrorBody parseErrorBody(JsonNode node) {
    if (node == null || node.isNull() || !node.isObject()) {
      throw new IllegalArgumentException("error: expected JSON object");
    }
    String error = requiredCode(node, "error");
    String message = requiredNonEmptyString(node, "message");
    return new ErrorBody(error, message);
  }

  public static String encodeErrorBody(ErrorBody e) {
    ObjectNode o = MAPPER.createObjectNode();
    o.put("error", e.error());
    o.put("message", e.message());
    parseErrorBody(o);
    try {
      return MAPPER.writeValueAsString(o);
    } catch (Exception ex) {
      throw new IllegalStateException("failed to encode error body", ex);
    }
  }

  // ---- helpers ----

  private static String wireString(JsonNode node, String name) {
    String value = node.asString();
    for (int i = 0; i < value.length(); i++) {
      char ch = value.charAt(i);
      if (Character.isHighSurrogate(ch)) {
        if (++i == value.length() || !Character.isLowSurrogate(value.charAt(i))) {
          throw new IllegalArgumentException(name + " must contain Unicode scalar values");
        }
      } else if (Character.isLowSurrogate(ch)) {
        throw new IllegalArgumentException(name + " must contain Unicode scalar values");
      }
    }
    return value;
  }

  private static JsonNode field(JsonNode node, String name) {
    return node.get(name);
  }

  private static boolean isAbsentOrNull(JsonNode n) {
    return n == null || n.isNull();
  }

  private static UUID requiredUuid(JsonNode node, String name) {
    JsonNode n = field(node, name);
    if (isAbsentOrNull(n) || !n.isTextual() || n.asString().isEmpty()) {
      throw new IllegalArgumentException(name + " is required and must be a UUID string");
    }
    try {
      String value = wireString(n, name);
      if (!UUID_PATTERN.matcher(value).matches()) {
        throw new IllegalArgumentException(name + " must use the full UUID form");
      }
      return UUID.fromString(value);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(name + " must be a UUID string", e);
    }
  }

  private static long requiredLongMin(JsonNode node, String name, long min) {
    JsonNode n = field(node, name);
    if (isAbsentOrNull(n) || !n.isIntegralNumber()) {
      throw new IllegalArgumentException(name + " is required and must be an integer");
    }
    if (!n.canConvertToLong()) {
      throw new IllegalArgumentException(name + " must fit a signed 64-bit integer");
    }
    long v = n.asLong();
    if (v < min) {
      throw new IllegalArgumentException(name + " must be >= " + min);
    }
    return v;
  }

  private static Long optionalLongMin(JsonNode node, String name, long min) {
    JsonNode n = field(node, name);
    if (isAbsentOrNull(n)) {
      return null;
    }
    if (!n.isIntegralNumber()) {
      throw new IllegalArgumentException(name + " must be an integer when present");
    }
    if (!n.canConvertToLong()) {
      throw new IllegalArgumentException(name + " must fit a signed 64-bit integer");
    }
    long v = n.asLong();
    if (v < min) {
      throw new IllegalArgumentException(name + " must be >= " + min);
    }
    return v;
  }

  private static boolean requiredBool(JsonNode node, String name) {
    JsonNode n = field(node, name);
    if (isAbsentOrNull(n) || !n.isBoolean()) {
      throw new IllegalArgumentException(name + " is required and must be a boolean");
    }
    return n.asBoolean();
  }

  private static String requiredNonEmptyString(JsonNode node, String name) {
    JsonNode n = field(node, name);
    if (isAbsentOrNull(n) || !n.isTextual() || n.asString().isEmpty()) {
      throw new IllegalArgumentException(name + " is required and must be a non-empty string");
    }
    return wireString(n, name);
  }

  private static String optionalString(JsonNode node, String name) {
    JsonNode n = field(node, name);
    if (isAbsentOrNull(n)) {
      return null;
    }
    if (!n.isTextual()) {
      throw new IllegalArgumentException(name + " must be a string when present");
    }
    return wireString(n, name);
  }

  private static ReportStatus requiredStatus(JsonNode node) {
    JsonNode n = field(node, "status");
    if (isAbsentOrNull(n) || !n.isTextual()) {
      throw new IllegalArgumentException("status is required and must be one of STARTING,RUNNING,SUCCEEDED,FAILED,HEARTBEAT");
    }
    try {
      return ReportStatus.valueOf(wireString(n, "status"));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "status must be one of STARTING,RUNNING,SUCCEEDED,FAILED,HEARTBEAT", e);
    }
  }

  private static Instant requiredInstant(JsonNode node, String name) {
    JsonNode n = field(node, name);
    if (isAbsentOrNull(n) || !n.isTextual() || n.asString().isEmpty()) {
      throw new IllegalArgumentException(name + " is required and must be an RFC 3339 timestamp");
    }
    String s = wireString(n, name);
    if (!TIMESTAMP_PATTERN.matcher(s).matches()) {
      throw new IllegalArgumentException(name + " must be an RFC 3339 timestamp with at most 9 fractional digits");
    }
    try {
      boolean utc = s.endsWith("Z") || s.endsWith("z");
      int end = s.length() - (utc ? 1 : 6);
      // RFC 3339 offsets extend beyond java.time.ZoneOffset's 18-hour limit.
      Instant instant = LocalDateTime.parse(s.substring(0, end).replace('t', 'T'))
          .toInstant(ZoneOffset.UTC);
      if (!utc) {
        int offset = Integer.parseInt(s.substring(end + 1, end + 3)) * 3600
            + Integer.parseInt(s.substring(end + 4)) * 60;
        instant = instant.minusSeconds(s.charAt(end) == '+' ? offset : -offset);
      }
      int year = instant.atOffset(ZoneOffset.UTC).getYear();
      if (year < 0 || year > 9999) {
        throw new IllegalArgumentException(name + " UTC year must be 0000..9999");
      }
      return instant;
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(name + " must be an RFC 3339 timestamp", e);
    }
  }

  private static String requiredCode(JsonNode node, String name) {
    String value = requiredNonEmptyString(node, name);
    if (!CODE_PATTERN.matcher(value).matches()) {
      throw new IllegalArgumentException(name + " must be a snake_case code");
    }
    return value;
  }

  private static List<String> requiredArgv(JsonNode node) {
    JsonNode n = field(node, "argv");
    if (isAbsentOrNull(n) || !n.isArray() || n.size() == 0 || n.size() > 128) {
      throw new IllegalArgumentException("argv is required and must be an array of 1..128 strings");
    }
    List<String> out = new ArrayList<>(n.size());
    for (JsonNode e : n) {
      if (!e.isTextual() || e.asString().isEmpty() || e.asString().length() > 4096) {
        throw new IllegalArgumentException("argv elements must be strings of 1..4096 chars");
      }
      out.add(wireString(e, "argv"));
    }
    return List.copyOf(out);
  }

  private static String requiredRunnerClass(JsonNode node) {
    JsonNode n = field(node, "runner_class");
    if (isAbsentOrNull(n) || !n.isTextual()) {
      throw new IllegalArgumentException("runner_class is required");
    }
    String s = wireString(n, "runner_class");
    if (!RUNNER_CLASS_PATTERN.matcher(s).matches()) {
      throw new IllegalArgumentException("runner_class must match [A-Za-z0-9._-]{1,128}");
    }
    return s;
  }
}
