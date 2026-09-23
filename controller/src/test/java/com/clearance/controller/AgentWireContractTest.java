package com.clearance.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.clearance.controller.agent.AgentProtocol;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Function;
import tools.jackson.core.json.JsonWriteFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** Shared fixtures plus actual peer-produced JSON exchanged by contracts/agent-v1/verify.sh. */
class AgentWireContractTest {

  private static final ObjectMapper MAPPER =
      JsonMapper.builder().enable(JsonWriteFeature.ESCAPE_NON_ASCII).build();

  @Test
  void rejectsMalformedBodies() {
    List<Function<String, ?>> decoders = List.of(
        AgentProtocol::parseReportRequest, AgentProtocol::parsePollResponse,
        AgentProtocol::parseReportResponse, AgentProtocol::parseErrorBody);
    for (var decode : decoders) {
      for (String body : List.of("", "null", "[]", "true", "{", "{} {}")) {
        assertThrows(IllegalArgumentException.class, () -> decode.apply(body), body);
      }
    }
    assertThrows(IllegalArgumentException.class,
        () -> AgentProtocol.parsePollResponse("{\"assigned\":false} {}"));
  }

  @Test
  void encodersRejectInvalidTypedValues() {
    var report = new AgentProtocol.ReportRequest(UUID.randomUUID(), 1, 0, 0,
        AgentProtocol.ReportStatus.RUNNING, Instant.now(), null, null);
    assertThrows(IllegalArgumentException.class, () -> AgentProtocol.encodeReportRequest(report));
    var poll = new AgentProtocol.PollResponse(false, null, null, null, null, null, -1L);
    assertThrows(IllegalArgumentException.class, () -> AgentProtocol.encodePollResponse(poll));
    assertThrows(IllegalArgumentException.class,
        () -> AgentProtocol.encodeReportResponse(new AgentProtocol.ReportResponse(true, "", false)));
    assertThrows(IllegalArgumentException.class,
        () -> AgentProtocol.encodeErrorBody(new AgentProtocol.ErrorBody("bad_request", "")));
    var cleanup = new AgentProtocol.ReportRequest(UUID.randomUUID(), 1, 0, 1,
        AgentProtocol.ReportStatus.CLEANUP, Instant.now(), null, null,
        new AgentProtocol.CleanupEvidence(true, true, true, "\uD800"));
    assertThrows(IllegalArgumentException.class, () -> AgentProtocol.encodeReportRequest(cleanup));
    for (long timeout : List.of(0L, -1L, 86400001L)) {
      var allocation = new AgentProtocol.PollResponse(true, UUID.randomUUID(), UUID.randomUUID(),
          1L, List.of("true"), "default", null, false, timeout);
      assertThrows(IllegalArgumentException.class, () -> AgentProtocol.encodePollResponse(allocation));
    }
  }

  @Test
  void discoveryPidCountIsBoundedWithoutTruncation() {
    for (int count : List.of(4096, 4097)) {
      var discovery = new AgentProtocol.DiscoveryEvidence(true, true, false,
          java.util.Collections.nCopies(count, 1L), null);
      var report = new AgentProtocol.ReportRequest(UUID.randomUUID(), 1, 0, 1,
          AgentProtocol.ReportStatus.RECOVERY, Instant.now(), null, null, null, discovery);
      if (count == 4096) {
        assertEquals(count, AgentProtocol.parseReportRequest(
            AgentProtocol.encodeReportRequest(report)).discovery().pids().size());
      } else {
        assertThrows(IllegalArgumentException.class, () -> AgentProtocol.encodeReportRequest(report));
      }
    }
  }

  private static Path fixturesDir() {
    List<Path> candidates =
        List.of(
            Path.of("../contracts/agent-v1/fixtures"),
            Path.of("contracts/agent-v1/fixtures"));
    for (Path p : candidates) {
      if (Files.isDirectory(p)) {
        return p;
      }
    }
    // Fallback: walk up from cwd looking for contracts/agent-v1/fixtures.
    Path cur = Path.of("").toAbsolutePath();
    while (cur != null) {
      Path p = cur.resolve("contracts/agent-v1/fixtures");
      if (Files.isDirectory(p)) {
        return p;
      }
      cur = cur.getParent();
    }
    throw new IllegalStateException("fixtures dir not found; tried " + candidates);
  }

  private record Fixture(
      String name, String kind, JsonNode wire, boolean expectValid, JsonNode expect, String errorContains) {}

  private static List<Fixture> loadFixtures() throws Exception {
    Path dir = fixturesDir();
    List<Fixture> out = new ArrayList<>();
    try (Stream<Path> s = Files.list(dir)) {
      List<Path> files =
          s.filter(p -> p.toString().endsWith(".json")).sorted(Comparator.comparing(Path::toString)).toList();
      assertTrue(!files.isEmpty(), "no fixtures in " + dir);
      for (Path f : files) {
        JsonNode env = MAPPER.readTree(Files.readString(f));
        String name = env.get("name").asString();
        String kind = env.get("kind").asString();
        JsonNode wire = env.get("wire");
        assertNotNull(wire, "case " + name + ": missing wire");
        boolean valid = env.get("expect_valid").asBoolean();
        JsonNode expect = env.get("expect");
        JsonNode errNode = env.get("expect_error_contains");
        String errContains = (errNode == null || errNode.isNull()) ? null : errNode.asString();
        out.add(new Fixture(name, kind, wire, valid, expect, errContains));
      }
    }
    return out;
  }

  @TestFactory
  Stream<DynamicTest> matrixPerCase() throws Exception {
    List<DynamicTest> tests = new ArrayList<>();
    for (Fixture fx : loadFixtures()) {
      tests.add(dynamicTest("fixture/" + fx.name(), () -> {
        checkNamed("fixture", fx);
        String exportDir = System.getenv("WIRE_EXPORT_DIR");
        if (exportDir != null) {
          var envelope = MAPPER.createObjectNode();
          // The JSON serializer preserves null/unknown/error inputs for the peer's decoder.
          envelope.set("wire", fx.wire());
          if (fx.expectValid()) {
            envelope.set("encoded", MAPPER.readTree(encode(fx)));
          }
          Files.createDirectories(Path.of(exportDir));
          Files.writeString(Path.of(exportDir, fx.name() + ".json"), MAPPER.writeValueAsString(envelope));
        }
      }));
      String peerDir = System.getenv("WIRE_PEER_DIR");
      if (peerDir != null) {
        tests.add(dynamicTest("go-to-java/" + fx.name(), () -> {
          JsonNode peer = MAPPER.readTree(Files.readString(Path.of(peerDir, fx.name() + ".json")));
          assertNotNull(peer.get("wire"), fx.name() + ": missing peer wire");
          checkNamed("go-to-java/wire", withWire(fx, peer.get("wire")));
          if (fx.expectValid()) {
            assertNotNull(peer.get("encoded"), fx.name() + ": missing Go codec output");
            checkNamed("go-to-java/codec", withWire(fx, peer.get("encoded")));
          }
        }));
      }
    }
    return tests.stream();
  }

  private static Fixture withWire(Fixture fx, JsonNode wire) {
    return new Fixture(fx.name(), fx.kind(), wire, fx.expectValid(), fx.expect(), fx.errorContains());
  }

  private static void checkNamed(String direction, Fixture fx) throws Exception {
    try {
      check(fx);
      System.out.println("PASS " + direction + "/" + fx.name());
    } catch (Exception | AssertionError failure) {
      System.out.println("FAIL " + direction + "/" + fx.name() + ": " + failure.getMessage());
      throw failure;
    }
  }

  private static void check(Fixture fx) throws Exception {
    switch (fx.kind()) {
      case "report_request" -> checkReport(fx);
      case "poll_response" -> checkPoll(fx);
      case "report_response" -> checkAck(fx);
      case "error" -> checkError(fx);
      default -> fail("case " + fx.name() + ": unknown kind " + fx.kind());
    }
    if (fx.expectValid()) {
      JsonNode encoded = MAPPER.readTree(encode(fx));
      // Exact known-key set: unknown fields and explicit nulls must never leak back out.
      var expected = fx.expect().deepCopy();
      for (var property : fx.expect().properties()) {
        if (property.getValue().isNull()) {
          ((tools.jackson.databind.node.ObjectNode) expected).remove(property.getKey());
        }
      }
      if (fx.kind().equals("report_request")) {
        var ts = OffsetDateTime.parse(expected.get("ts").asString()).toInstant();
        ((tools.jackson.databind.node.ObjectNode) expected).put("ts", DateTimeFormatter.ISO_INSTANT.format(ts));
      }
      assertEquals(expected, encoded, fx.name() + ": encoded fields");
    }

  }

  private static String encode(Fixture fx) {
    String wire = fx.wire().toString();
    return switch (fx.kind()) {
      case "report_request" -> AgentProtocol.encodeReportRequest(AgentProtocol.parseReportRequest(wire));
      case "poll_response" -> AgentProtocol.encodePollResponse(AgentProtocol.parsePollResponse(wire));
      case "report_response" -> AgentProtocol.encodeReportResponse(AgentProtocol.parseReportResponse(wire));
      case "error" -> AgentProtocol.encodeErrorBody(AgentProtocol.parseErrorBody(wire));
      default -> throw new IllegalArgumentException("unknown kind " + fx.kind());
    };
  }

  // ---- report ----

  private static void checkReport(Fixture fx) throws Exception {
    String wireJson = MAPPER.writeValueAsString(fx.wire());
    if (!fx.expectValid()) {
      // Invalid input must be rejected with the offending field.
      try {
        AgentProtocol.ReportRequest got = AgentProtocol.parseReportRequest(wireJson);
        fail(
            "case "
                + fx.name()
                + ": expected invalid but decoded to "
                + got
                + " (field hint '" + fx.errorContains() + "')");
      } catch (IllegalArgumentException e) {
        assertNotNull(fx.errorContains(), "case " + fx.name() + ": invalid fixture needs expect_error_contains");
        String msg = (e.getMessage() == null ? "" : e.getMessage()).toLowerCase();
        assertTrue(
            msg.contains(fx.errorContains().toLowerCase()),
            "case "
                + fx.name()
                + ": error '" + e.getMessage() + "' must mention field '" + fx.errorContains() + "'");
      }
      return;
    }

    // Decode either the shared fixture or real peer output.
    AgentProtocol.ReportRequest decoded;
    try {
      decoded = AgentProtocol.parseReportRequest(wireJson);
    } catch (IllegalArgumentException e) {
      fail("case " + fx.name() + ": valid wire rejected: " + e.getMessage());
      return;
    }
    JsonNode exp = fx.expect();
    assertEquals(
        exp.get("allocation_id").asString(),
        decoded.allocationId().toString(),
        "case " + fx.name() + ": field allocation_id");
    assertEquals(
        exp.get("runner_epoch").asLong(), decoded.runnerEpoch(), "case " + fx.name() + ": field runner_epoch");
    assertEquals(
        exp.get("agent_incarnation").asLong(),
        decoded.agentIncarnation(),
        "case " + fx.name() + ": field agent_incarnation");
    assertEquals(exp.get("seq").asLong(), decoded.seq(), "case " + fx.name() + ": field seq");
    assertEquals(
        exp.get("status").asString(), decoded.status().name(), "case " + fx.name() + ": field status");
    // Timestamp: instants must match (offset input normalizes to UTC).
    var expectedTs =
        OffsetDateTime.parse(exp.get("ts").asString(), DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant();
    assertEquals(expectedTs, decoded.ts(), "case " + fx.name() + ": field ts (instant)");
    assertOptionalString(fx.name(), "detail", exp, decoded.detail());
    assertOptionalString(fx.name(), "error", exp, decoded.error());
    JsonNode cleanup = exp.get("cleanup");
    if (cleanup == null || cleanup.isNull()) {
      assertNull(decoded.cleanup(), fx.name() + ": absent cleanup");
    } else {
      assertNotNull(decoded.cleanup(), fx.name() + ": cleanup evidence");
      assertEquals(cleanup.get("execution_empty").asBoolean(), decoded.cleanup().executionEmpty(),
          fx.name() + ": execution_empty");
      assertEquals(cleanup.get("descendants_reaped").asBoolean(), decoded.cleanup().descendantsReaped(),
          fx.name() + ": descendants_reaped");
      assertEquals(cleanup.get("workspace_clean").asBoolean(), decoded.cleanup().workspaceClean(),
          fx.name() + ": workspace_clean");
      assertOptionalString(fx.name(), "error", cleanup, decoded.cleanup().error());
    }

    // Check local canonical encoding; the exchange runner passes it to Go.
    String encoded = AgentProtocol.encodeReportRequest(decoded);
    JsonNode rewire = MAPPER.readTree(encoded);
    // Canonical shape: required snake_case present, no recoveryGeneration, no unknown leakage.
    for (String f : List.of("allocation_id", "runner_epoch", "agent_incarnation", "seq", "status", "ts")) {
      assertTrue(
          rewire.has(f) && !rewire.get(f).isNull(),
          "case " + fx.name() + ": java-encoded wire missing required field " + f);
    }
    assertTrue(
        !rewire.has("recoveryGeneration"),
        "case " + fx.name() + ": java-encoded wire must never contain recoveryGeneration");
    // Absent optionals must be omitted (not explicit null).
    if (decoded.detail() == null) {
      assertTrue(
          !rewire.has("detail"),
          "case " + fx.name() + ": absent detail must be omitted");
    }
    if (decoded.error() == null) {
      assertTrue(
          !rewire.has("error"),
          "case " + fx.name() + ": absent error must be omitted");
    }
    // Timestamp must be UTC Z and round-trip the instant.
    String tsOut = rewire.get("ts").asString();
    assertTrue(
        tsOut.endsWith("Z"),
        "case " + fx.name() + ": java-encoded ts must be UTC Z, got " + tsOut);
    AgentProtocol.ReportRequest roundTrip = AgentProtocol.parseReportRequest(encoded);
    assertEquals(decoded, roundTrip, "case " + fx.name() + ": Java codec round-trip");
    assertEquals(
        decoded.ts(), roundTrip.ts(), "case " + fx.name() + ": timestamp round-trip instant");
  }

  // ---- poll ----

  private static void checkPoll(Fixture fx) throws Exception {
    String wireJson = MAPPER.writeValueAsString(fx.wire());
    if (!fx.expectValid()) {
      try {
        AgentProtocol.PollResponse got = AgentProtocol.parsePollResponse(wireJson);
        fail("case " + fx.name() + ": expected invalid but decoded to " + got);
      } catch (IllegalArgumentException e) {
        assertNotNull(fx.errorContains());
        String msg = (e.getMessage() == null ? "" : e.getMessage()).toLowerCase();
        assertTrue(
            msg.contains(fx.errorContains().toLowerCase()),
            "case "
                + fx.name()
                + ": error '" + e.getMessage() + "' must mention field '" + fx.errorContains() + "'");
      }
      return;
    }
    AgentProtocol.PollResponse decoded;
    try {
      decoded = AgentProtocol.parsePollResponse(wireJson);
    } catch (IllegalArgumentException e) {
      fail("case " + fx.name() + ": valid poll wire rejected: " + e.getMessage());
      return;
    }
    JsonNode exp = fx.expect();
    assertEquals(exp.get("assigned").asBoolean(), decoded.assigned(), "case " + fx.name() + ": field assigned");
    if (decoded.assigned()) {
      assertEquals(
          exp.get("allocation_id").asString(),
          decoded.allocationId().toString(),
          "case " + fx.name() + ": field allocation_id");
      assertEquals(
          exp.get("job_id").asString(), decoded.jobId().toString(), "case " + fx.name() + ": field job_id");
      assertEquals(
          exp.get("runner_epoch").asLong(),
          decoded.runnerEpoch(),
          "case " + fx.name() + ": field runner_epoch");
      List<String> argv = new ArrayList<>();
      exp.get("argv").forEach(n -> argv.add(n.asString()));
      assertEquals(argv, decoded.argv(), "case " + fx.name() + ": field argv");
      assertEquals(
          exp.get("runner_class").asString(),
          decoded.runnerClass(),
          "case " + fx.name() + ": field runner_class");
    }
    if (!decoded.assigned()) {
      assertNull(decoded.allocationId(), fx.name() + ": idle allocation_id");
      assertNull(decoded.jobId(), fx.name() + ": idle job_id");
      assertNull(decoded.runnerEpoch(), fx.name() + ": idle runner_epoch");
      assertNull(decoded.argv(), fx.name() + ": idle argv");
      assertNull(decoded.runnerClass(), fx.name() + ": idle runner_class");
      assertNull(decoded.cancelRequested(), fx.name() + ": idle cancel_requested");
      assertNull(decoded.workloadTimeoutMs(), fx.name() + ": idle workload_timeout_ms");
    }
    // poll_after_ms optional: absent/null -> null.
    JsonNode pam = exp.get("poll_after_ms");
    if (pam == null || pam.isNull()) {
      assertNull(decoded.pollAfterMs(), "case " + fx.name() + ": field poll_after_ms");
    } else {
      assertEquals(pam.asLong(), decoded.pollAfterMs(), "case " + fx.name() + ": field poll_after_ms");
    }
    JsonNode cancel = exp.get("cancel_requested");
    assertEquals(cancel == null || cancel.isNull() ? null : cancel.asBoolean(), decoded.cancelRequested(),
        fx.name() + ": cancel_requested");
    JsonNode timeout = exp.get("workload_timeout_ms");
    assertEquals(timeout == null || timeout.isNull() ? null : timeout.asLong(), decoded.workloadTimeoutMs(),
        fx.name() + ": workload_timeout_ms");

    String encoded = AgentProtocol.encodePollResponse(decoded);
    JsonNode rewire = MAPPER.readTree(encoded);
    assertTrue(rewire.has("assigned"), "case " + fx.name() + ": java-encoded poll missing assigned");
    assertTrue(
        !rewire.has("recoveryGeneration"),
        "case " + fx.name() + ": java-encoded poll must never contain recoveryGeneration");
    AgentProtocol.PollResponse roundTrip = AgentProtocol.parsePollResponse(encoded);
    assertEquals(decoded, roundTrip, "case " + fx.name() + ": Java codec round-trip");
  }

  // ---- ack ----

  private static void checkAck(Fixture fx) throws Exception {
    String wireJson = MAPPER.writeValueAsString(fx.wire());
    if (!fx.expectValid()) {
      try {
        AgentProtocol.ReportResponse got = AgentProtocol.parseReportResponse(wireJson);
        fail("case " + fx.name() + ": expected invalid but decoded to " + got);
      } catch (IllegalArgumentException e) {
        assertNotNull(fx.errorContains());
        String msg = (e.getMessage() == null ? "" : e.getMessage()).toLowerCase();
        assertTrue(
            msg.contains(fx.errorContains().toLowerCase()),
            "case "
                + fx.name()
                + ": error '" + e.getMessage() + "' must mention field '" + fx.errorContains() + "'");
      }
      return;
    }
    AgentProtocol.ReportResponse decoded;
    try {
      decoded = AgentProtocol.parseReportResponse(wireJson);
    } catch (IllegalArgumentException e) {
      fail("case " + fx.name() + ": valid ack wire rejected: " + e.getMessage());
      return;
    }
    JsonNode exp = fx.expect();
    assertEquals(exp.get("accepted").asBoolean(), decoded.accepted(), "case " + fx.name() + ": field accepted");
    assertEquals(exp.get("reason").asString(), decoded.reason(), "case " + fx.name() + ": field reason");
    assertEquals(exp.get("terminal").asBoolean(), decoded.terminal(), "case " + fx.name() + ": field terminal");

    String encoded = AgentProtocol.encodeReportResponse(decoded);
    AgentProtocol.ReportResponse roundTrip = AgentProtocol.parseReportResponse(encoded);
    assertEquals(decoded, roundTrip, "case " + fx.name() + ": Java codec round-trip");
  }

  // ---- error ----

  private static void checkError(Fixture fx) throws Exception {
    String wireJson = MAPPER.writeValueAsString(fx.wire());
    if (!fx.expectValid()) {
      try {
        AgentProtocol.ErrorBody got = AgentProtocol.parseErrorBody(wireJson);
        fail("case " + fx.name() + ": expected invalid but decoded to " + got);
      } catch (IllegalArgumentException e) {
        assertNotNull(fx.errorContains());
        String msg = (e.getMessage() == null ? "" : e.getMessage()).toLowerCase();
        assertTrue(
            msg.contains(fx.errorContains().toLowerCase()),
            "case "
                + fx.name()
                + ": error '" + e.getMessage() + "' must mention field '" + fx.errorContains() + "'");
      }
      return;
    }
    AgentProtocol.ErrorBody decoded;
    try {
      decoded = AgentProtocol.parseErrorBody(wireJson);
    } catch (IllegalArgumentException e) {
      fail("case " + fx.name() + ": valid error wire rejected: " + e.getMessage());
      return;
    }
    JsonNode exp = fx.expect();
    assertEquals(exp.get("error").asString(), decoded.error(), "case " + fx.name() + ": field error");
    assertEquals(exp.get("message").asString(), decoded.message(), "case " + fx.name() + ": field message");

    String encoded = AgentProtocol.encodeErrorBody(decoded);
    AgentProtocol.ErrorBody roundTrip = AgentProtocol.parseErrorBody(encoded);
    assertEquals(decoded, roundTrip, "case " + fx.name() + ": Java codec round-trip");
  }

  private static void assertOptionalString(String kase, String field, JsonNode exp, String actual) {
    JsonNode n = exp.get(field);
    if (n == null || n.isNull()) {
      assertNull(actual, "case " + kase + ": field " + field + " must be null when absent/null");
    } else {
      assertEquals(n.asString(), actual, "case " + kase + ": field " + field);
    }
  }
}
