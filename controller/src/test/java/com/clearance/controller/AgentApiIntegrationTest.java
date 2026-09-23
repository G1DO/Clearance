package com.clearance.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.clearance.controller.agent.AgentProtocol;
import com.clearance.controller.agent.AgentProtocol.PollResponse;
import com.clearance.controller.agent.AgentProtocol.CleanupEvidence;
import com.clearance.controller.agent.AgentProtocol.DiscoveryEvidence;
import com.clearance.controller.agent.AgentService;
import com.clearance.controller.agent.AgentProtocol.ReportRequest;
import com.clearance.controller.agent.AgentProtocol.ReportResponse;
import com.clearance.controller.agent.AgentProtocol.ReportStatus;
import com.clearance.controller.auth.AuthProperties;
import com.clearance.controller.jobs.JobService;
import com.clearance.controller.scheduling.Claim;
import com.clearance.controller.scheduling.SchedulerService;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.client.RestTemplate;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/** HTTP and real-PostgreSQL evidence for committed delivery and durable report fencing (#12). */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class AgentApiIntegrationTest {

  private static final String POLL = "/internal/v1/agents/poll";
  private static final String REPORT = "/internal/v1/agents/report";
  private static final String FAULTS = "/internal/v1/agents/test/faults";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final RestTemplate REST = nonThrowingRestTemplate();
  private static final long INCARNATION = 7;

  @LocalServerPort int port;
  @Autowired JdbcTemplate jdbc;
  @Autowired AuthProperties auth;
  @Autowired SchedulerService scheduler;
  @Autowired JobService jobs;
  @Autowired AgentService agents;
  @Autowired PlatformTransactionManager transactions;
  @Autowired DataSource dataSource;

  @Test
  void lostReplyRecoversByteIdenticalCommittedAllocationWithoutMutatingOwnershipOnRetry() {
    Agent agent = newAgent();
    assertFalse(poll(agent, INCARNATION).assigned());
    Claim claim = claim(agent);
    String expected =
        AgentProtocol.encodePollResponse(
            new PollResponse(true, claim.allocationId(), agent.jobId(), claim.runnerEpoch(),
                List.of("echo", "hi"), "default", null, false, 3_600_000L));
    fault(agent, "{\"drop_next_poll\":true}");
    var before = snapshot();
    Map<String, Object> expectedAllocation = new LinkedHashMap<>(allocation(claim));
    expectedAllocation.put("agent_incarnation", INCARNATION);

    ResponseEntity<String> dropped = post(agent, POLL, pollBody(INCARNATION));
    assertEquals(HttpStatus.SERVICE_UNAVAILABLE, dropped.getStatusCode());
    assertTrue(dropped.getBody() == null || dropped.getBody().isEmpty());
    var afterDrop = snapshot();
    assertEquals(before.get("runners"), afterDrop.get("runners"));
    assertEquals(before.get("attempts"), afterDrop.get("attempts"));
    assertEquals(before.get("jobs"), afterDrop.get("jobs"));
    assertEquals(expectedAllocation, allocation(claim), "first delivery only binds the allocation incarnation");
    assertEquals(1, activeCount(agent));
    assertEquals(1, attemptCount(agent));
    assertEquals(claim.runnerEpoch(), runnerEpoch(agent));

    ResponseEntity<String> retry = post(agent, POLL, pollBody(INCARNATION));
    assertEquals(HttpStatus.OK, retry.getStatusCode());
    assertEquals(expected, retry.getBody(), "the retry must reproduce the committed wire bytes");
    assertEquals(expected, post(agent, POLL, pollBody(INCARNATION)).getBody());
    assertEquals(afterDrop, snapshot(), "retry must not rewrite rows, add allocations, or advance epochs");
  }

  @Test
  void staleEpochUnknownAllocationAndUnboundReportsAreZeroMutation() {
    Agent agent = newAgent();
    Claim claim = claim(agent);
    var unbound = snapshot();
    assertRejected(report(agent, claim, INCARNATION, 1, ReportStatus.RUNNING), "fenced_rejected");
    assertEquals(unbound, snapshot(), "reports cannot establish an incarnation");
    poll(agent, INCARNATION);

    for (long epoch : List.of(claim.runnerEpoch() - 1, claim.runnerEpoch() + 1)) {
      var before = snapshot();
      String body = reportBody(claim.allocationId(), epoch, INCARNATION, 12, ReportStatus.RUNNING);
      assertRejected(ack(post(agent, REPORT, body)), "fenced_rejected");
      assertEquals(before, snapshot(), "a non-current epoch must not even rewrite a row");
    }
    var before = snapshot();
    assertRejected(
        ack(post(agent, REPORT,
            reportBody(UUID.randomUUID(), claim.runnerEpoch(), INCARNATION, 12,
                ReportStatus.HEARTBEAT))),
        "fenced_rejected");
    assertEquals(before, snapshot(), "an unknown allocation must not write any table");
  }

  @Test
  void newerIncarnationFencesOldActorAndRetainsAllocationSequence() {
    Agent agent = newAgent();
    Claim claim = claim(agent);
    PollResponse original = poll(agent, INCARNATION);
    assertTrue(report(agent, claim, INCARNATION, 8, ReportStatus.RUNNING).accepted());

    assertEquals(original, poll(agent, INCARNATION + 1));
    assertEquals(INCARNATION + 1, jdbc.queryForObject(
        "SELECT agent_incarnation FROM runners WHERE runner_id = ?", Long.class, agent.runnerId()));
    assertEquals(INCARNATION + 1, allocation(claim).get("agent_incarnation"));
    assertEquals(8L, allocation(claim).get("max_seq"));
    var before = snapshot();
    assertRejected(report(agent, claim, INCARNATION, 100, ReportStatus.SUCCEEDED), "fenced_rejected");
    assertEquals(before, snapshot());
    ResponseEntity<String> stalePoll = post(agent, POLL, pollBody(INCARNATION));
    assertEquals(HttpStatus.CONFLICT, stalePoll.getStatusCode());
    assertJson(stalePoll);
    assertEquals(before, snapshot(), "a delayed old poll must not reclaim the incarnation");
    assertRejected(report(agent, claim, INCARNATION + 1, 8, ReportStatus.RUNNING), "dropped_stale");
    assertEquals(before, snapshot(), "restart does not reset per-allocation seq");
    assertTrue(report(agent, claim, INCARNATION + 1, 9, ReportStatus.HEARTBEAT).accepted());
    assertEquals(9L, allocation(claim).get("max_seq"));
    assertEquals("RUNNING", allocation(claim).get("report_status"));
    assertTrue(report(agent, claim, INCARNATION + 1, 10, ReportStatus.SUCCEEDED).accepted());
    assertEquals(original, poll(agent, Long.MAX_VALUE));
    assertEquals(10L, allocation(claim).get("max_seq"));
    assertEquals("SUCCEEDED", allocation(claim).get("report_status"));
    assertTrue(report(agent, claim, Long.MAX_VALUE, Long.MAX_VALUE, ReportStatus.HEARTBEAT).accepted());
    before = snapshot();
    assertRejected(report(agent, claim, INCARNATION + 1, Long.MAX_VALUE, ReportStatus.HEARTBEAT), "fenced_rejected");
    assertRejected(report(agent, claim, Long.MAX_VALUE, Long.MAX_VALUE, ReportStatus.HEARTBEAT), "dropped_stale");
    assertEquals(HttpStatus.CONFLICT, post(agent, POLL, pollBody(Long.MAX_VALUE - 1)).getStatusCode());
    assertEquals(before, snapshot(), "signed-64-bit boundary values cannot wrap or clear terminal state");
  }

  @ParameterizedTest
  @ValueSource(strings = {"SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"})
  void reorderedReportsKeepEitherTerminalStickyAndNeverReleaseTheRunner(String terminalName) {
    Agent agent = newAgent();
    Claim claim = claim(agent);
    poll(agent, INCARNATION);
    ReportStatus terminal = ReportStatus.valueOf(terminalName);
    assertTrue(report(agent, claim, INCARNATION, 8, ReportStatus.RUNNING).accepted());
    ReportResponse accepted = report(agent, claim, INCARNATION, 10, terminal);
    assertTrue(accepted.accepted());
    assertTrue(accepted.terminal());
    var before = snapshot();

    assertRejected(report(agent, claim, INCARNATION, 9, ReportStatus.RUNNING), "dropped_stale");
    assertEquals(before, snapshot());
    assertRejected(report(agent, claim, INCARNATION, 10, terminal), "dropped_stale");
    assertEquals(before, snapshot());
    assertRejected(report(agent, claim, INCARNATION, 11, ReportStatus.STARTING), "terminal_sticky");
    assertEquals(before, snapshot());
    ReportStatus opposite = terminal == ReportStatus.SUCCEEDED ? ReportStatus.FAILED : ReportStatus.SUCCEEDED;
    assertRejected(report(agent, claim, INCARNATION, 12, opposite), "terminal_sticky");
    assertEquals(before, snapshot(), "even a newer conflicting terminal is a no-op");

    ReportResponse heartbeat = report(agent, claim, INCARNATION, 13, ReportStatus.HEARTBEAT);
    assertTrue(heartbeat.accepted());
    assertTrue(heartbeat.terminal());
    assertTrue(report(agent, claim, INCARNATION, 14, terminal).accepted());
    assertEquals(terminalName, allocation(claim).get("report_status"));
    assertEquals(14L, allocation(claim).get("max_seq"));
    assertEquals("ACTIVE", allocation(claim).get("state"));
    assertEquals("CLEANING", runnerState(agent));
    assertEquals(terminalName, jobs.getForProject(agent.jobId(), "project-alpha").result());
    assertEquals(terminalName, jdbc.queryForObject("SELECT result FROM attempts WHERE attempt_id = ?",
        String.class, claim.attemptId()));
    assertEquals(claim.runnerEpoch(), runnerEpoch(agent));
    assertTrue(scheduler.claim(agent.jobId(), agent.runnerId()).isEmpty(),
        "a terminal report supplies no cleanup proof and cannot make the runner reusable");
  }

  @Test
  void heartbeatOnlyAdvancesSequenceAndCannotChangeQuarantineOrProgress() {
    Agent agent = newAgent();
    Claim claim = claim(agent);
    poll(agent, INCARNATION);
    var runnerBefore = runnerRows();
    assertTrue(report(agent, claim, INCARNATION, 1, ReportStatus.HEARTBEAT).accepted());
    assertEquals(runnerBefore, runnerRows());
    assertEquals(null, allocation(claim).get("report_status"));
    assertTrue(report(agent, claim, INCARNATION, 2, ReportStatus.RUNNING).accepted());
    assertTrue(report(agent, claim, INCARNATION, 3, ReportStatus.STARTING).accepted());
    assertEquals("RUNNING", allocation(claim).get("report_status"));
    assertEquals(runnerBefore, runnerRows(), "report vocabulary does not change ownership lifecycle");

    jdbc.update("UPDATE runners SET state = 'QUARANTINED' WHERE runner_id = ?", agent.runnerId());
    runnerBefore = runnerRows();
    assertTrue(report(agent, claim, INCARNATION, 4, ReportStatus.HEARTBEAT).accepted());
    assertTrue(report(agent, claim, INCARNATION, 5, ReportStatus.FAILED).accepted());
    assertEquals(runnerBefore, runnerRows(), "liveness or completion must not clear quarantine");
    var before = snapshot();
    assertRejected(report(agent, claim, INCARNATION - 1, 6, ReportStatus.HEARTBEAT), "fenced_rejected");
    assertEquals(before, snapshot());
    assertRejected(report(agent, claim, INCARNATION, 4, ReportStatus.HEARTBEAT), "dropped_stale");
    assertEquals(before, snapshot());
    assertEquals("QUARANTINED", runnerState(agent));
  }

  @Test
  void identitiesCannotCrossRunnerProjectOrApiBoundaries() {
    Agent agent = newAgent();
    Agent other = newAgent("project-beta");
    Claim claim = claim(agent);
    Claim otherClaim = claim(other);
    poll(agent, INCARNATION);
    poll(other, INCARNATION);
    var before = snapshot();

    for (String path : List.of(POLL, REPORT, FAULTS)) {
      String body = path.equals(REPORT)
          ? reportBody(claim.allocationId(), claim.runnerEpoch(), INCARNATION, 1, ReportStatus.RUNNING)
          : pollBody(INCARNATION);
      assertEquals(HttpStatus.UNAUTHORIZED, request(port, null, path, HttpMethod.POST, body).getStatusCode());
      assertEquals(HttpStatus.UNAUTHORIZED, request(port, "invalid-machine-key", path, HttpMethod.POST, body).getStatusCode());
      assertEquals(HttpStatus.UNAUTHORIZED, request(port, "test-key-alpha", path, HttpMethod.POST, body).getStatusCode());
      assertEquals(HttpStatus.UNAUTHORIZED, request(port, "test-key-beta", path, HttpMethod.POST, body).getStatusCode());
    }
    assertEquals(HttpStatus.FORBIDDEN, post(agent, POLL,
        "{\"agent_incarnation\":7,\"timeout_s\":0,\"runner_id\":\"" + other.runnerId() + "\"}").getStatusCode());
    ObjectNode claimedOther = (ObjectNode) MAPPER.readTree(
        reportBody(claim.allocationId(), claim.runnerEpoch(), INCARNATION, 1, ReportStatus.RUNNING));
    claimedOther.put("runner_id", other.runnerId().toString());
    assertEquals(HttpStatus.FORBIDDEN, post(agent, REPORT, claimedOther.toString()).getStatusCode());
    assertEquals(HttpStatus.FORBIDDEN, post(agent, FAULTS,
        "{\"runner_id\":\"" + other.runnerId() + "\",\"drop_next_poll\":true}").getStatusCode());
    assertEquals(HttpStatus.FORBIDDEN, post(agent, REPORT,
        reportBody(otherClaim.allocationId(), otherClaim.runnerEpoch(), INCARNATION, 1,
            ReportStatus.RUNNING)).getStatusCode());
    assertEquals(HttpStatus.UNAUTHORIZED, request(port, agent.key(), "/api/v1/jobs/" + other.jobId(),
        HttpMethod.GET, null).getStatusCode());
    assertEquals(HttpStatus.UNAUTHORIZED, request(port, agent.key(), "/api/v1/jobs", HttpMethod.POST,
        "{\"argv\":[\"echo\"],\"runnerClass\":\"default\",\"project_id\":\"project-beta\"}").getStatusCode());
    assertEquals(HttpStatus.NOT_FOUND, request(port, "test-key-alpha", "/api/v1/jobs/" + other.jobId(),
        HttpMethod.GET, null).getStatusCode());
    assertEquals(HttpStatus.FORBIDDEN, post(agent, "/internal/v1/operators/release", "{}").getStatusCode());
    String unseededKey = "unseeded-" + UUID.randomUUID();
    auth.getRunnerKeys().put(unseededKey, UUID.randomUUID());
    assertEquals(HttpStatus.FORBIDDEN,
        request(port, unseededKey, POLL, HttpMethod.POST, pollBody(INCARNATION)).getStatusCode());
    auth.getApiKeys().put(agent.key(), "project-alpha");
    try {
      assertEquals(HttpStatus.UNAUTHORIZED, post(agent, POLL, pollBody(INCARNATION)).getStatusCode());
      assertEquals(HttpStatus.UNAUTHORIZED,
          request(port, agent.key(), "/api/v1/jobs/" + agent.jobId(), HttpMethod.GET, null).getStatusCode());
    } finally {
      auth.getApiKeys().remove(agent.key());
    }
    assertEquals(before, snapshot(), "all rejected credentials and identities must leave every table unchanged");
  }

  @Test
  void invalidRequestsAreRejectedBeforeBindingAndReservedFieldsAreIgnored() {
    Agent agent = newAgent();
    Claim claim = claim(agent);
    var before = snapshot();
    for (String body : List.of("{}", "null", "[]", "{",
        "{\"agent_incarnation\":-1}", "{\"agent_incarnation\":1.0}",
        "{\"agent_incarnation\":\"7\"}", "{\"agent_incarnation\":9223372036854775808}",
        "{\"agent_incarnation\":7,\"timeout_s\":-1}",
        "{\"agent_incarnation\":7,\"runner_id\":\"1-1-1-1-1\"}")) {
      ResponseEntity<String> response = post(agent, POLL, body);
      assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode(), body);
      assertJson(response);
      assertEquals("bad_request", AgentProtocol.parseErrorBody(response.getBody()).error());
      assertEquals(before, snapshot());
    }
    ResponseEntity<String> bound = post(agent, POLL,
        "{\"agent_incarnation\":7,\"timeout_s\":null,\"runner_id\":null,"
            + "\"recoveryGeneration\":{\"nonsense\":true}}");
    assertEquals(HttpStatus.OK, bound.getStatusCode());
    before = snapshot();
    String valid = reportBody(claim.allocationId(), claim.runnerEpoch(), INCARNATION, 1, ReportStatus.RUNNING);
    for (String body : List.of("{}", valid.replace("\"seq\":1", "\"seq\":0"),
        valid.replace("\"RUNNING\"", "\"AVAILABLE\""), valid.replace("\"agent_incarnation\":7", "\"agent_incarnation\":null"))) {
      assertEquals(HttpStatus.BAD_REQUEST, post(agent, REPORT, body).getStatusCode());
      assertEquals(before, snapshot());
    }
    ObjectNode report = (ObjectNode) MAPPER.readTree(valid);
    report.putObject("recoveryGeneration").put("arbitrary", "ignored");
    report.putNull("detail");
    report.putNull("error");
    report.put("runner_id", agent.runnerId().toString());
    assertTrue(ack(post(agent, REPORT, report.toString())).accepted());
    before = snapshot();
    report.put("recoveryGeneration", Long.MIN_VALUE);
    assertRejected(ack(post(agent, REPORT, report.toString())), "dropped_stale");
    assertEquals(before, snapshot(), "reserved fields never alter ownership or sequencing");
  }

  @Test
  void waitingPollDeliversOnlyAfterClaimCommitAndNeverCreatesOwnership() throws Exception {
    Agent agent = newAgent();
    assertFalse(poll(agent, INCARNATION).assigned());
    CountDownLatch claimed = new CountDownLatch(1);
    CountDownLatch commit = new CountDownLatch(1);
    try (var executor = Executors.newFixedThreadPool(2)) {
      Future<Claim> claiming = executor.submit(() -> new TransactionTemplate(transactions).execute(status -> {
        Claim claim = claim(agent);
        claimed.countDown();
        await(commit);
        return claim;
      }));
      assertTrue(claimed.await(10, TimeUnit.SECONDS));
      Future<ResponseEntity<String>> polling = executor.submit(() -> post(agent, POLL,
          "{\"agent_incarnation\":7,\"timeout_s\":3}"));
      Thread.sleep(150);
      assertFalse(polling.isDone(), "an uncommitted claim must never reach the agent");
      assertEquals(0, activeCount(agent));
      commit.countDown();
      Claim claim = claiming.get(10, TimeUnit.SECONDS);
      ResponseEntity<String> response = polling.get(10, TimeUnit.SECONDS);
      assertEquals(HttpStatus.OK, response.getStatusCode());
      assertEquals(claim.allocationId(), AgentProtocol.parsePollResponse(response.getBody()).allocationId());
      assertEquals(1, activeCount(agent));
      assertEquals(1, attemptCount(agent));
    } finally {
      commit.countDown();
    }
  }

  @Test
  void rolledBackClaimIsNeverDeliveredAndIdlePollDoesNotHoldRunnerLock() throws Exception {
    Agent agent = newAgent();
    assertFalse(poll(agent, INCARNATION).assigned());
    try (var executor = Executors.newSingleThreadExecutor()) {
      Future<ResponseEntity<String>> polling = executor.submit(() -> post(agent, POLL,
          "{\"agent_incarnation\":7,\"timeout_s\":1}"));
      Thread.sleep(100);
      new TransactionTemplate(transactions).executeWithoutResult(status -> {
        // Fail promptly if long-poll sleeps with the runner row locked.
        jdbc.execute("SET LOCAL lock_timeout = '500ms'");
        claim(agent);
        status.setRollbackOnly();
      });
      ResponseEntity<String> response = polling.get(10, TimeUnit.SECONDS);
      assertEquals(HttpStatus.OK, response.getStatusCode());
      assertFalse(AgentProtocol.parsePollResponse(response.getBody()).assigned());
      assertEquals(0, activeCount(agent));
      assertEquals(0, attemptCount(agent));
      assertEquals("AVAILABLE", runnerState(agent));
      assertEquals(4, runnerEpoch(agent));
    }
  }

  @Test
  void delayedOldReportIsFencedAfterIncarnationRotation() throws Exception {
    Agent agent = newAgent();
    Claim claim = claim(agent);
    poll(agent, INCARNATION);
    fault(agent, "{\"delay_next_report_ms\":500}");
    try (var executor = Executors.newSingleThreadExecutor()) {
      Future<ReportResponse> delayed = executor.submit(() -> report(agent, claim, INCARNATION, 1, ReportStatus.SUCCEEDED));
      Thread.sleep(100);
      assertFalse(delayed.isDone());
      poll(agent, INCARNATION + 1);
      var before = snapshot();
      assertRejected(delayed.get(10, TimeUnit.SECONDS), "fenced_rejected");
      assertEquals(before, snapshot(), "delay must precede the transaction and fencing decision");
    }
  }

  @Test
  void independentControllersSerializeDuplicateReportsAndConcurrentPolls() throws Exception {
    Agent agent = newAgent();
    Claim claim = claim(agent);
    PollResponse assigned = poll(agent, INCARNATION);
    try (ConfigurableApplicationContext other = startApp("test")) {
      other.getBean(AuthProperties.class).getRunnerKeys().put(agent.key(), agent.runnerId());
      assertNotSame(dataSource, other.getBean(DataSource.class));
      int otherPort = appPort(other);
      CountDownLatch start = new CountDownLatch(1);
      try (var executor = Executors.newFixedThreadPool(8)) {
        List<Future<ResponseEntity<String>>> reports = new ArrayList<>();
        List<Future<ResponseEntity<String>>> polls = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
          int destination = i % 2 == 0 ? port : otherPort;
          reports.add(executor.submit(() -> {
            await(start);
            return request(destination, agent.key(), REPORT, HttpMethod.POST,
                reportBody(claim.allocationId(), claim.runnerEpoch(), INCARNATION, 20, ReportStatus.RUNNING));
          }));
          polls.add(executor.submit(() -> {
            await(start);
            return request(destination, agent.key(), POLL, HttpMethod.POST, pollBody(INCARNATION));
          }));
        }
        start.countDown();
        int accepted = 0;
        for (Future<ResponseEntity<String>> result : reports) {
          ReportResponse ack = ack(result.get(15, TimeUnit.SECONDS));
          if (ack.accepted()) accepted++;
          else assertEquals("dropped_stale", ack.reason());
        }
        assertEquals(1, accepted, "max_seq compare and update must commit atomically across instances");
        for (Future<ResponseEntity<String>> result : polls) {
          ResponseEntity<String> response = result.get(15, TimeUnit.SECONDS);
          assertEquals(HttpStatus.OK, response.getStatusCode());
          assertEquals(assigned, AgentProtocol.parsePollResponse(response.getBody()));
        }
      }
      assertEquals(20L, allocation(claim).get("max_seq"));
      assertEquals(1, activeCount(agent));
      assertEquals(1, attemptCount(agent));
      assertEquals(claim.runnerEpoch(), runnerEpoch(agent));
    }
  }

  @Test
  void reportFenceAndAssignmentSurviveControllerRestartAndFaultControlsAreAbsentInProd() {
    Agent agent = newAgent();
    Claim claim = claim(agent);
    String original;
    try (ConfigurableApplicationContext first = startApp("test")) {
      first.getBean(AuthProperties.class).getRunnerKeys().put(agent.key(), agent.runnerId());
      int firstPort = appPort(first);
      original = request(firstPort, agent.key(), POLL, HttpMethod.POST, pollBody(INCARNATION)).getBody();
      assertTrue(ack(request(firstPort, agent.key(), REPORT, HttpMethod.POST,
          reportBody(claim.allocationId(), claim.runnerEpoch(), INCARNATION, 10, ReportStatus.SUCCEEDED))).accepted());
    }
    var before = snapshot();
    try (ConfigurableApplicationContext restarted = startApp("prod")) {
      restarted.getBean(AuthProperties.class).getRunnerKeys().put(agent.key(), agent.runnerId());
      int restartPort = appPort(restarted);
      assertEquals("SUCCEEDED", restarted.getBean(JobService.class).getForProject(agent.jobId(), "project-alpha").result());
      assertEquals("SUCCEEDED", restarted.getBean(JdbcTemplate.class).queryForObject(
          "SELECT result FROM attempts WHERE attempt_id = ?", String.class, claim.attemptId()));
      assertEquals("CLEANING", runnerState(agent));
      assertEquals(original, request(restartPort, agent.key(), POLL, HttpMethod.POST, pollBody(INCARNATION)).getBody());
      assertRejected(ack(request(restartPort, agent.key(), REPORT, HttpMethod.POST,
          reportBody(claim.allocationId(), claim.runnerEpoch(), INCARNATION, 9, ReportStatus.RUNNING))), "dropped_stale");
      assertRejected(ack(request(restartPort, agent.key(), REPORT, HttpMethod.POST,
          reportBody(claim.allocationId(), claim.runnerEpoch(), INCARNATION - 1, 11, ReportStatus.HEARTBEAT))), "fenced_rejected");
      assertTrue(restarted.getBeansOfType(com.clearance.controller.agent.AgentFaultController.class).isEmpty());
      for (String token : new String[] {null, agent.key(), "test-key-alpha"}) {
        assertEquals(HttpStatus.NOT_FOUND, request(restartPort, token, FAULTS, HttpMethod.POST,
            "{\"drop_next_poll\":true,\"delay_next_poll_ms\":10,\"delay_next_report_ms\":10}").getStatusCode());
      }
      assertEquals(before, snapshot(), "restarting must preserve durable fences without rewriting rows");
    }
  }

  @Test
  void cleanupRequiresCompleteCurrentProofAndLostAcknowledgmentRetryCannotReleaseNewOwnership() {
    Agent agent = newAgent();
    Claim claim = claim(agent);
    poll(agent, INCARNATION);
    var before = snapshot();
    assertRejected(cleanup(agent, claim, INCARNATION, 1, positiveEvidence()), "terminal_required");
    assertEquals(before, snapshot());
    report(agent, claim, INCARNATION, 2, ReportStatus.SUCCEEDED);
    UUID nextJob = jobs.submit("project-alpha", "next-" + UUID.randomUUID(), List.of("true"), "default").job().jobId();
    assertTrue(scheduler.claim(nextJob, agent.runnerId()).isEmpty());
    before = snapshot();
    assertRejected(cleanup(agent, claim, INCARNATION, 3, null), "cleanup_required");
    for (String evidence : List.of("{}", "{\"execution_empty\":true,\"workspace_clean\":true}",
        "{\"execution_empty\":true,\"descendants_reaped\":null,\"workspace_clean\":true}")) {
      ObjectNode body = (ObjectNode) MAPPER.readTree(cleanupBody(claim, INCARNATION, 3, positiveEvidence()));
      body.set("cleanup", MAPPER.readTree(evidence));
      assertEquals(HttpStatus.BAD_REQUEST, post(agent, REPORT, body.toString()).getStatusCode());
    }
    assertRejected(cleanup(agent, claim, INCARNATION, 2, positiveEvidence()), "dropped_stale");
    assertRejected(cleanup(agent, claim, INCARNATION - 1, 3, positiveEvidence()), "fenced_rejected");
    Claim unknown = new Claim(UUID.randomUUID(), claim.attemptId(), claim.jobId(), claim.runnerId(),
        claim.runnerEpoch(), claim.createdAt());
    assertRejected(cleanup(agent, unknown, INCARNATION, 3, positiveEvidence()), "fenced_rejected");
    Claim oldEpoch = new Claim(claim.allocationId(), claim.attemptId(), claim.jobId(), claim.runnerId(),
        claim.runnerEpoch() - 1, claim.createdAt());
    assertRejected(cleanup(agent, oldEpoch, INCARNATION, 3, positiveEvidence()), "fenced_rejected");
    assertEquals(before, snapshot(), "missing, incomplete and fenced proof must not update any row");

    assertTrue(cleanup(agent, claim, INCARNATION, 3, positiveEvidence()).accepted());
    assertEquals("AVAILABLE", runnerState(agent));
    assertEquals("RELEASED", allocation(claim).get("state"));
    assertEquals(0, activeCount(agent));
    assertTrue(allocation(claim).get("cleanup_evidence").toString().contains("execution_empty"));
    before = snapshot();
    assertTrue(cleanup(agent, claim, INCARNATION, 3, positiveEvidence()).accepted());
    assertTrue(cleanup(agent, claim, INCARNATION, 4, positiveEvidence()).accepted());
    assertEquals(before, snapshot(), "lost acknowledgment retries are read-only");
    assertTrue(scheduler.claim(agent.jobId(), agent.runnerId()).isEmpty(), "terminal jobs never execute again");
    Claim next = scheduler.claim(nextJob, agent.runnerId()).orElseThrow();
    assertTrue(next.runnerEpoch() > claim.runnerEpoch());
    assertFalse(next.allocationId().equals(claim.allocationId()));
    assertFalse(next.attemptId().equals(claim.attemptId()));
    before = snapshot();
    assertRejected(cleanup(agent, claim, INCARNATION, 5, positiveEvidence()), "fenced_rejected");
    assertRejected(cleanup(agent, claim, INCARNATION, 6,
        new CleanupEvidence(false, false, false, "old inspection failed")), "fenced_rejected");
    assertEquals(before, snapshot(), "old proof can neither release nor quarantine the next allocation");
  }

  @ParameterizedTest
  @ValueSource(strings = {"execution", "reaping", "workspace", "error", "empty-error"})
  void cleanupFailureQuarantinesWithDurableReasonAndCannotBeCleared(String failure) {
    Agent agent = newAgent();
    Claim claim = claim(agent);
    poll(agent, INCARNATION);
    report(agent, claim, INCARNATION, 1, ReportStatus.TIMED_OUT);
    CleanupEvidence failed = new CleanupEvidence(!failure.equals("execution"), !failure.equals("reaping"),
        !failure.equals("workspace"), failure.equals("error") ? "inspection denied" : failure.equals("empty-error") ? "" : null);
    assertEquals("quarantined", cleanup(agent, claim, INCARNATION, 2, failed).reason());
    assertEquals("QUARANTINED", runnerState(agent));
    String reason = jdbc.queryForObject("SELECT quarantine_reason FROM runners WHERE runner_id = ?",
        String.class, agent.runnerId());
    assertTrue(reason.contains("cleanup failed"));
    assertTrue(reason.contains(failure.equals("error") ? "inspection denied" : failure.equals("empty-error") ? "error" : "false"));
    var before = snapshot();
    assertRejected(cleanup(agent, claim, INCARNATION, 3, positiveEvidence()), "quarantined");
    assertEquals(before, snapshot());
    assertTrue(report(agent, claim, INCARNATION, 4, ReportStatus.HEARTBEAT).accepted());
    assertTrue(report(agent, claim, INCARNATION, 5, ReportStatus.TIMED_OUT).accepted());
    assertEquals("TIMED_OUT", jobs.getForProject(agent.jobId(), "project-alpha").result());
    assertEquals(reason, jdbc.queryForObject("SELECT quarantine_reason FROM runners WHERE runner_id = ?",
        String.class, agent.runnerId()));
    assertEquals("QUARANTINED", runnerState(agent));
    UUID otherJob = jobs.submit("project-alpha", "quarantine-" + UUID.randomUUID(), List.of("true"), "default").job().jobId();
    assertTrue(scheduler.claim(otherJob, agent.runnerId()).isEmpty());
    try (ConfigurableApplicationContext restarted = startApp("prod")) {
      assertEquals("TIMED_OUT", restarted.getBean(JobService.class).getForProject(agent.jobId(), "project-alpha").result());
      assertTrue(restarted.getBean(SchedulerService.class).claim(otherJob, agent.runnerId()).isEmpty());
      assertEquals(reason, restarted.getBean(JdbcTemplate.class).queryForObject(
          "SELECT quarantine_reason FROM runners WHERE runner_id = ?", String.class, agent.runnerId()));
    }
  }

  @Test
  void cancellationIsProjectAuthorizedIdempotentAndDeliveredWithoutChangingSubmissionIdentity() {
    Agent queued = newAgent();
    String path = "/api/v1/jobs/" + queued.jobId() + "/cancel";
    var before = snapshot();
    for (String token : new String[] {null, "invalid", queued.key()}) {
      assertEquals(HttpStatus.UNAUTHORIZED, request(port, token, path, HttpMethod.POST, null).getStatusCode());
    }
    assertEquals(HttpStatus.NOT_FOUND, request(port, "test-key-beta", path, HttpMethod.POST, null).getStatusCode());
    assertEquals(HttpStatus.NOT_FOUND, request(port, "test-key-alpha", "/api/v1/jobs/" + UUID.randomUUID() + "/cancel",
        HttpMethod.POST, null).getStatusCode());
    assertEquals(before, snapshot());
    var original = jobs.getForProject(queued.jobId(), "project-alpha");
    assertEquals(HttpStatus.OK, request(port, "test-key-alpha", path, HttpMethod.POST, null).getStatusCode());
    assertEquals("CANCELLED", jobs.getForProject(queued.jobId(), "project-alpha").result());
    assertTrue(scheduler.claim(queued.jobId(), queued.runnerId()).isEmpty());
    before = snapshot();
    assertEquals(HttpStatus.OK, request(port, "test-key-alpha", path, HttpMethod.POST, null).getStatusCode());
    assertEquals(before, snapshot());
    var retry = jobs.submit("project-alpha", original.operationId(), original.argv(), original.runnerClass());
    assertFalse(retry.created());
    assertEquals(original.jobId(), retry.job().jobId());
    assertEquals(original.payloadHash(), retry.job().payloadHash());
    assertEquals("CANCELLED", retry.job().result());

    Agent running = newAgent();
    Claim claim = claim(running);
    assertEquals(false, poll(running, INCARNATION).cancelRequested());
    jobs.cancelForProject(running.jobId(), "project-alpha");
    assertEquals(null, jobs.getForProject(running.jobId(), "project-alpha").result());
    assertEquals(true, poll(running, INCARNATION).cancelRequested());
    report(running, claim, INCARNATION, 1, ReportStatus.CANCELLED);
    assertEquals("CANCELLED", jobs.getForProject(running.jobId(), "project-alpha").result());
    assertEquals("CLEANING", runnerState(running));
    assertTrue(cleanup(running, claim, INCARNATION, 2, positiveEvidence()).accepted());
  }

  @Test
  void cancellationAndCompletionRacePreservesTheFirstTerminalReport() throws Exception {
    Agent agent = newAgent();
    Claim claim = claim(agent);
    poll(agent, INCARNATION);
    CountDownLatch start = new CountDownLatch(1);
    try (var executor = Executors.newFixedThreadPool(2)) {
      var cancel = executor.submit(() -> { await(start); return jobs.cancelForProject(agent.jobId(), "project-alpha"); });
      var complete = executor.submit(() -> { await(start); return report(agent, claim, INCARNATION, 1, ReportStatus.SUCCEEDED); });
      start.countDown();
      assertTrue(complete.get(10, TimeUnit.SECONDS).accepted());
      cancel.get(10, TimeUnit.SECONDS);
    }
    assertEquals("SUCCEEDED", jobs.getForProject(agent.jobId(), "project-alpha").result());
    var before = snapshot();
    jobs.cancelForProject(agent.jobId(), "project-alpha");
    assertRejected(report(agent, claim, INCARNATION, 2, ReportStatus.CANCELLED), "terminal_sticky");
    assertEquals(before, snapshot());
  }

  @Test
  void releaseAndNextClaimSerializeAndExposeNoPartialRelease() throws Exception {
    Agent agent = newAgent();
    Claim prior = claim(agent);
    poll(agent, INCARNATION);
    report(agent, prior, INCARNATION, 1, ReportStatus.FAILED);
    UUID nextJob = jobs.submit("project-alpha", "race-" + UUID.randomUUID(), List.of("true"), "default").job().jobId();
    CountDownLatch released = new CountDownLatch(1);
    CountDownLatch commit = new CountDownLatch(1);
    try (var executor = Executors.newFixedThreadPool(2)) {
      var releasing = executor.submit(() -> new TransactionTemplate(transactions).execute(status -> {
        var reply = agents.report(agent.runnerId(), AgentProtocol.parseReportRequest(cleanupBody(prior, INCARNATION, 2, positiveEvidence())));
        assertTrue(reply.accepted());
        released.countDown();
        await(commit);
        return reply;
      }));
      assertTrue(released.await(10, TimeUnit.SECONDS));
      assertEquals("CLEANING", runnerState(agent));
      assertEquals("ACTIVE", allocation(prior).get("state"));
      var claiming = executor.submit(() -> scheduler.claim(nextJob, agent.runnerId()));
      Thread.sleep(100);
      assertFalse(claiming.isDone(), "next claim waits for the cleanup transaction to commit");
      commit.countDown();
      releasing.get(10, TimeUnit.SECONDS);
      Claim next = claiming.get(10, TimeUnit.SECONDS).orElseThrow();
      assertEquals("RELEASED", allocation(prior).get("state"));
      assertEquals("ASSIGNED", runnerState(agent));
      assertEquals(prior.runnerEpoch() + 1, next.runnerEpoch());
      assertEquals(1, activeCount(agent));
      var before = snapshot();
      assertRejected(cleanup(agent, prior, INCARNATION, 3, positiveEvidence()), "fenced_rejected");
      assertEquals(before, snapshot());
    } finally { commit.countDown(); }
  }

  @Test
  void sameJobCannotBeClaimedOnTwoDifferentRunners() throws Exception {
    Agent first = newAgent();
    Agent second = newAgent();
    CountDownLatch start = new CountDownLatch(1);
    try (var executor = Executors.newFixedThreadPool(2)) {
      var a = executor.submit(() -> { await(start); return scheduler.claim(first.jobId(), first.runnerId()); });
      var b = executor.submit(() -> { await(start); return scheduler.claim(first.jobId(), second.runnerId()); });
      start.countDown();
      assertEquals(1, (a.get(10, TimeUnit.SECONDS).isPresent() ? 1 : 0)
          + (b.get(10, TimeUnit.SECONDS).isPresent() ? 1 : 0));
    }
    assertEquals(1, attemptCount(first));
  }

  @Test
  void effectiveTimeoutIsBoundedAndStoredAtClaimBeforePollDelivery() {
    assertThrows(IllegalArgumentException.class, () -> new SchedulerService(jdbc, 0));
    for (long configured : List.of(125L, 86_400_001L)) {
      Agent agent = newAgent();
      Claim claim = new TransactionTemplate(transactions).execute(status ->
          new SchedulerService(jdbc, configured).claim(agent.jobId(), agent.runnerId()).orElseThrow());
      long expected = Math.min(configured, 86_400_000L);
      assertEquals(expected, allocation(claim).get("workload_timeout_ms"));
      // The default-configured controller delivers the snapshotted value, not its own default.
      assertEquals(expected, poll(agent, INCARNATION).workloadTimeoutMs());
      assertEquals(expected, poll(agent, INCARNATION).workloadTimeoutMs());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void cancellationSerializesWithAnUncommittedLaunchClaim(boolean cancellationFirst) throws Exception {
    Agent agent = newAgent();
    CountDownLatch firstWritten = new CountDownLatch(1);
    CountDownLatch commit = new CountDownLatch(1);
    try (var executor = Executors.newFixedThreadPool(2)) {
      var first = executor.submit(() -> new TransactionTemplate(transactions).execute(status -> {
        if (cancellationFirst) jobs.cancelForProject(agent.jobId(), "project-alpha");
        else claim(agent);
        firstWritten.countDown();
        await(commit);
        return true;
      }));
      assertTrue(firstWritten.await(10, TimeUnit.SECONDS));
      var second = executor.submit(() -> {
        if (cancellationFirst) return scheduler.claim(agent.jobId(), agent.runnerId()).isPresent();
        jobs.cancelForProject(agent.jobId(), "project-alpha");
        return true;
      });
      Thread.sleep(100);
      assertFalse(second.isDone(), "the job lock serializes cancellation and launch decisions");
      commit.countDown();
      first.get(10, TimeUnit.SECONDS);
      assertEquals(!cancellationFirst, second.get(10, TimeUnit.SECONDS));
      var job = jobs.getForProject(agent.jobId(), "project-alpha");
      assertTrue(job.cancelRequested());
      assertEquals(cancellationFirst ? "CANCELLED" : null, job.result());
      assertEquals(cancellationFirst ? 0 : 1, activeCount(agent));
      if (!cancellationFirst) assertEquals(true, poll(agent, INCARNATION).cancelRequested());
    } finally { commit.countDown(); }
  }

  private static CleanupEvidence positiveEvidence() {
    return new CleanupEvidence(true, true, true, null);
  }

  @Test
  void interruptedAttemptRetriesExactlyOnceOnlyAfterCurrentRecoveryAndCleanup() {
    Agent agent = newAgent();
    Claim original = claim(agent);
    poll(agent, INCARNATION);
    report(agent, original, INCARNATION, 1, ReportStatus.RUNNING);
    poll(agent, INCARNATION + 1);
    var before = snapshot();
    assertRejected(recovery(agent, original, INCARNATION, 2, discovery()), "fenced_rejected");
    assertRejected(recovery(agent, original, INCARNATION + 1, 1, discovery()), "dropped_stale");
    assertRejected(recovery(agent, original, INCARNATION + 1, 2, null), "discovery_required");
    assertEquals(before, snapshot(), "invalid discovery cannot mutate ownership or attempt history");

    ReportResponse decision = recovery(agent, original, INCARNATION + 1, 2, discovery());
    assertTrue(decision.accepted());
    assertTrue(decision.terminal());
    assertEquals("terminate", decision.reason());
    assertEquals("INTERRUPTED", allocation(original).get("report_status"));
    assertEquals("INTERRUPTED", jdbc.queryForObject("SELECT result FROM attempts WHERE attempt_id = ?",
        String.class, original.attemptId()));
    assertEquals("agent_restart_without_resume_proof", jdbc.queryForObject(
        "SELECT recovery_reason FROM attempts WHERE attempt_id = ?", String.class, original.attemptId()));
    assertEquals(null, jobs.getForProject(agent.jobId(), "project-alpha").result());
    assertEquals("CLEANING", runnerState(agent));
    assertTrue(allocation(original).get("recovery_evidence").toString().contains("101"));
    assertTrue(scheduler.claim(agent.jobId(), agent.runnerId()).isEmpty());
    assertTrue(scheduler.claim(agent.jobId(), newAgent().runnerId()).isEmpty(), "execution remains owned");
    before = snapshot();
    assertRejected(recovery(agent, original, INCARNATION + 1, 2, discovery()), "dropped_stale");
    assertRejected(report(agent, original, INCARNATION + 1, 3, ReportStatus.SUCCEEDED), "terminal_sticky");
    assertRejected(report(agent, original, INCARNATION + 1, 3, ReportStatus.RUNNING), "terminal_sticky");
    assertEquals(before, snapshot());
    assertEquals("terminate", recovery(agent, original, INCARNATION + 1, 3, discovery()).reason(),
        "a lost discovery response redelivers the same decision");

    poll(agent, INCARNATION + 2);
    before = snapshot();
    assertRejected(cleanup(agent, original, INCARNATION + 1, 4, positiveEvidence()), "fenced_rejected");
    assertRejected(cleanup(agent, original, INCARNATION + 2, 4, positiveEvidence()), "recovery_required");
    assertEquals(before, snapshot());
    assertEquals("terminate", recovery(agent, original, INCARNATION + 2, 4,
        new DiscoveryEvidence(false, false, true, List.of(), null)).reason(),
        "removal checkpoint plus fresh physical absence can finish interrupted cleanup");
    assertEquals(1, attemptCount(agent));
    assertTrue(cleanup(agent, original, INCARNATION + 2, 5, positiveEvidence()).accepted());
    assertEquals("RELEASED", allocation(original).get("state"));
    assertEquals("ASSIGNED", runnerState(agent));
    assertEquals(2, attemptCount(agent));
    PollResponse retry = poll(agent, INCARNATION + 2);
    assertEquals(agent.jobId(), retry.jobId());
    assertEquals(original.runnerEpoch() + 1, retry.runnerEpoch());
    assertFalse(original.allocationId().equals(retry.allocationId()));
    assertEquals(retry.allocationId(), allocation(original).get("retry_allocation_id"));
    assertEquals(null, jobs.getForProject(agent.jobId(), "project-alpha").result());
    before = snapshot();
    assertRejected(cleanup(agent, original, INCARNATION + 2, 6, positiveEvidence()), "fenced_rejected");
    assertRejected(recovery(agent, original, INCARNATION + 2, 7, discovery()), "fenced_rejected");
    assertEquals(before, snapshot(), "lost release acknowledgments cannot create another retry");

    assertTrue(ack(post(agent, REPORT, reportBody(retry.allocationId(), retry.runnerEpoch(),
        INCARNATION + 2, 1, ReportStatus.SUCCEEDED))).accepted());
    assertEquals("SUCCEEDED", jobs.getForProject(agent.jobId(), "project-alpha").result());
    assertRejected(report(agent, original, INCARNATION + 1, 100, ReportStatus.FAILED), "fenced_rejected");
    assertEquals("SUCCEEDED", jobs.getForProject(agent.jobId(), "project-alpha").result());
    assertEquals("INTERRUPTED", jdbc.queryForObject("SELECT result FROM attempts WHERE attempt_id = ?",
        String.class, original.attemptId()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"error", "empty-error", "missing-cgroup", "missing-workspace", "contradiction"})
  void unresolvedDiscoveryQuarantinesAndNeverReleasesOrRetries(String failure) {
    Agent agent = newAgent();
    Claim claim = claim(agent);
    poll(agent, INCARNATION);
    poll(agent, INCARNATION + 1);
    DiscoveryEvidence evidence = new DiscoveryEvidence(!failure.equals("missing-cgroup"),
        !failure.equals("missing-workspace"), failure.equals("contradiction"), List.of(101L),
        failure.equals("error") ? "allocation identity mismatch" : failure.equals("empty-error") ? "" : null);
    assertEquals("quarantined", recovery(agent, claim, INCARNATION + 1, 1, evidence).reason());
    assertEquals("QUARANTINED", runnerState(agent));
    assertEquals("QUARANTINE", allocation(claim).get("recovery_action"));
    assertTrue(jdbc.queryForObject("SELECT quarantine_reason FROM runners WHERE runner_id = ?",
        String.class, agent.runnerId()).contains("discovery unresolved"));
    assertEquals(null, jobs.getForProject(agent.jobId(), "project-alpha").result());
    assertEquals(1, attemptCount(agent));
    assertTrue(scheduler.claim(agent.jobId(), agent.runnerId()).isEmpty());
    var before = snapshot();
    assertRejected(recovery(agent, claim, INCARNATION + 1, 2, discovery()), "quarantined");
    assertRejected(cleanup(agent, claim, INCARNATION + 1, 2, positiveEvidence()), "terminal_required");
    assertEquals(before, snapshot());
  }

  @Test
  void recoveryCleanupFailurePreservesInterruptedHistoryAndQuarantineAcrossRestart() {
    Agent agent = newAgent();
    Claim claim = claim(agent);
    poll(agent, INCARNATION);
    recovery(agent, claim, INCARNATION, 1, discovery());
    assertEquals("quarantined", cleanup(agent, claim, INCARNATION, 2,
        new CleanupEvidence(false, false, false, "cgroup kill denied")).reason());
    poll(agent, INCARNATION + 1);
    var before = snapshot();
    assertRejected(recovery(agent, claim, INCARNATION + 1, 3, discovery()), "quarantined");
    assertEquals(before, snapshot());
    assertEquals(1, attemptCount(agent));
    assertEquals(null, jobs.getForProject(agent.jobId(), "project-alpha").result());
    assertEquals("ACTIVE", allocation(claim).get("state"));
    assertEquals("QUARANTINED", runnerState(agent));
    assertTrue(allocation(claim).get("cleanup_evidence").toString().contains("cgroup kill denied"));
  }

  @Test
  void recoveryOfAcceptedTerminalResultRequiresFreshDiscoveryButDoesNotRetry() {
    Agent agent = newAgent();
    Claim claim = claim(agent);
    poll(agent, INCARNATION);
    report(agent, claim, INCARNATION, 1, ReportStatus.SUCCEEDED);
    poll(agent, INCARNATION + 1);
    assertEquals("terminate", recovery(agent, claim, INCARNATION + 1, 2, discovery()).reason());
    poll(agent, INCARNATION + 2);
    var before = snapshot();
    assertRejected(cleanup(agent, claim, INCARNATION + 2, 3, positiveEvidence()), "recovery_required");
    assertEquals(before, snapshot(), "a remembered terminal result is not current physical proof");
    assertEquals("terminate", recovery(agent, claim, INCARNATION + 2, 3, discovery()).reason());
    assertEquals("SUCCEEDED", allocation(claim).get("report_status"));
    assertTrue(cleanup(agent, claim, INCARNATION + 2, 4, positiveEvidence()).accepted());
    assertEquals("AVAILABLE", runnerState(agent));
    assertEquals("SUCCEEDED", jobs.getForProject(agent.jobId(), "project-alpha").result());
    assertEquals(1, attemptCount(agent));
    assertEquals(null, allocation(claim).get("retry_allocation_id"));
  }

  @Test
  void restartBeforeLaunchRetainsNormalTerminalAndCleanupContract() {
    Agent agent = newAgent();
    Claim claim = claim(agent);
    poll(agent, INCARNATION);
    report(agent, claim, INCARNATION, 1, ReportStatus.STARTING);
    poll(agent, INCARNATION + 1);
    assertTrue(report(agent, claim, INCARNATION + 1, 2, ReportStatus.STARTING).accepted());
    assertTrue(report(agent, claim, INCARNATION + 1, 3, ReportStatus.SUCCEEDED).accepted());
    assertTrue(cleanup(agent, claim, INCARNATION + 1, 4, positiveEvidence()).accepted());
    assertEquals("AVAILABLE", runnerState(agent));
    assertEquals(1, attemptCount(agent));
  }

  @Test
  void cancellationDuringRecoverySuppressesRetryAfterVerifiedCleanup() {
    Agent agent = newAgent();
    Claim claim = claim(agent);
    poll(agent, INCARNATION);
    recovery(agent, claim, INCARNATION, 1, discovery());
    jobs.cancelForProject(agent.jobId(), "project-alpha");
    assertTrue(cleanup(agent, claim, INCARNATION, 2, positiveEvidence()).accepted());
    assertEquals("AVAILABLE", runnerState(agent));
    assertEquals("CANCELLED", jobs.getForProject(agent.jobId(), "project-alpha").result());
    assertEquals(1, attemptCount(agent));
    assertEquals("INTERRUPTED", jdbc.queryForObject("SELECT result FROM attempts WHERE attempt_id = ?",
        String.class, claim.attemptId()));
  }

  @Test
  void concurrentRecoveryCleanupReportsCommitOneRetry() throws Exception {
    Agent agent = newAgent();
    Claim claim = claim(agent);
    poll(agent, INCARNATION);
    recovery(agent, claim, INCARNATION, 1, discovery());
    CountDownLatch start = new CountDownLatch(1);
    try (var executor = Executors.newFixedThreadPool(2)) {
      var first = executor.submit(() -> { await(start); return cleanup(agent, claim, INCARNATION, 2, positiveEvidence()); });
      var second = executor.submit(() -> { await(start); return cleanup(agent, claim, INCARNATION, 2, positiveEvidence()); });
      start.countDown();
      ReportResponse a = first.get(10, TimeUnit.SECONDS);
      ReportResponse b = second.get(10, TimeUnit.SECONDS);
      assertEquals(1, (a.accepted() ? 1 : 0) + (b.accepted() ? 1 : 0));
      assertEquals("fenced_rejected", a.accepted() ? b.reason() : a.reason());
    }
    assertEquals(2, attemptCount(agent));
    assertEquals(1, activeCount(agent));
    assertEquals(claim.runnerEpoch() + 1, runnerEpoch(agent));
  }

  private static DiscoveryEvidence discovery() {
    return new DiscoveryEvidence(true, true, false, List.of(101L, 202L, 303L), null);
  }

  private ReportResponse recovery(Agent agent, Claim claim, long incarnation, long seq,
      DiscoveryEvidence evidence) {
    return ack(post(agent, REPORT, AgentProtocol.encodeReportRequest(new ReportRequest(claim.allocationId(),
        claim.runnerEpoch(), incarnation, seq, ReportStatus.RECOVERY,
        Instant.parse("2026-09-23T12:34:56Z"), null, null, null, evidence))));
  }

  private ReportResponse cleanup(Agent agent, Claim claim, long incarnation, long seq, CleanupEvidence evidence) {
    return ack(post(agent, REPORT, cleanupBody(claim, incarnation, seq, evidence)));
  }

  private static String cleanupBody(Claim claim, long incarnation, long seq, CleanupEvidence evidence) {
    return AgentProtocol.encodeReportRequest(new ReportRequest(claim.allocationId(), claim.runnerEpoch(),
        incarnation, seq, ReportStatus.CLEANUP, Instant.parse("2026-09-22T12:34:56Z"), null, null, evidence));
  }

  private Agent newAgent() {
    return newAgent("project-alpha");
  }

  private Agent newAgent(String project) {
    UUID runnerId = UUID.randomUUID();
    String key = "agent-test-" + UUID.randomUUID();
    jdbc.update("INSERT INTO runners (runner_id, runner_class, state, epoch) VALUES (?, 'default', 'AVAILABLE', 4)", runnerId);
    auth.getRunnerKeys().put(key, runnerId);
    UUID jobId = jobs.submit(project, "agent-" + UUID.randomUUID(), List.of("echo", "hi"), "default").job().jobId();
    return new Agent(runnerId, key, jobId);
  }

  private Claim claim(Agent agent) {
    return scheduler.claim(agent.jobId(), agent.runnerId()).orElseThrow();
  }

  private PollResponse poll(Agent agent, long incarnation) {
    ResponseEntity<String> response = post(agent, POLL, pollBody(incarnation));
    assertEquals(HttpStatus.OK, response.getStatusCode(), response.getBody());
    assertJson(response);
    return AgentProtocol.parsePollResponse(response.getBody());
  }

  private ReportResponse report(Agent agent, Claim claim, long incarnation, long seq, ReportStatus status) {
    return ack(post(agent, REPORT, reportBody(claim.allocationId(), claim.runnerEpoch(), incarnation, seq, status)));
  }

  private static String pollBody(long incarnation) {
    return "{\"agent_incarnation\":" + incarnation + ",\"timeout_s\":0}";
  }

  private static String reportBody(UUID allocationId, long epoch, long incarnation, long seq, ReportStatus status) {
    return AgentProtocol.encodeReportRequest(new ReportRequest(allocationId, epoch, incarnation, seq,
        status, Instant.parse("2026-09-22T12:34:56.123456789Z"), null, null));
  }

  private ResponseEntity<String> post(Agent agent, String path, String body) {
    return request(port, agent.key(), path, HttpMethod.POST, body);
  }

  private void fault(Agent agent, String body) {
    assertEquals(HttpStatus.OK, post(agent, FAULTS, body).getStatusCode());
  }

  private static ReportResponse ack(ResponseEntity<String> response) {
    assertEquals(HttpStatus.OK, response.getStatusCode(), response.getBody());
    assertJson(response);
    return AgentProtocol.parseReportResponse(response.getBody());
  }

  private static void assertJson(ResponseEntity<String> response) {
    assertTrue(MediaType.APPLICATION_JSON.isCompatibleWith(response.getHeaders().getContentType()),
        "agent JSON responses, including errors, must declare application/json");
  }

  private static void assertRejected(ReportResponse response, String reason) {
    assertFalse(response.accepted());
    assertEquals(reason, response.reason());
  }

  private Map<String, Object> allocation(Claim claim) {
    return jdbc.queryForMap("SELECT * FROM allocations WHERE allocation_id = ?", claim.allocationId());
  }

  private String runnerState(Agent agent) {
    return jdbc.queryForObject("SELECT state FROM runners WHERE runner_id = ?", String.class, agent.runnerId());
  }

  private long runnerEpoch(Agent agent) {
    return jdbc.queryForObject("SELECT epoch FROM runners WHERE runner_id = ?", Long.class, agent.runnerId());
  }

  private int activeCount(Agent agent) {
    return jdbc.queryForObject("SELECT count(*) FROM allocations WHERE runner_id = ? AND state = 'ACTIVE'", Integer.class, agent.runnerId());
  }

  private int attemptCount(Agent agent) {
    return jdbc.queryForObject("SELECT count(*) FROM attempts WHERE job_id = ?", Integer.class, agent.jobId());
  }

  /** Include PostgreSQL xmin so an UPDATE writing identical values still fails the comparison. */
  private Map<String, List<Map<String, Object>>> snapshot() {
    Map<String, List<Map<String, Object>>> tables = new LinkedHashMap<>();
    tables.put("runners", runnerRows());
    tables.put("allocations", jdbc.queryForList("SELECT xmin::text AS version, row_to_json(t)::text AS row FROM allocations t ORDER BY allocation_id"));
    tables.put("attempts", jdbc.queryForList("SELECT xmin::text AS version, row_to_json(t)::text AS row FROM attempts t ORDER BY attempt_id"));
    tables.put("jobs", jdbc.queryForList("SELECT xmin::text AS version, row_to_json(t)::text AS row FROM jobs t ORDER BY job_id"));
    return tables;
  }

  private List<Map<String, Object>> runnerRows() {
    return jdbc.queryForList("SELECT xmin::text AS version, row_to_json(t)::text AS row FROM runners t ORDER BY runner_id");
  }

  private static ResponseEntity<String> request(int destination, String token, String path, HttpMethod method, String body) {
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    if (token != null) headers.setBearerAuth(token);
    return REST.exchange("http://localhost:" + destination + path, method, new HttpEntity<>(body, headers), String.class);
  }

  private static RestTemplate nonThrowingRestTemplate() {
    SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
    factory.setConnectTimeout(2000);
    factory.setReadTimeout(10000);
    RestTemplate rest = new RestTemplate(factory);
    rest.setErrorHandler(new org.springframework.web.client.ResponseErrorHandler() {
      @Override
      public boolean hasError(org.springframework.http.client.ClientHttpResponse response) {
        return false;
      }

      @Override
      public void handleError(java.net.URI url, HttpMethod method, org.springframework.http.client.ClientHttpResponse response) {}
    });
    return rest;
  }

  private static ConfigurableApplicationContext startApp(String profile) {
    return new SpringApplicationBuilder(Application.class).web(WebApplicationType.SERVLET)
        .run("--server.port=0", "--spring.profiles.active=" + profile);
  }

  private static int appPort(ConfigurableApplicationContext context) {
    return context.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("test transaction gate timed out");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private record Agent(UUID runnerId, String key, UUID jobId) {}
}
