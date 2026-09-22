package com.clearance.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.clearance.controller.agent.AgentProtocol;
import com.clearance.controller.agent.AgentProtocol.PollResponse;
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
                List.of("echo", "hi"), "default", null));
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
  @ValueSource(strings = {"SUCCEEDED", "FAILED"})
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
    assertEquals("ASSIGNED", runnerState(agent));
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
