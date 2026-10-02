package com.clearance.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.clearance.controller.agent.AgentProtocol;
import com.clearance.controller.agent.AgentService;
import com.clearance.controller.agent.RecoveryService;
import com.clearance.controller.agent.AgentProtocol.ReportRequest;
import com.clearance.controller.agent.AgentProtocol.ReportResponse;
import com.clearance.controller.agent.AgentProtocol.ReportStatus;
import com.clearance.controller.jobs.JobService;
import com.clearance.controller.scheduling.Claim;
import com.clearance.controller.scheduling.SchedulerService;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * PostgreSQL-backed verification for issue #32: recovery-mode boot issues a fresh generation
 * and holds the fleet quarantined without scheduling.
 *
 * <p>Proves against real PostgreSQL (no in-memory substitution) that a simulated restore of an
 * older snapshot followed by recovery boot issues a distinct generation, quarantines every
 * runner regardless of restored row state, refuses claims on restored-idle runners even when
 * they claim AVAILABLE with no allocation, and rejects superseded-generation evidence with zero
 * mutation. The full destructive rewind drill belongs to a later issue; this proves the logic
 * without requiring it.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "clearance.heartbeat-timeout-ms=300000",
        "clearance.heartbeat-evaluator-enabled=false",
        "clearance.reconciliation-enabled=false",
        "clearance.recovery-mode=false"
    })
class RecoveryGenerationIntegrationTest {

  private static final long INCARNATION = 7;

  @Autowired JdbcTemplate jdbc;
  @Autowired SchedulerService scheduler;
  @Autowired JobService jobs;
  @Autowired AgentService agents;
  @Autowired RecoveryService recovery;

  @BeforeEach
  void clearAuthority() {
    jdbc.update("DELETE FROM recovery_authority");
  }

  @AfterEach
  void cleanAuthority() {
    jdbc.update("DELETE FROM recovery_authority");
  }

  @Test
  void schemaV9IncludesRecoveryAuthority() {
    List<String> versions =
        jdbc.queryForList("SELECT version FROM flyway_schema_history ORDER BY version", String.class);
    assertTrue(versions.contains("9"), "V9 recovery generation migration must be applied");

    List<String> runnerCols = jdbc.queryForList(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'runners'",
        String.class);
    assertTrue(runnerCols.contains("reconciled_generation"));

    List<String> allocCols = jdbc.queryForList(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'allocations'",
        String.class);
    assertTrue(allocCols.contains("recovery_generation"));

    List<String> tables = jdbc.queryForList(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'recovery_authority'",
        String.class);
    assertTrue(tables.contains("recovery_authority"));
  }

  @Test
  void recoveryBootIssuesFreshGenerationQuarantinesFleetAndBlocksClaims() {
    // Seed a fleet covering restored row states: idle AVAILABLE with no allocation,
    // ASSIGNED with active work, CLEANING with a terminal result, and pre-quarantined.
    UUID idleRunner = newRunner("AVAILABLE");
    Agent assigned = newAgent();
    Claim assignedClaim = scheduler.claim(assigned.jobId(), assigned.runnerId()).orElseThrow();
    agents.poll(assigned.runnerId(), INCARNATION);
    assertTrue(agents.report(assigned.runnerId(), report(assignedClaim, INCARNATION, 1,
        ReportStatus.RUNNING)).accepted());

    Agent cleaning = newAgent();
    Claim cleaningClaim = scheduler.claim(cleaning.jobId(), cleaning.runnerId()).orElseThrow();
    agents.poll(cleaning.runnerId(), INCARNATION);
    assertTrue(agents.report(cleaning.runnerId(), report(cleaningClaim, INCARNATION, 1,
        ReportStatus.SUCCEEDED)).accepted());
    assertEquals("CLEANING", runnerState(cleaning.runnerId()));

    Agent quarantined = newAgent();
    Claim quarantinedClaim =
        scheduler.claim(quarantined.jobId(), quarantined.runnerId()).orElseThrow();
    agents.poll(quarantined.runnerId(), INCARNATION);
    jdbc.update("UPDATE runners SET state = 'QUARANTINED', quarantine_reason = 'pre-existing test' "
        + "WHERE runner_id = ?", quarantined.runnerId());

    assertTrue(recovery.currentGeneration().isEmpty());

    UUID generation = recovery.enterRecoveryMode();

    // Current generation persisted as fleet authority.
    UUID persisted = jdbc.queryForObject(
        "SELECT current_generation FROM recovery_authority WHERE singleton = true", UUID.class);
    assertEquals(generation, persisted);

    // Every runner is QUARANTINED with an inspectable recovery reason, including idle
    // runners with no active allocation.
    for (UUID runnerId : List.of(idleRunner, assigned.runnerId(), cleaning.runnerId(),
        quarantined.runnerId())) {
      assertEquals("QUARANTINED", runnerState(runnerId), "runner " + runnerId + " must be quarantined");
      String reason = jdbc.queryForObject(
          "SELECT quarantine_reason FROM runners WHERE runner_id = ?", String.class, runnerId);
      assertTrue(reason != null && reason.contains(generation.toString()),
          "quarantine reason must reference the current generation, got: " + reason);
      assertTrue(reason.contains("physical reconciliation"),
          "quarantine reason must demand reconciliation, got: " + reason);
    }
    assertEquals(0, jdbc.queryForObject(
        "SELECT COUNT(*) FROM allocations WHERE runner_id = ? AND state = 'ACTIVE'",
        Integer.class, idleRunner));

    // Claims on restored-idle runners return empty, even though no allocation row exists.
    UUID idleJob = newJob();
    assertTrue(scheduler.claim(idleJob, idleRunner).isEmpty());

    // Even a manually restored AVAILABLE row cannot become schedulable without
    // current-generation reconciliation: absence of evidence is never safe reuse.
    jdbc.update("UPDATE runners SET state = 'AVAILABLE' WHERE runner_id = ?", idleRunner);
    UUID anotherJob = newJob();
    assertTrue(scheduler.claim(anotherJob, idleRunner).isEmpty(),
        "AVAILABLE without current reconciled generation must still refuse scheduling");
    // Restore quarantine for subsequent assertions in this test.
    jdbc.update("UPDATE runners SET state = 'QUARANTINED' WHERE runner_id = ?", idleRunner);

    // Claims on runners with active work are also refused while unreconciled.
    assertTrue(scheduler.claim(newJob(), assigned.runnerId()).isEmpty());
    assertTrue(scheduler.claim(newJob(), cleaning.runnerId()).isEmpty());
    assertTrue(scheduler.claim(newJob(), quarantined.runnerId()).isEmpty());

    // Stale-generation evidence leaves every table unchanged, including no state
    // change, no allocation change, and no generation advancement.
    var before = snapshot();
    ReportResponse stale = agents.report(assigned.runnerId(),
        report(assignedClaim, INCARNATION, 2, ReportStatus.HEARTBEAT));
    assertFalse(stale.accepted());
    assertEquals("fenced_rejected", stale.reason());
    assertEquals(before, snapshot(),
        "superseded-generation evidence must make zero writes to any table");
    assertEquals(generation, jdbc.queryForObject(
        "SELECT current_generation FROM recovery_authority WHERE singleton = true", UUID.class),
        "rejected evidence must not advance the generation");

    // Superseded RECONCILE is also rejected without mutation in this issue; advancement
    // to the current generation belongs to the later reconciliation issue.
    var beforeReconcile = snapshot();
    ReportResponse staleReconcile = agents.report(assigned.runnerId(), new ReportRequest(
        assignedClaim.allocationId(), assignedClaim.runnerEpoch(), INCARNATION, 3,
        ReportStatus.RECONCILE, Instant.parse("2026-09-22T12:34:56Z"), null, null, null, null,
        new AgentProtocol.ReconcileEvidence(false, false, List.of(), true, true, true, null, null)));
    assertFalse(staleReconcile.accepted());
    assertEquals(beforeReconcile, snapshot());
  }

  @Test
  void successiveRecoveryBootsIssueDistinctGenerationsAcrossSimulatedRewind() {
    UUID first = recovery.enterRecoveryMode();
    assertEquals(first, recovery.currentGeneration().orElseThrow());

    // Simulate a rewind that forgets the authority table: the next boot must still issue
    // a value that never equals the forgotten generation (random UUIDv4, not a DB increment).
    jdbc.update("DELETE FROM recovery_authority");
    assertTrue(recovery.currentGeneration().isEmpty());

    UUID second = recovery.enterRecoveryMode();
    assertNotEquals(first, second, "post-rewind generation must never repeat the pre-rewind value");

    jdbc.update("DELETE FROM recovery_authority");
    UUID third = recovery.enterRecoveryMode();
    assertNotEquals(first, third);
    assertNotEquals(second, third, "repeated rewinds must keep issuing fresh generations");

    for (UUID generation : List.of(first, second, third)) {
      assertTrue(generation != null);
    }
  }

  @Test
  void recoveryModeBootRunnerIssuesGenerationAndQuarantinesFleet() {
    UUID idleRunner = newRunner("AVAILABLE");
    assertTrue(recovery.currentGeneration().isEmpty());

    try (ConfigurableApplicationContext ctx = new SpringApplicationBuilder(Application.class)
        .web(WebApplicationType.NONE)
        .run("--clearance.recovery-mode=true")) {
      UUID current = ctx.getBean(RecoveryService.class).currentGeneration().orElseThrow();
      assertEquals(current, recovery.currentGeneration().orElseThrow());
      assertEquals("QUARANTINED", runnerState(idleRunner));
      String reason = jdbc.queryForObject(
          "SELECT quarantine_reason FROM runners WHERE runner_id = ?", String.class, idleRunner);
      assertTrue(reason != null && reason.contains(current.toString()));
    }
  }

  @Test
  void withoutRecoveryAuthorityLegacyAvailabilityStillApplies() {
    assertTrue(recovery.currentGeneration().isEmpty());
    UUID runnerId = newRunner("AVAILABLE");
    UUID jobId = newJob();
    Optional<Claim> claim = scheduler.claim(jobId, runnerId);
    assertTrue(claim.isPresent(), "without any recovery boot, AVAILABLE runners remain claimable");
    assertEquals("ASSIGNED", runnerState(runnerId));
  }

  private UUID newRunner(String state) {
    UUID runnerId = UUID.randomUUID();
    jdbc.update("INSERT INTO runners (runner_id, runner_class, state, epoch) VALUES (?, 'default', ?, 0)",
        runnerId, state);
    return runnerId;
  }

  private record Agent(UUID runnerId, UUID jobId) {}

  private Agent newAgent() {
    UUID runnerId = UUID.randomUUID();
    jdbc.update("INSERT INTO runners (runner_id, runner_class, state, epoch) "
        + "VALUES (?, 'default', 'AVAILABLE', 4)", runnerId);
    UUID jobId = newJob();
    return new Agent(runnerId, jobId);
  }

  private UUID newJob() {
    return jobs.submit("project-alpha", "recovery-" + UUID.randomUUID(),
        List.of("echo", "hi"), "default").job().jobId();
  }

  private String runnerState(UUID runnerId) {
    return jdbc.queryForObject("SELECT state FROM runners WHERE runner_id = ?", String.class, runnerId);
  }

  private static ReportRequest report(Claim claim, long incarnation, long seq, ReportStatus status) {
    return new ReportRequest(claim.allocationId(), claim.runnerEpoch(), incarnation, seq, status,
        Instant.parse("2026-09-22T12:34:56.123456789Z"), null, null);
  }

  /** Include xmin so an UPDATE writing identical values still fails the comparison. */
  private Map<String, List<Map<String, Object>>> snapshot() {
    Map<String, List<Map<String, Object>>> tables = new LinkedHashMap<>();
    tables.put("runners", jdbc.queryForList(
        "SELECT xmin::text AS version, row_to_json(t)::text AS row FROM runners t ORDER BY runner_id"));
    tables.put("allocations", jdbc.queryForList(
        "SELECT xmin::text AS version, row_to_json(t)::text AS row FROM allocations t ORDER BY allocation_id"));
    tables.put("attempts", jdbc.queryForList(
        "SELECT xmin::text AS version, row_to_json(t)::text AS row FROM attempts t ORDER BY attempt_id"));
    tables.put("jobs", jdbc.queryForList(
        "SELECT xmin::text AS version, row_to_json(t)::text AS row FROM jobs t ORDER BY job_id"));
    tables.put("recovery_authority", jdbc.queryForList(
        "SELECT xmin::text AS version, row_to_json(t)::text AS row FROM recovery_authority t ORDER BY singleton"));
    return tables;
  }
}
