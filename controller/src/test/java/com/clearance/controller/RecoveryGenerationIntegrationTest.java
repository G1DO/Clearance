package com.clearance.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.clearance.controller.agent.AgentProtocol;
import com.clearance.controller.agent.AgentService;
import com.clearance.controller.agent.RecoveryGenerationSource;
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
        "clearance.recovery-mode=false",
        // Pin the generation log to test scratch: the production default is a persistent
        // host path (/var/lib/clearance) that tests must not touch.
        "clearance.recovery-generation-log=${java.io.tmpdir}/clearance-recovery-generations-test.log"
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
    assertTrue(versions.contains("10"), "V10 idle reconcile fencing migration must be applied");
    assertTrue(versions.contains("11"), "V11 recovery generation evidence migration must be applied");

    List<String> runnerCols = jdbc.queryForList(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'runners'",
        String.class);
    assertTrue(runnerCols.contains("reconciled_generation"));
    assertTrue(runnerCols.contains("idle_reconcile_seq"));

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

    // Stale-generation RECONCILE is also rejected with zero mutation.
    var beforeReconcile = snapshot();
    ReportResponse staleReconcile = agents.report(assigned.runnerId(), new ReportRequest(
        assignedClaim.allocationId(), assignedClaim.runnerEpoch(), INCARNATION, 3,
        ReportStatus.RECONCILE, Instant.parse("2026-09-22T12:34:56Z"), null, null, null, null,
        new AgentProtocol.ReconcileEvidence(false, false, List.of(), true, true, true, null, null),
        UUID.randomUUID()));
    assertFalse(staleReconcile.accepted());
    assertEquals("fenced_rejected", staleReconcile.reason());
    assertEquals(beforeReconcile, snapshot());
  }

  @Test
  void successiveRecoveryBootsIssueDistinctGenerationsAcrossSimulatedRewind() {
    UUID first = recovery.enterRecoveryMode();
    assertEquals(first, recovery.currentGeneration().orElseThrow());
    assertEquals(7, first.version());

    // Simulate a rewind that forgets the authority table: the next boot must still issue
    // a strictly newer value from the external monotonic source (UUIDv7 plus the
    // rewind-surviving log), not randomness stored only inside the rewound database.
    jdbc.update("DELETE FROM recovery_authority");
    assertTrue(recovery.currentGeneration().isEmpty());

    UUID second = recovery.enterRecoveryMode();
    assertNotEquals(first, second, "post-rewind generation must never repeat the pre-rewind value");
    assertTrue(RecoveryGenerationSource.timestampMillis(second)
        > RecoveryGenerationSource.timestampMillis(first));

    jdbc.update("DELETE FROM recovery_authority");
    UUID third = recovery.enterRecoveryMode();
    assertNotEquals(first, third);
    assertNotEquals(second, third, "repeated rewinds must keep issuing fresh generations");
    assertTrue(RecoveryGenerationSource.timestampMillis(third)
        > RecoveryGenerationSource.timestampMillis(second));

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
        .run("--clearance.recovery-mode=true",
            "--clearance.recovery-generation-log=" + System.getProperty("java.io.tmpdir")
                + "/clearance-recovery-generations-test.log")) {
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

  @Test
  void quarantinedRunnerWithTerminalWorkReconcilesAlreadyCleanAndBecomesSchedulable() {
    Agent agent = newAgent();
    Claim claim = scheduler.claim(agent.jobId(), agent.runnerId()).orElseThrow();
    agents.poll(agent.runnerId(), INCARNATION);
    assertTrue(agents.report(agent.runnerId(), report(claim, INCARNATION, 1, ReportStatus.SUCCEEDED)).accepted());
    assertEquals("CLEANING", runnerState(agent.runnerId()));

    UUID generation = recovery.enterRecoveryMode();
    assertEquals("QUARANTINED", runnerState(agent.runnerId()));
    assertTrue(scheduler.claim(newJob(), agent.runnerId()).isEmpty(), "unreconciled runner cannot be claimed");

    // Fresh physical inspection reveals host is already clean.
    ReportResponse response = agents.report(agent.runnerId(), new ReportRequest(
        claim.allocationId(), claim.runnerEpoch(), INCARNATION, 2, ReportStatus.RECONCILE,
        Instant.parse("2026-09-22T13:00:00Z"), null, null, null, null,
        new AgentProtocol.ReconcileEvidence(false, false, List.of(), true, true, true, null, null),
        generation));
    assertTrue(response.accepted());
    assertEquals("reconcile_attested", response.reason());
    assertEquals("AVAILABLE", runnerState(agent.runnerId()));

    UUID reconciledGen = jdbc.queryForObject(
        "SELECT reconciled_generation FROM runners WHERE runner_id = ?", UUID.class, agent.runnerId());
    assertEquals(generation, reconciledGen);

    String allocState = jdbc.queryForObject(
        "SELECT state FROM allocations WHERE allocation_id = ?", String.class, claim.allocationId());
    assertEquals("RELEASED", allocState);

    // Now schedulable under current generation!
    UUID nextJob = newJob();
    Optional<Claim> nextClaim = scheduler.claim(nextJob, agent.runnerId());
    assertTrue(nextClaim.isPresent(), "reconciled runner must now be schedulable");
    assertEquals("ASSIGNED", runnerState(agent.runnerId()));
  }

  @Test
  void quarantinedRunnerNeedingCleanupDirectsCleanupAndAdvancesOnVerifiedCleanup() {
    Agent agent = newAgent();
    Claim claim = scheduler.claim(agent.jobId(), agent.runnerId()).orElseThrow();
    agents.poll(agent.runnerId(), INCARNATION);
    assertTrue(agents.report(agent.runnerId(), report(claim, INCARNATION, 1, ReportStatus.SUCCEEDED)).accepted());

    UUID generation = recovery.enterRecoveryMode();
    assertEquals("QUARANTINED", runnerState(agent.runnerId()));

    // 1. Reconcile with dirty workspace -> returns reconcile_cleanup_required
    ReportResponse reconcileResp = agents.report(agent.runnerId(), new ReportRequest(
        claim.allocationId(), claim.runnerEpoch(), INCARNATION, 2, ReportStatus.RECONCILE,
        Instant.parse("2026-09-22T13:00:00Z"), null, null, null, null,
        new AgentProtocol.ReconcileEvidence(false, true, List.of(), true, true, false, null, null),
        generation));
    assertTrue(reconcileResp.accepted());
    assertEquals("reconcile_cleanup_required", reconcileResp.reason());
    assertEquals("QUARANTINED", runnerState(agent.runnerId()));
    assertTrue(scheduler.claim(newJob(), agent.runnerId()).isEmpty());

    // 2. Verified positive cleanup advances generation and releases runner
    ReportResponse goodCleanup = agents.report(agent.runnerId(), new ReportRequest(
        claim.allocationId(), claim.runnerEpoch(), INCARNATION, 3, ReportStatus.CLEANUP,
        Instant.parse("2026-09-22T13:02:00Z"), null, null,
        new AgentProtocol.CleanupEvidence(true, true, true, null),
        null, null, generation));
    assertTrue(goodCleanup.accepted());
    assertEquals("ok", goodCleanup.reason());
    assertEquals("AVAILABLE", runnerState(agent.runnerId()));

    UUID advancedGen = jdbc.queryForObject(
        "SELECT reconciled_generation FROM runners WHERE runner_id = ?", UUID.class, agent.runnerId());
    assertEquals(generation, advancedGen);

    // Runner is now schedulable
    assertTrue(scheduler.claim(newJob(), agent.runnerId()).isPresent());
  }

  @Test
  void failedReconciledCleanupIsDurableStopAndNeverAdvancesGeneration() {
    Agent agent = newAgent();
    Claim claim = scheduler.claim(agent.jobId(), agent.runnerId()).orElseThrow();
    agents.poll(agent.runnerId(), INCARNATION);
    assertTrue(agents.report(agent.runnerId(), report(claim, INCARNATION, 1, ReportStatus.SUCCEEDED)).accepted());

    UUID generation = recovery.enterRecoveryMode();
    assertEquals("QUARANTINED", runnerState(agent.runnerId()));

    // 1. Reconcile directs cleanup
    ReportResponse reconcileResp = agents.report(agent.runnerId(), new ReportRequest(
        claim.allocationId(), claim.runnerEpoch(), INCARNATION, 2, ReportStatus.RECONCILE,
        Instant.parse("2026-09-22T13:00:00Z"), null, null, null, null,
        new AgentProtocol.ReconcileEvidence(false, true, List.of(), true, true, false, null, null),
        generation));
    assertTrue(reconcileResp.accepted());

    // 2. Failed cleanup is accepted as durable stop and preserves quarantine
    ReportResponse failedCleanup = agents.report(agent.runnerId(), new ReportRequest(
        claim.allocationId(), claim.runnerEpoch(), INCARNATION, 3, ReportStatus.CLEANUP,
        Instant.parse("2026-09-22T13:01:00Z"), null, null,
        new AgentProtocol.CleanupEvidence(true, true, false, "rmdir failed"),
        null, null, generation));
    assertTrue(failedCleanup.accepted());
    assertEquals("quarantined", failedCleanup.reason());
    assertEquals("QUARANTINED", runnerState(agent.runnerId()));

    UUID unadvancedGen = jdbc.queryForObject(
        "SELECT reconciled_generation FROM runners WHERE runner_id = ?", UUID.class, agent.runnerId());
    assertEquals(null, unadvancedGen);
    assertTrue(scheduler.claim(newJob(), agent.runnerId()).isEmpty());

    // 3. Subsequent cleanup attempt is refused: failed cleanup is sticky
    ReportResponse subsequent = agents.report(agent.runnerId(), new ReportRequest(
        claim.allocationId(), claim.runnerEpoch(), INCARNATION, 4, ReportStatus.CLEANUP,
        Instant.parse("2026-09-22T13:02:00Z"), null, null,
        new AgentProtocol.CleanupEvidence(true, true, true, null),
        null, null, generation));
    assertFalse(subsequent.accepted());
    assertEquals("quarantined", subsequent.reason());
    assertEquals("QUARANTINED", runnerState(agent.runnerId()));
    assertEquals(null, jdbc.queryForObject(
        "SELECT reconciled_generation FROM runners WHERE runner_id = ?", UUID.class, agent.runnerId()));
    assertTrue(scheduler.claim(newJob(), agent.runnerId()).isEmpty());
  }

  @Test
  void idleAtBackupRunnerPhysicallyCleanAdvancesGenerationAndUnblocksClaims() {
    UUID idleRunner = newRunner("AVAILABLE");
    agents.poll(idleRunner, INCARNATION);
    UUID generation = recovery.enterRecoveryMode();
    assertEquals("QUARANTINED", runnerState(idleRunner));
    assertTrue(scheduler.claim(newJob(), idleRunner).isEmpty());

    // Reconcile with allocation_id omitted and epoch 0 via the real wire codec:
    // never-claimed idle runners remain epoch 0 (V3 DEFAULT 0). This proves the
    // codec accepts 0 for idle RECONCILE and the service advances generation.
    ReportRequest wire = AgentProtocol.parseReportRequest("""
        {"runner_epoch": 0, "agent_incarnation": 7, "seq": 1, "status": "RECONCILE",
         "ts": "2026-09-22T13:00:00Z",
         "reconcile": {"cgroup_present": false, "workspace_present": false, "pids": [],
           "execution_empty": true, "descendants_reaped": true, "workspace_clean": true},
         "recovery_generation": "%s"}""".formatted(generation));
    assertEquals(0, wire.runnerEpoch());
    ReportResponse resp = agents.report(idleRunner, wire);
    assertTrue(resp.accepted());
    assertEquals("reconcile_attested", resp.reason());
    assertEquals("AVAILABLE", runnerState(idleRunner));

    UUID reconciledGen = jdbc.queryForObject(
        "SELECT reconciled_generation FROM runners WHERE runner_id = ?", UUID.class, idleRunner);
    assertEquals(generation, reconciledGen);

    // Schedulable!
    assertTrue(scheduler.claim(newJob(), idleRunner).isPresent());
  }

  @Test
  void idleReconcileDuplicatesAreDroppedStaleWithZeroMutation() {
    UUID idleRunner = newRunner("AVAILABLE");
    agents.poll(idleRunner, INCARNATION);
    UUID generation = recovery.enterRecoveryMode();
    assertEquals("QUARANTINED", runnerState(idleRunner));

    // First dirty observation (seq 1) is accepted and records quarantine + seq.
    ReportRequest first = AgentProtocol.parseReportRequest("""
        {"runner_epoch": 0, "agent_incarnation": 7, "seq": 1, "status": "RECONCILE",
         "ts": "2026-09-22T13:00:00Z",
         "reconcile": {"cgroup_present": false, "workspace_present": false, "pids": [9999],
           "execution_empty": true, "descendants_reaped": true, "workspace_clean": true},
         "recovery_generation": "%s"}""".formatted(generation));
    ReportResponse accepted = agents.report(idleRunner, first);
    assertTrue(accepted.accepted());
    assertEquals("reconcile_quarantined", accepted.reason());
    assertEquals(Long.valueOf(1), jdbc.queryForObject(
        "SELECT idle_reconcile_seq FROM runners WHERE runner_id = ?", Long.class, idleRunner));

    // Duplicate and reordered reports with same/older seq are rejected with zero
    // mutation, including no quarantine rewrite (xmin proves no UPDATE ran).
    var before = snapshot();
    ReportRequest duplicate = AgentProtocol.parseReportRequest("""
        {"runner_epoch": 0, "agent_incarnation": 7, "seq": 1, "status": "RECONCILE",
         "ts": "2026-09-22T13:01:00Z",
         "reconcile": {"cgroup_present": false, "workspace_present": false, "pids": [],
           "execution_empty": true, "descendants_reaped": true, "workspace_clean": true},
         "recovery_generation": "%s"}""".formatted(generation));
    ReportResponse dropped = agents.report(idleRunner, duplicate);
    assertFalse(dropped.accepted());
    assertEquals("dropped_stale", dropped.reason());
    assertEquals(before, snapshot(),
        "duplicate idle RECONCILE must make zero writes");

    // A newer seq is still accepted and advances the stored sequence.
    ReportRequest newer = AgentProtocol.parseReportRequest("""
        {"runner_epoch": 0, "agent_incarnation": 7, "seq": 2, "status": "RECONCILE",
         "ts": "2026-09-22T13:02:00Z",
         "reconcile": {"cgroup_present": false, "workspace_present": false, "pids": [],
           "execution_empty": true, "descendants_reaped": true, "workspace_clean": true},
         "recovery_generation": "%s"}""".formatted(generation));
    ReportResponse attested = agents.report(idleRunner, newer);
    assertTrue(attested.accepted());
    assertEquals("reconcile_attested", attested.reason());
    assertEquals("AVAILABLE", runnerState(idleRunner));
  }

  @Test
  void idleAtBackupRunnerWithForeignExecutionPreservesQuarantineAndBlocksClaims() {
    UUID idleRunner = newRunner("AVAILABLE");
    agents.poll(idleRunner, INCARNATION);
    UUID generation = recovery.enterRecoveryMode();
    assertEquals("QUARANTINED", runnerState(idleRunner));

    // Idle RECONCILE via wire (epoch 0, no allocation_id) with orphaned process.
    ReportRequest wire = AgentProtocol.parseReportRequest("""
        {"runner_epoch": 0, "agent_incarnation": 7, "seq": 1, "status": "RECONCILE",
         "ts": "2026-09-22T13:00:00Z",
         "reconcile": {"cgroup_present": false, "workspace_present": false, "pids": [9999],
           "execution_empty": true, "descendants_reaped": true, "workspace_clean": true},
         "recovery_generation": "%s"}""".formatted(generation));
    ReportResponse resp = agents.report(idleRunner, wire);
    assertTrue(resp.accepted());
    assertEquals("reconcile_quarantined", resp.reason());
    assertEquals("QUARANTINED", runnerState(idleRunner));

    String reason = jdbc.queryForObject(
        "SELECT quarantine_reason FROM runners WHERE runner_id = ?", String.class, idleRunner);
    assertTrue(reason.contains("ORPHANED_EXECUTION"));

    UUID reconciledGen = jdbc.queryForObject(
        "SELECT reconciled_generation FROM runners WHERE runner_id = ?", UUID.class, idleRunner);
    assertEquals(null, reconciledGen);
    assertTrue(scheduler.claim(newJob(), idleRunner).isEmpty());
  }

  @Test
  void quarantinedRunnerStillRunningPreservesQuarantineAndRefusesClaims() {
    Agent agent = newAgent();
    Claim claim = scheduler.claim(agent.jobId(), agent.runnerId()).orElseThrow();
    agents.poll(agent.runnerId(), INCARNATION);
    assertTrue(agents.report(agent.runnerId(), report(claim, INCARNATION, 1, ReportStatus.RUNNING)).accepted());

    UUID generation = recovery.enterRecoveryMode();
    assertEquals("QUARANTINED", runnerState(agent.runnerId()));

    // Reconcile with still-running execution
    ReportResponse resp = agents.report(agent.runnerId(), new ReportRequest(
        claim.allocationId(), claim.runnerEpoch(), INCARNATION, 2, ReportStatus.RECONCILE,
        Instant.parse("2026-09-22T13:00:00Z"), null, null, null, null,
        new AgentProtocol.ReconcileEvidence(true, true, List.of(1234L), false, false, false, null, null),
        generation));
    assertTrue(resp.accepted());
    assertEquals("reconcile_still_running", resp.reason());
    assertEquals("QUARANTINED", runnerState(agent.runnerId()));

    UUID reconciledGen = jdbc.queryForObject(
        "SELECT reconciled_generation FROM runners WHERE runner_id = ?", UUID.class, agent.runnerId());
    assertEquals(null, reconciledGen);
    assertTrue(scheduler.claim(newJob(), agent.runnerId()).isEmpty());
  }

  @Test
  void observationCategoriesContradictoryAndInsufficientEvidenceKeepQuarantine() {
    Agent agent = newAgent();
    Claim claim = scheduler.claim(agent.jobId(), agent.runnerId()).orElseThrow();
    agents.poll(agent.runnerId(), INCARNATION);
    assertTrue(agents.report(agent.runnerId(), report(claim, INCARNATION, 1, ReportStatus.SUCCEEDED)).accepted());

    UUID generation = recovery.enterRecoveryMode();

    // Contradictory: cgroup absent but execution not empty
    ReportResponse contradictoryResp = agents.report(agent.runnerId(), new ReportRequest(
        claim.allocationId(), claim.runnerEpoch(), INCARNATION, 2, ReportStatus.RECONCILE,
        Instant.parse("2026-09-22T13:00:00Z"), null, null, null, null,
        new AgentProtocol.ReconcileEvidence(false, true, List.of(), false, true, true, null, null),
        generation));
    assertTrue(contradictoryResp.accepted());
    assertEquals("reconcile_quarantined", contradictoryResp.reason());
    String reason1 = jdbc.queryForObject(
        "SELECT quarantine_reason FROM runners WHERE runner_id = ?", String.class, agent.runnerId());
    assertTrue(reason1.contains("CONTRADICTORY"));

    // Insufficient evidence: error reported
    ReportResponse errorResp = agents.report(agent.runnerId(), new ReportRequest(
        claim.allocationId(), claim.runnerEpoch(), INCARNATION, 3, ReportStatus.RECONCILE,
        Instant.parse("2026-09-22T13:01:00Z"), null, null, null, null,
        new AgentProtocol.ReconcileEvidence(false, false, List.of(), true, true, true, null, "discovery failed"),
        generation));
    assertTrue(errorResp.accepted());
    assertEquals("reconcile_quarantined", errorResp.reason());
    String reason2 = jdbc.queryForObject(
        "SELECT quarantine_reason FROM runners WHERE runner_id = ?", String.class, agent.runnerId());
    assertTrue(reason2.contains("INSUFFICIENT_EVIDENCE"));
  }

  @Test
  void wireEvolutionV1AndCurrentGenerationAcceptance() {
    UUID idleRunner = newRunner("AVAILABLE");
    agents.poll(idleRunner, INCARNATION);
    UUID generation = recovery.enterRecoveryMode();

    // 1. Stale generation report rejected with zero mutation (via wire codec).
    var beforeStale = snapshot();
    UUID staleGen = UUID.randomUUID();
    ReportResponse staleResp = agents.report(idleRunner, AgentProtocol.parseReportRequest("""
        {"runner_epoch": 0, "agent_incarnation": 7, "seq": 1, "status": "RECONCILE",
         "ts": "2026-09-22T13:00:00Z",
         "reconcile": {"cgroup_present": false, "workspace_present": false, "pids": [],
           "execution_empty": true, "descendants_reaped": true, "workspace_clean": true},
         "recovery_generation": "%s"}""".formatted(staleGen)));
    assertFalse(staleResp.accepted());
    assertEquals("fenced_rejected", staleResp.reason());
    assertEquals(beforeStale, snapshot());

    // 2. v1 report without recovery_generation accepted (via wire codec, epoch 0).
    ReportResponse v1Resp = agents.report(idleRunner, AgentProtocol.parseReportRequest("""
        {"runner_epoch": 0, "agent_incarnation": 7, "seq": 2, "status": "RECONCILE",
         "ts": "2026-09-22T13:01:00Z",
         "reconcile": {"cgroup_present": false, "workspace_present": false, "pids": [],
           "execution_empty": true, "descendants_reaped": true, "workspace_clean": true}}"""));
    assertTrue(v1Resp.accepted());
    assertEquals("reconcile_attested", v1Resp.reason());
    assertEquals("AVAILABLE", runnerState(idleRunner));
    assertEquals(generation, jdbc.queryForObject(
        "SELECT reconciled_generation FROM runners WHERE runner_id = ?", UUID.class, idleRunner));
  }

  @Test
  void assignedQuarantinedRunnerIdleReconcileIsFencedWithZeroMutation() {
    Agent agent = newAgent();
    Claim claim = scheduler.claim(agent.jobId(), agent.runnerId()).orElseThrow();
    agents.poll(agent.runnerId(), INCARNATION);
    assertTrue(agents.report(agent.runnerId(), report(claim, INCARNATION, 1, ReportStatus.SUCCEEDED)).accepted());

    UUID generation = recovery.enterRecoveryMode();
    assertEquals("QUARANTINED", runnerState(agent.runnerId()));

    // Idle RECONCILE (no allocation_id) from a runner that still holds an ACTIVE
    // allocation must be fenced with zero mutation, even with clean evidence and
    // matching epoch/incarnation: only the allocated path may release the row.
    var before = snapshot();
    ReportResponse idleResp = agents.report(agent.runnerId(), AgentProtocol.parseReportRequest("""
        {"runner_epoch": %d, "agent_incarnation": 7, "seq": 2, "status": "RECONCILE",
         "ts": "2026-09-22T13:00:00Z",
         "reconcile": {"cgroup_present": false, "workspace_present": false, "pids": [],
           "execution_empty": true, "descendants_reaped": true, "workspace_clean": true},
         "recovery_generation": "%s"}""".formatted(claim.runnerEpoch(), generation)));
    assertFalse(idleResp.accepted());
    assertEquals("fenced_rejected", idleResp.reason());
    assertEquals(before, snapshot(), "fenced idle RECONCILE must make zero writes");
    assertEquals("QUARANTINED", runnerState(agent.runnerId()));
    assertEquals("ACTIVE", jdbc.queryForObject(
        "SELECT state FROM allocations WHERE allocation_id = ?", String.class, claim.allocationId()));
    assertEquals(null, jdbc.queryForObject(
        "SELECT reconciled_generation FROM runners WHERE runner_id = ?", UUID.class, agent.runnerId()));
    assertEquals(null, jdbc.queryForObject(
        "SELECT idle_reconcile_seq FROM runners WHERE runner_id = ?", Long.class, agent.runnerId()));

    // The runner is not wedged: the allocated path still reconciles and releases.
    ReportResponse allocatedResp = agents.report(agent.runnerId(), new ReportRequest(
        claim.allocationId(), claim.runnerEpoch(), INCARNATION, 2, ReportStatus.RECONCILE,
        Instant.parse("2026-09-22T13:01:00Z"), null, null, null, null,
        new AgentProtocol.ReconcileEvidence(false, false, List.of(), true, true, true, null, null),
        generation));
    assertTrue(allocatedResp.accepted());
    assertEquals("reconcile_attested", allocatedResp.reason());
    assertEquals("AVAILABLE", runnerState(agent.runnerId()));
    assertEquals("RELEASED", jdbc.queryForObject(
        "SELECT state FROM allocations WHERE allocation_id = ?", String.class, claim.allocationId()));
  }

  @Test
  void preBootReconcileBindingCannotAuthorizePostBootCleanup() {
    Agent agent = newAgent();
    Claim claim = scheduler.claim(agent.jobId(), agent.runnerId()).orElseThrow();
    agents.poll(agent.runnerId(), INCARNATION);
    assertTrue(agents.report(agent.runnerId(), report(claim, INCARNATION, 1, ReportStatus.SUCCEEDED)).accepted());

    // Quarantine before any recovery boot, then reconcile dirty without a generation:
    // this TERMINATE_CLEANUP binding is not under any current authority.
    jdbc.update("UPDATE runners SET state = 'QUARANTINED', quarantine_reason = 'pre-boot test' "
        + "WHERE runner_id = ?", agent.runnerId());
    ReportResponse preBoot = agents.report(agent.runnerId(), new ReportRequest(
        claim.allocationId(), claim.runnerEpoch(), INCARNATION, 2, ReportStatus.RECONCILE,
        Instant.parse("2026-09-22T13:00:00Z"), null, null, null, null,
        new AgentProtocol.ReconcileEvidence(false, true, List.of(), true, true, false, null, null)));
    assertTrue(preBoot.accepted());
    assertEquals("reconcile_cleanup_required", preBoot.reason());

    UUID generation = recovery.enterRecoveryMode();
    assertEquals("QUARANTINED", runnerState(agent.runnerId()));

    // Post-boot CLEANUP under the stale pre-boot binding must not release or advance:
    // reuse requires a fresh observation accepted under the current generation.
    var before = snapshot();
    ReportResponse staleCleanup = agents.report(agent.runnerId(), new ReportRequest(
        claim.allocationId(), claim.runnerEpoch(), INCARNATION, 3, ReportStatus.CLEANUP,
        Instant.parse("2026-09-22T13:01:00Z"), null, null,
        new AgentProtocol.CleanupEvidence(true, true, true, null),
        null, null));
    assertFalse(staleCleanup.accepted());
    assertEquals(before, snapshot(), "stale-binding cleanup must make zero release writes");
    assertEquals("QUARANTINED", runnerState(agent.runnerId()));
    assertEquals("ACTIVE", jdbc.queryForObject(
        "SELECT state FROM allocations WHERE allocation_id = ?", String.class, claim.allocationId()));
    assertEquals(null, jdbc.queryForObject(
        "SELECT reconciled_generation FROM runners WHERE runner_id = ?", UUID.class, agent.runnerId()));
    assertTrue(scheduler.claim(newJob(), agent.runnerId()).isEmpty());

    // A fresh post-boot observation re-authorizes the path and releases.
    ReportResponse freshReconcile = agents.report(agent.runnerId(), new ReportRequest(
        claim.allocationId(), claim.runnerEpoch(), INCARNATION, 4, ReportStatus.RECONCILE,
        Instant.parse("2026-09-22T13:02:00Z"), null, null, null, null,
        new AgentProtocol.ReconcileEvidence(false, true, List.of(), true, true, false, null, null),
        generation));
    assertTrue(freshReconcile.accepted());
    assertEquals("reconcile_cleanup_required", freshReconcile.reason());
    ReportResponse freshCleanup = agents.report(agent.runnerId(), new ReportRequest(
        claim.allocationId(), claim.runnerEpoch(), INCARNATION, 5, ReportStatus.CLEANUP,
        Instant.parse("2026-09-22T13:03:00Z"), null, null,
        new AgentProtocol.CleanupEvidence(true, true, true, null),
        null, null, generation));
    assertTrue(freshCleanup.accepted());
    assertEquals("ok", freshCleanup.reason());
    assertEquals("AVAILABLE", runnerState(agent.runnerId()));
    assertEquals(generation, jdbc.queryForObject(
        "SELECT reconciled_generation FROM runners WHERE runner_id = ?", UUID.class, agent.runnerId()));
    assertTrue(scheduler.claim(newJob(), agent.runnerId()).isPresent());
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
