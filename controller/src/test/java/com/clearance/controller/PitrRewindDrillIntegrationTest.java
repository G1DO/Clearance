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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.ObjectMapper;

/**
 * Destructive PITR rewind drill for issue #34: real PostgreSQL backup at T0,
 * T1 execution, rewind to T0, recovery-mode boot with a fresh non-repeating
 * generation, fleet quarantine without scheduling, verified reconciliation
 * with graceful-then-forceful cleanup semantics, and repeated-rewind freshness.
 *
 * <p>Uses real PostgreSQL as the durable authority (no in-memory substitution).
 * T0 is a logical backup via a real backup schema ({@code CREATE TABLE backup AS
 * TABLE public WITH DATA}); rewind is a real restore ({@code DELETE + INSERT
 * SELECT}). Physical cgroup/workspace termination is proven by the companion Go
 * drill ({@code agent/pitr_rewind_drill_integration_test.go}); this drill proves
 * the controller safety property end to end with durable artifacts under
 * {@code controller/target/pitr-drill/}.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "clearance.heartbeat-timeout-ms=300000",
        "clearance.heartbeat-evaluator-enabled=false",
        "clearance.reconciliation-enabled=false",
        "clearance.recovery-mode=false"
    })
class PitrRewindDrillIntegrationTest {

  private static final long INCARNATION = 7;
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final List<String> BACKUP_TABLES =
      List.of("runners", "jobs", "attempts", "allocations", "recovery_authority");

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
  void cleanAuthorityAndBackup() {
    jdbc.update("DELETE FROM recovery_authority");
    jdbc.execute("DROP SCHEMA IF EXISTS pitr_drill_backup CASCADE");
  }

  @Test
  void destructiveRewindDrillHoldsQuarantineReconcilesAndProvesFreshGeneration() throws Exception {
    Path evidence = Path.of("target", "pitr-drill");
    Files.createDirectories(evidence);

    // T0: seed a fleet with idle, running (claim-only, no terminal), and
    // terminal (SUCCEEDED, CLEANING) runners. Backup captures allocations so
    // post-rewind reconciliation has owned rows to gate on.
    UUID idleRunner = newRunner("AVAILABLE");
    agents.poll(idleRunner, INCARNATION);
    Agent running = newAgent();
    Claim runningClaim = scheduler.claim(running.jobId(), running.runnerId()).orElseThrow();
    agents.poll(running.runnerId(), INCARNATION);
    Agent cleaning = newAgent();
    Claim cleaningClaim = scheduler.claim(cleaning.jobId(), cleaning.runnerId()).orElseThrow();
    agents.poll(cleaning.runnerId(), INCARNATION);
    assertTrue(agents.report(cleaning.runnerId(), report(cleaningClaim, INCARNATION, 1,
        ReportStatus.SUCCEEDED)).accepted());
    assertEquals("CLEANING", runnerState(cleaning.runnerId()));

    writeArtifact(evidence, "t0-seeded-database.json", snapshot());
    backupT0();
    writeArtifact(evidence, "t0-backup-marker.json", Map.of(
        "t0", Instant.now().toString(),
        "note", "logical PostgreSQL backup via backup schema pitr_drill_backup (CREATE TABLE AS TABLE WITH DATA)",
        "idle_runner", idleRunner.toString(),
        "running_allocation", runningClaim.allocationId().toString(),
        "cleaning_allocation", cleaningClaim.allocationId().toString()));
    writeArtifact(evidence, "t0-runners.json",
        jdbc.queryForList("SELECT row_to_json(t)::text AS row FROM runners t ORDER BY runner_id"));

    // T1: surviving execution progresses beyond T0 (RUNNING on the claim-only
    // runner; the terminal runner already holds SUCCEEDED). Then issue the
    // pre-rewind generation that the rewind must forget.
    assertTrue(agents.report(running.runnerId(), report(runningClaim, INCARNATION, 1,
        ReportStatus.RUNNING)).accepted());
    writeArtifact(evidence, "t1-pre-restore-database.json", snapshot());
    UUID generationBefore = recovery.enterRecoveryMode();
    writeArtifact(evidence, "pre-rewind-generation.json",
        Map.of("generation_before_rewind", generationBefore.toString()));
    writeArtifact(evidence, "pre-restore-database.json", snapshot());

    // Rewind the database to T0: forget T1 progress (RUNNING) and the
    // pre-rewind authority row, restoring owned allocations with T0 content.
    // Physical execution is simulated as surviving via later RECONCILE PIDs.
    rewindToT0();
    writeArtifact(evidence, "rewind-marker.json", Map.of(
        "rewound_to", "T0 backup schema pitr_drill_backup",
        "forgot_generation", generationBefore.toString(),
        "method", "DELETE FROM public tables + INSERT SELECT FROM backup (real PostgreSQL restore)"));
    var postRestore = snapshot();
    writeArtifact(evidence, "post-restore-database.json", postRestore);
    assertTrue(recovery.currentGeneration().isEmpty(), "rewind must forget the pre-rewind authority");
    assertEquals("AVAILABLE", runnerState(idleRunner), "T0 idle state restored");
    assertEquals("ASSIGNED", runnerState(running.runnerId()), "T0 claim-only state restored");
    assertEquals("CLEANING", runnerState(cleaning.runnerId()), "T0 terminal state restored");
    assertEquals(0, runningMaxSeq(runningClaim.allocationId()),
        "T1 RUNNING progress must be forgotten by the rewind");

    // Recovery boot issues a fresh generation that never repeats the forgotten
    // value: random UUIDv4, never derived from rewound database state.
    UUID generationAfter = recovery.enterRecoveryMode();
    assertNotEquals(generationBefore, generationAfter,
        "post-rewind generation must never repeat the pre-rewind value");
    writeArtifact(evidence, "generations.json", Map.of(
        "generation_before_rewind", generationBefore.toString(),
        "generation_after_rewind", generationAfter.toString(),
        "source", "random UUIDv4 issued on recovery-mode boot, not a rewound DB increment",
        "authority", "recovery_authority.current_generation"));
    writeArtifact(evidence, "recovery-boot-quarantine.json", snapshot());

    // After rewind and recovery boot the fleet is unsafe and quarantined
    // regardless of restored rows claiming idle, and scheduling stays disabled.
    for (UUID runnerId : List.of(idleRunner, running.runnerId(), cleaning.runnerId())) {
      assertEquals("QUARANTINED", runnerState(runnerId));
      String reason = jdbc.queryForObject(
          "SELECT quarantine_reason FROM runners WHERE runner_id = ?", String.class, runnerId);
      assertTrue(reason != null && reason.contains(generationAfter.toString()));
    }
    assertTrue(scheduler.claim(newJob(), idleRunner).isEmpty());
    assertTrue(scheduler.claim(newJob(), running.runnerId()).isEmpty());
    assertTrue(scheduler.claim(newJob(), cleaning.runnerId()).isEmpty());
    jdbc.update("UPDATE runners SET state = 'AVAILABLE' WHERE runner_id = ?", idleRunner);
    assertTrue(scheduler.claim(newJob(), idleRunner).isEmpty(),
        "restored AVAILABLE without current reconciliation must still refuse scheduling");
    jdbc.update("UPDATE runners SET state = 'QUARANTINED' WHERE runner_id = ?", idleRunner);
    writeArtifact(evidence, "claim-refusal-proof.json", Map.of(
        "quarantined_claims_refused", true,
        "restored_available_without_reconciliation_refused", true,
        "generation", generationAfter.toString()));

    // Stale-generation evidence makes no writes (including no xmin change).
    var beforeStale = snapshot();
    ReportResponse stale = agents.report(running.runnerId(),
        report(runningClaim, INCARNATION, 2, ReportStatus.HEARTBEAT));
    assertFalse(stale.accepted());
    assertEquals("fenced_rejected", stale.reason());
    assertEquals(beforeStale, snapshot(), "superseded-generation progress must make zero writes");
    var beforeStaleReconcile = snapshot();
    ReportResponse staleReconcile = agents.report(running.runnerId(), new ReportRequest(
        runningClaim.allocationId(), runningClaim.runnerEpoch(), INCARNATION, 2,
        ReportStatus.RECONCILE, Instant.parse("2026-09-22T12:34:56Z"), null, null, null, null,
        new AgentProtocol.ReconcileEvidence(false, false, List.of(), true, true, true, null, null),
        UUID.randomUUID()));
    assertFalse(staleReconcile.accepted());
    assertEquals(beforeStaleReconcile, snapshot());
    writeArtifact(evidence, "stale-generation-zero-mutation.json",
        Map.of("stale_progress_rejected", "fenced_rejected", "stale_reconcile_rejected", "fenced_rejected"));

    // Still-running live execution is recognized without release: reuse without
    // positive cleanup does not occur.
    ReportResponse stillRunning = agents.report(running.runnerId(), new ReportRequest(
        runningClaim.allocationId(), runningClaim.runnerEpoch(), INCARNATION, 2,
        ReportStatus.RECONCILE, Instant.parse("2026-09-22T13:00:00Z"), null, null, null, null,
        new AgentProtocol.ReconcileEvidence(true, true, List.of(1234L), false, false, false, null, null),
        generationAfter));
    assertTrue(stillRunning.accepted());
    assertEquals("reconcile_still_running", stillRunning.reason());
    assertEquals("QUARANTINED", runnerState(running.runnerId()));
    assertTrue(scheduler.claim(newJob(), running.runnerId()).isEmpty());
    writeArtifact(evidence, "still-running-evidence.json", snapshot());

    // Finished work needing cleanup directs termination/scrub, then verified
    // positive cleanup advances the generation and returns the runner.
    ReportResponse needsCleanup = agents.report(cleaning.runnerId(), new ReportRequest(
        cleaningClaim.allocationId(), cleaningClaim.runnerEpoch(), INCARNATION, 2,
        ReportStatus.RECONCILE, Instant.parse("2026-09-22T13:00:00Z"), null, null, null, null,
        new AgentProtocol.ReconcileEvidence(false, true, List.of(), true, true, false, null, null),
        generationAfter));
    assertTrue(needsCleanup.accepted());
    assertEquals("reconcile_cleanup_required", needsCleanup.reason());
    assertEquals("QUARANTINED", runnerState(cleaning.runnerId()));
    assertTrue(scheduler.claim(newJob(), cleaning.runnerId()).isEmpty(),
        "reuse without positive cleanup must not occur");
    ReportResponse cleaned = agents.report(cleaning.runnerId(), new ReportRequest(
        cleaningClaim.allocationId(), cleaningClaim.runnerEpoch(), INCARNATION, 3,
        ReportStatus.CLEANUP, Instant.parse("2026-09-22T13:02:00Z"), null, null,
        new AgentProtocol.CleanupEvidence(true, true, true, null), null, null, generationAfter));
    assertTrue(cleaned.accepted());
    assertEquals("ok", cleaned.reason());
    assertEquals("AVAILABLE", runnerState(cleaning.runnerId()));
    assertEquals(generationAfter, jdbc.queryForObject(
        "SELECT reconciled_generation FROM runners WHERE runner_id = ?", UUID.class, cleaning.runnerId()));
    assertTrue(scheduler.claim(newJob(), cleaning.runnerId()).isPresent());
    writeArtifact(evidence, "cleanup-attestation.json", snapshot());

    // Idle-at-backup runners that are physically idle still require one fresh
    // observation and generation advancement before reuse.
    ReportRequest idleWire = AgentProtocol.parseReportRequest("""
        {"runner_epoch": 0, "agent_incarnation": 7, "seq": 1, "status": "RECONCILE",
         "ts": "2026-09-22T13:00:00Z",
         "reconcile": {"cgroup_present": false, "workspace_present": false, "pids": [],
           "execution_empty": true, "descendants_reaped": true, "workspace_clean": true},
         "recovery_generation": "%s"}""".formatted(generationAfter));
    ReportResponse idleResp = agents.report(idleRunner, idleWire);
    assertTrue(idleResp.accepted());
    assertEquals("reconcile_attested", idleResp.reason());
    assertEquals("AVAILABLE", runnerState(idleRunner));
    assertTrue(scheduler.claim(newJob(), idleRunner).isPresent());
    writeArtifact(evidence, "idle-attestation.json", snapshot());

    // Repeating the T0 rewind drill issues a still-new generation.
    rewindToT0();
    UUID generationThird = recovery.enterRecoveryMode();
    assertNotEquals(generationBefore, generationThird);
    assertNotEquals(generationAfter, generationThird,
        "repeated rewinds must keep issuing fresh generations");
    writeArtifact(evidence, "repeated-rewind-generations.json", Map.of(
        "first_post_restore", generationAfter.toString(),
        "second_post_restore", generationThird.toString(),
        "pre_rewind", generationBefore.toString()));
    writeArtifact(evidence, "final-database.json", snapshot());
  }

  private void backupT0() {
    jdbc.execute("DROP SCHEMA IF EXISTS pitr_drill_backup CASCADE");
    jdbc.execute("CREATE SCHEMA pitr_drill_backup");
    for (String table : BACKUP_TABLES) {
      jdbc.execute("CREATE TABLE pitr_drill_backup." + table + " AS TABLE " + table + " WITH DATA");
    }
  }

  private void rewindToT0() {
    // Real PostgreSQL restore: clear current rows, then reinsert the T0 backup.
    // FK order alone is relied upon (children-first DELETE, parents-first
    // INSERT): each jdbc call may borrow a different pooled connection outside
    // a transaction, so SET session_replication_role here would be ineffective
    // (and could leak replica mode into the pool). The backup itself was taken
    // from consistent committed state.
    jdbc.update("DELETE FROM allocations");
    jdbc.update("DELETE FROM attempts");
    jdbc.update("DELETE FROM jobs");
    jdbc.update("DELETE FROM runners");
    jdbc.update("DELETE FROM recovery_authority");
    jdbc.update("INSERT INTO runners SELECT * FROM pitr_drill_backup.runners");
    jdbc.update("INSERT INTO jobs SELECT * FROM pitr_drill_backup.jobs");
    jdbc.update("INSERT INTO recovery_authority SELECT * FROM pitr_drill_backup.recovery_authority");
    jdbc.update("INSERT INTO attempts SELECT * FROM pitr_drill_backup.attempts");
    jdbc.update("INSERT INTO allocations SELECT * FROM pitr_drill_backup.allocations");
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
    return jobs.submit("project-alpha", "pitr-" + UUID.randomUUID(),
        List.of("echo", "hi"), "default").job().jobId();
  }

  private String runnerState(UUID runnerId) {
    return jdbc.queryForObject("SELECT state FROM runners WHERE runner_id = ?", String.class, runnerId);
  }

  private int runningMaxSeq(UUID allocationId) {
    Integer max = jdbc.queryForObject(
        "SELECT max_seq FROM allocations WHERE allocation_id = ?", Integer.class, allocationId);
    return max == null ? 0 : max;
  }

  private static ReportRequest report(Claim claim, long incarnation, long seq, ReportStatus status) {
    return new ReportRequest(claim.allocationId(), claim.runnerEpoch(), incarnation, seq, status,
        Instant.parse("2026-09-22T12:34:56.123456789Z"), null, null);
  }

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

  private static void writeArtifact(Path dir, String name, Object value) throws Exception {
    String rendered = JSON.writeValueAsString(value);
    Files.writeString(dir.resolve(name), rendered + "\n");
  }
}
