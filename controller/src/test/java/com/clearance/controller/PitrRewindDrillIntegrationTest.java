package com.clearance.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.ObjectMapper;

/**
 * Destructive rewind drill for issues #34 and #40: logic-only precursor rehearsal (NOT
 * physical PITR; issue #40 remains open) with a rows-only full-schema backup at T0, T1
 * execution including post-T0 CREATE TABLE/SEQUENCE probe writes, logic-only rewind to T0
 * rows, recovery-mode boot with a fresh non-repeating generation, fleet quarantine without
 * scheduling, verified reconciliation with graceful-then-forceful cleanup semantics, and
 * repeated-rewind freshness.
 *
 * <p>Uses real PostgreSQL as the durable authority (no in-memory substitution).
 * T0 is a rows-only full-schema backup into {@code pitr_drill_backup} ({@code CREATE TABLE backup AS
 * TABLE public WITH DATA} rows only for every user table in {@code public}: no PK/FK/indexes/
 * defaults/identity/views/functions/types) plus a WAL restore point
 * ({@code pg_create_restore_point}) with recorded {@code pg_current_wal_lsn()},
 * {@code pg_walfile_name()} (recorded only), and {@code pg_control_checkpoint()} timeline IDs
 * checked as hygiene only ({@code pg_current_wal_lsn} ordering, restore-after-start,
 * no-branch guard; strict {@code restore <= backupDone} NOT required), not
 * consumed by restore and NOT AC1 timeline-history validation
 * (see {@code PitrTimelineValidator}). Only post-T0 CREATE TABLE/SEQUENCE drop is exercised:
 * DROP/ALTER of pre-existing objects and pre-existing sequence values are NOT covered.
 * Rewind is a logic-only restore to T0 rows ({@code DELETE +
 * INSERT SELECT} in a single transaction with {@code session_replication_role='replica'}, host
 * execution untouched, post-backup CREATEs dropped; no WAL replay / timeline branch). Before each rewind
 * the serving controller is stopped (with an explicit negative check proving claims fail
 * while stopped, so no unreconciled runner can receive work while the authority is empty),
 * and a new OS/JVM child process is booted with {@code clearance.recovery-mode=true}
 * exercising {@code RecoveryBootRunner} against the restored database and the same
 * rewind-surviving generation log; freshness and quarantine are asserted from its retained
 * boot logs, and service-level assertions run against a normal-mode replacement context
 * over the same database. The child boot genuinely exercises a fresh JVM (its own args/env,
 * static state, FDs, and Flyway-plus-runners startup path). Cluster-level
 * {@code pg_basebackup} + WAL replay with timeline branching plus history-file validation is
 * the production runbook and is NOT exercised here (see controller operation docs); this drill
 * is a logic-only precursor rehearsing the backup/rewind/boot sequence, not the same safety
 * property as real PITR restore/replay (issue #40 remains open). Physical cgroup/workspace termination is proven by the
 * companion Go drill ({@code agent/pitr_rewind_drill_integration_test.go}); this drill
 * proves the controller safety property end to end with durable artifacts under
 * {@code controller/target/pitr-drill/}.
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
class PitrRewindDrillIntegrationTest {

  private static final long INCARNATION = 7;
  private static final ObjectMapper JSON = new ObjectMapper();

  @Autowired JdbcTemplate jdbc;
  @Autowired SchedulerService scheduler;
  @Autowired JobService jobs;
  @Autowired AgentService agents;
  @Autowired RecoveryService recovery;

  // Serving controllers are explicitly built Spring contexts owned by the drill (same JVM):
  // the phase-A instance, then one normal-mode replacement context after each OS/JVM
  // recovery child (recovery issuance happens in the child; the replacement only hosts
  // service assertions and preserves the fresh authority untouched). The
  // SpringBootTest context itself is never closed (it only backs setup/teardown), so the
  // shared context cache stays valid for sibling test classes. A standalone pool survives
  // context stops so the rewind window and cleanup always have a live connection. Open
  // contexts and pools are closed in cleanAuthorityAndBackup.
  private ConfigurableApplicationContext serving;
  private HikariDataSource standaloneDs;
  private JdbcTemplate standaloneJdbc;
  private Process recoveryChild;

  @BeforeEach
  void clearAuthority() {
    jdbc.update("DELETE FROM recovery_authority");
  }

  @AfterEach
  void cleanAuthorityAndBackup() {
    if (recoveryChild != null) {
      recoveryChild.destroyForcibly();
      recoveryChild = null;
    }
    jdbc.update("DELETE FROM recovery_authority");
    jdbc.execute("DROP SCHEMA IF EXISTS pitr_drill_backup CASCADE");
    jdbc.execute("DROP TABLE IF EXISTS pitr_drill_ddl_probe CASCADE");
    jdbc.execute("DROP SEQUENCE IF EXISTS pitr_drill_seq_probe CASCADE");
    if (serving != null) {
      serving.close();
      serving = null;
    }
    if (standaloneDs != null && !standaloneDs.isClosed()) {
      standaloneDs.close();
      standaloneDs = null;
    }
  }

  @Test
  void destructiveRewindDrillHoldsQuarantineReconcilesAndProvesFreshGeneration() throws Exception {
    Path evidence = Path.of("target", "pitr-drill");
    Files.createDirectories(evidence);

    // The drill owns its serving controllers explicitly: boot the phase-A instance and run
    // everything below against explicitly built contexts, stopping each one before rewinds.
    bootServingInstance();

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
    String backupLsn = currentWalLsn();
    String backupTimeline = currentWalTimeline();
    String backupWalFile = currentWalFile();
    List<String> backupTables = backupT0();
    List<String> backupSequences = listSequences("public");
    // Recorded WAL restore point at T0 (logic-only precursor; NOT consumed by restore, no
    // WAL replay / timeline branch). The LSN/timeline markers below are hygiene-only checks
    // (pg_current_wal_lsn ordering, restore-after-start, no-branch guard); WAL file recorded
    // only. The restore-point LSN is excluded from pg_current_wal_lsn ordering: a
    // pg_current_wal_lsn() immediately after pg_create_restore_point() can lag behind the
    // returned restore LSN, so only backupStart <= backupDone is ordered here.
    String restorePointLsn = createRestorePoint("pitr_T0_" + System.currentTimeMillis());
    String backupDoneLsn = currentWalLsn();
    String backupDoneTimeline = currentWalTimeline();
    String backupDoneWalFile = currentWalFile();
    PitrTimelineValidator.assertOrdered(backupLsn, backupDoneLsn);
    PitrTimelineValidator.assertRestorePointReachable(backupLsn, restorePointLsn, backupDoneLsn);
    PitrTimelineValidator.assertNoTimelineBranch(backupTimeline, backupDoneTimeline);
    Map<String, Object> backupMarker = new LinkedHashMap<>();
    backupMarker.put("t0", Instant.now().toString());
    backupMarker.put("note", "logic-only precursor rows-only backup via backup schema pitr_drill_backup (CREATE TABLE AS TABLE WITH DATA rows only for every public user table excluding flyway_schema_history; no WAL replay / timeline branch; issue #40 remains open) plus WAL restore point with hygiene-only marker checks (WAL file recorded only, restore does not consume LSN)");
    backupMarker.put("backup_schema", "pitr_drill_backup");
    backupMarker.put("backup_tables", backupTables);
    backupMarker.put("backup_sequences", backupSequences);
    backupMarker.put("restore_point_lsn", restorePointLsn);
    backupMarker.put("restore_point_name_prefix", "pitr_T0_");
    backupMarker.put("timeline_backup_lsn", backupLsn);
    backupMarker.put("timeline_backup_timeline", backupTimeline);
    backupMarker.put("timeline_backup_walfile", backupWalFile);
    backupMarker.put("timeline_backup_done_lsn", backupDoneLsn);
    backupMarker.put("timeline_backup_done_timeline", backupDoneTimeline);
    backupMarker.put("timeline_backup_done_walfile", backupDoneWalFile);
    backupMarker.put("idle_runner", idleRunner.toString());
    backupMarker.put("running_allocation", runningClaim.allocationId().toString());
    backupMarker.put("cleaning_allocation", cleaningClaim.allocationId().toString());
    writeArtifact(evidence, "t0-backup-marker.json", backupMarker);
    writeArtifact(evidence, "t0-runners.json",
        jdbc.queryForList("SELECT row_to_json(t)::text AS row FROM runners t ORDER BY runner_id"));

    // T1: surviving execution progresses beyond T0 (RUNNING on the claim-only
    // runner; the terminal runner already holds SUCCEEDED). Post-T0 CREATE TABLE/SEQUENCE
    // probe writes after T0 are dropped by the rewind (only that narrow case is covered;
    // DROP/ALTER of pre-existing objects and pre-existing sequence values are NOT covered).
    assertTrue(agents.report(running.runnerId(), report(runningClaim, INCARNATION, 1,
        ReportStatus.RUNNING)).accepted());
    createDdlProbe();
    writeArtifact(evidence, "t1-ddl-probe.json", ddlProbeState());
    writeArtifact(evidence, "t1-pre-restore-database.json", snapshot());
    String preRewindLsn = currentWalLsn();
    String preRewindTimeline = currentWalTimeline();
    String preRewindWalFile = currentWalFile();
    // Hygiene-only marker checks across the T0->T1 window (NOT AC1 timeline-history
    // validation): LSNs advance, the restore point stays inside the backup window, and the
    // timeline shows no branch. WAL file recorded only.
    PitrTimelineValidator.assertOrdered(backupDoneLsn, preRewindLsn);
    PitrTimelineValidator.assertNoTimelineBranch(backupTimeline, backupDoneTimeline,
        preRewindTimeline);
    PitrTimelineValidator.assertRestorePointReachable(backupLsn, restorePointLsn, backupDoneLsn);
    UUID generationBefore = bootRecoveryMode(evidence, "pre-rewind-recovery-boot.json");
    writeArtifact(evidence, "pre-rewind-generation.json",
        Map.of("generation_before_rewind", generationBefore.toString(),
            "timeline_lsn", preRewindLsn,
            "timeline_id", preRewindTimeline,
            "timeline_walfile", preRewindWalFile,
            "restore_point_lsn", restorePointLsn,
            "boot", "RecoveryBootRunner with clearance.recovery-mode=true in an isolated child context (logic pin; same-process precursor, not a new OS/JVM process)"));
    writeArtifact(evidence, "pre-restore-database.json", snapshot());

    // Stop the controller before rewind and prove it: with the authority about to be
    // empty, a live instance would grant legacy-availability claims on rewound rows. No
    // instance may serve work in this window.
    captureDatasource();
    stopServingBeforeRewind(evidence, "controller-stopped-proof.json", idleRunner, running.jobId());
    useStandaloneJdbc();

    // Logic-only rewind to T0 rows (NOT a PITR restore/replay to a restore-point LSN):
    // forget T1 progress (RUNNING), the post-T0 CREATE probe, and the pre-rewind authority row,
    // restoring owned allocations with T0 row content. Physical execution is simulated as
    // surviving via later RECONCILE PIDs.
    String rewindLsn = currentWalLsn();
    String rewindTimeline = currentWalTimeline();
    String rewindWalFile = currentWalFile();
    PitrTimelineValidator.assertOrdered(preRewindLsn, rewindLsn);
    PitrTimelineValidator.assertNoTimelineBranch(backupTimeline, rewindTimeline);
    rewindToT0(backupTables, backupSequences);
    String postRestoreLsn = currentWalLsn();
    String postRestoreTimeline = currentWalTimeline();
    String postRestoreWalFile = currentWalFile();
    // The logic-only restore itself writes, so WAL must advance; hygiene-only checks (NOT AC1
    // timeline-history validation): no branch observed and the recorded T0 restore point stays
    // inside the backup window (it was NOT consumed by restore; WAL files recorded only).
    PitrTimelineValidator.assertStrictlyOrdered(rewindLsn, postRestoreLsn, "restore writes");
    PitrTimelineValidator.assertNoTimelineBranch(backupTimeline, backupDoneTimeline,
        preRewindTimeline, rewindTimeline, postRestoreTimeline);
    PitrTimelineValidator.assertRestorePointReachable(backupLsn, restorePointLsn, backupDoneLsn);
    Map<String, Object> timelineValidation = new LinkedHashMap<>();
    timelineValidation.put("backup_lsn", backupLsn);
    timelineValidation.put("restore_point_lsn", restorePointLsn);
    timelineValidation.put("backup_done_lsn", backupDoneLsn);
    timelineValidation.put("pre_rewind_lsn", preRewindLsn);
    timelineValidation.put("rewind_lsn", rewindLsn);
    timelineValidation.put("post_restore_lsn", postRestoreLsn);
    timelineValidation.put("timeline", postRestoreTimeline);
    timelineValidation.put("backup_walfile", backupWalFile);
    timelineValidation.put("backup_done_walfile", backupDoneWalFile);
    timelineValidation.put("pre_rewind_walfile", preRewindWalFile);
    timelineValidation.put("rewind_walfile", rewindWalFile);
    timelineValidation.put("post_restore_walfile", postRestoreWalFile);
    timelineValidation.put("result", "ordered pg_current_wal_lsn, restore-after-start, no-branch (hygiene-only; NOT AC1 timeline-history validation; WAL files recorded only; logical restore did not consume LSN; restore <= backupDone NOT required)");
    timelineValidation.put("note", "logic-only precursor: no base backup / WAL replay / timeline branch; issue #40 remains open");
    writeArtifact(evidence, "timeline-validation.json", timelineValidation);
    writeArtifact(evidence, "rewind-marker.json", Map.of(
        "rewound_to", "T0 rows from backup schema pitr_drill_backup (logic-only precursor; recorded restore point " + restorePointLsn + " NOT consumed by restore; no WAL replay / timeline branch)",
        "forgot_generation", generationBefore.toString(),
        "restore_point_lsn", restorePointLsn,
        "timeline_rewind_lsn", rewindLsn,
        "timeline_rewind_timeline", rewindTimeline,
        "timeline_rewind_walfile", rewindWalFile,
        "timeline_post_restore_lsn", postRestoreLsn,
        "timeline_post_restore_timeline", postRestoreTimeline,
        "timeline_post_restore_walfile", postRestoreWalFile,
        "method", "logic-only rows-only restore to T0 rows: DELETE FROM public tables + INSERT SELECT FROM backup in one transaction with session_replication_role=replica plus drop of post-backup tables/sequences only (DROP/ALTER of pre-existing objects and sequence values NOT covered); host execution untouched; issue #40 remains open"));
    var postRestore = snapshot();
    writeArtifact(evidence, "post-restore-database.json", postRestore);
    assertTrue(jdbc.queryForList("SELECT current_generation FROM recovery_authority").isEmpty(),
        "rewind must forget the pre-rewind authority");
    assertEquals("AVAILABLE", runnerState(idleRunner), "T0 idle state restored");
    assertEquals("ASSIGNED", runnerState(running.runnerId()), "T0 claim-only state restored");
    assertEquals("CLEANING", runnerState(cleaning.runnerId()), "T0 terminal state restored");
    assertEquals(0, runningMaxSeq(runningClaim.allocationId()),
        "T1 RUNNING progress must be forgotten by the rewind");
    assertDdlProbeRewound(evidence);

    // Recovery boot starts a new OS/JVM child process with clearance.recovery-mode=true,
    // exercising RecoveryBootRunner against the restored database. It issues a strictly
    // newer generation from the external monotonic source (time-ordered UUIDv7 plus the
    // rewind-surviving log), never derived from rewound database state alone; freshness and
    // quarantine are asserted from its retained boot logs. Service-level assertions below
    // run against a normal-mode replacement context over the same database.
    UUID generationAfter =
        bootRecoveryOsProcess(evidence, "post-restore-recovery-boot", generationBefore);
    bootServiceContext();
    assertNotEquals(generationBefore, generationAfter,
        "post-rewind generation must never repeat the pre-rewind value");
    assertTrue(RecoveryGenerationSource.timestampMillis(generationAfter)
        > RecoveryGenerationSource.timestampMillis(generationBefore));
    writeArtifact(evidence, "generations.json", Map.of(
        "generation_before_rewind", generationBefore.toString(),
        "generation_after_rewind", generationAfter.toString(),
        "source", "time-ordered UUIDv7 from the external monotonic source plus rewind-surviving log via RecoveryBootRunner in a new OS/JVM child process (boot logs retained)",
        "boot", "clearance.recovery-mode=true (child OS/JVM process; service assertions run against a normal-mode replacement context)",
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

    // Repeating the T0 rows-only rewind issues a still-new generation: stop the serving
    // replacement first (same negative check), rewind to the same T0 rows, and boot
    // another OS/JVM child process for recovery. No-branch hygiene is re-checked and only
    // generation freshness is asserted afterwards, straight from the database and the retained
    // boot logs. NOT a PITR restore/replay (issue #40 remains open).
    stopServingBeforeRewind(evidence, "repeated-rewind-stopped-proof.json", idleRunner,
        running.jobId());
    useStandaloneJdbc();
    String repeatedRewindLsn = currentWalLsn();
    String repeatedRewindTimeline = currentWalTimeline();
    PitrTimelineValidator.assertNoTimelineBranch(backupTimeline, repeatedRewindTimeline);
    rewindToT0(backupTables, backupSequences);
    String repeatedPostRestoreLsn = currentWalLsn();
    PitrTimelineValidator.assertStrictlyOrdered(repeatedRewindLsn, repeatedPostRestoreLsn,
        "repeated restore writes");
    PitrTimelineValidator.assertNoTimelineBranch(backupTimeline,
        currentWalTimeline());
    assertDdlProbeRewound(evidence);
    UUID generationThird =
        bootRecoveryOsProcess(evidence, "repeated-rewind-recovery-boot", generationAfter);
    assertNotEquals(generationBefore, generationThird);
    assertNotEquals(generationAfter, generationThird,
        "repeated rewinds must keep issuing fresh generations");
    assertTrue(RecoveryGenerationSource.timestampMillis(generationThird)
        > RecoveryGenerationSource.timestampMillis(generationAfter));
    writeArtifact(evidence, "repeated-rewind-generations.json", Map.of(
        "first_post_restore", generationAfter.toString(),
        "second_post_restore", generationThird.toString(),
        "pre_rewind", generationBefore.toString()));
    writeArtifact(evidence, "final-database.json", snapshot());
  }

  private List<String> backupT0() {
    // Rows-only full-schema backup on a single connection: every user table in public
    // (excluding flyway_schema_history bookkeeping, which never changes during the
    // drill) is copied into pitr_drill_backup with CREATE TABLE AS (rows only: no
    // PK/FK/indexes/defaults/identity/views/functions/types). REPEATABLE READ
    // keeps the copied view consistent on one connection with no pooled-connection split.
    // Together with the WAL restore point recorded by the caller this is a logic-only
    // precursor rehearsal for issue #40 (issue #40 remains open): markers are hygiene-only
    // and the later logical restore does not consume the restore-point LSN.
    try (Connection con = jdbc.getDataSource().getConnection()) {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      try (Statement stmt = con.createStatement()) {
        stmt.execute("DROP SCHEMA IF EXISTS pitr_drill_backup CASCADE");
        stmt.execute("CREATE SCHEMA pitr_drill_backup");
        List<String> tables = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery(
            "SELECT table_name FROM information_schema.tables "
                + "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
                + "AND table_name <> 'flyway_schema_history' ORDER BY table_name")) {
          while (rs.next()) {
            tables.add(rs.getString(1));
          }
        }
        for (String table : tables) {
          stmt.execute("CREATE TABLE pitr_drill_backup.\"" + table.replace("\"", "\"\"")
              + "\" AS TABLE public.\"" + table.replace("\"", "\"\"") + "\" WITH DATA");
        }
        con.commit();
        return tables;
      } catch (Exception e) {
        con.rollback();
        throw e;
      }
    } catch (Exception e) {
      throw new IllegalStateException("T0 backup failed", e);
    }
  }

  private void rewindToT0(List<String> backupTables, List<String> backupSequences) {
    // Logic-only restore to T0 rows in one transaction on one connection (NOT a PITR
    // restore/replay; does NOT consume the restore-point LSN; no WAL replay / timeline
    // branch; issue #40 remains open):
    // SET LOCAL session_replication_role applies to this transaction only (no pool
    // leak), so FK order is irrelevant and the rewind is atomic. Only tables/sequences
    // created after T0 (including the DDL probe) are dropped; pre-existing tables get
    // DELETE + INSERT SELECT (rows only, definitions/constraints/indexes not restored,
    // pre-existing sequence values not reset, a T0 table dropped at T1 is NOT recreated).
    // Host execution is untouched; only database rows return to T0.
    try (Connection con = jdbc.getDataSource().getConnection()) {
      con.setAutoCommit(false);
      try (Statement stmt = con.createStatement()) {
        stmt.execute("SET LOCAL session_replication_role = 'replica'");
        List<String> current = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery(
            "SELECT table_name FROM information_schema.tables "
                + "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
                + "AND table_name <> 'flyway_schema_history' ORDER BY table_name")) {
          while (rs.next()) {
            current.add(rs.getString(1));
          }
        }
        List<String> backed = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery(
            "SELECT table_name FROM information_schema.tables "
                + "WHERE table_schema = 'pitr_drill_backup' AND table_type = 'BASE TABLE' "
                + "ORDER BY table_name")) {
          while (rs.next()) {
            backed.add(rs.getString(1));
          }
        }
        if (!backed.containsAll(backupTables) || !backupTables.containsAll(backed)) {
          throw new IllegalStateException(
              "backup schema drifted from T0 (expected " + backupTables + " got " + backed + ")");
        }
        for (String table : current) {
          if (!backed.contains(table)) {
            stmt.execute("DROP TABLE public.\"" + table.replace("\"", "\"\"") + "\" CASCADE");
          }
        }
        // Refresh the surviving list after drops (DDL probe table gone).
        current.removeIf(t -> !backed.contains(t));
        for (String table : current) {
          stmt.execute("DELETE FROM public.\"" + table.replace("\"", "\"\"") + "\"");
        }
        for (String table : backed) {
          stmt.execute("INSERT INTO public.\"" + table.replace("\"", "\"\"")
              + "\" SELECT * FROM pitr_drill_backup.\"" + table.replace("\"", "\"\"") + "\"");
        }
        // Sequences: only post-T0 sequences are dropped. Pre-existing sequence values are
        // NOT reset and a T0 table dropped at T1 is NOT recreated (see method header):
        // only post-T0 CREATE TABLE/SEQUENCE drop is covered.
        List<String> currentSeqs = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery(
            "SELECT sequence_name FROM information_schema.sequences "
                + "WHERE sequence_schema = 'public' ORDER BY sequence_name")) {
          while (rs.next()) {
            currentSeqs.add(rs.getString(1));
          }
        }
        for (String seq : currentSeqs) {
          if (backupSequences == null || !backupSequences.contains(seq)) {
            stmt.execute("DROP SEQUENCE public.\"" + seq.replace("\"", "\"\"") + "\"");
          }
        }
        con.commit();
      } catch (Exception e) {
        con.rollback();
        throw e;
      }
    } catch (Exception e) {
      throw new IllegalStateException("rewind to T0 failed", e);
    }
  }

  private List<String> listSequences(String schema) {
    return jdbc.queryForList(
        "SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = ? "
            + "ORDER BY sequence_name",
        String.class, schema);
  }

  private String createRestorePoint(String name) {
    // Recorded WAL restore point at T0 for hygiene-only marker checks (NOT consumed by the
    // logic-only restore; no WAL replay / timeline branch). Requires superuser (the test
    // database user is); a failure fails the drill rather than silently degrading to
    // informational markers.
    String safe = name.replaceAll("[^A-Za-z0-9_]", "_");
    return jdbc.queryForObject("SELECT pg_create_restore_point(?)::text", String.class, safe);
  }

  private void createDdlProbe() {
    // Post-T0 CREATE TABLE/INDEX/SEQUENCE probe writes: the rewind drops them (only this
    // narrow post-T0 CREATE case is covered, not DROP/ALTER of pre-existing objects).
    jdbc.execute("DROP TABLE IF EXISTS pitr_drill_ddl_probe");
    jdbc.execute("DROP SEQUENCE IF EXISTS pitr_drill_seq_probe");
    jdbc.execute("CREATE TABLE pitr_drill_ddl_probe (id BIGINT PRIMARY KEY, data TEXT NOT NULL)");
    jdbc.execute("CREATE INDEX ix_pitr_drill_ddl_probe_data ON pitr_drill_ddl_probe (data)");
    jdbc.execute("CREATE SEQUENCE pitr_drill_seq_probe START 100");
    jdbc.queryForObject("SELECT nextval('pitr_drill_seq_probe')", Long.class);
    jdbc.queryForObject("SELECT nextval('pitr_drill_seq_probe')", Long.class);
    jdbc.update("INSERT INTO pitr_drill_ddl_probe (id, data) VALUES (nextval('pitr_drill_seq_probe'), 't1')");
  }

  private Map<String, Object> ddlProbeState() {
    Map<String, Object> state = new LinkedHashMap<>();
    state.put("table_exists", tableExists("pitr_drill_ddl_probe"));
    state.put("sequence_exists", sequenceExists("pitr_drill_seq_probe"));
    if (tableExists("pitr_drill_ddl_probe")) {
      state.put("rows", jdbc.queryForObject("SELECT COUNT(*) FROM pitr_drill_ddl_probe", Long.class));
    }
    if (sequenceExists("pitr_drill_seq_probe")) {
      state.put("last_value",
          jdbc.queryForObject("SELECT last_value FROM pitr_drill_seq_probe", Long.class));
    }
    return state;
  }

  private void assertDdlProbeRewound(Path evidence) throws Exception {
    assertFalse(tableExists("pitr_drill_ddl_probe"),
        "rewind must drop the post-backup DDL probe table");
    assertFalse(sequenceExists("pitr_drill_seq_probe"),
        "rewind must drop the post-backup sequence");
    writeArtifact(evidence, "ddl-probe-rewound.json",
        Map.of("table_exists", false, "sequence_exists", false,
            "note", "only post-T0 CREATE TABLE/SEQUENCE drop is covered; DROP/ALTER of pre-existing objects and pre-existing sequence values are NOT covered"));
  }

  private boolean tableExists(String table) {
    Integer count = jdbc.queryForObject(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' "
            + "AND table_name = ?",
        Integer.class, table);
    return count != null && count > 0;
  }

  private boolean sequenceExists(String sequence) {
    Integer count = jdbc.queryForObject(
        "SELECT COUNT(*) FROM information_schema.sequences WHERE sequence_schema = 'public' "
            + "AND sequence_name = ?",
        Integer.class, sequence);
    return count != null && count > 0;
  }

  private String currentWalLsn() {
    return jdbc.queryForObject("SELECT pg_current_wal_lsn()::text", String.class);
  }

  private String currentWalTimeline() {
    // Current WAL timeline, recorded alongside each LSN marker and checked for no unexpected
    // branch across the drill window (no-branch guard only, NOT AC1 timeline-history
    // validation; see PitrTimelineValidator).
    return jdbc.queryForObject("SELECT timeline_id::text FROM pg_control_checkpoint()", String.class);
  }

  private String currentWalFile() {
    // Recorded only; never validated for restore selection (logic-only precursor).
    return jdbc.queryForObject("SELECT pg_walfile_name(pg_current_wal_lsn())", String.class);
  }

  /**
   * Exercises the production {@code RecoveryBootRunner} path in an isolated child context
   * while the parent stays up. Used only for the pre-rewind generation, where the authority
   * is present and no legacy-availability window exists.
   */
  private UUID bootRecoveryMode(Path evidence, String artifact) throws Exception {
    String logPath = System.getProperty("java.io.tmpdir") + "/clearance-recovery-generations-test.log";
    try (ConfigurableApplicationContext ctx = new SpringApplicationBuilder(Application.class)
        .web(WebApplicationType.NONE)
        .run("--clearance.recovery-mode=true",
            "--clearance.recovery-generation-log=" + logPath,
            "--clearance.heartbeat-timeout-ms=300000",
            "--clearance.heartbeat-evaluator-enabled=false",
            "--clearance.reconciliation-enabled=false")) {
      UUID generation =
          ctx.getBean(RecoveryService.class).currentGeneration().orElseThrow();
      assertEquals(generation, recovery.currentGeneration().orElseThrow(),
          "recovery reboot must persist the generation in the shared database");
      writeArtifact(evidence, artifact, Map.of(
          "generation", generation.toString(),
          "boot", "RecoveryBootRunner with clearance.recovery-mode=true in an isolated child context (pre-rewind; authority present, no window)",
          "log", logPath,
          "quarantine_snapshot", snapshot()));
      return generation;
    }
  }

  /**
   * Captures the test datasource coordinates before the serving context is stopped, so the
   * rewind window and cleanup can run over a standalone pool that outlives context stops.
   */
  private void captureDatasource() {
    HikariDataSource hikari = (HikariDataSource) jdbc.getDataSource();
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(hikari.getJdbcUrl());
    config.setUsername(hikari.getUsername());
    config.setPassword(hikari.getPassword());
    config.setMaximumPoolSize(2);
    config.setPoolName("pitr-drill-standalone");
    standaloneDs = new HikariDataSource(config);
    standaloneJdbc = new JdbcTemplate(standaloneDs);
  }

  private void useStandaloneJdbc() {
    this.jdbc = standaloneJdbc;
  }

  /**
   * Stops the currently serving controller before a rewind and proves the window is safe:
   * the context reports inactive and a claim attempt through the stopped instance fails
   * instead of granting legacy-availability work on rewound rows. No instance serves work
   * between this stop and the replacement recovery-mode boot.
   */
  private void stopServingBeforeRewind(Path evidence, String artifact, UUID idleRunner, UUID anyJobId)
      throws Exception {
    ConfigurableApplicationContext stopped = serving;
    SchedulerService stoppedScheduler = scheduler;
    stopped.close();
    assertFalse(stopped.isActive(), "controller must stay stopped across the rewind window");
    assertThrows(RuntimeException.class, () -> stoppedScheduler.claim(anyJobId, idleRunner),
        "claims must fail while the controller is stopped for rewind");
    writeArtifact(evidence, artifact, Map.of(
        "serving_active", false,
        "claim_in_window", "refused",
        "note", "serving Spring context closed before rewind; claim via the stopped instance "
            + "throws (pool closed) so no unreconciled runner receives work while authority is empty"));
  }

  /**
   * Boots the phase-A serving controller owned by the drill (normal mode, same test
   * properties as the class) and rewire the drill to its beans.
   */
  private void bootServingInstance() {
    ConfigurableApplicationContext ctx = new SpringApplicationBuilder(Application.class)
        .web(WebApplicationType.NONE)
        .run("--clearance.recovery-mode=false",
            "--clearance.recovery-generation-log=" + generationLogPath(),
            "--clearance.heartbeat-timeout-ms=300000",
            "--clearance.heartbeat-evaluator-enabled=false",
            "--clearance.reconciliation-enabled=false");
    rewire(ctx);
  }

  private static String generationLogPath() {
    return System.getProperty("java.io.tmpdir") + "/clearance-recovery-generations-test.log";
  }

  private void rewire(ConfigurableApplicationContext ctx) {
    this.jdbc = ctx.getBean(JdbcTemplate.class);
    this.scheduler = ctx.getBean(SchedulerService.class);
    this.jobs = ctx.getBean(JobService.class);
    this.agents = ctx.getBean(AgentService.class);
    this.recovery = ctx.getBean(RecoveryService.class);
    this.serving = ctx;
  }

  /**
   * Boots a new OS/JVM child process running the controller with
   * {@code clearance.recovery-mode=true} against the restored database, waits until its
   * {@code RecoveryBootRunner} persists a fresh generation, and asserts freshness and
   * quarantine from its retained boot logs. The child exercises a genuinely fresh JVM
   * (own args/env, static state, FDs, Flyway-plus-runners startup); it is destroyed before
   * returning, and service-level assertions continue against a replacement context. The
   * child process handle is tracked in {@code recoveryChild} so a failure still cleans up
   * in {@code cleanAuthorityAndBackup}.
   *
   * @param forgotten the pre-rewind generation that must never reappear
   * @return the fresh generation issued by the child process
   */
  private UUID bootRecoveryOsProcess(Path evidence, String artifactPrefix, UUID forgotten)
      throws Exception {
    String javaBin = System.getProperty("java.home") + "/bin/java";
    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    List<String> command = new ArrayList<>(List.of(
        javaBin, "-cp", System.getProperty("java.class.path"),
        "com.clearance.controller.Application",
        "--server.port=" + port,
        "--clearance.recovery-mode=true",
        "--clearance.recovery-generation-log=" + generationLogPath(),
        "--clearance.heartbeat-timeout-ms=300000",
        "--clearance.heartbeat-evaluator-enabled=false",
        "--clearance.reconciliation-enabled=false"));
    String datasourceUrl = System.getProperty("spring.datasource.url");
    if (datasourceUrl != null && !datasourceUrl.isBlank()) {
      command.add("--spring.datasource.url=" + datasourceUrl);
      String username = System.getProperty("spring.datasource.username");
      if (username != null && !username.isBlank()) {
        command.add("--spring.datasource.username=" + username);
      }
      String password = System.getProperty("spring.datasource.password");
      if (password != null && !password.isBlank()) {
        command.add("--spring.datasource.password=" + password);
      }
    }
    Path bootLog = evidence.resolve(artifactPrefix + "-child-boot.log");
    recoveryChild = new ProcessBuilder(command)
        .redirectOutput(bootLog.toFile())
        .redirectErrorStream(true)
        .start();
    try {
      UUID generation = awaitRecoveryGeneration(forgotten, Duration.ofSeconds(120));
      String log = Files.readString(bootLog);
      assertTrue(log.contains("Recovery-mode boot requested"),
          "child boot log must show RecoveryBootRunner ran");
      assertTrue(log.contains(generation.toString()),
          "child boot log must name the issued generation");
      writeArtifact(evidence, artifactPrefix + ".json", Map.of(
          "generation", generation.toString(),
          "boot", "new OS/JVM child process running com.clearance.controller.Application "
              + "with clearance.recovery-mode=true (RecoveryBootRunner; fresh JVM, boot log retained)",
          "log", generationLogPath(),
          "boot_log", bootLog.getFileName().toString(),
          "quarantine_snapshot", snapshot()));
      return generation;
    } finally {
      recoveryChild.destroyForcibly();
      recoveryChild.waitFor(30, TimeUnit.SECONDS);
      recoveryChild = null;
    }
  }

  /**
   * Boots a normal-mode replacement context hosting the service beans that the
   * service-level assertions drive. Recovery issuance already happened in the child OS/JVM
   * process; this context preserves the authority untouched (normal boot path) and only
   * provides beans over the same database. Stays open for the rest of the drill.
   */
  private void bootServiceContext() {
    ConfigurableApplicationContext ctx = new SpringApplicationBuilder(Application.class)
        .web(WebApplicationType.NONE)
        .run("--clearance.recovery-mode=false",
            "--clearance.recovery-generation-log=" + generationLogPath(),
            "--clearance.heartbeat-timeout-ms=300000",
            "--clearance.heartbeat-evaluator-enabled=false",
            "--clearance.reconciliation-enabled=false");
    rewire(ctx);
  }

  private UUID awaitRecoveryGeneration(UUID forgotten, Duration timeout) {
    Instant deadline = Instant.now().plus(timeout);
    while (Instant.now().isBefore(deadline)) {
      if (!recoveryChild.isAlive()) {
        throw new IllegalStateException(
            "recovery child JVM exited before issuing a generation");
      }
      UUID current = jdbc.queryForList(
              "SELECT current_generation FROM recovery_authority", UUID.class)
          .stream().findFirst().orElse(null);
      if (current != null && !current.equals(forgotten)) {
        return current;
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted waiting for recovery child", e);
      }
    }
    throw new IllegalStateException("timed out waiting for the recovery child generation");
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
    // Cover every remaining user table (e.g. bootstrap_check) so pre/post-restore
    // snapshots and stale-generation zero-mutation checks prove the rows-only full-schema
    // rewind (rows only, not definitions);
    // ordering by row JSON keeps the snapshot deterministic for equality checks.
    List<String> extra = jdbc.queryForList(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' "
            + "AND table_type = 'BASE TABLE' AND table_name NOT IN "
            + "('runners', 'allocations', 'attempts', 'jobs', 'recovery_authority', "
            + "'flyway_schema_history') ORDER BY table_name",
        String.class);
    for (String table : extra) {
      String quoted = "\"" + table.replace("\"", "\"\"") + "\"";
      tables.put(table, jdbc.queryForList(
          "SELECT xmin::text AS version, row_to_json(t)::text AS row FROM public." + quoted
              + " t ORDER BY row_to_json(t)::text"));
    }
    return tables;
  }

  private static void writeArtifact(Path dir, String name, Object value) throws Exception {
    String rendered = JSON.writeValueAsString(value);
    Files.writeString(dir.resolve(name), rendered + "\n");
  }
}
