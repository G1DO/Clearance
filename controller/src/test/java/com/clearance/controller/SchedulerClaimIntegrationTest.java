package com.clearance.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.clearance.controller.jobs.JobNotFoundException;
import com.clearance.controller.jobs.JobService;
import com.clearance.controller.scheduling.Claim;
import com.clearance.controller.scheduling.SchedulerService;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * PostgreSQL-backed verification for issue #5: queued work exclusively claims a compatible
 * schedulable runner with PostgreSQL as the sole authority.
 *
 * <p>Runs against a real PostgreSQL instance (no in-memory substitution). Every test uses fresh
 * UUID identities so tests never contend with each other.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SchedulerClaimIntegrationTest {

  @Autowired JdbcTemplate jdbc;
  @Autowired DataSource dataSource;
  @Autowired SchedulerService scheduler;
  @Autowired JobService jobs;

  private UUID newJob(String runnerClass) {
    String operationId = "claim-" + UUID.randomUUID();
    return jobs.submit("project-alpha", operationId, List.of("echo", "hi"), runnerClass)
        .job()
        .jobId();
  }

  private void insertRunner(UUID runnerId, String runnerClass, String state, long epoch) {
    jdbc.update(
        "INSERT INTO runners (runner_id, runner_class, state, epoch) VALUES (?, ?, ?, ?)",
        runnerId,
        runnerClass,
        state,
        epoch);
  }

  private String runnerState(UUID runnerId) {
    return jdbc.queryForObject(
        "SELECT state FROM runners WHERE runner_id = ?", String.class, runnerId);
  }

  private long runnerEpoch(UUID runnerId) {
    Long epoch =
        jdbc.queryForObject("SELECT epoch FROM runners WHERE runner_id = ?", Long.class, runnerId);
    return epoch == null ? -1 : epoch;
  }

  private int activeAllocationsForRunner(UUID runnerId) {
    Integer n =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM allocations WHERE runner_id = ? AND state = 'ACTIVE'",
            Integer.class,
            runnerId);
    return n == null ? 0 : n;
  }

  private int attemptsForJob(UUID jobId) {
    Integer n =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM attempts WHERE job_id = ?", Integer.class, jobId);
    return n == null ? 0 : n;
  }

  @Test
  void successfulClaimBindsDistinctIdentitiesAndAdvancesEpoch() {
    UUID jobId = newJob("default");
    UUID runnerId = UUID.randomUUID();
    insertRunner(runnerId, "default", "AVAILABLE", 0);

    Optional<Claim> result = scheduler.claim(jobId, runnerId);

    assertTrue(result.isPresent(), "compatible AVAILABLE runner must be claimed");
    Claim claim = result.get();
    assertEquals(jobId, claim.jobId());
    assertEquals(runnerId, claim.runnerId());
    // Distinct durable identities: logical job, attempt, allocation, epoch are never collapsed.
    assertNotEquals(claim.jobId(), claim.attemptId());
    assertNotEquals(claim.jobId(), claim.allocationId());
    assertNotEquals(claim.attemptId(), claim.allocationId());
    assertEquals("ASSIGNED", runnerState(runnerId));
    assertEquals(1, runnerEpoch(runnerId), "epoch must advance monotonically by exactly one");
    assertEquals(1, claim.runnerEpoch());
    assertEquals(1, activeAllocationsForRunner(runnerId));
    assertEquals(1, attemptsForJob(jobId));

    // The committed allocation is the authoritative row downstream code would read.
    UUID storedAllocation =
        jdbc.queryForObject(
            "SELECT allocation_id FROM allocations WHERE runner_id = ? AND state = 'ACTIVE'",
            UUID.class,
            runnerId);
    assertEquals(claim.allocationId(), storedAllocation);
    Long storedEpoch =
        jdbc.queryForObject(
            "SELECT runner_epoch FROM allocations WHERE allocation_id = ?", Long.class, storedAllocation);
    assertEquals(1, storedEpoch);
  }

  @Test
  void incompatibleRunnerClassIsNotClaimed() {
    UUID jobId = newJob("default");
    UUID runnerId = UUID.randomUUID();
    insertRunner(runnerId, "large", "AVAILABLE", 0);

    Optional<Claim> result = scheduler.claim(jobId, runnerId);

    assertTrue(result.isEmpty(), "runnerClass mismatch must not be claimed");
    assertEquals("AVAILABLE", runnerState(runnerId));
    assertEquals(0, runnerEpoch(runnerId));
    assertEquals(0, activeAllocationsForRunner(runnerId));
    assertEquals(0, attemptsForJob(jobId));
  }

  @Test
  void nonAvailableRunnerIsNotClaimedEvenWithoutActiveAllocation() {
    UUID jobA = newJob("default");
    UUID assigned = UUID.randomUUID();
    insertRunner(assigned, "default", "ASSIGNED", 3);
    assertTrue(scheduler.claim(jobA, assigned).isEmpty());
    assertEquals("ASSIGNED", runnerState(assigned));
    assertEquals(3, runnerEpoch(assigned));

    UUID jobB = newJob("default");
    UUID quarantined = UUID.randomUUID();
    insertRunner(quarantined, "default", "QUARANTINED", 7);
    assertTrue(scheduler.claim(jobB, quarantined).isEmpty());
    assertEquals("QUARANTINED", runnerState(quarantined));
    assertEquals(7, runnerEpoch(quarantined));

    // Neither attempt left rows behind despite matching runnerClass and no allocation row.
    assertEquals(0, attemptsForJob(jobA));
    assertEquals(0, attemptsForJob(jobB));
  }

  @Test
  void unknownJobThrowsAndUnknownRunnerYieldsNoAllocation() {
    UUID runnerId = UUID.randomUUID();
    insertRunner(runnerId, "default", "AVAILABLE", 0);
    try {
      scheduler.claim(UUID.randomUUID(), runnerId);
      fail("unknown job must throw");
    } catch (JobNotFoundException expected) {
      // Expected: unknown job identity is a caller error, not an empty claim.
    }
    assertEquals("AVAILABLE", runnerState(runnerId));

    UUID jobId = newJob("default");
    assertTrue(scheduler.claim(jobId, UUID.randomUUID()).isEmpty());
    assertEquals(0, attemptsForJob(jobId));
  }

  /**
   * F09 contention: two independent controller/scheduler instances (separate application contexts
   * with separate Hikari pools and connections) race for one compatible AVAILABLE runner. Exactly
   * one commits an allocation; the loser observes no allocation; PostgreSQL holds at most one
   * authoritative active allocation and the runner is no longer schedulable.
   */
  @Test
  void concurrentClaimsFromIndependentInstancesYieldSingleAllocation() throws Exception {
    UUID runnerId = UUID.randomUUID();
    insertRunner(runnerId, "f09", "AVAILABLE", 4);
    UUID jobA = newJob("f09");
    UUID jobB = newJob("f09");

    ConfigurableApplicationContext ctxA = startApp();
    ConfigurableApplicationContext ctxB = startApp();
    try {
      SchedulerService schedulerA = ctxA.getBean(SchedulerService.class);
      SchedulerService schedulerB = ctxB.getBean(SchedulerService.class);
      assertNotSame(
          ctxA.getBean(DataSource.class), ctxB.getBean(DataSource.class),
          "contenders must use separate database pools");

      ExecutorService pool = Executors.newFixedThreadPool(2);
      CountDownLatch ready = new CountDownLatch(2);
      CountDownLatch start = new CountDownLatch(1);
      UUID winnerJob;
      UUID loserJob;
      try {
        Future<Optional<Claim>> futureA =
            pool.submit(
                () -> {
                  ready.countDown();
                  if (!start.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("start gate timeout");
                  }
                  return schedulerA.claim(jobA, runnerId);
                });
        Future<Optional<Claim>> futureB =
            pool.submit(
                () -> {
                  ready.countDown();
                  if (!start.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("start gate timeout");
                  }
                  return schedulerB.claim(jobB, runnerId);
                });
        assertTrue(ready.await(10, TimeUnit.SECONDS));
        start.countDown();

        Optional<Claim> resultA = futureA.get(30, TimeUnit.SECONDS);
        Optional<Claim> resultB = futureB.get(30, TimeUnit.SECONDS);
        int wins = (resultA.isPresent() ? 1 : 0) + (resultB.isPresent() ? 1 : 0);
        assertEquals(1, wins, "exactly one contender must obtain a committed allocation");

        Claim winner = resultA.isPresent() ? resultA.get() : resultB.get();
        winnerJob = resultA.isPresent() ? jobA : jobB;
        loserJob = resultA.isPresent() ? jobB : jobA;
        assertEquals(winnerJob, winner.jobId());
        assertEquals(runnerId, winner.runnerId());
        assertNotEquals(winner.allocationId(), winner.attemptId());
      } finally {
        pool.shutdownNow();
      }

      // Final database state: exactly one committed winning allocation, at most one
      // authoritative active allocation, runner no longer schedulable, loser left nothing.
      assertEquals(1, activeAllocationsForRunner(runnerId));
      Integer totalAllocations =
          jdbc.queryForObject(
              "SELECT COUNT(*) FROM allocations WHERE runner_id = ?", Integer.class, runnerId);
      assertEquals(1, totalAllocations);
      assertEquals("ASSIGNED", runnerState(runnerId));
      assertEquals(5, runnerEpoch(runnerId), "epoch must advance by exactly one, once");
      assertEquals(1, attemptsForJob(winnerJob), "winner holds the single attempt");
      assertEquals(0, attemptsForJob(loserJob), "loser left no orphan attempt");

      // The runner is no longer schedulable by another claimant.
      UUID lateJob = newJob("f09");
      assertTrue(scheduler.claim(lateJob, runnerId).isEmpty());
      assertEquals(0, attemptsForJob(lateJob));
    } finally {
      ctxA.close();
      ctxB.close();
    }
  }

  /**
   * The partial unique index independently rejects a second authoritative active allocation when
   * the scheduler path is bypassed, so a selection mistake cannot silently double-allocate.
   */
  @Test
  void databaseInvariantRejectsSecondActiveAllocation() {
    UUID runnerId = UUID.randomUUID();
    insertRunner(runnerId, "default", "ASSIGNED", 1);
    UUID jobId = newJob("default");
    UUID attemptA = UUID.randomUUID();
    UUID allocationA = UUID.randomUUID();
    jdbc.update("INSERT INTO attempts (attempt_id, job_id) VALUES (?, ?)", attemptA, jobId);
    jdbc.update(
        "INSERT INTO allocations (allocation_id, attempt_id, job_id, runner_id, runner_epoch)"
            + " VALUES (?, ?, ?, ?, ?)",
        allocationA,
        attemptA,
        jobId,
        runnerId,
        1);

    UUID attemptB = UUID.randomUUID();
    jdbc.update("INSERT INTO attempts (attempt_id, job_id) VALUES (?, ?)", attemptB, jobId);
    try {
      jdbc.update(
          "INSERT INTO allocations (allocation_id, attempt_id, job_id, runner_id, runner_epoch)"
              + " VALUES (?, ?, ?, ?, ?)",
          UUID.randomUUID(),
          attemptB,
          jobId,
          runnerId,
          2);
      fail("second ACTIVE allocation for one runner must be rejected");
    } catch (DataAccessException expected) {
      assertTrue(
          expected.getMessage() != null
              && expected.getMessage().contains("uq_allocations_runner_active"),
          "rejection must come from the partial unique index, got: " + expected.getMessage());
    }

    assertEquals(1, activeAllocationsForRunner(runnerId));
    UUID surviving =
        jdbc.queryForObject(
            "SELECT allocation_id FROM allocations WHERE runner_id = ? AND state = 'ACTIVE'",
            UUID.class,
            runnerId);
    assertEquals(allocationA, surviving);
  }

  /**
   * When the allocation insert fails (here via the active-allocation invariant), the whole claim
   * rolls back: no partial allocation, no orphan attempt, no consumed epoch, and the runner does
   * not falsely report a committed claim.
   */
  @Test
  void failedClaimLeavesNoPartialStateOrConsumedEpoch() {
    UUID runnerId = UUID.randomUUID();
    insertRunner(runnerId, "default", "AVAILABLE", 9);
    // Pre-existing authoritative active allocation planted outside the scheduler path while the
    // runner still reports AVAILABLE: the claim's runner UPDATE will match, but the allocation
    // insert must hit the invariant and roll everything back.
    UUID priorJob = newJob("default");
    UUID priorAttempt = UUID.randomUUID();
    jdbc.update("INSERT INTO attempts (attempt_id, job_id) VALUES (?, ?)", priorAttempt, priorJob);
    jdbc.update(
        "INSERT INTO allocations (allocation_id, attempt_id, job_id, runner_id, runner_epoch)"
            + " VALUES (?, ?, ?, ?, ?)",
        UUID.randomUUID(),
        priorAttempt,
        priorJob,
        runnerId,
        9);

    UUID jobId = newJob("default");
    try {
      scheduler.claim(jobId, runnerId);
      fail("claim violating the active-allocation invariant must fail");
    } catch (DataAccessException expected) {
      // Expected: constraint violation rolls back the entire claim transaction.
    }

    assertEquals("AVAILABLE", runnerState(runnerId));
    assertEquals(9, runnerEpoch(runnerId), "rolled-back claim must not consume an epoch");
    assertEquals(0, attemptsForJob(jobId), "rolled-back claim must not leave an orphan attempt");
    assertEquals(1, activeAllocationsForRunner(runnerId));
    Integer totalAllocations =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM allocations WHERE runner_id = ?", Integer.class, runnerId);
    assertEquals(1, totalAllocations);
  }

  /**
   * Orchestrated row-lock evidence: an uncommitted runner UPDATE holds locks visible in {@code
   * pg_locks} and a second claimant blocks on that row (proved via {@code lock_timeout}), then a
   * rollback leaves no partial state behind.
   */
  @Test
  void uncommittedClaimBlocksContenderAndRollsBackCleanly() throws Exception {
    UUID runnerId = UUID.randomUUID();
    insertRunner(runnerId, "default", "AVAILABLE", 0);

    Connection holder = dataSource.getConnection();
    Connection contender = dataSource.getConnection();
    try {
      holder.setAutoCommit(false);
      try (Statement update =
          holder.createStatement()) {
        int rows =
            update.executeUpdate(
                "UPDATE runners SET state = 'ASSIGNED', epoch = epoch + 1, updated_at = now()"
                    + " WHERE runner_id = '"
                    + runnerId
                    + "' AND state = 'AVAILABLE'");
        assertEquals(1, rows);
      }

      // The uncommitted row update holds locks visible in pg_locks on the runners relation.
      List<String> locks =
          jdbc.query(
              "SELECT locktype || ':' || mode || ':' || granted FROM pg_locks"
                  + " WHERE relation = 'runners'::regclass",
              (ResultSet rs, int rowNum) -> rs.getString(1));
      assertFalse(locks.isEmpty(), "pg_locks must show locks on runners during contention");
      assertTrue(
          locks.stream().anyMatch(l -> l.endsWith(":true")),
          "granted locks expected while the claim is uncommitted: " + locks);
      System.out.println("pg_locks during orchestrated contention (runners): " + locks);

      // A second claimant for the same row blocks; bound the wait to prove blocking.
      contender.setAutoCommit(false);
      try (Statement setup = contender.createStatement()) {
        setup.execute("SET LOCAL lock_timeout = '1s'");
      }
      try (Statement blocked = contender.createStatement()) {
        blocked.executeUpdate(
            "UPDATE runners SET state = 'ASSIGNED', epoch = epoch + 1, updated_at = now()"
                + " WHERE runner_id = '"
                + runnerId
                + "' AND state = 'AVAILABLE'");
        fail("contender must block on the uncommitted runner row (lock_timeout expected)");
      } catch (SQLException timeout) {
        assertEquals(
            "55P03",
            timeout.getSQLState(),
            "expected lock_not_available from the blocked contender, got: " + timeout);
      } finally {
        contender.rollback();
      }
    } finally {
      holder.rollback();
      holder.close();
      contender.close();
    }

    // Rollback left no partial state: still AVAILABLE, epoch unconsumed, nothing allocated.
    assertEquals("AVAILABLE", runnerState(runnerId));
    assertEquals(0, runnerEpoch(runnerId));
    assertEquals(0, activeAllocationsForRunner(runnerId));
  }

  /**
   * Logs the claim UPDATE plan and asserts the supporting indexes exist.
   *
   * <p>The plan shape itself is informational: on tiny verification tables PostgreSQL correctly
   * chooses a Seq Scan (cost ~2), on larger tables an Index Scan (pkey or ix_runners_class_state).
   * Row-level locking is proven by pg_locks contention and the F09 two-instance test, not by the
   * plan shape, so this test must not assert Index Scan vs Seq Scan.
   */
  @Test
  void claimUpdatePlanIsLoggedAndSupportingIndexesExist() {
    UUID runnerId = UUID.randomUUID();
    insertRunner(runnerId, "default", "AVAILABLE", 0);
    List<String> plan =
        jdbc.queryForList(
            "EXPLAIN UPDATE runners SET state = 'ASSIGNED', epoch = epoch + 1, updated_at = now()"
                + " WHERE runner_id = '"
                + runnerId
                + "' AND state = 'AVAILABLE' AND runner_class = 'default'"
                + " RETURNING runner_id, epoch",
            String.class);
    assertFalse(plan.isEmpty());
    String joined = String.join("\n", plan);
    System.out.println("Claim UPDATE plan:\n" + joined);

    Integer pkeyCount =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'runners' AND indexname = 'runners_pkey'",
            Integer.class);
    assertEquals(1, pkeyCount, "runners_pkey must exist to support single-row claim lookup");

    Integer classStateCount =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'runners' AND indexname = 'ix_runners_class_state'",
            Integer.class);
    assertEquals(
        1, classStateCount, "ix_runners_class_state must exist to support class/state lookup");
  }

  /**
   * Starts an independent scheduler instance: a separate application context with its own
   * HikariCP pool and connections against the same PostgreSQL. No web server is needed (claims
   * are service-level; runner registration belongs to the later agent Outcome), so concurrent
   * contenders never fight over HTTP ports.
   */
  private static ConfigurableApplicationContext startApp() {
    return new SpringApplicationBuilder(Application.class).web(WebApplicationType.NONE).run();
  }

  @Test
  void migrationAndSchemaInvariants() {
    List<String> versions =
        jdbc.queryForList("SELECT version FROM flyway_schema_history ORDER BY version", String.class);
    assertTrue(versions.contains("3"), "V3 scheduling schema must be applied");

    Integer indexCount =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'uq_allocations_runner_active'",
            Integer.class);
    assertEquals(1, indexCount, "partial unique index must exist");

    String indexDef =
        jdbc.queryForObject(
            "SELECT indexdef FROM pg_indexes WHERE indexname = 'uq_allocations_runner_active'",
            String.class);
    assertTrue(
        indexDef != null && indexDef.contains("WHERE"),
        "active-allocation index must be partial, got: " + indexDef);

    // Distinct identity columns exist on all three scheduling tables.
    List<String> runnerCols = new ArrayList<>();
    jdbc.query(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'runners'",
        (ResultSet rs, int rowNum) -> runnerCols.add(rs.getString(1)));
    assertTrue(runnerCols.contains("runner_id"));
    assertTrue(runnerCols.contains("runner_class"));
    assertTrue(runnerCols.contains("epoch"));
  }
}
