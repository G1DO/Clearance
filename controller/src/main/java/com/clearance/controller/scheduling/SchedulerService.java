package com.clearance.controller.scheduling;

import com.clearance.controller.jobs.JobNotFoundException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Exclusively claims a compatible schedulable runner for queued work with PostgreSQL as the sole
 * authority for ownership (issue #5).
 *
 * <p>Transaction boundary: each {@link #claim} call runs in a single database transaction at
 * {@code READ_COMMITTED} isolation (declared explicitly). The correctness-sensitive path is
 * explicit SQL via {@code JdbcTemplate}, not opaque ORM:
 *
 * <pre>
 * -- 1. load the immutable job row for its runnerClass
 * SELECT runner_class FROM jobs WHERE job_id = ?;
 * -- 2. atomic compare-and-swap: only an AVAILABLE runner whose class exactly
 * --    equals the job's class is claimed; the row lock serializes contenders
 * UPDATE runners
 *    SET state = 'ASSIGNED', epoch = epoch + 1, updated_at = now()
 *  WHERE runner_id = ? AND state = 'AVAILABLE' AND runner_class = ?
 *  RETURNING runner_id, epoch;
 * -- 3. durable execution-attempt identity for this claim
 * INSERT INTO attempts (attempt_id, job_id) VALUES (?, ?);
 * -- 4. authoritative ownership binding (partial-unique backstop)
 * INSERT INTO allocations (allocation_id, attempt_id, job_id, runner_id, runner_epoch)
 * VALUES (?, ?, ?, ?, ?);
 * </pre>
 *
 * <p>Concurrency semantics (observed PostgreSQL behavior, READ COMMITTED): a concurrent claimant
 * for the same runner blocks on the row lock taken by step 2 until the winner commits or rolls
 * back, then re-evaluates the {@code WHERE} predicate against the newest committed row version.
 * After a committed win the runner is {@code ASSIGNED}, so the loser matches no row and observes
 * no allocation. After a rollback the runner is {@code AVAILABLE} again and the loser may win.
 * The partial unique index {@code uq_allocations_runner_active} independently rejects a second
 * {@code ACTIVE} allocation for one runner, so a scheduler-selection mistake surfaces as a
 * constraint violation (rolling back the whole claim) rather than a silent double allocation.
 *
 * <p>No process-local mutexes, Redis, Kafka, etcd, or other coordination are used. No threads or
 * pools are created here: callers and the configured HikariCP pool (finite, see {@code
 * application.yml}) bound concurrency. Compatibility is exact {@code runnerClass} equality only;
 * generalized labels, priorities, bin-packing, affinity, and autoscaling are out of scope.
 *
 * <p>The returned {@link Claim} is constructed inside the transaction but handed to the caller
 * only when the transaction commits (Spring commits on method return), so downstream delivery
 * code never observes a claim as authoritative before PostgreSQL has committed it.
 */
@Service
public class SchedulerService {

  // Inspectable critical-path SQL. No ORM hides this boundary.
  static final String SELECT_JOB_CLASS_SQL = "SELECT runner_class FROM jobs WHERE job_id = ?";

  static final String CLAIM_RUNNER_SQL =
      "UPDATE runners SET state = 'ASSIGNED', epoch = epoch + 1, updated_at = now() "
          + "WHERE runner_id = ? AND state = 'AVAILABLE' AND runner_class = ? "
          + "RETURNING runner_id, epoch";

  static final String INSERT_ATTEMPT_SQL = "INSERT INTO attempts (attempt_id, job_id) VALUES (?, ?)";

  static final String INSERT_ALLOCATION_SQL =
      "INSERT INTO allocations (allocation_id, attempt_id, job_id, runner_id, runner_epoch) "
          + "VALUES (?, ?, ?, ?, ?) "
          + "RETURNING allocation_id, attempt_id, job_id, runner_id, runner_epoch, created_at";

  private final JdbcTemplate jdbc;

  public SchedulerService(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  /**
   * Atomically claims {@code runnerId} for {@code jobId} when the runner is authoritatively {@code
   * AVAILABLE} and its {@code runnerClass} exactly equals the job's {@code runnerClass}.
   *
   * @return the committed claim, or empty when no allocation was obtained (runner missing,
   *     incompatible, or not {@code AVAILABLE}); absence of an active-allocation row alone never
   *     counts as schedulable
   * @throws JobNotFoundException when {@code jobId} is unknown
   */
  @Transactional(isolation = Isolation.READ_COMMITTED)
  public Optional<Claim> claim(UUID jobId, UUID runnerId) {
    List<String> classes = jdbc.queryForList(SELECT_JOB_CLASS_SQL, String.class, jobId);
    if (classes.isEmpty()) {
      throw new JobNotFoundException();
    }
    String runnerClass = classes.get(0);

    UUID attemptId = UUID.randomUUID();
    UUID allocationId = UUID.randomUUID();

    List<ClaimedRunner> claimed =
        jdbc.query(CLAIM_RUNNER_SQL, claimedRunnerMapper(), runnerId, runnerClass);
    if (claimed.isEmpty()) {
      // Runner missing, incompatible by runnerClass, or not authoritatively AVAILABLE.
      // Nothing was written; the transaction commits as a no-op.
      return Optional.empty();
    }
    long newEpoch = claimed.get(0).epoch();

    jdbc.update(INSERT_ATTEMPT_SQL, attemptId, jobId);

    // Partial unique index uq_allocations_runner_active is the database backstop: a second
    // ACTIVE allocation for this runner fails here and rolls back the entire claim,
    // including the runner update and the attempt row above.
    List<Claim> allocations =
        jdbc.query(
            INSERT_ALLOCATION_SQL,
            claimMapper(),
            allocationId,
            attemptId,
            jobId,
            runnerId,
            newEpoch);
    return Optional.of(allocations.get(0));
  }

  private record ClaimedRunner(UUID runnerId, long epoch) {}

  private RowMapper<ClaimedRunner> claimedRunnerMapper() {
    return (ResultSet rs, int rowNum) ->
        new ClaimedRunner((UUID) rs.getObject("runner_id"), rs.getLong("epoch"));
  }

  private RowMapper<Claim> claimMapper() {
    return (ResultSet rs, int rowNum) -> mapClaim(rs);
  }

  private Claim mapClaim(ResultSet rs) throws SQLException {
    return new Claim(
        (UUID) rs.getObject("allocation_id"),
        (UUID) rs.getObject("attempt_id"),
        (UUID) rs.getObject("job_id"),
        (UUID) rs.getObject("runner_id"),
        rs.getLong("runner_epoch"),
        rs.getObject("created_at", OffsetDateTime.class));
  }
}
