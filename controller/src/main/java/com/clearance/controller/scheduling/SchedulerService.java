package com.clearance.controller.scheduling;

import com.clearance.controller.jobs.JobNotFoundException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Exclusive runner claims with PostgreSQL as the sole ownership authority. Transactions lock
 * runner, then job, before checking cancellation/result and inserting an attempt/allocation.
 * This serializes against reports and cancellation without a job/runner lock-order cycle.
 * Only AVAILABLE is schedulable; absence of an allocation is not safe-reuse proof.
 *
 * <p>The partial unique index independently rejects double allocation of a runner and rolls back
 * the whole claim (including epoch and attempt). The job row lock also prevents concurrent claims
 * of the same job on different runners. No process-local coordination or new pools are used.
 * A claim is returned through the transaction proxy only after commit.
 */
@Service
public class SchedulerService {

  // Inspectable critical-path SQL. No ORM hides this boundary.
  static final String LOCK_RUNNER_SQL = "SELECT runner_id FROM runners WHERE runner_id = ? FOR UPDATE";
  static final String SELECT_JOB_SQL =
      "SELECT runner_class, result, cancel_requested FROM jobs WHERE job_id = ? FOR UPDATE";

  static final String CLAIM_RUNNER_SQL =
      "UPDATE runners SET state = 'ASSIGNED', epoch = epoch + 1, updated_at = now() "
          + "WHERE runner_id = ? AND state = 'AVAILABLE' AND runner_class = ? "
          + "RETURNING runner_id, epoch";

  static final String INSERT_ATTEMPT_SQL = "INSERT INTO attempts (attempt_id, job_id) VALUES (?, ?)";

  static final String INSERT_ALLOCATION_SQL =
      "INSERT INTO allocations (allocation_id, attempt_id, job_id, runner_id, runner_epoch, workload_timeout_ms) "
          + "VALUES (?, ?, ?, ?, ?, ?) "
          + "RETURNING allocation_id, attempt_id, job_id, runner_id, runner_epoch, created_at";

  private final JdbcTemplate jdbc;
  private final long workloadTimeoutMs;

  public SchedulerService(JdbcTemplate jdbc,
      @Value("${clearance.workload-timeout-ms:3600000}") long workloadTimeoutMs) {
    if (workloadTimeoutMs < 1) throw new IllegalArgumentException("workload timeout must be positive");
    this.jdbc = jdbc;
    this.workloadTimeoutMs = Math.min(workloadTimeoutMs, 86_400_000);
  }

  private record QueuedJob(String runnerClass, String result, boolean cancelRequested) {}

  /**
   * Atomically claims {@code runnerId} for {@code jobId} when the runner is authoritatively {@code
   * AVAILABLE} and its {@code runnerClass} exactly equals the job's {@code runnerClass}.
   *
   * @return the committed claim, or empty when no allocation was obtained (runner missing,
   *     incompatible, or not {@code AVAILABLE}; job terminal, cancelled, or already active);
   *     absence of an active-allocation row alone never
   *     counts as schedulable
   * @throws JobNotFoundException when {@code jobId} is unknown
   */
  @Transactional(isolation = Isolation.READ_COMMITTED)
  public Optional<Claim> claim(UUID jobId, UUID runnerId) {
    // Runner is always locked first, including when a claim will be refused.
    List<UUID> runners = jdbc.queryForList(LOCK_RUNNER_SQL, UUID.class, runnerId);
    List<QueuedJob> jobs = jdbc.query(SELECT_JOB_SQL,
        (rs, n) -> new QueuedJob(rs.getString("runner_class"), rs.getString("result"),
            rs.getBoolean("cancel_requested")), jobId);
    if (jobs.isEmpty()) throw new JobNotFoundException();
    QueuedJob job = jobs.getFirst();
    if (runners.isEmpty() || job.result() != null || job.cancelRequested()
        || Boolean.TRUE.equals(jdbc.queryForObject("""
            SELECT EXISTS (SELECT 1 FROM allocations WHERE job_id = ? AND state = 'ACTIVE')
            """, Boolean.class, jobId))) return Optional.empty();
    String runnerClass = job.runnerClass();

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
            newEpoch,
            workloadTimeoutMs);
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
