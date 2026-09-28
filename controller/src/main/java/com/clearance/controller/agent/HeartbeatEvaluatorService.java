package com.clearance.controller.agent;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;

/**
 * Background heartbeat-timeout evaluator with PostgreSQL as the sole ownership authority.
 *
 * <p>Detects lost contact with runners holding unresolved work and durably quarantines them.
 * Evaluates active allocations across all lifecycle phases (assigned through cleanup),
 * including allocations that have not supplied their first heartbeat.
 *
 * <p>Transactions always acquire locks in the repository-wide lock order: runner first,
 * then allocation. Authority and contact are re-verified under row locks before committing.
 * Unresolved executions are preserved: timeout alone neither produces a workload result
 * nor creates a retry. Quarantine is durable in PostgreSQL and survives controller restart.
 */
@Service
public class HeartbeatEvaluatorService {

  private static final Logger log = LoggerFactory.getLogger(HeartbeatEvaluatorService.class);

  static final String FIND_CANDIDATES_SQL = """
      SELECT a.allocation_id
      FROM allocations a
      JOIN runners r ON r.runner_id = a.runner_id
      WHERE a.state = 'ACTIVE'
        AND r.state NOT IN ('AVAILABLE', 'QUARANTINED')
        AND (a.last_contact_at + interval '1 millisecond' * a.heartbeat_timeout_ms) <= now()
      ORDER BY a.last_contact_at ASC, a.allocation_id ASC
      LIMIT ?
      """;

  static final String FIND_RUNNER_FOR_ALLOCATION_SQL = """
      SELECT runner_id FROM allocations WHERE allocation_id = ?
      """;

  static final String LOCK_RUNNER_SQL = """
      SELECT runner_id, epoch, agent_incarnation, state FROM runners WHERE runner_id = ? FOR UPDATE
      """;

  static final String LOCK_ALLOCATION_SQL = """
      SELECT allocation_id, runner_id, job_id, attempt_id, runner_epoch, agent_incarnation,
             max_seq, report_status, state, last_contact_at, heartbeat_timeout_ms
      FROM allocations WHERE allocation_id = ? FOR UPDATE
      """;

  static final String CHECK_EXPIRED_SQL = """
      SELECT (last_contact_at + interval '1 millisecond' * heartbeat_timeout_ms) <= now()
      FROM allocations WHERE allocation_id = ?
      """;

  static final String QUARANTINE_RUNNER_SQL = """
      UPDATE runners
      SET state = 'QUARANTINED', quarantine_reason = ?, updated_at = now()
      WHERE runner_id = ? AND state NOT IN ('AVAILABLE', 'QUARANTINED') AND epoch = ?
      """;

  private record RunnerRow(UUID runnerId, long epoch, Long agentIncarnation, String state) {}

  private record AllocationRow(
      UUID allocationId,
      UUID runnerId,
      UUID jobId,
      UUID attemptId,
      long runnerEpoch,
      Long agentIncarnation,
      long maxSeq,
      String reportStatus,
      String state,
      Instant lastContactAt,
      long heartbeatTimeoutMs) {}

  private final JdbcTemplate jdbc;
  private final ObjectMapper mapper;
  private final TransactionTemplate tx;
  private final int batchSize;
  private final boolean enabled;

  public HeartbeatEvaluatorService(
      JdbcTemplate jdbc,
      ObjectMapper mapper,
      PlatformTransactionManager txManager,
      @Value("${clearance.heartbeat-evaluator-batch-size:50}") int batchSize,
      @Value("${clearance.heartbeat-evaluator-enabled:true}") boolean enabled) {
    this.jdbc = jdbc;
    this.mapper = mapper;
    this.tx = new TransactionTemplate(txManager);
    this.tx.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
    this.batchSize = Math.max(1, Math.min(batchSize, 1000));
    this.enabled = enabled;
  }

  @Scheduled(
      fixedDelayString = "${clearance.heartbeat-evaluator-interval-ms:1000}",
      initialDelayString = "${clearance.heartbeat-evaluator-interval-ms:1000}")
  public void scheduledEvaluation() {
    if (!enabled) return;
    try {
      evaluateOnce();
    } catch (Exception e) {
      log.error("Heartbeat evaluator pass encountered error", e);
    }
  }

  /**
   * Executes a bounded evaluation pass over expired candidate allocations.
   *
   * @return the number of runners quarantined during this pass
   */
  public int evaluateOnce() {
    List<UUID> candidateIds = jdbc.queryForList(FIND_CANDIDATES_SQL, UUID.class, batchSize);
    int quarantined = 0;
    for (UUID allocId : candidateIds) {
      try {
        if (evaluateAllocation(allocId)) {
          quarantined++;
        }
      } catch (Exception e) {
        log.warn("Failed to evaluate allocation {} for heartbeat timeout", allocId, e);
      }
    }
    return quarantined;
  }

  /**
   * Evaluates a single allocation for heartbeat timeout under authoritative locks.
   * Acquires row lock on runner first, then allocation.
   *
   * @return true if the runner was transitioned to QUARANTINED, false otherwise
   */
  public boolean evaluateAllocation(UUID allocationId) {
    Boolean result = tx.execute(status -> doEvaluateAllocation(allocationId));
    return Boolean.TRUE.equals(result);
  }

  private boolean doEvaluateAllocation(UUID allocationId) {
    List<UUID> runnerIds = jdbc.queryForList(FIND_RUNNER_FOR_ALLOCATION_SQL, UUID.class, allocationId);
    if (runnerIds.isEmpty()) return false;
    UUID runnerId = runnerIds.getFirst();

    // 1. Lock runner first (Clearance global lock ordering: runner, then allocation, then job)
    List<RunnerRow> runners = jdbc.query(LOCK_RUNNER_SQL, (rs, n) -> new RunnerRow(
        (UUID) rs.getObject("runner_id"),
        rs.getLong("epoch"),
        rs.getObject("agent_incarnation", Long.class),
        rs.getString("state")), runnerId);
    if (runners.isEmpty()) return false;
    RunnerRow runner = runners.getFirst();

    if ("QUARANTINED".equals(runner.state()) || "AVAILABLE".equals(runner.state())) {
      return false;
    }

    // 2. Lock allocation
    List<AllocationRow> allocations = jdbc.query(LOCK_ALLOCATION_SQL, allocationRowMapper(), allocationId);
    if (allocations.isEmpty()) return false;
    AllocationRow alloc = allocations.getFirst();

    // 3. Recheck authority and current ownership under row locks
    if (!"ACTIVE".equals(alloc.state())) return false;
    if (!runnerId.equals(alloc.runnerId())) return false;
    if (alloc.runnerEpoch() != runner.epoch()) return false;
    if (runner.agentIncarnation() != null && alloc.agentIncarnation() != null
        && !Objects.equals(runner.agentIncarnation(), alloc.agentIncarnation())) {
      return false;
    }

    // 4. Recheck contact time against database clock under the locks
    Boolean expired = jdbc.queryForObject(CHECK_EXPIRED_SQL, Boolean.class, allocationId);
    if (!Boolean.TRUE.equals(expired)) {
      return false;
    }

    // 5. Build inspectable quarantine reason with ownership and contact evidence
    Map<String, Object> evidence = new LinkedHashMap<>();
    evidence.put("allocation_id", alloc.allocationId().toString());
    evidence.put("runner_epoch", alloc.runnerEpoch());
    if (alloc.agentIncarnation() != null) {
      evidence.put("agent_incarnation", alloc.agentIncarnation());
    }
    evidence.put("last_seq", alloc.maxSeq());
    evidence.put("last_contact_at", alloc.lastContactAt() != null ? alloc.lastContactAt().toString() : "");
    evidence.put("timeout_ms", alloc.heartbeatTimeoutMs());
    if (alloc.reportStatus() != null) {
      evidence.put("report_status", alloc.reportStatus());
    }
    String encoded;
    try {
      encoded = mapper.writeValueAsString(evidence);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to encode heartbeat timeout evidence", e);
    }
    String reason = "heartbeat timeout: " + encoded;

    // 6. Durably record QUARANTINED on the runner.
    // Unfinished execution remains unresolved: timeout alone neither produces a workload
    // TIMED_OUT/FAILED result nor creates a retry.
    int updated = jdbc.update(QUARANTINE_RUNNER_SQL, reason, runnerId, runner.epoch());
    if (updated > 0) {
      log.info("Runner {} quarantined due to heartbeat timeout on allocation {}", runnerId, allocationId);
      return true;
    }
    return false;
  }

  private RowMapper<AllocationRow> allocationRowMapper() {
    return (ResultSet rs, int rowNum) -> mapAllocationRow(rs);
  }

  private AllocationRow mapAllocationRow(ResultSet rs) throws SQLException {
    var ts = rs.getTimestamp("last_contact_at");
    Instant lastContact = ts != null ? ts.toInstant() : null;
    return new AllocationRow(
        (UUID) rs.getObject("allocation_id"),
        (UUID) rs.getObject("runner_id"),
        (UUID) rs.getObject("job_id"),
        (UUID) rs.getObject("attempt_id"),
        rs.getLong("runner_epoch"),
        rs.getObject("agent_incarnation", Long.class),
        rs.getLong("max_seq"),
        rs.getString("report_status"),
        rs.getString("state"),
        lastContact,
        rs.getLong("heartbeat_timeout_ms"));
  }
}
