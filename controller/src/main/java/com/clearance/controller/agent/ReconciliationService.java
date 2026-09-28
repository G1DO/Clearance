package com.clearance.controller.agent;

import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Background reconciliation trigger with PostgreSQL as the sole ownership authority.
 *
 * <p>Flags quarantined runners holding active work for fresh physical observation.
 * The flag is delivered via assigned polls as {@code reconcile_requested} so the
 * same agent (same incarnation) or a restarted agent can send a fresh {@code RECONCILE}
 * report without manual database edits or a restart merely to trigger discovery.
 * The service never inspects host state, never releases ownership, and never clears
 * quarantine: it only requests evidence. Classification, durable resolution, and
 * release remain in {@link AgentService}.
 *
 * <p>Transactions acquire locks in the repository-wide order: runner first, then
 * allocation. Each pass is bounded by batch size; per-allocation failures are
 * isolated. Evidence retention stays in {@code allocations.reconcile_*} and
 * {@code runners.quarantine_reason}. Controller restarts preserve the durable flag.
 */
@Service
public class ReconciliationService {

  private static final Logger log = LoggerFactory.getLogger(ReconciliationService.class);

  static final String FIND_CANDIDATES_SQL = """
      SELECT a.allocation_id
      FROM allocations a
      JOIN runners r ON r.runner_id = a.runner_id
      WHERE a.state = 'ACTIVE'
        AND r.state = 'QUARANTINED'
        AND COALESCE(a.reconcile_requested, false) = false
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
      SELECT allocation_id, runner_id, runner_epoch, state, reconcile_requested
      FROM allocations WHERE allocation_id = ? FOR UPDATE
      """;

  static final String REQUEST_RECONCILIATION_SQL = """
      UPDATE allocations SET reconcile_requested = true WHERE allocation_id = ?
        AND state = 'ACTIVE' AND COALESCE(reconcile_requested, false) = false
      """;

  private record RunnerRow(UUID runnerId, long epoch, Long agentIncarnation, String state) {}

  private record AllocationRow(UUID allocationId, UUID runnerId, long runnerEpoch,
      String state, boolean reconcileRequested) {}

  private final JdbcTemplate jdbc;
  private final TransactionTemplate tx;
  private final int batchSize;
  private final boolean enabled;

  public ReconciliationService(
      JdbcTemplate jdbc,
      PlatformTransactionManager txManager,
      @Value("${clearance.reconciliation-batch-size:50}") int batchSize,
      @Value("${clearance.reconciliation-enabled:true}") boolean enabled) {
    this.jdbc = jdbc;
    this.tx = new TransactionTemplate(txManager);
    this.tx.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
    this.batchSize = Math.max(1, Math.min(batchSize, 1000));
    this.enabled = enabled;
  }

  @org.springframework.scheduling.annotation.Scheduled(
      fixedDelayString = "${clearance.reconciliation-interval-ms:1000}",
      initialDelayString = "${clearance.reconciliation-interval-ms:1000}")
  public void scheduledEvaluation() {
    if (!enabled) return;
    try {
      evaluateOnce();
    } catch (Exception e) {
      log.error("Reconciliation pass encountered error", e);
    }
  }

  /**
   * Executes a bounded pass flagging quarantined runners for fresh observation.
   *
   * @return the number of allocations flagged during this pass
   */
  public int evaluateOnce() {
    List<UUID> candidateIds = jdbc.queryForList(FIND_CANDIDATES_SQL, UUID.class, batchSize);
    int flagged = 0;
    for (UUID allocId : candidateIds) {
      try {
        if (evaluateAllocation(allocId)) {
          flagged++;
        }
      } catch (Exception e) {
        log.warn("Failed to flag allocation {} for reconciliation", allocId, e);
      }
    }
    return flagged;
  }

  /**
   * Flags a single allocation for reconciliation under authoritative locks.
   * Acquires row lock on runner first, then allocation.
   *
   * @return true if the allocation was flagged, false otherwise
   */
  public boolean evaluateAllocation(UUID allocationId) {
    Boolean result = tx.execute(status -> doEvaluateAllocation(allocationId));
    return Boolean.TRUE.equals(result);
  }

  private boolean doEvaluateAllocation(UUID allocationId) {
    List<UUID> runnerIds = jdbc.queryForList(FIND_RUNNER_FOR_ALLOCATION_SQL, UUID.class, allocationId);
    if (runnerIds.isEmpty()) return false;
    UUID runnerId = runnerIds.getFirst();

    List<RunnerRow> runners = jdbc.query(LOCK_RUNNER_SQL, (rs, n) -> new RunnerRow(
        (UUID) rs.getObject("runner_id"),
        rs.getLong("epoch"),
        rs.getObject("agent_incarnation", Long.class),
        rs.getString("state")), runnerId);
    if (runners.isEmpty()) return false;
    RunnerRow runner = runners.getFirst();
    if (!"QUARANTINED".equals(runner.state())) return false;

    List<AllocationRow> allocations = jdbc.query(LOCK_ALLOCATION_SQL, (rs, n) ->
        new AllocationRow(
            (UUID) rs.getObject("allocation_id"),
            (UUID) rs.getObject("runner_id"),
            rs.getLong("runner_epoch"),
            rs.getString("state"),
            rs.getBoolean("reconcile_requested")), allocationId);
    if (allocations.isEmpty()) return false;
    AllocationRow alloc = allocations.getFirst();
    if (!"ACTIVE".equals(alloc.state())) return false;
    if (!runnerId.equals(alloc.runnerId())) return false;
    if (alloc.runnerEpoch() != runner.epoch()) return false;
    if (alloc.reconcileRequested()) return false;

    int updated = jdbc.update(REQUEST_RECONCILIATION_SQL, allocationId);
    if (updated > 0) {
      log.info("Runner {} flagged for reconciliation on allocation {}", runnerId, allocationId);
      return true;
    }
    return false;
  }
}
