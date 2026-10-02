package com.clearance.controller.agent;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Recovery generation authority for post-restore safety (issue #32).
 *
 * <p>PostgreSQL remains the sole durable ownership authority; this service persists the single
 * current recovery generation as fleet authority in {@code recovery_authority}. Generation values
 * are random UUIDv4 issued on recovery-mode boot, never derived from database state, so a restore
 * that rewinds the authority table cannot cause a repeat: the next recovery boot issues a fresh
 * value that has never been used before.
 *
 * <p>Recovery boot quarantines the whole fleet regardless of restored row state, including idle
 * runners with no active allocation. Claims and reports are gated on the current generation in
 * {@link SchedulerService} and {@link AgentService}; only per-runner reconciliation (a later
 * issue) can advance a runner to the current generation and return it to schedulable state.
 */
@Service
public class RecoveryService {

  private static final Logger log = LoggerFactory.getLogger(RecoveryService.class);

  static final String SELECT_CURRENT_SQL =
      "SELECT current_generation FROM recovery_authority WHERE singleton = true";

  static final String UPSERT_AUTHORITY_SQL =
      "INSERT INTO recovery_authority (singleton, current_generation, updated_at) "
          + "VALUES (true, ?, now()) "
          + "ON CONFLICT (singleton) DO UPDATE SET current_generation = EXCLUDED.current_generation, "
          + "updated_at = now() RETURNING current_generation";

  static final String QUARANTINE_FLEET_SQL =
      "UPDATE runners SET state = 'QUARANTINED', quarantine_reason = ?, updated_at = now()";

  private final JdbcTemplate jdbc;

  public RecoveryService(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  /** Returns the current recovery generation when a recovery boot has persisted one. */
  public Optional<UUID> currentGeneration() {
    List<UUID> rows = jdbc.queryForList(SELECT_CURRENT_SQL, UUID.class);
    if (rows.isEmpty() || rows.getFirst() == null) {
      return Optional.empty();
    }
    return Optional.of(rows.getFirst());
  }

  /**
   * Enters recovery mode: issues one fresh generation, persists it as the fleet authority, and
   * quarantines every runner with an inspectable recovery reason.
   *
   * @return the newly issued current generation
   */
  @Transactional(isolation = Isolation.READ_COMMITTED)
  public UUID enterRecoveryMode() {
    UUID generation = UUID.randomUUID();
    log.info("Entering recovery mode: issuing new recovery generation {}", generation);
    UUID persisted =
        jdbc.queryForObject(UPSERT_AUTHORITY_SQL, UUID.class, generation);
    if (persisted == null || !persisted.equals(generation)) {
      throw new IllegalStateException("recovery authority persistence did not return the issued generation");
    }
    log.info("Persisted current recovery generation {} as fleet authority", persisted);
    String reason =
        "recovery quarantine generation " + persisted
            + ": post-restore unsafe, physical reconciliation required";
    int quarantined = jdbc.update(QUARANTINE_FLEET_SQL, reason);
    log.info(
        "Recovery boot quarantined {} runner(s) under generation {}", quarantined, persisted);
    return persisted;
  }
}
