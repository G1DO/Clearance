package com.clearance.controller.agent;

import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Recovery generation authority for post-restore safety (issues #32 and #39).
 *
 * <p>PostgreSQL remains the sole durable ownership authority; this service persists the single
 * current recovery generation as fleet authority in {@code recovery_authority}. Generation values
 * keep the UUID representation but are time-ordered UUIDv7 issued by {@link
 * RecoveryGenerationSource} from state outside the rewound database (external wall-clock
 * timestamp plus a durable append-only log file on persistent storage outside the rewound
 * PostgreSQL state, default {@code /var/lib/clearance/recovery-generations.log}). The log
 * survives a restore that rewinds the authority table, and issuance is fenced against it: an
 * auto-issued value is bumped past the logged maximum when the clock has not advanced, and an
 * operator-supplied UUIDv7 value that duplicates history or is not monotonically newer is
 * rejected before any database write. A recovery boot with authority present but an empty or
 * missing log (evidence of durability loss such as a cleared {@code /tmp}, replaced host, or
 * restarted container) is refused unless the operator supplies an externally-attested UUIDv7
 * generation. Freshness therefore does not rely on randomness stored only inside the rewound
 * database.
 *
 * <p>Recovery boot quarantines the whole fleet regardless of restored row state, including idle
 * runners with no active allocation. Claims and reports are gated on the current generation in
 * {@link SchedulerService} and {@link AgentService}; only per-runner verified reconciliation
 * (issue #33) advances a runner to the current generation and returns it to schedulable state.
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
  private final RecoveryGenerationSource generations;

  @Autowired
  public RecoveryService(JdbcTemplate jdbc, RecoveryGenerationSource generations) {
    this.jdbc = jdbc;
    this.generations = generations;
  }

  /** Direct construction with the default external source (non-Spring usage). */
  public RecoveryService(JdbcTemplate jdbc) {
    this(jdbc, new RecoveryGenerationSource(
        Path.of("/var/lib/clearance/recovery-generations.log"),
        Clock.systemUTC()));
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
   * Enters recovery mode: issues one fresh generation from the external monotonic source,
   * persists it as the fleet authority, and quarantines every runner with an inspectable
   * recovery reason.
   *
   * @return the newly issued current generation
   */
  @Transactional(isolation = Isolation.READ_COMMITTED)
  public UUID enterRecoveryMode() {
    return enterRecoveryMode(null);
  }

  /**
   * Enters recovery mode with an operator-supplied externally-attested UUIDv7 generation when
   * non-null, or an auto-issued UUIDv7 when null. A supplied value that is not UUIDv7, duplicates
   * logged history, or is not monotonically newer is rejected before any database write. A
   * supplied value already equal to the current authority is idempotent: it converges instead of
   * failing as a duplicate, so retries after a transient failure and second instances supplied
   * with one shared value reach the same authority.
   *
   * @return the newly issued current generation
   */
  @Transactional(isolation = Isolation.READ_COMMITTED)
  public UUID enterRecoveryMode(UUID explicitGeneration) {
    if (explicitGeneration != null) {
      return enterRecoveryModeWithExplicit(explicitGeneration);
    }
    Optional<UUID> current = currentGeneration();
    // The empty-log fence and the log append share one file lock inside the source, so a
    // concurrent issuance or log deletion cannot slip between the check and the write. The
    // authority read itself cannot join that lock (separate PostgreSQL resource); a rewind
    // racing this read fails closed (refuses) rather than issuing a repeat.
    UUID generation = generations.issueAutoFenced(current.isPresent());
    log.info("Entering recovery mode: issued recovery generation {} from external source {}",
        generation, generations.logFile());
    return persistAndQuarantine(generation);
  }

  private UUID enterRecoveryModeWithExplicit(UUID explicitGeneration) {
    Optional<UUID> current = currentGeneration();
    boolean idempotent = current.isPresent() && current.get().equals(explicitGeneration);
    if (!idempotent) {
      generations.checkExplicitFresh(explicitGeneration);
    } else {
      log.info("Recovery boot converging on current generation {} (idempotent re-issue)",
          explicitGeneration);
    }
    log.info("Entering recovery mode: issued recovery generation {} from external source {}",
        explicitGeneration, generations.logFile());
    UUID persisted = persistAndQuarantine(explicitGeneration);
    generations.recordIssued(persisted);
    return persisted;
  }

  private UUID persistAndQuarantine(UUID generation) {
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
