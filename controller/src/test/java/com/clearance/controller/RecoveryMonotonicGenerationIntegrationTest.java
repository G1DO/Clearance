package com.clearance.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.clearance.controller.agent.RecoveryGenerationSource;
import com.clearance.controller.agent.RecoveryService;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.ObjectMapper;

/**
 * PostgreSQL-backed verification for issue #39: the generation issued after a rewind of
 * {@code recovery_authority} to T0 is provably new because issuance uses an external monotonic
 * source (time-ordered UUIDv7 plus a durable log outside the rewound database), not randomness
 * stored only inside the rewound database.
 *
 * <p>Proves against real PostgreSQL that issuing G1, snapshotting T0, issuing G2, rewinding the
 * authority to T0, and booting recovery issues G3 distinct from both predecessors and strictly
 * newer per the external source, with the log file as durable evidence. A second rewind issues
 * a still-newer G4, and a stale operator-supplied value is fenced before any write.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "clearance.heartbeat-timeout-ms=300000",
        "clearance.heartbeat-evaluator-enabled=false",
        "clearance.reconciliation-enabled=false",
        "clearance.recovery-mode=false",
        // Pin the context-wide generation log to test scratch: the production default is
        // a persistent host path (/var/lib/clearance) that tests must not touch. Tests
        // below use isolated per-test logs via TempDir.
        "clearance.recovery-generation-log=${java.io.tmpdir}/clearance-recovery-generations-test.log"
    })
class RecoveryMonotonicGenerationIntegrationTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  @Autowired JdbcTemplate jdbc;

  @TempDir Path temp;

  @BeforeEach
  void clearAuthority() {
    jdbc.update("DELETE FROM recovery_authority");
    jdbc.execute("DROP TABLE IF EXISTS recovery_authority_backup_t0");
  }

  @AfterEach
  void cleanAuthority() {
    jdbc.update("DELETE FROM recovery_authority");
    jdbc.execute("DROP TABLE IF EXISTS recovery_authority_backup_t0");
  }

  @Test
  void rewindToT0CannotCauseRepeatAndRepeatedRewindStaysFresh() throws Exception {
    Path evidence = Path.of("target", "recovery-monotonic");
    Files.createDirectories(evidence);
    Path logFile = temp.resolve("recovery-generations.log");
    RecoveryGenerationSource source = new RecoveryGenerationSource(logFile, Clock.systemUTC());
    RecoveryService isolated = new RecoveryService(jdbc, source);

    UUID runner = newRunner("AVAILABLE");

    UUID g1 = isolated.enterRecoveryMode();
    assertEquals(7, g1.version(), "generations keep the UUID representation as time-ordered UUIDv7");
    assertEquals(List.of(g1), source.history());

    // T0 snapshot captures the authority at G1; the log file lives outside the database.
    jdbc.execute("CREATE TABLE recovery_authority_backup_t0 AS TABLE recovery_authority WITH DATA");
    assertTrue(Files.exists(logFile), "external log must exist as durable evidence");

    UUID g2 = isolated.enterRecoveryMode();
    assertNotEquals(g1, g2);
    assertTrue(RecoveryGenerationSource.timestampMillis(g2)
        > RecoveryGenerationSource.timestampMillis(g1), "generations must be monotonically fresh");
    assertEquals(List.of(g1, g2), source.history());

    // Rewind the database to T0: the authority forgets G2 but the external log does not.
    jdbc.update("DELETE FROM recovery_authority");
    jdbc.update("INSERT INTO recovery_authority SELECT * FROM recovery_authority_backup_t0");
    assertEquals(g1, isolated.currentGeneration().orElseThrow(),
        "rewind must restore the T0 authority");

    UUID g3 = isolated.enterRecoveryMode();
    assertNotEquals(g1, g3, "post-rewind generation must never repeat the T0 value");
    assertNotEquals(g2, g3, "post-rewind generation must never repeat the forgotten value");
    assertTrue(RecoveryGenerationSource.timestampMillis(g3)
        > RecoveryGenerationSource.timestampMillis(g2), "post-rewind generation must be strictly newer");
    assertEquals(g3, isolated.currentGeneration().orElseThrow());
    assertEquals(List.of(g1, g2, g3), source.history(),
        "external log survives the rewind and proves issuance order");
    assertEquals("QUARANTINED", runnerState(runner));
    String reason = jdbc.queryForObject(
        "SELECT quarantine_reason FROM runners WHERE runner_id = ?", String.class, runner);
    assertTrue(reason != null && reason.contains(g3.toString()));

    // Second rewind still issues a fresh generation.
    jdbc.update("DELETE FROM recovery_authority");
    jdbc.update("INSERT INTO recovery_authority SELECT * FROM recovery_authority_backup_t0");
    assertEquals(g1, isolated.currentGeneration().orElseThrow());
    UUID g4 = isolated.enterRecoveryMode();
    assertNotEquals(g1, g4);
    assertNotEquals(g2, g4);
    assertNotEquals(g3, g4, "repeated rewinds must keep issuing fresh generations");
    assertTrue(RecoveryGenerationSource.timestampMillis(g4)
        > RecoveryGenerationSource.timestampMillis(g3));
    assertEquals(List.of(g1, g2, g3, g4), source.history());

    // A stale operator-supplied value is fenced before any database write.
    UUID before = isolated.currentGeneration().orElseThrow();
    assertThrows(IllegalStateException.class, () -> isolated.enterRecoveryMode(g1));
    assertEquals(before, isolated.currentGeneration().orElseThrow(),
        "fenced explicit generation must not advance the authority");
    assertEquals(List.of(g1, g2, g3, g4), source.history(),
        "fenced explicit generation must not append to the log");

    writeArtifact(evidence, "generations.json", Map.of(
        "g1", g1.toString(),
        "g2", g2.toString(),
        "g3_post_rewind", g3.toString(),
        "g4_repeated_rewind", g4.toString()));
    writeArtifact(evidence, "source-state.json", Map.of(
        "log_file", logFile.toString(),
        "history", source.history().stream().map(UUID::toString).toList(),
        "g1_timestamp_ms", RecoveryGenerationSource.timestampMillis(g1),
        "g2_timestamp_ms", RecoveryGenerationSource.timestampMillis(g2),
        "g3_timestamp_ms", RecoveryGenerationSource.timestampMillis(g3),
        "g4_timestamp_ms", RecoveryGenerationSource.timestampMillis(g4)));
    Files.writeString(evidence.resolve("recovery-generations.log"),
        Files.readString(logFile) + "\n");
  }

  @Test
  void authorityPresentWithMissingLogRefusesAutoBootUnlessExplicitIsSupplied() throws Exception {
    Path logFile = temp.resolve("recovery-generations-durability.log");
    RecoveryGenerationSource source = new RecoveryGenerationSource(logFile, Clock.systemUTC());
    RecoveryService isolated = new RecoveryService(jdbc, source);

    UUID g1 = isolated.enterRecoveryMode();
    assertEquals(g1, isolated.currentGeneration().orElseThrow());

    // Simulate durability loss of the log (cleared /tmp, replaced host, restarted
    // container): the authority still holds G1 but issuance evidence is gone.
    Files.deleteIfExists(logFile);
    Files.deleteIfExists(Path.of(logFile + ".lock"));

    UUID before = isolated.currentGeneration().orElseThrow();
    assertThrows(IllegalStateException.class, isolated::enterRecoveryMode,
        "auto-issue with authority present but log missing must fail fast instead of "
            + "falling back to wall-clock randomness");
    assertEquals(before, isolated.currentGeneration().orElseThrow(),
        "fenced boot must not advance the authority");

    // An externally-attested UUIDv7 generation is the escape hatch: strictly newer than
    // the forgotten history is unknowable, so the operator attests freshness out of band.
    UUID attested = RecoveryGenerationSource.newUuidV7(System.currentTimeMillis() + 60_000);
    UUID persisted = isolated.enterRecoveryMode(attested);
    assertEquals(attested, persisted);
    assertEquals(attested, isolated.currentGeneration().orElseThrow());
    assertEquals(List.of(attested), source.history());
  }

  @Test
  void explicitReissueOfCurrentAuthorityConvergesInsteadOfFailingDuplicate() {
    Path logFile = temp.resolve("recovery-generations-idempotent.log");
    RecoveryGenerationSource source = new RecoveryGenerationSource(logFile, Clock.systemUTC());
    RecoveryService isolated = new RecoveryService(jdbc, source);

    // A retry after a transient database failure, or a second instance supplied with one
    // shared externally-attested value, must converge on the same authority.
    UUID attested = RecoveryGenerationSource.newUuidV7(System.currentTimeMillis() + 60_000);
    UUID first = isolated.enterRecoveryMode(attested);
    assertEquals(attested, first);

    RecoveryGenerationSource secondSource =
        new RecoveryGenerationSource(logFile, Clock.systemUTC());
    RecoveryService secondInstance = new RecoveryService(jdbc, secondSource);
    UUID second = secondInstance.enterRecoveryMode(attested);
    assertEquals(attested, second);
    assertEquals(attested, isolated.currentGeneration().orElseThrow());
    assertEquals(List.of(attested), secondSource.history(),
        "convergent re-issue must not duplicate the log");

    // A non-v7 operator value is rejected before any write, even with an empty log.
    Path freshLog = temp.resolve("recovery-generations-non-v7-boot.log");
    RecoveryService fresh = new RecoveryService(
        jdbc, new RecoveryGenerationSource(freshLog, Clock.systemUTC()));
    jdbc.update("DELETE FROM recovery_authority");
    UUID v4 = UUID.randomUUID();
    assertThrows(IllegalStateException.class, () -> fresh.enterRecoveryMode(v4));
    assertTrue(fresh.currentGeneration().isEmpty(),
        "rejected non-v7 generation must not advance the authority");
  }

  @Test
  void staleExplicitMatchingRewoundAuthorityIsRejectedInsteadOfConverging() {
    Path logFile = temp.resolve("recovery-generations-stale-resupply.log");
    RecoveryGenerationSource source = new RecoveryGenerationSource(logFile, Clock.systemUTC());
    RecoveryService isolated = new RecoveryService(jdbc, source);

    UUID g1 = isolated.enterRecoveryMode();
    jdbc.execute("CREATE TABLE recovery_authority_backup_t0 AS TABLE recovery_authority WITH DATA");
    UUID g2 = isolated.enterRecoveryMode();
    assertNotEquals(g1, g2);
    assertEquals(List.of(g1, g2), source.history());

    // Rewind restores the older authority while the external log retains newer history;
    // re-supplying the rewound value (e.g. leftover clearance.recovery-generation config)
    // must be fenced as stale instead of converging on a repeat.
    jdbc.update("DELETE FROM recovery_authority");
    jdbc.update("INSERT INTO recovery_authority SELECT * FROM recovery_authority_backup_t0");
    assertEquals(g1, isolated.currentGeneration().orElseThrow());

    assertThrows(IllegalStateException.class, () -> isolated.enterRecoveryMode(g1),
        "stale re-supply matching the rewound authority must be rejected");
    assertEquals(g1, isolated.currentGeneration().orElseThrow(),
        "fenced stale re-supply must not advance the authority");
    assertEquals(List.of(g1, g2), source.history(),
        "fenced stale re-supply must not append to the log");
  }

  @Test
  void explicitRetryAfterPersistWithoutRecordConvergesAndAppends() throws Exception {
    Path logFile = temp.resolve("recovery-generations-crash-retry.log");
    RecoveryGenerationSource source = new RecoveryGenerationSource(logFile, Clock.systemUTC());
    RecoveryService isolated = new RecoveryService(jdbc, source);

    UUID attested = RecoveryGenerationSource.newUuidV7(System.currentTimeMillis() + 60_000);
    assertEquals(attested, isolated.enterRecoveryMode(attested));
    assertEquals(List.of(attested), source.history());

    // Simulate a crash between authority persist and log record: the database holds the
    // generation but issuance evidence is missing. A retry with the same attested value
    // must still converge and repair the log.
    Files.writeString(logFile, "", StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);

    assertEquals(attested, isolated.enterRecoveryMode(attested));
    assertEquals(attested, isolated.currentGeneration().orElseThrow());
    assertEquals(List.of(attested), source.history(),
        "retry must append the missing issuance evidence exactly once");
  }

  private UUID newRunner(String state) {
    UUID runnerId = UUID.randomUUID();
    jdbc.update("INSERT INTO runners (runner_id, runner_class, state, epoch) VALUES (?, 'default', ?, 0)",
        runnerId, state);
    return runnerId;
  }

  private String runnerState(UUID runnerId) {
    return jdbc.queryForObject("SELECT state FROM runners WHERE runner_id = ?", String.class, runnerId);
  }

  private static void writeArtifact(Path dir, String name, Object value) throws Exception {
    Files.writeString(dir.resolve(name), JSON.writeValueAsString(value) + "\n");
  }
}
