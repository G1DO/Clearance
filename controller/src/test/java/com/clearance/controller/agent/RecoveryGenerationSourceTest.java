package com.clearance.controller.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Database-free verification for issue #39: the external monotonic source issues strictly
 * newer UUIDv7 generations, survives callers that forget history (simulating a rewound
 * authority table), and fences operator-supplied repeats.
 */
class RecoveryGenerationSourceTest {

  @TempDir Path temp;

  @Test
  void autoIssuanceIsUuidV7AndStrictlyIncreasesEvenWithinTheSameMillisecond() {
    Path log = temp.resolve("generations.log");
    Clock fixed = Clock.fixed(Instant.parse("2026-10-01T00:00:00Z"), ZoneOffset.UTC);
    RecoveryGenerationSource source = new RecoveryGenerationSource(log, fixed);

    UUID first = source.issueFreshGeneration();
    UUID second = source.issueFreshGeneration();
    UUID third = source.issueFreshGeneration();

    assertEquals(7, first.version());
    assertNotEquals(first, second);
    assertNotEquals(first, third);
    assertNotEquals(second, third);
    assertTrue(RecoveryGenerationSource.timestampMillis(second)
        > RecoveryGenerationSource.timestampMillis(first));
    assertTrue(RecoveryGenerationSource.timestampMillis(third)
        > RecoveryGenerationSource.timestampMillis(second));
    assertEquals(List.of(first, second, third), source.history());
  }

  @Test
  void regressedClockCannotCauseRepeatBecauseTimestampBumpsPastLoggedMaximum() {
    Path log = temp.resolve("generations.log");
    Clock ahead = Clock.fixed(Instant.parse("2026-10-01T00:00:10Z"), ZoneOffset.UTC);
    RecoveryGenerationSource first = new RecoveryGenerationSource(log, ahead);
    UUID before = first.issueFreshGeneration();

    // Simulate a controller restart with a regressed clock after the database forgot everything:
    // the log file survives, so issuance must still move forward.
    Clock regressed = Clock.fixed(Instant.parse("2026-10-01T00:00:00Z"), ZoneOffset.UTC);
    RecoveryGenerationSource rebooted = new RecoveryGenerationSource(log, regressed);
    UUID after = rebooted.issueFreshGeneration();

    assertNotEquals(before, after);
    assertTrue(RecoveryGenerationSource.timestampMillis(after)
        > RecoveryGenerationSource.timestampMillis(before));
    assertEquals(List.of(before, after), rebooted.history());
  }

  @Test
  void operatorSuppliedDuplicateOrRegressedValueIsFenced() {
    Path log = temp.resolve("generations.log");
    RecoveryGenerationSource source = new RecoveryGenerationSource(log, Clock.systemUTC());
    UUID issued = source.issueFreshGeneration();

    assertThrows(IllegalStateException.class, () -> source.issueFreshGeneration(issued));

    UUID regressed = RecoveryGenerationSource.newUuidV7(
        RecoveryGenerationSource.timestampMillis(issued));
    assertThrows(IllegalStateException.class, () -> source.issueFreshGeneration(regressed));

    // The fence wrote nothing: history still holds only the first issuance.
    assertEquals(List.of(issued), source.history());
  }

  @Test
  void historySurvivesAcrossInstancesViaTheLogFile() throws Exception {
    Path log = temp.resolve("nested").resolve("generations.log");
    RecoveryGenerationSource first = new RecoveryGenerationSource(log, Clock.systemUTC());
    UUID one = first.issueFreshGeneration();
    UUID two = first.issueFreshGeneration();

    RecoveryGenerationSource second = new RecoveryGenerationSource(log, Clock.systemUTC());
    assertEquals(List.of(one, two), second.history());
    UUID three = second.issueFreshGeneration();
    assertNotEquals(one, three);
    assertNotEquals(two, three);
    assertEquals(List.of(one, two, three),
        Files.readAllLines(log).stream().map(UUID::fromString).toList());
  }

  @Test
  void nonV7ExplicitGenerationIsRejectedBeforeAnyWrite() {
    Path log = temp.resolve("generations-non-v7.log");
    RecoveryGenerationSource source = new RecoveryGenerationSource(log, Clock.systemUTC());
    UUID issued = source.issueFreshGeneration();

    UUID v4 = UUID.randomUUID();
    assertEquals(4, v4.version());
    assertThrows(IllegalStateException.class, () -> source.issueFreshGeneration(v4));
    assertThrows(IllegalStateException.class, () -> source.checkExplicitFresh(v4));

    // The fence wrote nothing: history still holds only the first issuance.
    assertEquals(List.of(issued), source.history());
  }

  @Test
  void recordIssuedIsIdempotentAndSkipsDuplicateAppends() throws Exception {
    Path log = temp.resolve("generations-idempotent.log");
    RecoveryGenerationSource source = new RecoveryGenerationSource(log, Clock.systemUTC());
    UUID issued = source.issueFreshGeneration();

    // Retry with the same attested value (transient DB failure, or a second instance
    // converging on one supplied value) must not duplicate the log or fail.
    source.recordIssued(issued);
    source.recordIssued(issued);
    assertEquals(List.of(issued), source.history());
    assertEquals(1, Files.readAllLines(log).size());

    UUID fresh = RecoveryGenerationSource.newUuidV7(
        RecoveryGenerationSource.timestampMillis(issued) + 1);
    source.checkExplicitFresh(fresh);
    // Validation alone appends nothing; the explicit path records only after persistence.
    assertEquals(List.of(issued), source.history());
    source.recordIssued(fresh);
    assertEquals(List.of(issued, fresh), source.history());
  }

  @Test
  void autoFencedRefusesWhenAuthorityPresentButLogMissing() throws Exception {
    Path log = temp.resolve("generations-fenced.log");
    RecoveryGenerationSource source = new RecoveryGenerationSource(log, Clock.systemUTC());

    // No authority yet: auto-issue proceeds and creates the log.
    UUID first = source.issueAutoFenced(false);
    assertEquals(7, first.version());
    assertEquals(List.of(first), source.history());

    // Durability loss with authority still present: refuse instead of falling back
    // to wall-clock randomness that could repeat a forgotten value.
    Files.deleteIfExists(log);
    Files.deleteIfExists(Path.of(log + ".lock"));
    assertThrows(IllegalStateException.class, () -> source.issueAutoFenced(true));
    assertTrue(source.history().isEmpty());

    // Fresh authority (for example after the operator clears the rewound row) may
    // auto-issue again; the fence only blocks the authority-present case.
    UUID fresh = source.issueAutoFenced(false);
    assertEquals(List.of(fresh), source.history());
  }
}
