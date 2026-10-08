package com.clearance.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Database-free validation for physical PITR history helpers (issue #40, F14).
 * Covers the real AC1 path: {@code *.history} parsing, expected timeline branch,
 * and restore-point reachability through the WAL stream.
 */
class PitrHistoryValidatorTest {

  private static Path writeHistory(String name, String content) throws Exception {
    Path dir = Files.createTempDirectory("pitr-history-test");
    Path file = dir.resolve(name);
    Files.writeString(file, content, StandardCharsets.UTF_8);
    return file;
  }

  @Test
  void parsesHistoryBranchRecord() throws Exception {
    Path file = writeHistory("00000002.history",
        "# timeline history\n\n1 0/14000060 no recovery target specified\n");
    List<PitrHistoryValidator.HistoryBranch> branches =
        PitrHistoryValidator.parseHistoryFile(file);
    assertEquals(1, branches.size());
    assertEquals(1, branches.get(0).parentTimeline());
    assertEquals("0/14000060", branches.get(0).branchLsn());
    assertTrue(branches.get(0).reason().contains("no recovery target"));
  }

  @Test
  void rejectsBlankAndMalformedHistory() throws Exception {
    Path empty = writeHistory("00000002.history", "# only a comment\n\n");
    assertThrows(IllegalArgumentException.class,
        () -> PitrHistoryValidator.parseHistoryFile(empty));
    Path bad = writeHistory("00000002.history", "not-a-record\n");
    assertThrows(IllegalArgumentException.class,
        () -> PitrHistoryValidator.parseHistoryFile(bad));
    Path badLsn = writeHistory("00000002.history", "1 NOT_AN_LSN reason\n");
    assertThrows(IllegalArgumentException.class,
        () -> PitrHistoryValidator.parseHistoryFile(badLsn));
  }

  @Test
  void timelineBranchRequiresExpectedParentChildAndWindow() throws Exception {
    Path file = writeHistory("00000002.history", "1 0/14000060 no recovery target specified\n");
    PitrHistoryValidator.assertTimelineBranched(file, 1, 2, "0/10000000", "0/15000000");
    assertThrows(AssertionError.class, () -> PitrHistoryValidator.assertTimelineBranched(
        file, 2, 2, "0/10000000", "0/15000000"));
    Path wrongName = writeHistory("00000003.history", "1 0/14000060 no recovery target specified\n");
    assertThrows(AssertionError.class, () -> PitrHistoryValidator.assertTimelineBranched(
        wrongName, 1, 2, "0/10000000", "0/15000000"));
    assertThrows(AssertionError.class, () -> PitrHistoryValidator.assertTimelineBranched(
        file, 1, 2, "0/15000000", "0/16000000"));
  }

  @Test
  void restorePointMustBeReachedThroughWal() {
    PitrHistoryValidator.assertRestorePointReachableThroughWal("0/100", "0/150", "0/200");
    PitrHistoryValidator.assertRestorePointReachableThroughWal("0/100", "0/100", "0/100");
    // Strict unlike the hygiene-only precursor: replay must reach the target.
    assertThrows(AssertionError.class, () -> PitrHistoryValidator.assertRestorePointReachableThroughWal(
        "0/100", "0/400", "0/300"));
    assertThrows(AssertionError.class, () -> PitrHistoryValidator.assertRestorePointReachableThroughWal(
        "0/200", "0/100", "0/300"));
  }
}
