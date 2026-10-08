package com.clearance.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Database-free validation for issue #40 timeline helpers: LSN parsing/ordering and
 * no-branch-guard checks used by the logic-only precursor rewind drill (NOT AC1
 * timeline-history validation; closes #40 together with the physical drill).
 */
class PitrTimelineValidatorTest {

  @Test
  void parsesLsnHighLowHexAsCombinedValue() {
    assertEquals(0L, PitrTimelineValidator.parseLsn("0/0"));
    assertEquals(0x16B1970L, PitrTimelineValidator.parseLsn("0/16B1970"));
    assertEquals((1L << 32) | 0x12345678L, PitrTimelineValidator.parseLsn("1/12345678"));
    assertTrue(PitrTimelineValidator.compareLsn("0/16B1970", "0/16B1971") < 0);
    assertTrue(PitrTimelineValidator.compareLsn("0/FFFFFFFE", "1/0") < 0);
    assertEquals(0, PitrTimelineValidator.compareLsn("0/ABC", "0/ABC"));
  }

  @Test
  void rejectsBlankAndMalformedLsn() {
    assertThrows(IllegalArgumentException.class, () -> PitrTimelineValidator.parseLsn(null));
    assertThrows(IllegalArgumentException.class, () -> PitrTimelineValidator.parseLsn("  "));
    assertThrows(IllegalArgumentException.class, () -> PitrTimelineValidator.parseLsn("16B1970"));
    assertThrows(IllegalArgumentException.class, () -> PitrTimelineValidator.parseLsn("0/XYZ"));
    assertThrows(IllegalArgumentException.class, () -> PitrTimelineValidator.parseLsn("0/"));
  }

  @Test
  void orderedLsnSequencePassesAndRegressionFails() {
    PitrTimelineValidator.assertOrdered("0/100", "0/100", "0/200", "1/0");
    assertThrows(AssertionError.class, () -> PitrTimelineValidator.assertOrdered("0/200", "0/100"));
    assertThrows(AssertionError.class,
        () -> PitrTimelineValidator.assertOrdered("0/100", "0/200", "0/150"));
  }

  @Test
  void strictOrderingRequiresAdvance() {
    PitrTimelineValidator.assertStrictlyOrdered("0/100", "0/101", "writes");
    assertThrows(AssertionError.class,
        () -> PitrTimelineValidator.assertStrictlyOrdered("0/100", "0/100", "writes"));
    assertThrows(AssertionError.class,
        () -> PitrTimelineValidator.assertStrictlyOrdered("0/200", "0/100", "writes"));
  }

  @Test
  void timelineContinuityRequiresIdenticalIds() {
    PitrTimelineValidator.assertNoTimelineBranch("1", "1", "1");
    assertThrows(AssertionError.class,
        () -> PitrTimelineValidator.assertNoTimelineBranch("1", "2"));
    assertThrows(IllegalArgumentException.class,
        () -> PitrTimelineValidator.assertNoTimelineBranch("1", " "));
  }

  @Test
  void restorePointMustSitInsideBackupWindow() {
    PitrTimelineValidator.assertRestorePointReachable("0/100", "0/150", "0/200");
    PitrTimelineValidator.assertRestorePointReachable("0/100", "0/100", "0/100");
    // Strict inside-window (restore <= backupDone) is NOT required: pg_current_wal_lsn()
    // after a restore point can lag behind the restore LSN, so restore after backupDone
    // is accepted as long as both follow the backup start.
    PitrTimelineValidator.assertRestorePointReachable("0/100", "0/400", "0/300");
    assertThrows(AssertionError.class,
        () -> PitrTimelineValidator.assertRestorePointReachable("0/200", "0/100", "0/300"));
    assertThrows(AssertionError.class,
        () -> PitrTimelineValidator.assertRestorePointReachable("0/200", "0/300", "0/100"));
  }
}
