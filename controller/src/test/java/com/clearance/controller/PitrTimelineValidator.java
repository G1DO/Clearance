package com.clearance.controller;

/**
 * Documented PITR-timeline markers for issue #40: logic-only precursor, NOT physical PITR
 * (issue #40 remains open).
 *
 * <p>The destructive rewind drill backs up rows, records a WAL restore point
 * ({@code pg_create_restore_point}) plus LSN/timeline/WAL-file markers, and rewinds rows with
 * a logic-only restore. These helpers check the recorded markers as hygiene only: {@code
 * pg_current_wal_lsn()} markers must advance monotonically, the timeline must show no
 * unexpected branch during the drill window, and both the restore point and backup completion
 * must follow the backup start (strict inside-window {@code restore <= backupDone} is NOT
 * required; see {@link #assertRestorePointReachable}). The logical restore does NOT
 * consume the LSN, performs no WAL replay, and branches no timeline ({@code pg_walfile_name}
 * is recorded only). A real cluster-level PITR recovery branches the timeline (new timeline
 * ID plus {@code *.history} file) with restore/replay to a target, which the no-branch guard
 * below would reject. Do NOT reuse this as AC1 timeline-history validation; production
 * requires base-backup plus WAL replay with history-file validation.
 *
 * <p>PostgreSQL LSN format is {@code high/low} hex (e.g. {@code 0/16B1970}); numeric comparison
 * uses the combined 64-bit value. Timeline IDs come from {@code pg_control_checkpoint()}.
 */
final class PitrTimelineValidator {

  private PitrTimelineValidator() {}

  /** Parses a PostgreSQL LSN ({@code high/low} hex) to its combined 64-bit value. */
  static long parseLsn(String lsn) {
    if (lsn == null || lsn.isBlank()) {
      throw new IllegalArgumentException("LSN must not be blank: " + lsn);
    }
    String[] parts = lsn.trim().split("/", -1);
    if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
      throw new IllegalArgumentException("not a PostgreSQL LSN (expected high/low hex): " + lsn);
    }
    try {
      long high = Long.parseUnsignedLong(parts[0], 16);
      long low = Long.parseUnsignedLong(parts[1], 16);
      if (high > 0xFFFFFFFFL || low > 0xFFFFFFFFL) {
        throw new IllegalArgumentException("LSN segment out of 32-bit range: " + lsn);
      }
      return (high << 32) | low;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("not a PostgreSQL LSN (expected high/low hex): " + lsn, e);
    }
  }

  /** Compares two LSNs numerically; negative when {@code a} precedes {@code b}. */
  static int compareLsn(String a, String b) {
    return Long.compareUnsigned(parseLsn(a), parseLsn(b));
  }

  /**
   * Asserts LSNs advance monotonically (each {@code <=} its successor). Fails with an
   * inspectable message naming the regressing pair.
   */
  static void assertOrdered(String... lsns) {
    for (int i = 1; i < lsns.length; i++) {
      if (compareLsn(lsns[i - 1], lsns[i]) > 0) {
        throw new AssertionError(
            "WAL LSN regression: " + lsns[i - 1] + " precedes " + lsns[i]
                + " at positions " + (i - 1) + "->" + i + " (full sequence: "
                + String.join(" -> ", lsns) + ")");
      }
    }
  }

  /**
   * Asserts LSNs advance strictly (each {@code <} its successor). Used where the drill performed
   * writes between markers (backup, T1 progress, restore) so equal LSNs would mean lost writes.
   */
  static void assertStrictlyOrdered(String before, String after, String context) {
    if (compareLsn(before, after) >= 0) {
      throw new AssertionError(
          "WAL LSN did not advance for " + context + ": before=" + before + " after=" + after);
    }
  }

  /**
   * Asserts all recorded timeline IDs are identical (no unexpected timeline branch during the
   * logic-only rehearsal window). This is a no-branch guard, NOT AC1 timeline-history
   * validation: a real cluster-level PITR recovery would branch the timeline and produce a
   * history file, which this check would reject. Any switch fails the drill as external
   * interference; do NOT reuse for production PITR validation.
   */
  static void assertNoTimelineBranch(String... timelines) {
    if (timelines.length == 0) {
      throw new IllegalArgumentException("at least one timeline required");
    }
    String first = timelines[0] == null ? "" : timelines[0].trim();
    if (first.isEmpty()) {
      throw new IllegalArgumentException("timeline must not be blank");
    }
    for (int i = 1; i < timelines.length; i++) {
      String current = timelines[i] == null ? "" : timelines[i].trim();
      if (current.isEmpty()) {
        throw new IllegalArgumentException("timeline must not be blank at marker " + i);
      }
      if (!first.equals(current)) {
        throw new AssertionError(
            "WAL timeline branch during drill window: expected " + first + " but marker " + i
                + " reports " + current + " (full sequence: " + String.join(",", timelines) + ")");
      }
    }
  }

  /**
   * Asserts the recorded restore point and backup completion both follow the backup start.
   * Hygiene-only check; the logic-only restore does NOT consume this LSN (no WAL replay /
   * restore-target selection).
   *
   * <p>Strict inside-window containment ({@code restorePoint <= backupDone}) is intentionally
   * NOT required: {@code pg_create_restore_point()} LSNs and a subsequent
   * {@code pg_current_wal_lsn()} are not strictly comparable for {@code <=} after the point.
   * Probing shows {@code pg_current_wal_lsn()} immediately after a restore point can lag
   * behind the returned restore LSN by ~104 bytes (e.g. restore {@code 0/6E2911E8} with
   * after {@code 0/6E291180}, or restore {@code 0/6E266318} with after {@code 0/6E2662B0}),
   * while {@code pg_current_wal_lsn()} markers among themselves stay monotonic and restore
   * LSNs always advance past the pre-point value. Requiring {@code restore <= backupDone}
   * is therefore flaky; only {@code backupStart <= restorePoint} and {@code backupStart <=
   * backupDone} are asserted.
   */
  static void assertRestorePointReachable(String backupStartLsn, String restorePointLsn,
      String backupDoneLsn) {
    if (compareLsn(backupStartLsn, restorePointLsn) > 0) {
      throw new AssertionError("restore point " + restorePointLsn
          + " precedes backup start " + backupStartLsn + ": T0 marker inconsistent");
    }
    if (compareLsn(backupStartLsn, backupDoneLsn) > 0) {
      throw new AssertionError("backup completion " + backupDoneLsn
          + " precedes backup start " + backupStartLsn + ": T0 marker inconsistent");
    }
  }
}
