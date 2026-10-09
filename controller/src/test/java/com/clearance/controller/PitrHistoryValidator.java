package com.clearance.controller;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Real timeline-history validation for physical PITR (issue #40, F14).
 *
 * <p>Unlike {@link PitrTimelineValidator} (hygiene-only guards for the rows-only
 * logic-only precursor that explicitly rejects timeline branches), this validator
 * consumes WAL archive history: it parses {@code *.history} files produced by
 * timeline branching during restore/replay, asserts the expected parent-to-child
 * branch, and asserts restore-point reachability through the WAL stream
 * ({@code backupStart <= restorePoint <= replayEnd}).
 *
 * <p>History file format (one record per line, blank lines and {@code #}
 * comments ignored):
 * <pre>{@code <parentTimeline> <branchLsn high/low hex> <reason...>}</pre>
 * e.g. {@code 1 0/14000060 no recovery target specified}. The file name must be
 * {@code <8-hex-timeline>.history} (e.g. {@code 00000002.history}) and match the
 * child timeline observed via {@code pg_control_checkpoint()} after replay.
 */
final class PitrHistoryValidator {

  /** One parsed history record: parent timeline branched at a WAL LSN. */
  record HistoryBranch(int parentTimeline, String branchLsn, String reason) {}

  private PitrHistoryValidator() {}

  /** Parses a {@code *.history} file into branch records (ignores blanks/comments). */
  static List<HistoryBranch> parseHistoryFile(Path historyFile) {
    List<String> lines;
    try {
      lines = Files.readAllLines(historyFile, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException("cannot read timeline history file " + historyFile, e);
    }
    List<HistoryBranch> branches = new ArrayList<>();
    for (String raw : lines) {
      String line = raw.trim();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      String[] parts = line.split("\\s+", 3);
      if (parts.length < 2) {
        throw new IllegalArgumentException(
            "not a timeline history record (expected '<parentTimeline> <branchLsn> [reason]'): " + raw);
      }
      int parent;
      try {
        parent = Integer.parseInt(parts[0]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("bad parent timeline in history record: " + raw, e);
      }
      // Validates LSN shape via the shared parser (throws on malformed).
      PitrTimelineValidator.parseLsn(parts[1]);
      String reason = parts.length > 2 ? parts[2] : "";
      branches.add(new HistoryBranch(parent, parts[1], reason));
    }
    if (branches.isEmpty()) {
      throw new IllegalArgumentException("timeline history file has no branch records: " + historyFile);
    }
    return List.copyOf(branches);
  }

  /**
   * Asserts the history file name matches the expected child timeline and its
   * last record branches from the expected parent timeline at an LSN inside
   * {@code [backupStartLsn, replayEndLsn]}.
   *
   * <p>Multi-hop histories (e.g. {@code 00000003.history} holding {@code 1 …}
   * then {@code 2 …} after a repeated physical rewind) carry the full chain;
   * only the last record is the new branch, so the expected parent is checked
   * against the last record, not the first.
   */
  static void assertTimelineBranched(Path historyFile, int expectedParentTimeline,
      int expectedChildTimeline, String backupStartLsn, String replayEndLsn) {
    String name = historyFile.getFileName().toString();
    String expectedName = String.format("%08X.history", expectedChildTimeline);
    if (!name.equalsIgnoreCase(expectedName)) {
      throw new AssertionError("timeline history file " + name
          + " does not match expected child timeline file " + expectedName);
    }
    List<HistoryBranch> branches = parseHistoryFile(historyFile);
    HistoryBranch last = branches.get(branches.size() - 1);
    if (last.parentTimeline() != expectedParentTimeline) {
      throw new AssertionError("timeline branch parent " + last.parentTimeline()
          + " != expected parent " + expectedParentTimeline + " in " + name);
    }
    if (PitrTimelineValidator.compareLsn(backupStartLsn, last.branchLsn()) > 0) {
      throw new AssertionError("branch LSN " + last.branchLsn()
          + " precedes backup start " + backupStartLsn + " in " + name);
    }
    if (PitrTimelineValidator.compareLsn(last.branchLsn(), replayEndLsn) > 0) {
      throw new AssertionError("branch LSN " + last.branchLsn()
          + " follows replay end " + replayEndLsn + " in " + name);
    }
  }

  /**
   * Asserts restore-point reachability through the WAL stream for a real
   * restore: {@code backupStart <= restorePoint <= replayEnd}. Strict unlike
   * {@link PitrTimelineValidator#assertRestorePointReachable} because a physical
   * restore consumes the target (no WAL-lag exemption).
   */
  static void assertRestorePointReachableThroughWal(String backupStartLsn,
      String restorePointLsn, String replayEndLsn) {
    if (PitrTimelineValidator.compareLsn(backupStartLsn, restorePointLsn) > 0) {
      throw new AssertionError("restore point " + restorePointLsn
          + " precedes backup start " + backupStartLsn + ": T0 marker inconsistent");
    }
    if (PitrTimelineValidator.compareLsn(restorePointLsn, replayEndLsn) > 0) {
      throw new AssertionError("replay end " + replayEndLsn
          + " precedes restore point " + restorePointLsn + ": restore target not reached");
    }
  }
}
