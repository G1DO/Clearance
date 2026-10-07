package agent

import (
	"fmt"
	"strconv"
	"strings"
)

// Documented PITR-timeline markers for issue #40: logic-only precursor, NOT
// physical PITR (issue #40 remains open).
//
// The rewind drill records a WAL restore point (pg_create_restore_point) plus
// LSN/timeline/WAL-file markers and checks them as hygiene only: pg_current_wal_lsn
// markers must advance monotonically, the timeline must show no unexpected branch, and
// both the restore point and backup completion must follow the backup start. Strict
// inside-window containment (restore <= backupDone) is intentionally NOT required:
// a pg_current_wal_lsn() immediately after pg_create_restore_point() can lag behind the
// returned restore LSN by ~104 bytes, while pg_current_wal_lsn markers among themselves
// stay monotonic and restore LSNs always advance past the pre-point value. The logical
// restore (DELETE + INSERT SELECT) does NOT consume the LSN, performs no WAL replay,
// and branches no timeline; pg_walfile_name is recorded only, not validated for selection.
// A real cluster-level PITR recovery branches the timeline (new timeline ID +
// *.history file) with restore/replay to a target LSN/time/name, which this
// guard would reject. Do NOT reuse this as AC1 timeline-history validation;
// production requires base-backup + WAL replay with history-file validation.
// PostgreSQL LSN format is high/low hex (e.g. 0/16B1970); comparison uses the
// combined 64-bit value.

func parsePitrLsn(lsn string) (uint64, error) {
	trimmed := strings.TrimSpace(lsn)
	if trimmed == "" {
		return 0, fmt.Errorf("LSN must not be blank")
	}
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, fmt.Errorf("not a PostgreSQL LSN (expected high/low hex): %s", lsn)
	}
	high, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("not a PostgreSQL LSN (expected high/low hex): %s: %w", lsn, err)
	}
	low, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("not a PostgreSQL LSN (expected high/low hex): %s: %w", lsn, err)
	}
	return (high << 32) | low, nil
}

func comparePitrLsn(a, b string) (int, error) {
	av, err := parsePitrLsn(a)
	if err != nil {
		return 0, err
	}
	bv, err := parsePitrLsn(b)
	if err != nil {
		return 0, err
	}
	switch {
	case av < bv:
		return -1, nil
	case av > bv:
		return 1, nil
	default:
		return 0, nil
	}
}

// assertPitrOrdered requires each LSN <= its successor.
func assertPitrOrdered(lsns []string) error {
	for i := 1; i < len(lsns); i++ {
		cmp, err := comparePitrLsn(lsns[i-1], lsns[i])
		if err != nil {
			return err
		}
		if cmp > 0 {
			return fmt.Errorf("WAL LSN regression: %s precedes %s at positions %d->%d (sequence %s)",
				lsns[i-1], lsns[i], i-1, i, strings.Join(lsns, " -> "))
		}
	}
	return nil
}

// assertPitrStrictlyOrdered requires before < after (writes occurred between markers).
func assertPitrStrictlyOrdered(before, after, context string) error {
	cmp, err := comparePitrLsn(before, after)
	if err != nil {
		return err
	}
	if cmp >= 0 {
		return fmt.Errorf("WAL LSN did not advance for %s: before=%s after=%s", context, before, after)
	}
	return nil
}

// assertPitrNoTimelineBranch requires identical timeline IDs (no unexpected branch
// during the logic-only rehearsal). This is a no-branch guard, NOT AC1
// timeline-history validation: real PITR branches the timeline and this check
// would fail it.
func assertPitrNoTimelineBranch(timelines []string) error {
	if len(timelines) == 0 {
		return fmt.Errorf("at least one timeline required")
	}
	first := strings.TrimSpace(timelines[0])
	if first == "" {
		return fmt.Errorf("timeline must not be blank")
	}
	for i := 1; i < len(timelines); i++ {
		current := strings.TrimSpace(timelines[i])
		if current == "" {
			return fmt.Errorf("timeline must not be blank at marker %d", i)
		}
		if current != first {
			return fmt.Errorf("WAL timeline branch during drill window: expected %s but marker %d reports %s (sequence %s)",
				first, i, current, strings.Join(timelines, ","))
		}
	}
	return nil
}

// assertPitrRestorePointReachable requires both the restore point and backup completion
// to follow the backup start. Hygiene-only check; the logical restore does NOT consume
// this LSN (no WAL replay / restore-target selection). Strict restore <= backupDone is
// intentionally NOT required (see package docs for WAL lag evidence).
func assertPitrRestorePointReachable(backupStart, restorePoint, backupDone string) error {
	if cmp, err := comparePitrLsn(backupStart, restorePoint); err != nil {
		return err
	} else if cmp > 0 {
		return fmt.Errorf("restore point %s precedes backup start %s", restorePoint, backupStart)
	}
	if cmp, err := comparePitrLsn(backupStart, backupDone); err != nil {
		return err
	} else if cmp > 0 {
		return fmt.Errorf("backup completion %s precedes backup start %s", backupDone, backupStart)
	}
	return nil
}
