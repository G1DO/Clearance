package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Physical PITR timeline-history validation for issue #40, F14.
//
// Unlike pitr_timeline.go (hygiene-only guards for the rows-only logic-only
// precursor that reject timeline branches), these helpers consume WAL archive
// history: they parse *.history files produced by timeline branching during
// restore/replay, assert the expected parent-to-child branch, and assert
// restore-point reachability through the WAL stream
// (backupStart <= restorePoint <= replayEnd, strict because a physical restore
// consumes the target).
//
// History file format (blank lines and # comments ignored):
// "<parentTimeline> <branchLsn high/low hex> <reason...>"
// e.g. "1 0/14000060 no recovery target specified". The file name must be
// "<8-hex-timeline>.history" (e.g. "00000002.history") matching the child
// timeline observed via pg_control_checkpoint() after replay.

type pitrHistoryBranch struct {
	parent int
	lsn    string
	reason string
}

func parsePitrHistoryFile(path string) ([]pitrHistoryBranch, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read timeline history file %s: %w", path, err)
	}
	var branches []pitrHistoryBranch
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// Normalize whitespace first (tabs/spaces) via Fields.
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return nil, fmt.Errorf("not a timeline history record (expected '<parentTimeline> <branchLsn> [reason]'): %s", raw)
		}
		parent, err := strconv.Atoi(fields[0])
		if err != nil {
			return nil, fmt.Errorf("bad parent timeline in history record: %s: %w", raw, err)
		}
		if _, err := parsePitrLsn(fields[1]); err != nil {
			return nil, fmt.Errorf("bad branch LSN in history record %s: %w", raw, err)
		}
		reason := ""
		if len(fields) > 2 {
			reason = strings.Join(fields[2:], " ")
		}
		branches = append(branches, pitrHistoryBranch{parent: parent, lsn: fields[1], reason: reason})
	}
	if len(branches) == 0 {
		return nil, fmt.Errorf("timeline history file has no branch records: %s", path)
	}
	return branches, nil
}

func assertPitrTimelineBranched(historyFile string, expectedParent, expectedChild int, backupStart, replayEnd string) error {
	base := filepath.Base(historyFile)
	expectedName := fmt.Sprintf("%08X.history", expectedChild)
	if !strings.EqualFold(base, expectedName) {
		return fmt.Errorf("timeline history file %s does not match expected child timeline file %s", base, expectedName)
	}
	branches, err := parsePitrHistoryFile(historyFile)
	if err != nil {
		return err
	}
	first := branches[0]
	if first.parent != expectedParent {
		return fmt.Errorf("timeline branch parent %d != expected parent %d in %s", first.parent, expectedParent, base)
	}
	if cmp, err := comparePitrLsn(backupStart, first.lsn); err != nil {
		return err
	} else if cmp > 0 {
		return fmt.Errorf("branch LSN %s precedes backup start %s in %s", first.lsn, backupStart, base)
	}
	if cmp, err := comparePitrLsn(first.lsn, replayEnd); err != nil {
		return err
	} else if cmp > 0 {
		return fmt.Errorf("branch LSN %s follows replay end %s in %s", first.lsn, replayEnd, base)
	}
	return nil
}

// assertPitrRestorePointReachableThroughWal requires backupStart <= restorePoint <= replayEnd.
// Strict unlike assertPitrRestorePointReachable because a physical restore consumes the target.
func assertPitrRestorePointReachableThroughWal(backupStart, restorePoint, replayEnd string) error {
	if cmp, err := comparePitrLsn(backupStart, restorePoint); err != nil {
		return err
	} else if cmp > 0 {
		return fmt.Errorf("restore point %s precedes backup start %s", restorePoint, backupStart)
	}
	if cmp, err := comparePitrLsn(restorePoint, replayEnd); err != nil {
		return err
	} else if cmp > 0 {
		return fmt.Errorf("replay end %s precedes restore point %s: restore target not reached", replayEnd, restorePoint)
	}
	return nil
}
