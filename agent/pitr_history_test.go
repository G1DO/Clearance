package agent

import (
	"os"
	"path/filepath"
	"testing"
)

func writePitrHistory(t *testing.T, name, content string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPitrHistoryParsingAndBranch(t *testing.T) {
	p := writePitrHistory(t, "00000002.history", "# comment\n\n1 0/14000060 no recovery target specified\n")
	branches, err := parsePitrHistoryFile(p)
	if err != nil || len(branches) != 1 || branches[0].parent != 1 || branches[0].lsn != "0/14000060" {
		t.Fatalf("parse history: %v %+v", err, branches)
	}
	if err := assertPitrTimelineBranched(p, 1, 2, "0/10000000", "0/15000000"); err != nil {
		t.Fatal(err)
	}
	if err := assertPitrTimelineBranched(p, 2, 2, "0/10000000", "0/15000000"); err == nil {
		t.Fatal("expected parent mismatch error")
	}
	wrong := writePitrHistory(t, "00000003.history", "1 0/14000060 no recovery target specified\n")
	if err := assertPitrTimelineBranched(wrong, 1, 2, "0/10000000", "0/15000000"); err == nil {
		t.Fatal("expected filename mismatch error")
	}
	empty := writePitrHistory(t, "00000002.history", "# only comment\n")
	if _, err := parsePitrHistoryFile(empty); err == nil {
		t.Fatal("expected empty history error")
	}
	bad := writePitrHistory(t, "00000002.history", "1 NOT_AN_LSN reason\n")
	if _, err := parsePitrHistoryFile(bad); err == nil {
		t.Fatal("expected bad LSN error")
	}
}

func TestPitrRestorePointReachableThroughWal(t *testing.T) {
	if err := assertPitrRestorePointReachableThroughWal("0/100", "0/150", "0/200"); err != nil {
		t.Fatal(err)
	}
	// Strict: replay must reach the target.
	if err := assertPitrRestorePointReachableThroughWal("0/100", "0/400", "0/300"); err == nil {
		t.Fatal("expected replay-before-target error")
	}
	if err := assertPitrRestorePointReachableThroughWal("0/200", "0/100", "0/300"); err == nil {
		t.Fatal("expected restore-before-start error")
	}
}
