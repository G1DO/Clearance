package agent

import "testing"

func TestPitrLsnParsingAndOrdering(t *testing.T) {
	if v, err := parsePitrLsn("0/0"); err != nil || v != 0 {
		t.Fatalf("parse 0/0: %v %d", err, v)
	}
	if v, err := parsePitrLsn("0/16B1970"); err != nil || v != 0x16B1970 {
		t.Fatalf("parse 0/16B1970: %v %d", err, v)
	}
	if v, err := parsePitrLsn("1/12345678"); err != nil || v != (1<<32|0x12345678) {
		t.Fatalf("parse 1/12345678: %v %d", err, v)
	}
	if cmp, err := comparePitrLsn("0/16B1970", "0/16B1971"); err != nil || cmp >= 0 {
		t.Fatalf("ordering: %v %d", err, cmp)
	}
	if cmp, err := comparePitrLsn("0/FFFFFFFE", "1/0"); err != nil || cmp >= 0 {
		t.Fatalf("segment rollover: %v %d", err, cmp)
	}
	for _, bad := range []string{"", "  ", "16B1970", "0/XYZ", "0/"} {
		if _, err := parsePitrLsn(bad); err == nil {
			t.Fatalf("expected error for %q", bad)
		}
	}
}

func TestPitrOrderedAndStrict(t *testing.T) {
	if err := assertPitrOrdered([]string{"0/100", "0/100", "0/200", "1/0"}); err != nil {
		t.Fatal(err)
	}
	if err := assertPitrOrdered([]string{"0/200", "0/100"}); err == nil {
		t.Fatal("expected regression error")
	}
	if err := assertPitrStrictlyOrdered("0/100", "0/101", "writes"); err != nil {
		t.Fatal(err)
	}
	if err := assertPitrStrictlyOrdered("0/100", "0/100", "writes"); err == nil {
		t.Fatal("expected strict advance error")
	}
}

func TestPitrTimelineContinuity(t *testing.T) {
	if err := assertPitrNoTimelineBranch([]string{"1", "1", "1"}); err != nil {
		t.Fatal(err)
	}
	if err := assertPitrNoTimelineBranch([]string{"1", "2"}); err == nil {
		t.Fatal("expected branch error")
	}
	if err := assertPitrRestorePointReachable("0/100", "0/150", "0/200"); err != nil {
		t.Fatal(err)
	}
	// Strict restore <= backupDone is NOT required (WAL lag); restore after backupDone
	// is accepted as long as both follow the backup start.
	if err := assertPitrRestorePointReachable("0/100", "0/400", "0/300"); err != nil {
		t.Fatal(err)
	}
	if err := assertPitrRestorePointReachable("0/200", "0/100", "0/300"); err == nil {
		t.Fatal("expected reachability error")
	}
	if err := assertPitrRestorePointReachable("0/200", "0/300", "0/100"); err == nil {
		t.Fatal("expected backupDone-before-start error")
	}
}
