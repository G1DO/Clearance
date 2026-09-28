package agent

import (
	"reflect"
	"testing"
)

func strPtr(s string) *string { return &s }

func TestBuildReconcileEvidenceAlreadyClean(t *testing.T) {
	discovery := DiscoveryEvidence{CgroupPresent: false, WorkspacePresent: false, PIDs: []int64{}}
	cleanup := &CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}
	got := BuildReconcileEvidence(discovery, cleanup)
	want := ReconcileEvidence{CgroupPresent: false, WorkspacePresent: false, PIDs: []int64{},
		ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("already-clean got %+v want %+v", got, want)
	}
	if !positiveCleanup(cleanup) {
		t.Fatal("test setup must use positive cleanup")
	}
}

func TestBuildReconcileEvidenceStillRunningMakesNoPositiveClaims(t *testing.T) {
	discovery := DiscoveryEvidence{CgroupPresent: true, WorkspacePresent: true, PIDs: []int64{101}}
	got := BuildReconcileEvidence(discovery, nil)
	if got.ExecutionEmpty || got.DescendantsReaped || got.WorkspaceClean {
		t.Fatalf("still-running must not claim cleanup: %+v", got)
	}
	if len(got.PIDs) != 1 || got.PIDs[0] != 101 {
		t.Fatalf("PIDs not preserved: %+v", got)
	}
	if got.Error != nil {
		t.Fatalf("unexpected error: %+v", got)
	}
}

func TestBuildReconcileEvidenceDiscoveryErrorPreventsPositiveClaims(t *testing.T) {
	msg := "discovery incomplete"
	discovery := DiscoveryEvidence{CgroupPresent: true, WorkspacePresent: true,
		PIDs: []int64{}, Error: &msg}
	cleanup := &CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}
	got := BuildReconcileEvidence(discovery, cleanup)
	if got.Error == nil || *got.Error != msg {
		t.Fatalf("discovery error must propagate: %+v", got)
	}
	if got.ExecutionEmpty || got.DescendantsReaped || got.WorkspaceClean {
		t.Fatalf("discovery error must prevent positive claims: %+v", got)
	}
}

func TestBuildReconcileEvidenceCleanupErrorPropagates(t *testing.T) {
	discovery := DiscoveryEvidence{CgroupPresent: true, WorkspacePresent: true, PIDs: []int64{}}
	msg := "scrub failed"
	cleanup := &CleanupEvidence{ExecutionEmpty: false, DescendantsReaped: false,
		WorkspaceClean: false, Error: &msg}
	got := BuildReconcileEvidence(discovery, cleanup)
	if got.Error == nil || *got.Error != msg {
		t.Fatalf("cleanup error must propagate: %+v", got)
	}
	if got.ExecutionEmpty || got.DescendantsReaped || got.WorkspaceClean {
		t.Fatalf("negative cleanup must not claim positive: %+v", got)
	}
}

func TestBuildReconcileEvidenceEmptyErrorStringIsFailure(t *testing.T) {
	empty := ""
	discovery := DiscoveryEvidence{CgroupPresent: true, WorkspacePresent: true,
		PIDs: []int64{}, Error: &empty}
	got := BuildReconcileEvidence(discovery, nil)
	if got.Error == nil {
		t.Fatal("empty error string must remain present as failure evidence")
	}
}

func TestBuildReconcileEvidenceDirtyWorkspacePreserved(t *testing.T) {
	discovery := DiscoveryEvidence{CgroupPresent: true, WorkspacePresent: true, PIDs: []int64{}}
	cleanup := &CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: false}
	got := BuildReconcileEvidence(discovery, cleanup)
	if got.WorkspaceClean {
		t.Fatalf("dirty workspace must prevent attest-only: %+v", got)
	}
	if !got.ExecutionEmpty || !got.DescendantsReaped {
		t.Fatalf("other flags must be preserved: %+v", got)
	}
}
