package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestContainmentRejectsOrdinaryDirectory(t *testing.T) {
	_, err := newContainment(containmentConfig{
		CgroupRoot: t.TempDir(), WorkspaceRoot: filepath.Join(t.TempDir(), "work"), StateDir: t.TempDir(),
		GracePeriod: time.Millisecond, KillTimeout: time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), "cgroup v2") {
		t.Fatalf("ordinary directory accepted as containment: %v", err)
	}
}

func TestContainmentWorkspaceSymlinksDoNotTouchStateOrOtherAllocation(t *testing.T) {
	root, state, other := t.TempDir(), t.TempDir(), t.TempDir()
	for _, path := range []string{state, other} {
		if err := os.WriteFile(filepath.Join(path, "sentinel"), []byte("keep"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	workspace := filepath.Join(root, "allocation")
	if err := os.Mkdir(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(state, filepath.Join(workspace, "state")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(other, filepath.Join(workspace, "other")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(workspace, "nested"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "nested", "data"), []byte("erase"), 0600); err != nil {
		t.Fatal(err)
	}
	rootInfo, _ := os.Lstat(root)
	info, _ := os.Lstat(workspace)
	w := &containedWorkload{owner: &containment{cfg: containmentConfig{WorkspaceRoot: root}, workspaceInfo: rootInfo}, workspacePath: workspace, workspaceInfo: info}
	if err := w.scrubWorkspace(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(workspace); !os.IsNotExist(err) {
		t.Fatalf("workspace not removed: %v", err)
	}
	for _, path := range []string{state, other} {
		data, err := os.ReadFile(filepath.Join(path, "sentinel"))
		if err != nil || string(data) != "keep" {
			t.Fatalf("cleanup changed external path %s: %q %v", path, data, err)
		}
	}
}

func TestContainmentRefusesReplacedWorkspace(t *testing.T) {
	root, other := t.TempDir(), t.TempDir()
	workspace := filepath.Join(root, "allocation")
	if err := os.Mkdir(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	rootInfo, _ := os.Lstat(root)
	info, _ := os.Lstat(workspace)
	if err := os.Remove(workspace); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(other, workspace); err != nil {
		t.Fatal(err)
	}
	w := &containedWorkload{owner: &containment{cfg: containmentConfig{WorkspaceRoot: root}, workspaceInfo: rootInfo}, workspacePath: workspace, workspaceInfo: info}
	if err := w.scrubWorkspace(); err == nil {
		t.Fatal("replaced workspace accepted")
	}
	if _, err := os.Lstat(workspace); err != nil {
		t.Fatalf("replacement was modified: %v", err)
	}
}

func TestContainmentCleanupDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, message, want string
	}{
		{"invalid_filename", "unlink workspace/bad-\xff: permission denied", "unlink workspace/bad-\ufffd: permission denied\nallocation cleanup could not establish all required evidence"},
		{"ascii_limit", strings.Repeat("x", 2048), strings.Repeat("x", 2048)},
		{"two_byte_boundary", strings.Repeat("x", 2047) + "é", strings.Repeat("x", 2047)},
		{"three_byte_boundary", strings.Repeat("x", 2047) + "界", strings.Repeat("x", 2047)},
		{"four_byte_boundary", strings.Repeat("x", 2047) + "😀", strings.Repeat("x", 2047)},
		{"complete_character", strings.Repeat("x", 2045) + "界", strings.Repeat("x", 2045) + "界"},
		{"replacement_boundary", strings.Repeat("x", 2047) + "\xff", strings.Repeat("x", 2047)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workspace := t.TempDir()
			info, err := os.Lstat(workspace)
			if err != nil {
				t.Fatal(err)
			}
			failure := errors.New(tc.message)
			w := &containedWorkload{
				cgroupPath: filepath.Join(t.TempDir(), "absent"), workspacePath: workspace, workspaceInfo: info,
				testScrub: func() error { return failure },
			}
			proof, err := w.Cleanup()
			if !errors.Is(err, failure) || !proof.ExecutionEmpty || !proof.DescendantsReaped || proof.WorkspaceClean {
				t.Fatalf("cleanup lost the failure or changed evidence: %+v %v", proof, err)
			}
			if proof.Error == nil {
				t.Fatal("cleanup diagnostic missing")
			}
			if !utf8.ValidString(*proof.Error) || *proof.Error != tc.want {
				t.Fatalf("unexpected cleanup diagnostic: %q", *proof.Error)
			}
		})
	}
}

func TestContainmentCleanupUnreadableInvalidUTF8Filename(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can read directories without read permission")
	}
	root := t.TempDir()
	workspace := filepath.Join(root, "allocation")
	unreadable := filepath.Join(workspace, "unreadable-\xff")
	if err := os.MkdirAll(unreadable, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(unreadable, "data"), []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(unreadable, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(unreadable, 0700); err != nil {
			t.Error(err)
		}
	})
	rootInfo, err := os.Lstat(root)
	if err != nil {
		t.Fatal(err)
	}
	workspaceInfo, err := os.Lstat(workspace)
	if err != nil {
		t.Fatal(err)
	}
	w := &containedWorkload{
		owner:      &containment{cfg: containmentConfig{WorkspaceRoot: root}, workspaceInfo: rootInfo},
		cgroupPath: filepath.Join(root, "absent"), workspacePath: workspace, workspaceInfo: workspaceInfo,
	}
	proof, err := w.Cleanup()
	if !errors.Is(err, os.ErrPermission) || proof.WorkspaceClean || proof.Error == nil {
		t.Fatalf("unreadable workspace did not retain negative evidence: %+v %v", proof, err)
	}
	if !utf8.ValidString(*proof.Error) || !strings.Contains(*proof.Error, "unreadable-\ufffd") {
		t.Fatalf("invalid filename was not normalized: %q", *proof.Error)
	}
}

func TestContainmentCanonicalRootsDetectStateAliases(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(state, alias); err != nil {
		t.Fatal(err)
	}
	workspace, err := canonicalCreationPath(filepath.Join(alias, "new", "workspace"))
	if err != nil {
		t.Fatal(err)
	}
	if !pathsOverlap(workspace, state) {
		t.Fatalf("state alias was not resolved: %s", workspace)
	}
	if pathsOverlap(state, state+"-workspaces") {
		t.Fatal("sibling roots incorrectly overlap")
	}
}

// This suite deliberately skips without an explicitly delegated target. A skip
// does not establish physical containment. See README for the privileged target
// command; ordinary test execution still covers fail-closed and scrub behavior.
func physicalContainment(t *testing.T) *containment {
	t.Helper()
	root := os.Getenv("AGENT_CGROUP_TEST_ROOT")
	if root == "" {
		t.Skip("set AGENT_CGROUP_TEST_ROOT to a writable delegated cgroup v2 root for physical verification")
	}
	cg := filepath.Join(root, fmt.Sprintf("test-%d-%d", os.Getpid(), time.Now().UnixNano()))
	if err := os.Mkdir(cg, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Remove(cg); err != nil {
			t.Error(err)
		}
	})
	c, err := newContainment(containmentConfig{CgroupRoot: cg, WorkspaceRoot: filepath.Join(t.TempDir(), "workspaces"), StateDir: t.TempDir(), GracePeriod: 100 * time.Millisecond, KillTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func physicalAssignment(script string) PollResponse {
	id, epoch := "11111111-1111-1111-1111-111111111111", int64(1)
	return PollResponse{Assigned: true, AllocationID: &id, RunnerEpoch: &epoch, Argv: []string{"/bin/sh", "-c", script}}
}

func cleanupPhysicalFixture(t *testing.T, w *containedWorkload) {
	t.Helper()
	// Fault cases deliberately retain a quarantined tree; test teardown removes
	// the injected fault and explicitly cleans it, without changing cached proof.
	w.testInspect, w.testKill, w.testScrub = nil, nil, nil
	if _, err := os.Lstat(w.cgroupPath); err == nil {
		if err := w.terminate(); err != nil {
			t.Error(err)
		}
		if err := w.reap(time.Now().Add(time.Second)); err != nil {
			t.Error(err)
		}
		if err := w.removeCgroup(); err != nil {
			t.Error(err)
		}
	}
	if _, err := os.Lstat(w.workspacePath); err == nil {
		if err := w.scrubWorkspace(); err != nil {
			t.Error(err)
		}
	}
}

const detachedPhysicalScript = `
mkdir -p nested
printf owned > nested/data
setsid /bin/sh -c '
  trap "" TERM
  echo "$$" > detached.pid
  /bin/sh -c '\''trap "" TERM; echo "$$" > grandchild.pid; while :; do sleep 0.1; done'\'' &
  wait
' &
while [ ! -s grandchild.pid ]; do sleep 0.01; done
exit 0
`

func TestPhysicalContainmentDetachedDescendantsAndWorkspace(t *testing.T) {
	c := physicalContainment(t)
	w, err := c.Prepare(physicalAssignment(detachedPhysicalScript))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupPhysicalFixture(t, w) })
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	if err := w.Wait(); err != nil {
		t.Fatal(err)
	}
	before, err := w.allocationPIDs()
	if err != nil || len(before) < 2 {
		t.Fatalf("detached descendants absent: pids=%v error=%v", before, err)
	}
	for _, pid := range before {
		owned, err := w.ownsPID(pid)
		if err != nil || !owned {
			t.Fatalf("descendant escaped allocation: pid=%d owned=%t err=%v", pid, owned, err)
		}
	}
	start := time.Now()
	proof, err := w.Cleanup()
	elapsed := time.Since(start)
	if err != nil || !proof.ExecutionEmpty || !proof.DescendantsReaped || !proof.WorkspaceClean || proof.Error != nil {
		t.Fatalf("physical cleanup: %+v %v", proof, err)
	}
	if elapsed < c.cfg.GracePeriod || elapsed > c.cfg.GracePeriod+2*c.cfg.KillTimeout+time.Second {
		t.Fatalf("cleanup did not use bounded TERM/SIGKILL escalation: %s", elapsed)
	}
	for _, pid := range before {
		if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); !os.IsNotExist(err) {
			t.Fatalf("descendant %d remains, including zombie: %v", pid, err)
		}
	}
	for _, path := range []string{w.cgroupPath, w.workspacePath} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("allocation path remains: %s: %v", path, err)
		}
	}
	t.Logf("physical evidence: cgroup=%s before_pids=%v cleanup=%+v escalation_elapsed=%s cgroup_removed=true workspace_removed=true", w.membership, before, proof, elapsed)
}

func TestPhysicalContainmentFailedStartNeverExecutesOutsideCgroup(t *testing.T) {
	c := physicalContainment(t)
	assignment := physicalAssignment("")
	assignment.Argv = []string{"/clearance-does-not-exist"}
	w, err := c.Prepare(assignment)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupPhysicalFixture(t, w) })
	if err := w.Start(); err == nil {
		t.Fatal("nonexistent executable started")
	}
	if err := w.Start(); err == nil {
		t.Fatal("failed launch retried")
	}
	proof, err := w.Cleanup()
	if err != nil || !proof.ExecutionEmpty || !proof.DescendantsReaped || !proof.WorkspaceClean {
		t.Fatalf("failed launch cleanup: %+v %v", proof, err)
	}
}

func TestPhysicalContainmentCleanupFaultsRetainNegativeEvidence(t *testing.T) {
	for _, fault := range []string{"inspection", "termination", "workspace"} {
		t.Run(fault, func(t *testing.T) {
			c := physicalContainment(t)
			w, err := c.Prepare(physicalAssignment(detachedPhysicalScript))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { cleanupPhysicalFixture(t, w) })
			if err := w.Start(); err != nil {
				t.Fatal(err)
			}
			if err := w.Wait(); err != nil {
				t.Fatal(err)
			}
			injected := func() error { return errors.New("injected " + fault + " failure") }
			switch fault {
			case "inspection":
				w.testInspect = injected
			case "termination":
				w.testKill = injected
			case "workspace":
				w.testScrub = injected
			}
			proof, err := w.Cleanup()
			if err == nil || proof.Error == nil || !strings.Contains(*proof.Error, fault) || proof.WorkspaceClean {
				t.Fatalf("fault yielded positive or undiagnosable proof: %+v %v", proof, err)
			}
			if fault != "workspace" && proof.ExecutionEmpty {
				t.Fatalf("live tree accepted: %+v", proof)
			}
			if _, err := os.Lstat(w.workspacePath); err != nil {
				t.Fatalf("failed cleanup removed workspace: %v", err)
			}
			replay, replayErr := w.Cleanup()
			if replayErr != err || replay != proof {
				t.Fatal("repeated cleanup altered quarantined evidence")
			}
			t.Logf("negative cleanup evidence: fault=%s proof=%+v reason=%s", fault, proof, *proof.Error)
		})
	}
}

func TestPhysicalContainmentDoesNotStealAnotherAllocationWait(t *testing.T) {
	c := physicalContainment(t)
	first, err := c.Prepare(physicalAssignment(detachedPhysicalScript))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupPhysicalFixture(t, first) })
	secondAssignment := physicalAssignment("sleep 0.25; exit 7")
	id := "22222222-2222-2222-2222-222222222222"
	secondAssignment.AllocationID = &id
	second, err := c.Prepare(secondAssignment)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupPhysicalFixture(t, second) })
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	if err := second.Start(); err != nil {
		t.Fatal(err)
	}
	if err := first.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if err := second.Wait(); err == nil || !strings.Contains(err.Error(), "exit status 7") {
		t.Fatalf("second allocation wait was lost: %v", err)
	}
	if _, err := second.Cleanup(); err != nil {
		t.Fatal(err)
	}
}
