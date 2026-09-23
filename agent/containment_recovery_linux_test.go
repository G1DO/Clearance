package agent

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Ordinary directories deliberately substitute only for filesystem identity
// tests. Actual adoption, process discovery and termination use cgroup v2 below.
func recordedContainmentFixture(t *testing.T) (*containment, *containedWorkload) {
	t.Helper()
	cgroupRoot, workspaceRoot, state := t.TempDir(), t.TempDir(), t.TempDir()
	cgInfo, _ := os.Lstat(cgroupRoot)
	wsInfo, _ := os.Lstat(workspaceRoot)
	c := &containment{cfg: containmentConfig{CgroupRoot: cgroupRoot, WorkspaceRoot: workspaceRoot, StateDir: state}, cgroupRootInfo: cgInfo, workspaceInfo: wsInfo,
		membershipRoot: fmt.Sprintf("/clearance-test-absent-%d-%d", os.Getpid(), time.Now().UnixNano())}
	assignment := physicalAssignment("exit 0")
	name := *assignment.AllocationID + "-" + strconv.FormatInt(*assignment.RunnerEpoch, 10)
	w := &containedWorkload{owner: c, assignment: assignment, cgroupPath: filepath.Join(cgroupRoot, name), workspacePath: filepath.Join(workspaceRoot, name), membership: filepath.Join(c.membershipRoot, name)}
	for _, path := range []string{w.cgroupPath, w.workspacePath} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	w.cgroupInfo, _ = os.Lstat(w.cgroupPath)
	w.workspaceInfo, _ = os.Lstat(w.workspacePath)
	if err := os.WriteFile(filepath.Join(w.cgroupPath, "cgroup.procs"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(w.cgroupPath, "cgroup.events"), []byte("populated 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := w.recordPreparation(); err != nil {
		t.Fatal(err)
	}
	return c, w
}

func TestContainmentDiscoveryRejectsMissingAndContradictoryIdentity(t *testing.T) {
	for _, fault := range []string{"missing_record", "corrupt_record", "other_allocation", "other_epoch", "missing_cgroup", "missing_workspace", "replaced_cgroup", "replaced_workspace", "workspace_symlink", "changed_root"} {
		t.Run(fault, func(t *testing.T) {
			c, original := recordedContainmentFixture(t)
			switch fault {
			case "missing_record":
				if err := os.Remove(filepath.Join(c.cfg.StateDir, "containment.json")); err != nil {
					t.Fatal(err)
				}
			case "corrupt_record":
				if err := os.WriteFile(filepath.Join(c.cfg.StateDir, "containment.json"), []byte(`{"version":1}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "other_allocation", "other_epoch":
				record := *original.recoveryRecord
				if fault == "other_allocation" {
					record.AllocationID = "22222222-2222-2222-2222-222222222222"
				} else {
					record.RunnerEpoch++
				}
				if err := c.saveContainmentRecord(record); err != nil {
					t.Fatal(err)
				}
			case "missing_cgroup", "missing_workspace":
				path := original.cgroupPath
				if fault == "missing_workspace" {
					path = original.workspacePath
				}
				if err := os.RemoveAll(path); err != nil {
					t.Fatal(err)
				}
			case "replaced_cgroup", "replaced_workspace", "workspace_symlink", "changed_root":
				path := original.workspacePath
				if fault == "replaced_cgroup" {
					path = original.cgroupPath
				} else if fault == "changed_root" {
					path = c.cfg.WorkspaceRoot
				}
				// Keep the old inode allocated so inode reuse cannot obscure the
				// replacement that this test intentionally injects.
				if err := os.Rename(path, path+"-old"); err != nil {
					t.Fatal(err)
				}
				if fault == "workspace_symlink" {
					if err := os.Symlink(path+"-old", path); err != nil {
						t.Fatal(err)
					}
				} else if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			}
			adopted, proof, err := c.Discover(original.assignment)
			if err == nil || proof.Error == nil || proof.CleanupVerified {
				t.Fatalf("contradictory discovery accepted: %+v %v", proof, err)
			}
			if err := adopted.Start(); err == nil {
				t.Fatal("recovery handle launched command")
			}
			cleanup, err := adopted.Cleanup()
			if err == nil || cleanup.ExecutionEmpty || cleanup.DescendantsReaped || cleanup.WorkspaceClean || cleanup.Error == nil {
				t.Fatalf("contradictory identity yielded cleanup proof: %+v %v", cleanup, err)
			}
			// Whatever was present at discovery must still be present after the
			// rejected cleanup, including directories owned by somebody else.
			for path, present := range map[string]bool{original.cgroupPath: proof.CgroupPresent, original.workspacePath: proof.WorkspacePresent} {
				if _, err := os.Lstat(path); present && err != nil {
					t.Fatalf("rejected recovery modified %s: %v", path, err)
				}
			}
		})
	}
}

func TestContainmentDiscoveryAdoptsIdentityButCannotRelaunch(t *testing.T) {
	c, original := recordedContainmentFixture(t)
	adopted, proof, err := c.Discover(original.assignment)
	if err != nil || !proof.CgroupPresent || !proof.WorkspacePresent || proof.CleanupVerified || len(proof.PIDs) != 0 || proof.Error != nil {
		t.Fatalf("discovery: %+v %v", proof, err)
	}
	if err := adopted.Start(); err == nil {
		t.Fatal("adopted allocation replayed its command")
	}
}

func TestContainmentDiscoveryResumesVerifiedRemoval(t *testing.T) {
	for _, workspacePresent := range []bool{false, true} {
		t.Run(fmt.Sprint(workspacePresent), func(t *testing.T) {
			c, original := recordedContainmentFixture(t)
			if err := original.authorizeRemoval(); err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(original.cgroupPath); err != nil {
				t.Fatal(err)
			}
			if workspacePresent {
				if err := os.WriteFile(filepath.Join(original.workspacePath, "dirty"), []byte("erase"), 0600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.RemoveAll(original.workspacePath); err != nil {
				t.Fatal(err)
			}
			adopted, proof, err := c.Discover(original.assignment)
			if err != nil || proof.CgroupPresent || proof.WorkspacePresent != workspacePresent || !proof.CleanupVerified || len(proof.PIDs) != 0 {
				t.Fatalf("interrupted removal not discovered: %+v %v", proof, err)
			}
			cleanup, err := adopted.Cleanup()
			if err != nil || !cleanup.ExecutionEmpty || !cleanup.DescendantsReaped || !cleanup.WorkspaceClean {
				t.Fatalf("interrupted removal failed: %+v %v", cleanup, err)
			}
			// Another restart must inspect absence again, without recreating paths.
			again, next, err := c.Discover(original.assignment)
			if err != nil || next.CgroupPresent || next.WorkspacePresent || !next.CleanupVerified {
				t.Fatalf("completed removal not rediscovered: %+v %v", next, err)
			}
			if _, err := again.Cleanup(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestContainmentRecoveryCleanupRechecksAbsentPathRoots(t *testing.T) {
	for _, root := range []string{"cgroup", "workspace"} {
		t.Run(root, func(t *testing.T) {
			c, original := recordedContainmentFixture(t)
			if err := original.authorizeRemoval(); err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{original.cgroupPath, original.workspacePath} {
				if err := os.RemoveAll(path); err != nil {
					t.Fatal(err)
				}
			}
			adopted, discovery, err := c.Discover(original.assignment)
			if err != nil || !discovery.CleanupVerified {
				t.Fatalf("expected verified physical absence: %+v %v", discovery, err)
			}
			path := c.cfg.CgroupRoot
			if root == "workspace" {
				path = c.cfg.WorkspaceRoot
			}
			if err := os.Rename(path, path+"-old"); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(path, 0700); err != nil {
				t.Fatal(err)
			}
			proof, err := adopted.Cleanup()
			if err == nil || proof.ExecutionEmpty || proof.DescendantsReaped || proof.WorkspaceClean || proof.Error == nil {
				t.Fatalf("replacement root accepted after discovery: %+v %v", proof, err)
			}
		})
	}
}

func TestPhysicalContainmentRecoveryHelper(t *testing.T) {
	encoded := os.Getenv("CLEARANCE_CONTAINMENT_RECOVERY_HELPER")
	if encoded == "" {
		return
	}
	var cfg containmentConfig
	if err := json.Unmarshal([]byte(encoded), &cfg); err != nil {
		t.Fatal(err)
	}
	c, err := newContainment(cfg)
	if err != nil {
		t.Fatal(err)
	}
	w, err := c.Prepare(physicalAssignment(detachedPhysicalScript))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	if err := w.Wait(); err != nil {
		t.Fatal(err)
	}
	pids, err := w.allocationPIDs()
	if err != nil || len(pids) < 2 {
		t.Fatalf("survivors missing: %v %v", pids, err)
	}
	data, err := json.Marshal(pids)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfg.StateDir, "helper-ready.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	select {}
}

func TestPhysicalContainmentRecoveryAfterSIGKILL(t *testing.T) {
	c := physicalContainment(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	config, err := json.Marshal(c.cfg)
	if err != nil {
		t.Fatal(err)
	}
	process := exec.Command(binary, "-test.run=^TestPhysicalContainmentRecoveryHelper$", "-test.timeout=30s")
	process.Env = append(os.Environ(), "CLEARANCE_CONTAINMENT_RECOVERY_HELPER="+string(config))
	process.Stdout, process.Stderr = os.Stdout, os.Stderr
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = process.Process.Kill() })
	ready := filepath.Join(c.cfg.StateDir, "helper-ready.json")
	var before []int
	deadline := time.Now().Add(10 * time.Second)
	for {
		data, err := os.ReadFile(ready)
		if err == nil && json.Unmarshal(data, &before) == nil && len(before) >= 2 {
			break
		}
		if !time.Now().Before(deadline) {
			_ = process.Process.Kill()
			_ = process.Wait()
			t.Fatal("helper did not establish surviving descendants")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := process.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if err := process.Wait(); err == nil {
		t.Fatal("helper unexpectedly exited successfully")
	}
	for _, pid := range before {
		if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); err != nil {
			t.Fatalf("workload did not survive helper SIGKILL: pid=%d %v", pid, err)
		}
	}
	adopted, evidence, err := c.Discover(physicalAssignment(detachedPhysicalScript))
	if err != nil {
		t.Fatalf("discover surviving workload: %+v %v", evidence, err)
	}
	t.Cleanup(func() { cleanupPhysicalFixture(t, adopted) })
	if !evidence.CgroupPresent || !evidence.WorkspacePresent || len(evidence.PIDs) < 2 || evidence.CleanupVerified {
		t.Fatalf("incomplete surviving execution evidence: %+v", evidence)
	}
	if err := adopted.Start(); err == nil {
		t.Fatal("surviving command was relaunched")
	}
	proof, err := adopted.Cleanup()
	if err != nil || !proof.ExecutionEmpty || !proof.DescendantsReaped || !proof.WorkspaceClean {
		t.Fatalf("surviving workload cleanup: %+v %v", proof, err)
	}
	for _, pid := range before {
		if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); !os.IsNotExist(err) {
			t.Fatalf("recovered process or zombie remains: %d %v", pid, err)
		}
	}
	t.Logf("SIGKILL recovery evidence: helper_pid=%d surviving_pids=%v discovery=%+v cleanup=%+v", process.Process.Pid, before, evidence, proof)
}

func TestPhysicalContainmentRecoveryCheckpointPersistenceFailure(t *testing.T) {
	c := physicalContainment(t)
	w, err := c.Prepare(physicalAssignment("exit 0"))
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
	if err := os.Mkdir(filepath.Join(c.cfg.StateDir, "containment.tmp"), 0700); err != nil {
		t.Fatal(err)
	}
	proof, err := w.Cleanup()
	if err == nil || proof.WorkspaceClean || proof.Error == nil || !strings.Contains(*proof.Error, "containment identity") {
		t.Fatalf("failed durable checkpoint allowed removal: %+v %v", proof, err)
	}
	for _, path := range []string{w.cgroupPath, w.workspacePath} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("checkpoint failure removed allocation path: %s %v", path, err)
		}
	}
}

func TestPhysicalContainmentPreparationCheckpointFailurePreventsLaunch(t *testing.T) {
	c := physicalContainment(t)
	if err := os.Mkdir(filepath.Join(c.cfg.StateDir, "containment.tmp"), 0700); err != nil {
		t.Fatal(err)
	}
	w, err := c.Prepare(physicalAssignment("touch should-not-run"))
	if err == nil || !strings.Contains(err.Error(), "containment identity") {
		t.Fatalf("preparation accepted failed durable identity: %v", err)
	}
	t.Cleanup(func() { cleanupPhysicalFixture(t, w) })
	if err := w.Start(); err == nil {
		t.Fatal("command started without durable containment identity")
	}
	if _, err := os.Lstat(filepath.Join(w.workspacePath, "should-not-run")); !os.IsNotExist(err) {
		t.Fatalf("uncheckpointed command executed: %v", err)
	}
	proof, err := w.Cleanup()
	if err != nil || !proof.ExecutionEmpty || !proof.DescendantsReaped || !proof.WorkspaceClean {
		t.Fatalf("unlaunched allocation cleanup: %+v %v", proof, err)
	}
}
