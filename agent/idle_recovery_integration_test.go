//go:build integration

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Idle-at-backup recovery through the real Go daemon on Linux: a quarantined
// runner with no allocation advances only via fresh idle RECONCILE observations
// (allocation_id omitted) built from cgroup.procs plus /proc plus workspace
// checks. Clean hosts attest and become claimable with generation advancement;
// dirty or foreign execution stays quarantined and unclaimable; duplicates are
// rejected with zero mutation.

type idleRunnerFixture struct {
	RunnerID    string `json:"runner_id"`
	Token       string `json:"token"`
	EvidenceDir string `json:"evidence_dir"`
}

type idleRunnerRows struct {
	State                string  `json:"state"`
	Epoch                int64   `json:"epoch"`
	Incarnation          *int64  `json:"agent_incarnation"`
	QuarantineReason     *string `json:"quarantine_reason"`
	ReconciledGeneration *string `json:"reconciled_generation"`
	IdleSeq              *int64  `json:"idle_reconcile_seq"`
	Version              string  `json:"version"`
	Active               int     `json:"active"`
	raw                  string
}

func idleLifecycleFixture(idle idleRunnerFixture) lifecycleFixture {
	return lifecycleFixture{
		controllerFixture: controllerFixture{RunnerID: idle.RunnerID, Token: idle.Token},
		EvidenceDir:       idle.EvidenceDir,
	}
}

func idleRows(t *testing.T, h lifecycleHarness, runnerID string) idleRunnerRows {
	t.Helper()
	if !isValidUUID(runnerID) {
		t.Fatal("invalid idle runner identity")
	}
	query := fmt.Sprintf(`SELECT json_build_object(
 'state', r.state, 'epoch', r.epoch, 'agent_incarnation', r.agent_incarnation,
 'quarantine_reason', r.quarantine_reason, 'reconciled_generation', r.reconciled_generation,
 'idle_reconcile_seq', r.idle_reconcile_seq, 'version', r.xmin::text,
 'active', (SELECT count(*) FROM %[1]s.allocations WHERE runner_id=r.runner_id AND state='ACTIVE'))
 FROM %[1]s.runners r WHERE r.runner_id='%[2]s'`, h.Schema, runnerID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data, err := exec.CommandContext(ctx, "psql", "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-c", query).CombinedOutput()
	if err != nil {
		t.Fatalf("read idle runner rows: %v: %s", err, data)
	}
	var rows idleRunnerRows
	if err := json.Unmarshal(bytes.TrimSpace(data), &rows); err != nil {
		t.Fatalf("decode idle runner rows: %v: %s", data, err)
	}
	rows.raw = string(bytes.TrimSpace(data))
	return rows
}

func waitIdleRows(t *testing.T, h lifecycleHarness, runnerID string, predicate func(idleRunnerRows) bool) idleRunnerRows {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	for {
		rows := idleRows(t, h, runnerID)
		if predicate(rows) {
			return rows
		}
		if time.Now().After(deadline) {
			t.Fatalf("idle runner rows did not converge: %s", rows.raw)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func enterIdleRecovery(t *testing.T, h lifecycleHarness) string {
	t.Helper()
	data, status, err := lifecycleHTTP(h.ControlURL+"/enter-recovery", "X-Harness-Token", h.ControlToken,
		http.MethodPost, map[string]string{})
	if err != nil || status != 200 {
		t.Fatalf("enter recovery mode: status=%d body=%s error=%v", status, data, err)
	}
	var response struct {
		Generation string `json:"generation"`
	}
	if err := json.Unmarshal(data, &response); err != nil || !isValidUUID(response.Generation) {
		t.Fatalf("invalid recovery generation: %s %v", data, err)
	}
	return strings.ToLower(response.Generation)
}

func submitIdleJob(t *testing.T, h lifecycleHarness, name string) string {
	t.Helper()
	data, status, err := lifecycleHTTP(h.ControlURL+"/submit", "X-Harness-Token", h.ControlToken,
		http.MethodPost, map[string]any{"name": name, "argv": []string{"echo", "hi"}})
	if err != nil || status != 200 {
		t.Fatalf("submit job: status=%d body=%s error=%v", status, data, err)
	}
	var response struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(data, &response); err != nil || !isValidUUID(response.JobID) {
		t.Fatalf("invalid submitted job: %s %v", data, err)
	}
	return response.JobID
}

func claimIdleRunner(t *testing.T, h lifecycleHarness, runnerID, jobID string) lifecycleClaim {
	t.Helper()
	data, status, err := lifecycleHTTP(h.ControlURL+"/claim", "X-Harness-Token", h.ControlToken,
		http.MethodPost, map[string]string{"runner_id": runnerID, "job_id": jobID})
	if err != nil || status != 200 {
		t.Fatalf("claim idle runner: status=%d body=%s error=%v", status, data, err)
	}
	var claim lifecycleClaim
	if err := json.Unmarshal(data, &claim); err != nil {
		t.Fatal(err)
	}
	return claim
}

// idleDaemon builds a real daemon for an idle runner. An empty stateDir starts
// fresh; reusing a stateDir continues the durable per-runner idle sequence and
// reserves a new incarnation, exactly like a production restart.
func idleDaemon(t *testing.T, h lifecycleHarness, idle idleRunnerFixture, cgroupRoot, stateDir string) (*Daemon, string, string) {
	t.Helper()
	if stateDir == "" {
		stateDir = filepath.Join(t.TempDir(), "state")
	}
	workspace := stateDir + "-workspaces"
	d, err := NewDaemon(Config{ControllerURL: h.URL, MachineToken: idle.Token, StateDir: stateDir,
		CgroupRoot: cgroupRoot, WorkspaceRoot: workspace,
		PollTimeout: time.Second, HeartbeatInterval: 100 * time.Millisecond, RetryInterval: 50 * time.Millisecond,
		GracePeriod: 200 * time.Millisecond, KillTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return d, stateDir, workspace
}

func idleIncarnation(t *testing.T, rows idleRunnerRows) int64 {
	t.Helper()
	if rows.Incarnation == nil {
		t.Fatalf("idle runner has no bound incarnation: %s", rows.raw)
	}
	return *rows.Incarnation
}

// plantIdleCgroup creates an allocation-shaped hierarchy holding one live
// process. On a real cgroup v2 filesystem the kernel provides cgroup.procs and
// the write migrates the process; on a plain directory the file stands in for
// it. Either way the daemon's scan observes the same evidence.
func plantIdleCgroup(t *testing.T, root, name string, pid int) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cgroup.procs"), []byte(strconv.Itoa(pid)), 0600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func idleCleanEvidence() *ReconcileEvidence {
	return &ReconcileEvidence{PIDs: []int64{}, ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}
}

func TestControllerIdleRecoveryReconciliation(t *testing.T) {
	h := loadLifecycleHarness(t)
	for _, name := range []string{"idle-clean", "idle-dirty"} {
		idle, ok := h.IdleRunners[name]
		if !ok || !isValidUUID(idle.RunnerID) || idle.Token == "" || idle.EvidenceDir == "" {
			t.Fatalf("idle runner fixture %s missing", name)
		}
	}

	t.Run("clean", func(t *testing.T) {
		idle := h.IdleRunners["idle-clean"]
		f := idleLifecycleFixture(idle)
		generation := enterIdleRecovery(t, h)
		waitIdleRows(t, h, idle.RunnerID, func(rows idleRunnerRows) bool {
			return rows.State == "QUARANTINED"
		})
		if claim := claimIdleRunner(t, h, idle.RunnerID, submitIdleJob(t, h, "idle-clean-blocked")); claim.Assigned {
			t.Fatalf("quarantined idle runner accepted a claim: %+v", claim)
		}

		cgroup := filepath.Join(os.Getenv("CLEARANCE_CGROUP_ROOT"), fmt.Sprintf("idle-clean-%d", os.Getpid()))
		if err := os.MkdirAll(cgroup, 0700); err != nil {
			t.Fatal(err)
		}
		d, _, _ := idleDaemon(t, h, idle, cgroup, "")
		stop := startIntegrationDaemon(t, d)
		attested := waitIdleRows(t, h, idle.RunnerID, func(rows idleRunnerRows) bool {
			return rows.State == "AVAILABLE" && rows.ReconciledGeneration != nil &&
				*rows.ReconciledGeneration == generation && rows.IdleSeq != nil
		})
		if attested.Epoch != 0 {
			t.Fatalf("idle runner epoch changed without a claim: %s", attested.raw)
		}
		writeLifecycleEvidence(t, f, "idle-attested", json.RawMessage(attested.raw))

		// An idempotent re-attest with a fresh sequence performs zero writes:
		// the release was already committed and must not move again.
		stop()
		client := integrationClient(t, h.URL, idle.Token)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		ack, err := client.Report(ctx, ReportRequest{RunnerEpoch: 0,
			AgentIncarnation: idleIncarnation(t, attested), Seq: *attested.IdleSeq + 1,
			Status: StatusReconcile, Ts: time.Now().UTC(), Reconcile: idleCleanEvidence()})
		if err != nil || !ack.Accepted || ack.Reason != "reconcile_attested" {
			t.Fatalf("idempotent idle attest rejected: %+v %v", ack, err)
		}
		if after := idleRows(t, h, idle.RunnerID); after.raw != attested.raw {
			t.Fatalf("idempotent attest mutated rows: before=%s after=%s", attested.raw, after.raw)
		}

		// Advancement unlocked scheduling: the first claim moves epoch 0 -> 1.
		claim := claimIdleRunner(t, h, idle.RunnerID, submitIdleJob(t, h, "idle-clean-next"))
		if !claim.Assigned || claim.RunnerEpoch != 1 {
			t.Fatalf("advanced idle runner not claimable from epoch 0: %+v", claim)
		}
		writeLifecycleEvidence(t, f, "idle-claimed", map[string]any{"allocation_id": claim.AllocationID})
	})

	t.Run("dirty-and-orphaned", func(t *testing.T) {
		idle := h.IdleRunners["idle-dirty"]
		f := idleLifecycleFixture(idle)
		generation := enterIdleRecovery(t, h)
		waitIdleRows(t, h, idle.RunnerID, func(rows idleRunnerRows) bool {
			return rows.State == "QUARANTINED"
		})

		cgroup := filepath.Join(os.Getenv("CLEARANCE_CGROUP_ROOT"), fmt.Sprintf("idle-dirty-%d", os.Getpid()))
		if err := os.MkdirAll(cgroup, 0700); err != nil {
			t.Fatal(err)
		}
		d, stateDir, workspace := idleDaemon(t, h, idle, cgroup, "")
		if err := os.MkdirAll(workspace, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(workspace, "leftover"), []byte("dirty"), 0600); err != nil {
			t.Fatal(err)
		}
		stop := startIntegrationDaemon(t, d)
		dirty := waitIdleRows(t, h, idle.RunnerID, func(rows idleRunnerRows) bool {
			return rows.State == "QUARANTINED" && rows.IdleSeq != nil &&
				rows.QuarantineReason != nil && strings.Contains(*rows.QuarantineReason, "STALE_EXECUTION")
		})
		if dirty.ReconciledGeneration != nil {
			t.Fatalf("dirty idle observation advanced generation: %s", dirty.raw)
		}
		if claim := claimIdleRunner(t, h, idle.RunnerID, submitIdleJob(t, h, "idle-dirty-blocked")); claim.Assigned {
			t.Fatalf("dirty idle runner accepted a claim: %+v", claim)
		}
		writeLifecycleEvidence(t, f, "idle-dirty-quarantined", json.RawMessage(dirty.raw))

		// A duplicate of the accepted sequence is rejected with zero mutation,
		// even though the evidence is now clean: sequencing precedes content.
		stop()
		client := integrationClient(t, h.URL, idle.Token)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		dup, err := client.Report(ctx, ReportRequest{RunnerEpoch: 0,
			AgentIncarnation: idleIncarnation(t, dirty), Seq: *dirty.IdleSeq,
			Status: StatusReconcile, Ts: time.Now().UTC(), Reconcile: idleCleanEvidence()})
		assertIntegrationRejected(t, dup, err, "dropped_stale")
		if after := idleRows(t, h, idle.RunnerID); after.raw != dirty.raw {
			t.Fatalf("duplicate idle report mutated rows: before=%s after=%s", dirty.raw, after.raw)
		}

		// Foreign execution under the same root keeps quarantine: the scan
		// reports cgroup.procs plus /proc membership with a foreign identity.
		if err := os.Remove(filepath.Join(workspace, "leftover")); err != nil {
			t.Fatal(err)
		}
		sleeper := exec.Command("sleep", "60")
		if err := sleeper.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sleeper.Process.Kill(); _ = sleeper.Wait() }()
		foreign := "44444444-4444-4444-4444-444444444444-9"
		plantIdleCgroup(t, cgroup, foreign, sleeper.Process.Pid)
		d2, _, _ := idleDaemon(t, h, idle, cgroup, stateDir)
		stop2 := startIntegrationDaemon(t, d2)
		orphaned := waitIdleRows(t, h, idle.RunnerID, func(rows idleRunnerRows) bool {
			return rows.State == "QUARANTINED" && rows.IdleSeq != nil && *rows.IdleSeq > *dirty.IdleSeq &&
				rows.QuarantineReason != nil && strings.Contains(*rows.QuarantineReason, "STALE_EXECUTION")
		})
		if orphaned.ReconciledGeneration != nil {
			t.Fatalf("orphaned idle observation advanced generation: %s", orphaned.raw)
		}
		if claim := claimIdleRunner(t, h, idle.RunnerID, submitIdleJob(t, h, "idle-dirty-blocked-2")); claim.Assigned {
			t.Fatalf("orphaned idle runner accepted a claim: %+v", claim)
		}
		writeLifecycleEvidence(t, f, "idle-orphaned-quarantined", json.RawMessage(orphaned.raw))

		// Removing the foreign execution lets the next fresh observation attest.
		stop2()
		if err := sleeper.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		_ = sleeper.Wait()
		if err := os.RemoveAll(filepath.Join(cgroup, foreign)); err != nil {
			t.Fatal(err)
		}
		d3, _, _ := idleDaemon(t, h, idle, cgroup, stateDir)
		stop3 := startIntegrationDaemon(t, d3)
		attested := waitIdleRows(t, h, idle.RunnerID, func(rows idleRunnerRows) bool {
			return rows.State == "AVAILABLE" && rows.ReconciledGeneration != nil &&
				*rows.ReconciledGeneration == generation && rows.IdleSeq != nil && *rows.IdleSeq > *orphaned.IdleSeq
		})
		writeLifecycleEvidence(t, f, "idle-attested-after-cleanup", json.RawMessage(attested.raw))
		stop3()
		claim := claimIdleRunner(t, h, idle.RunnerID, submitIdleJob(t, h, "idle-dirty-next"))
		if !claim.Assigned || claim.RunnerEpoch != 1 {
			t.Fatalf("reconciled idle runner not claimable from epoch 0: %+v", claim)
		}
	})
}
