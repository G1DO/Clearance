package agent

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Unit tests isolate protocol/durability behavior from privileged cgroups. The
// production constructor has no switch that permits execution outside cgroups.
type testWorkload struct {
	command     *exec.Cmd
	done        chan struct{}
	err         error
	cleanupOnce sync.Once
	workspace   string
	proof       CleanupEvidence
	cleanupErr  error
}

func (w *testWorkload) Start() error {
	if err := w.command.Start(); err != nil {
		return err
	}
	w.done = make(chan struct{})
	go func() { w.err = w.command.Wait(); close(w.done) }()
	return nil
}
func (w *testWorkload) Wait() error { <-w.done; return w.err }
func (w *testWorkload) Cleanup() (CleanupEvidence, error) {
	w.cleanupOnce.Do(func() {
		if w.done != nil {
			_ = w.command.Process.Kill()
			<-w.done
		}
		_ = os.RemoveAll(w.workspace)
	})
	return w.proof, w.cleanupErr
}
func useTestExecution(t *testing.T, d *Daemon) {
	t.Helper()
	root := t.TempDir()
	d.prepare = func(p PollResponse) (allocationWorkload, error) {
		workspace := filepath.Join(root, *p.AllocationID)
		if err := os.MkdirAll(workspace, 0700); err != nil {
			return nil, err
		}
		cmd := exec.Command(p.Argv[0], p.Argv[1:]...)
		cmd.Dir = workspace
		return &testWorkload{command: cmd, workspace: workspace,
			proof: CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}}, nil
	}
	// These protocol tests stop their original worker before restart. Physical
	// survival/discovery is exercised separately with delegated Linux cgroups.
	d.discover = func(PollResponse) (allocationWorkload, DiscoveryEvidence, error) {
		return &testWorkload{workspace: filepath.Join(root, "recovered"),
				proof: CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}},
			DiscoveryEvidence{CleanupVerified: true, PIDs: []int64{}}, nil
	}
}

func TestDaemonCancellationAndDeadline(t *testing.T) {
	for _, scenario := range []string{"cancel_before_launch", "cancel_running", "timeout", "completion_before_cancel"} {
		t.Run(scenario, func(t *testing.T) {
			marker := filepath.Join(t.TempDir(), "executions")
			p := testAssignment("/bin/sh", "-c", `printf x > "$1"; exec sleep 60`, "test", marker)
			want := StatusCancelled
			yes := true
			switch scenario {
			case "cancel_before_launch":
				p.CancelRequested = &yes
			case "timeout":
				ms := int64(100)
				p.WorkloadTimeoutMs = &ms
				want = StatusTimedOut
			case "completion_before_cancel":
				p.Argv = []string{"/bin/true"}
				want = StatusSucceeded
			}
			f := &daemonFixture{assignment: p}
			server := f.serve(t)
			defer server.Close()
			d, stop := runTestDaemon(t, server.URL, t.TempDir())
			if scenario == "cancel_running" {
				f.wait(t, func() bool {
					for _, r := range f.reports {
						if r.Status == StatusRunning {
							return true
						}
					}
					return false
				})
				f.mu.Lock()
				f.assignment.CancelRequested = &yes
				f.mu.Unlock()
			}
			f.wait(t, func() bool {
				for _, r := range f.reports {
					if r.Status == StatusCleanup {
						return true
					}
				}
				return false
			})
			if scenario == "completion_before_cancel" {
				f.mu.Lock()
				f.assignment.CancelRequested = &yes
				f.mu.Unlock()
			}
			stop()
			if d.store.state.Allocation.Terminal != want {
				t.Fatalf("terminal=%s want %s", d.store.state.Allocation.Terminal, want)
			}
			if scenario == "cancel_before_launch" {
				if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("cancelled command started: %v", err)
				}
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			terminals := 0
			for _, r := range f.reports {
				if r.Status == want {
					terminals++
				}
				if r.Status == StatusCleanup && !positiveCleanup(r.Cleanup) {
					t.Fatal("missing positive physical proof")
				}
			}
			if terminals != 1 {
				t.Fatalf("terminal reports=%d", terminals)
			}
		})
	}
}

func TestDaemonDeadlineIndependentOfReportOutage(t *testing.T) {
	f := &daemonFixture{assignment: testAssignment("/bin/sleep", "60")}
	ms := int64(80)
	f.assignment.WorkloadTimeoutMs = &ms
	server := f.serve(t)
	defer server.Close()
	d, err := NewDaemon(Config{ControllerURL: server.URL, MachineToken: "machine-key", StateDir: t.TempDir(), HeartbeatInterval: 10 * time.Millisecond, RetryInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	useTestExecution(t, d)
	prepare := d.prepare
	cleaned := make(chan struct{})
	d.prepare = func(p PollResponse) (allocationWorkload, error) {
		w, err := prepare(p)
		return &cleanupNotification{allocationWorkload: w, cleaned: cleaned}, err
	}
	// Every report after STARTING gets a transient failure; execution must still
	// time out and physically terminate without a controller acknowledgment.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	f.wait(t, func() bool { return len(f.reports) >= 1 })
	f.mu.Lock()
	f.failReports = true
	f.mu.Unlock()
	select {
	case <-cleaned:
	case <-time.After(time.Second):
		t.Fatal("deadline depended on controller contact")
	}
	cancel()
	<-done
}

type cleanupNotification struct {
	allocationWorkload
	cleaned chan struct{}
}

func (w *cleanupNotification) Cleanup() (CleanupEvidence, error) {
	p, e := w.allocationWorkload.Cleanup()
	close(w.cleaned)
	return p, e
}
