package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDaemonRestartAfterLostCleanupAckReinspectsBeforeNewAssignment(t *testing.T) {
	for _, outcome := range []string{"verified", "discovery_error", "negative_cleanup"} {
		t.Run(outcome, func(t *testing.T) {
			dir := t.TempDir()
			old := seedRecovery(t, dir, func(a *allocationState) {
				a.Terminal, a.TerminalAcknowledged = terminalInterrupted, true
				a.Cleanup = &CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}
				a.CleanupIncarnation = 1
				// The controller accepted proof and committed a retry, but the
				// acknowledgment was lost before this agent restarted.
				a.CleanupAcknowledged = false
			})
			manifest := []byte(fmt.Sprintf(`{"allocation_id":%q,"runner_epoch":%d}`, *old.AllocationID, *old.RunnerEpoch))
			if err := os.WriteFile(filepath.Join(dir, "containment.json"), manifest, 0600); err != nil {
				t.Fatal(err)
			}
			next := testAssignment("/bin/true")
			id, epoch := "33333333-3333-3333-3333-333333333333", int64(2)
			next.AllocationID, next.RunnerEpoch = &id, &epoch
			f := &daemonFixture{assignment: next, notify: make(chan struct{}, 1)}
			var quarantined bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				f.mu.Lock()
				defer f.mu.Unlock()
				defer func() {
					select {
					case f.notify <- struct{}{}:
					default:
					}
				}()
				if r.URL.Path == "/internal/v1/agents/poll" {
					body, err := EncodePollResponse(next)
					if err != nil {
						t.Error(err)
					}
					_, _ = w.Write(body)
					return
				}
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				report, err := ParseReportRequest(body)
				if err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				f.reports = append(f.reports, report)
				if quarantined {
					fmt.Fprint(w, `{"accepted":false,"reason":"quarantined","terminal":false}`)
				} else if report.Status == StatusRecovery && report.Discovery != nil && report.Discovery.Error != nil {
					quarantined = true
					fmt.Fprint(w, `{"accepted":true,"reason":"quarantined","terminal":false}`)
				} else {
					fmt.Fprint(w, `{"accepted":true,"reason":"ok","terminal":false}`)
				}
			}))
			defer server.Close()
			d, err := NewDaemon(Config{ControllerURL: server.URL, MachineToken: "machine-key", StateDir: dir,
				HeartbeatInterval: 10 * time.Millisecond, RetryInterval: 10 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			if d.Incarnation() != 2 {
				t.Fatalf("test did not reserve a fresh incarnation: %d", d.Incarnation())
			}
			previous := &reviewWorkload{proof: CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}}
			if outcome == "negative_cleanup" {
				previous.proof.WorkspaceClean = false
			}
			var discoveries, prepares atomic.Int32
			d.discover = func(p PollResponse) (allocationWorkload, DiscoveryEvidence, error) {
				discoveries.Add(1)
				if *p.AllocationID != *old.AllocationID || *p.RunnerEpoch != *old.RunnerEpoch {
					t.Errorf("handoff inspected a different allocation: %+v", p)
				}
				evidence := DiscoveryEvidence{CleanupVerified: true, PIDs: []int64{}}
				if outcome == "discovery_error" {
					return previous, evidence, errors.New("allocation directory identity changed")
				}
				return previous, evidence, nil
			}
			gate := make(chan struct{})
			close(gate)
			workload := &reviewWorkload{waitGate: gate, proof: CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}}
			d.prepare = func(p PollResponse) (allocationWorkload, error) {
				prepares.Add(1)
				if discoveries.Load() != 1 || previous.cleanups.Load() != 1 || *p.AllocationID != id || *p.RunnerEpoch != epoch {
					t.Errorf("new execution preceded physical reinspection or used stale identity: %+v", p)
				}
				return workload, nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			finished := make(chan struct{})
			var runErr error
			go func() { runErr = d.Run(ctx); close(finished) }()
			defer func() { cancel(); <-finished }()
			if outcome == "verified" {
				f.wait(t, func() bool {
					for _, report := range f.reports {
						if report.AllocationID == id && report.Status == StatusCleanup {
							return true
						}
					}
					return false
				})
				cancel()
				<-finished
				if !errors.Is(runErr, context.Canceled) || prepares.Load() != 1 || workload.starts.Load() != 1 {
					t.Fatalf("verified handoff did not execute exactly once: err=%v prepares=%d starts=%d", runErr, prepares.Load(), workload.starts.Load())
				}
			} else {
				select {
				case <-finished:
				case <-time.After(2 * time.Second):
					t.Fatal("contradictory prior cleanup did not stop handoff")
				}
				if runErr == nil || !strings.Contains(runErr.Error(), "quarantined") || prepares.Load() != 0 || workload.starts.Load() != 0 {
					t.Fatalf("unsafe handoff accepted: err=%v prepares=%d starts=%d", runErr, prepares.Load(), workload.starts.Load())
				}
				if !d.store.state.Allocation.Started || d.store.state.Allocation.Cleanup != nil {
					t.Fatal("failed handoff did not persist a never-launch marker with no cleanup proof")
				}
				// Another boot must not reinterpret the new assignment as a launch.
				restarted, err := NewDaemon(d.cfg)
				if err != nil {
					t.Fatal(err)
				}
				restarted.prepare = d.prepare
				restarted.discover = func(p PollResponse) (allocationWorkload, DiscoveryEvidence, error) {
					if *p.AllocationID != id {
						t.Errorf("quarantine restart did not retain current controller identity: %+v", p)
					}
					return nil, DiscoveryEvidence{PIDs: []int64{}}, errors.New("durable containment allocation identity contradicts assignment")
				}
				restartCtx, stopRestart := context.WithTimeout(context.Background(), time.Second)
				err = restarted.Run(restartCtx)
				stopRestart()
				if err == nil || !strings.Contains(err.Error(), "quarantined") || prepares.Load() != 0 {
					t.Fatalf("another boot bypassed quarantined handoff: %v prepares=%d", err, prepares.Load())
				}
			}
			if discoveries.Load() != 1 || previous.starts.Load() != 0 {
				t.Fatalf("old execution was replayed or reinspection repeated: discoveries=%d starts=%d", discoveries.Load(), previous.starts.Load())
			}
			wantCleanups := int32(1)
			if outcome == "discovery_error" {
				wantCleanups = 0
			}
			if previous.cleanups.Load() != wantCleanups {
				t.Fatalf("old cleanup calls=%d, want %d", previous.cleanups.Load(), wantCleanups)
			}
			data, err := os.ReadFile(filepath.Join(dir, "containment.json"))
			if outcome != "verified" && (err != nil || string(data) != string(manifest)) {
				t.Fatalf("handoff erased prior physical identity: %s %v", data, err)
			}
			data, err = os.ReadFile(filepath.Join(dir, "state.json"))
			if err != nil {
				t.Fatal(err)
			}
			state, err := decodeState(data)
			if err != nil || state.Allocation == nil || *state.Allocation.Assignment.AllocationID != id {
				t.Fatalf("current controller identity was not durable: %+v %v", state, err)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if outcome != "verified" {
				if !quarantined || len(f.reports) != 2 || f.reports[0].Discovery == nil || f.reports[0].Discovery.Error == nil || !strings.Contains(*f.reports[0].Discovery.Error, *old.AllocationID) {
					t.Fatalf("failed handoff lacks inspectable quarantine evidence: %+v", f.reports)
				}
				for i, report := range f.reports {
					if report.Status != StatusRecovery || report.AllocationID != id || report.RunnerEpoch != epoch || report.AgentIncarnation != int64(i+2) || report.Seq != int64(i+2) {
						t.Fatalf("quarantine report lost current fencing or replayed execution: %+v", report)
					}
				}
			} else {
				for i, report := range f.reports {
					if report.AllocationID != id || report.RunnerEpoch != epoch || report.AgentIncarnation != d.Incarnation() || report.Seq != int64(i+1) {
						t.Fatalf("new allocation lost identity or sequence: %+v", report)
					}
					if i == 0 && report.Status != StatusStarting {
						t.Fatalf("new execution did not begin with STARTING: %+v", report)
					}
				}
			}
		})
	}
}
