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
	"sync/atomic"
	"testing"
	"time"
)

// This fixture controls acknowledgments while the actual daemon owns durable
// state, its report sequence, polling and worker lifecycle.
func reconciliationServer(t *testing.T, assignment func() PollResponse, report func(ReportRequest, http.ResponseWriter)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/v1/agents/poll" {
			data, err := EncodePollResponse(assignment())
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			_, _ = w.Write(data)
			return
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		request, err := ParseReportRequest(data)
		if err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		report(request, w)
	}))
}

func startReconciliationDaemon(t *testing.T, d *Daemon) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	stop := func() {
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("daemon stopped unexpectedly: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("daemon did not stop")
		}
	}
	t.Cleanup(stop)
	return func() {
		select {
		case err := <-done:
			t.Fatalf("daemon stopped early: %v", err)
		default:
		}
	}
}

func awaitReconciliation(t *testing.T, done <-chan ReportRequest) ReportRequest {
	t.Helper()
	select {
	case r := <-done:
		return r
	case <-time.After(3 * time.Second):
		t.Fatal("reconciliation report did not arrive")
		return ReportRequest{}
	}
}

func TestDaemonReconciliationRetryReobservesAndPersistsInspectionError(t *testing.T) {
	dir := t.TempDir()
	assignment := seedRecovery(t, dir, func(a *allocationState) { a.Terminal, a.TerminalAcknowledged = StatusSucceeded, true })
	reports := make(chan ReportRequest, 8)
	var observations atomic.Int32
	var reconciles atomic.Int32
	server := reconciliationServer(t, func() PollResponse { return assignment }, func(r ReportRequest, w http.ResponseWriter) {
		switch r.Status {
		case StatusRecovery:
			fmt.Fprint(w, `{"accepted":false,"reason":"quarantined","terminal":true}`)
		case StatusReconcile:
			reports <- r
			if reconciles.Add(1) == 1 {
				w.WriteHeader(503)
				return
			}
			fmt.Fprint(w, `{"accepted":true,"reason":"reconcile_attested","terminal":true}`)
		default:
			t.Errorf("unexpected report: %s", r.Status)
			w.WriteHeader(400)
		}
	})
	defer server.Close()
	d, err := NewDaemon(Config{ControllerURL: server.URL, MachineToken: "machine-key", StateDir: dir, HeartbeatInterval: 10 * time.Millisecond, RetryInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	d.prepare = func(PollResponse) (allocationWorkload, error) {
		t.Error("reconciliation relaunched work")
		return nil, errors.New("unexpected launch")
	}
	d.discover = func(PollResponse) (allocationWorkload, DiscoveryEvidence, error) {
		n := observations.Add(1)
		if n <= 2 {
			return nil, DiscoveryEvidence{PIDs: []int64{}}, errors.New("inspection unavailable")
		}
		return nil, DiscoveryEvidence{PIDs: []int64{}, CleanupVerified: true}, nil
	}
	alive := startReconciliationDaemon(t, d)
	first, second := awaitReconciliation(t, reports), awaitReconciliation(t, reports)
	if first.Reconcile.Error == nil || first.Reconcile.ExecutionEmpty || second.Reconcile.Error != nil || !second.Reconcile.ExecutionEmpty || !second.Reconcile.WorkspaceClean || second.Seq <= first.Seq || observations.Load() != 3 {
		t.Fatalf("retry reused evidence or lost failure: first=%+v second=%+v observations=%d", first, second, observations.Load())
	}
	alive()
}

func TestDaemonReconciliationRestartPerformsAuthorizedCleanupOnce(t *testing.T) {
	dir := t.TempDir()
	assignment := seedRecovery(t, dir, func(a *allocationState) {
		a.Terminal, a.TerminalAcknowledged = StatusSucceeded, true
		a.Cleanup = &CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}
		a.CleanupIncarnation = 1
	})
	cleaned := make(chan ReportRequest, 1)
	server := reconciliationServer(t, func() PollResponse { return assignment }, func(r ReportRequest, w http.ResponseWriter) {
		switch r.Status {
		case StatusRecovery:
			fmt.Fprint(w, `{"accepted":false,"reason":"quarantined","terminal":true}`)
		case StatusReconcile:
			if r.Reconcile.WorkspaceClean {
				t.Error("cached cleanup bypassed dirty workspace")
			}
			fmt.Fprint(w, `{"accepted":true,"reason":"reconcile_cleanup_required","terminal":true}`)
		case StatusCleanup:
			cleaned <- r
			fmt.Fprint(w, `{"accepted":true,"reason":"ok","terminal":true}`)
		default:
			fmt.Fprint(w, `{"accepted":true,"reason":"ok","terminal":true}`)
		}
	})
	defer server.Close()
	d, err := NewDaemon(Config{ControllerURL: server.URL, MachineToken: "machine-key", StateDir: dir, HeartbeatInterval: 10 * time.Millisecond, RetryInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w := &reviewWorkload{proof: CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}}
	d.prepare = func(PollResponse) (allocationWorkload, error) { t.Error("restart prepared execution"); return w, nil }
	d.discover = func(PollResponse) (allocationWorkload, DiscoveryEvidence, error) {
		return w, DiscoveryEvidence{CgroupPresent: true, WorkspacePresent: true, PIDs: []int64{}}, nil
	}
	alive := startReconciliationDaemon(t, d)
	report := awaitReconciliation(t, cleaned)
	if !positiveCleanup(report.Cleanup) || w.cleanups.Load() != 1 || w.starts.Load() != 0 {
		t.Fatalf("authorized cleanup missing or duplicated: %+v cleanups=%d starts=%d", report, w.cleanups.Load(), w.starts.Load())
	}
	alive()
}

func TestDaemonLostRecoveryAckThenQuarantinedRestartRestoresInterruption(t *testing.T) {
	for _, discoveryFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("rediscovery_failure=%t", discoveryFails), func(t *testing.T) {
			dir := t.TempDir()
			assignment := seedRecovery(t, dir, nil)
			firstCtx, cancelFirst := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancelFirst()
			var quarantined atomic.Bool
			lost := make(chan ReportRequest, 1)
			cleaned := make(chan ReportRequest, 1)
			server := reconciliationServer(t, func() PollResponse { return assignment }, func(r ReportRequest, w http.ResponseWriter) {
				switch r.Status {
				case StatusRecovery:
					if !quarantined.Load() {
						// The controller commits INTERRUPTED, but the reply never
						// reaches this incarnation before it stops.
						lost <- r
						cancelFirst()
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					fmt.Fprint(w, `{"accepted":false,"reason":"quarantined","terminal":true}`)
				case StatusReconcile:
					if r.Reconcile == nil || len(r.Reconcile.PIDs) != 1 || r.Reconcile.ExecutionEmpty {
						t.Errorf("reconciliation lost surviving execution: %+v", r)
					}
					fmt.Fprint(w, `{"accepted":true,"reason":"reconcile_cleanup_required","terminal":true}`)
				case StatusCleanup:
					cleaned <- r
					if discoveryFails {
						fmt.Fprint(w, `{"accepted":true,"reason":"quarantined","terminal":true}`)
						return
					}
					fmt.Fprint(w, `{"accepted":true,"reason":"ok","terminal":true}`)
				case StatusHeartbeat:
					fmt.Fprint(w, `{"accepted":true,"reason":"ok","terminal":true}`)
				default:
					t.Errorf("recovery invented an execution report: %s", r.Status)
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			defer server.Close()
			cfg := Config{ControllerURL: server.URL, MachineToken: "machine-key", StateDir: dir,
				HeartbeatInterval: 10 * time.Millisecond, RetryInterval: 10 * time.Millisecond}
			work := &reviewWorkload{proof: CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}}
			var discoveries atomic.Int32
			discover := func(PollResponse) (allocationWorkload, DiscoveryEvidence, error) {
				evidence := DiscoveryEvidence{CgroupPresent: true, WorkspacePresent: true, PIDs: []int64{101}}
				if discoveries.Add(1) == 4 {
					// The authoritative disposition must already be durable when
					// the daemon obtains the handle for directed cleanup.
					data, err := os.ReadFile(filepath.Join(dir, "state.json"))
					if err != nil {
						t.Error(err)
					}
					state, err := decodeState(data)
					if err != nil || state.Allocation == nil || state.Allocation.Terminal != terminalInterrupted || !state.Allocation.TerminalAcknowledged {
						t.Errorf("interruption was not persisted before cleanup discovery: %+v %v", state, err)
					}
					if discoveryFails {
						return nil, evidence, errors.New("allocation directory identity changed")
					}
				}
				return work, evidence, nil
			}
			first, err := NewDaemon(cfg)
			if err != nil {
				t.Fatal(err)
			}
			first.discover = discover
			first.prepare = func(PollResponse) (allocationWorkload, error) {
				t.Error("recovery prepared another launch")
				return work, nil
			}
			if err := first.Run(firstCtx); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			prior := awaitReconciliation(t, lost)
			if first.store.state.Allocation.Terminal != "" || work.cleanups.Load() != 0 {
				t.Fatal("lost recovery acknowledgment authorized cleanup")
			}
			// Heartbeat expiry quarantines the committed interruption before boot.
			quarantined.Store(true)
			second, err := NewDaemon(cfg)
			if err != nil {
				t.Fatal(err)
			}
			second.discover, second.prepare = discover, first.prepare
			alive := startReconciliationDaemon(t, second)
			report := awaitReconciliation(t, cleaned)
			if report.Seq <= prior.Seq || report.AgentIncarnation != prior.AgentIncarnation+1 || report.AllocationID != prior.AllocationID || report.RunnerEpoch != prior.RunnerEpoch {
				t.Fatalf("restart lost recovery fencing: prior=%+v cleanup=%+v", prior, report)
			}
			wantCleanups := int32(1)
			if discoveryFails {
				wantCleanups = 0
				if report.Cleanup == nil || report.Cleanup.Error == nil || positiveCleanup(report.Cleanup) {
					t.Fatalf("rediscovery failure lost negative proof: %+v", report)
				}
			} else if !positiveCleanup(report.Cleanup) {
				t.Fatalf("authorized recovery did not produce positive proof: %+v", report)
			}
			if discoveries.Load() != 4 || work.cleanups.Load() != wantCleanups || work.starts.Load() != 0 {
				t.Fatalf("recovery repeated execution or used an uncertain handle: discoveries=%d cleanups=%d starts=%d", discoveries.Load(), work.cleanups.Load(), work.starts.Load())
			}
			alive()
		})
	}
	for _, tc := range []struct {
		name            string
		loseAttestAck   bool
		restartReleased bool
	}{
		{name: "attest_ack_received"},
		{name: "attest_ack_lost", loseAttestAck: true},
		{name: "attest_ack_lost_then_restart", loseAttestAck: true, restartReleased: true},
	} {
		t.Run("already_clean/"+tc.name, func(t *testing.T) {
			dir := t.TempDir()
			old := seedRecovery(t, dir, nil)
			next := testAssignment("/bin/true")
			id, job, epoch := "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", int64(2)
			next.AllocationID, next.JobID, next.RunnerEpoch = &id, &job, &epoch
			firstCtx, cancelFirst := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancelFirst()
			secondCtx, cancelSecond := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancelSecond()
			var quarantined, released atomic.Bool
			lost := make(chan ReportRequest, 1)
			attested := make(chan ReportRequest, 8)
			cleaned := make(chan ReportRequest, 1)
			server := reconciliationServer(t, func() PollResponse {
				if released.Load() {
					return next
				}
				return old
			}, func(r ReportRequest, w http.ResponseWriter) {
				if r.AllocationID == *old.AllocationID {
					switch r.Status {
					case StatusRecovery:
						if !quarantined.Load() {
							// Physical cleanup completed before the original daemon
							// saved its result. Recovery commits INTERRUPTED, but its
							// response is lost before this incarnation stops.
							lost <- r
							cancelFirst()
							w.WriteHeader(http.StatusServiceUnavailable)
							return
						}
						fmt.Fprint(w, `{"accepted":false,"reason":"quarantined","terminal":true}`)
					case StatusReconcile:
						if r.Reconcile == nil || !r.Reconcile.ExecutionEmpty || !r.Reconcile.DescendantsReaped || !r.Reconcile.WorkspaceClean || r.Reconcile.Error != nil {
							t.Errorf("clean checkpoint lacked fresh attestation: %+v", r)
						}
						// Check the durable file before the controller can release
						// ownership, including when no acknowledgment will arrive.
						data, err := os.ReadFile(filepath.Join(dir, "state.json"))
						if err != nil {
							t.Error(err)
						}
						state, err := decodeState(data)
						if err != nil || state.Allocation == nil || state.Allocation.Terminal != terminalInterrupted || !state.Allocation.TerminalAcknowledged || !positiveCleanup(state.Allocation.Cleanup) || state.Allocation.CleanupIncarnation != r.AgentIncarnation {
							t.Errorf("interruption and fresh cleanup were not durable before release: %+v %v", state.Allocation, err)
						}
						if released.CompareAndSwap(false, true) {
							attested <- r
							if tc.loseAttestAck {
								if tc.restartReleased {
									cancelSecond()
								}
								w.WriteHeader(http.StatusServiceUnavailable)
								return
							}
							fmt.Fprint(w, `{"accepted":true,"reason":"reconcile_attested","terminal":true}`)
							return
						}
						fmt.Fprint(w, `{"accepted":false,"reason":"fenced_rejected","terminal":true}`)
					default:
						t.Errorf("old allocation unexpectedly reported %s", r.Status)
						w.WriteHeader(http.StatusBadRequest)
					}
					return
				}
				if r.AllocationID != *next.AllocationID {
					t.Errorf("unexpected allocation: %s", r.AllocationID)
				}
				if r.Status == StatusCleanup {
					cleaned <- r
					fmt.Fprint(w, `{"accepted":true,"reason":"ok","terminal":true}`)
					return
				}
				fmt.Fprintf(w, `{"accepted":true,"reason":"ok","terminal":%t}`, r.Status == StatusSucceeded)
			})
			defer server.Close()
			cfg := Config{ControllerURL: server.URL, MachineToken: "machine-key", StateDir: dir,
				HeartbeatInterval: 10 * time.Millisecond, RetryInterval: 10 * time.Millisecond}
			proof := CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}
			priorWork := &reviewWorkload{proof: proof}
			gate := make(chan struct{})
			close(gate)
			nextWork := &reviewWorkload{waitGate: gate, proof: proof}
			newDaemon := func() *Daemon {
				t.Helper()
				d, err := NewDaemon(cfg)
				if err != nil {
					t.Fatal(err)
				}
				d.discover = func(p PollResponse) (allocationWorkload, DiscoveryEvidence, error) {
					if *p.AllocationID != *old.AllocationID {
						t.Errorf("unexpected discovery: %+v", p)
					}
					return priorWork, DiscoveryEvidence{CleanupVerified: true, PIDs: []int64{}}, nil
				}
				d.prepare = func(p PollResponse) (allocationWorkload, error) {
					if *p.AllocationID != *next.AllocationID || !released.Load() {
						t.Errorf("launch preceded prior release: %+v", p)
					}
					return nextWork, nil
				}
				return d
			}
			first := newDaemon()
			if err := first.Run(firstCtx); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			prior := awaitReconciliation(t, lost)
			if first.store.state.Allocation.Terminal != "" || first.store.state.Allocation.Cleanup != nil || priorWork.cleanups.Load() != 0 {
				t.Fatal("lost recovery acknowledgment changed local disposition or cleanup")
			}
			quarantined.Store(true)
			second := newDaemon()
			if tc.restartReleased {
				if err := second.Run(secondCtx); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				second = newDaemon()
			}
			alive := startReconciliationDaemon(t, second)
			attestation := awaitReconciliation(t, attested)
			if attestation.Seq <= prior.Seq || attestation.AgentIncarnation != prior.AgentIncarnation+1 || attestation.RunnerEpoch != prior.RunnerEpoch {
				t.Fatalf("restart lost recovery fencing: prior=%+v attestation=%+v", prior, attestation)
			}
			report := awaitReconciliation(t, cleaned)
			wantPriorCleanups := int32(0)
			if tc.restartReleased {
				wantPriorCleanups = 1 // Reverify the previous incarnation's cleanup before handoff.
			}
			if report.AllocationID != *next.AllocationID || !positiveCleanup(report.Cleanup) || nextWork.starts.Load() != 1 || priorWork.starts.Load() != 0 || priorWork.cleanups.Load() != wantPriorCleanups {
				t.Fatalf("attestation stranded or duplicated ownership: report=%+v old_starts=%d new_starts=%d old_cleanups=%d", report, priorWork.starts.Load(), nextWork.starts.Load(), priorWork.cleanups.Load())
			}
			alive()
		})
	}

}

func TestDaemonReconciliationDoesNotReplaceLostStartOrTerminal(t *testing.T) {
	for _, lost := range []ReportStatus{StatusStarting, StatusSucceeded} {
		t.Run(string(lost), func(t *testing.T) {
			assignment := testAssignment("/bin/true")
			var requested atomic.Bool
			var losses atomic.Int32
			cleanup := make(chan ReportRequest, 1)
			server := reconciliationServer(t, func() PollResponse {
				p := assignment
				if requested.Load() {
					v := true
					p.ReconcileRequested = &v
				}
				return p
			}, func(r ReportRequest, w http.ResponseWriter) {
				if r.Status == lost && losses.Add(1) == 1 {
					requested.Store(true)
					w.WriteHeader(503)
					return
				}
				switch r.Status {
				case StatusCleanup:
					cleanup <- r
					fmt.Fprint(w, `{"accepted":true,"reason":"ok","terminal":true}`)
				case StatusReconcile:
					if losses.Load() < 2 {
						t.Errorf("reconciliation discarded unacknowledged %s", lost)
					}
					fmt.Fprint(w, `{"accepted":true,"reason":"reconcile_still_running","terminal":false}`)
				default:
					fmt.Fprint(w, `{"accepted":true,"reason":"ok","terminal":false}`)
				}
			})
			defer server.Close()
			d, err := NewDaemon(Config{ControllerURL: server.URL, MachineToken: "machine-key", StateDir: t.TempDir(), HeartbeatInterval: 20 * time.Millisecond, RetryInterval: time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			gate := make(chan struct{})
			close(gate)
			w := &reviewWorkload{waitGate: gate, proof: CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}}
			d.prepare = func(PollResponse) (allocationWorkload, error) { return w, nil }
			d.discover = func(PollResponse) (allocationWorkload, DiscoveryEvidence, error) {
				return w, DiscoveryEvidence{PIDs: []int64{}}, nil
			}
			alive := startReconciliationDaemon(t, d)
			awaitReconciliation(t, cleanup)
			if losses.Load() != 2 || w.starts.Load() != 1 {
				t.Fatalf("lost report not retried exactly once before lifecycle progressed: retries=%d starts=%d", losses.Load(), w.starts.Load())
			}
			alive()
		})
	}
}

func TestDaemonResolvedStartAcknowledgmentNeverLaunches(t *testing.T) {
	assignment := testAssignment("/bin/true")
	server := reconciliationServer(t, func() PollResponse { return assignment }, func(r ReportRequest, w http.ResponseWriter) {
		fmt.Fprint(w, `{"accepted":true,"reason":"ok","terminal":true}`)
	})
	defer server.Close()
	d, err := NewDaemon(Config{ControllerURL: server.URL, MachineToken: "machine-key", StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	d.prepare = func(PollResponse) (allocationWorkload, error) {
		t.Error("terminal START ack launched work")
		return nil, errors.New("unexpected launch")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := d.Run(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("resolved START did not fail closed: %v", err)
	}
}

func TestDaemonLostCheckpointAttestationAckAllowsNewOwnershipWithoutRelaunch(t *testing.T) {
	dir := t.TempDir()
	old := seedRecovery(t, dir, func(a *allocationState) {
		a.Terminal, a.TerminalAcknowledged = StatusSucceeded, true
		// Cleanup finished physically before the old process saved its proof.
		// Only fresh verification of the durable removal checkpoint can prove it.
	})
	next := testAssignment("/bin/true")
	id, job, epoch := "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444", int64(2)
	next.AllocationID, next.JobID, next.RunnerEpoch = &id, &job, &epoch
	var released atomic.Bool
	var attestations atomic.Int32
	cleaned := make(chan ReportRequest, 1)
	server := reconciliationServer(t, func() PollResponse {
		if released.Load() {
			return next
		}
		return old
	}, func(r ReportRequest, w http.ResponseWriter) {
		if r.AllocationID == *old.AllocationID {
			switch r.Status {
			case StatusRecovery:
				fmt.Fprint(w, `{"accepted":false,"reason":"quarantined","terminal":true}`)
			case StatusReconcile:
				if r.Reconcile == nil || !r.Reconcile.ExecutionEmpty || !r.Reconcile.DescendantsReaped || !r.Reconcile.WorkspaceClean || r.Reconcile.Error != nil {
					t.Errorf("checkpoint attestation lacked current positive evidence: %+v", r)
				}
				if released.CompareAndSwap(false, true) {
					attestations.Add(1)
					// Commit release and the next claim, then lose the response.
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				fmt.Fprint(w, `{"accepted":false,"reason":"fenced_rejected","terminal":true}`)
			default:
				t.Errorf("old allocation unexpectedly reported %s", r.Status)
				w.WriteHeader(http.StatusBadRequest)
			}
			return
		}
		if r.AllocationID != *next.AllocationID {
			t.Errorf("unexpected allocation: %s", r.AllocationID)
		}
		if r.Status == StatusCleanup {
			cleaned <- r
			fmt.Fprint(w, `{"accepted":true,"reason":"ok","terminal":true}`)
			return
		}
		fmt.Fprintf(w, `{"accepted":true,"reason":"ok","terminal":%t}`, r.Status == StatusSucceeded)
	})
	defer server.Close()
	d, err := NewDaemon(Config{ControllerURL: server.URL, MachineToken: "machine-key", StateDir: dir,
		HeartbeatInterval: 10 * time.Millisecond, RetryInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	close(gate)
	w := &reviewWorkload{waitGate: gate, proof: CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}}
	d.prepare = func(p PollResponse) (allocationWorkload, error) {
		if *p.AllocationID != *next.AllocationID || !released.Load() {
			t.Errorf("launch preceded verified prior release: %+v", p)
		}
		return w, nil
	}
	d.discover = func(p PollResponse) (allocationWorkload, DiscoveryEvidence, error) {
		if *p.AllocationID != *old.AllocationID {
			t.Errorf("unexpected discovery: %+v", p)
		}
		return nil, DiscoveryEvidence{CleanupVerified: true, PIDs: []int64{}}, nil
	}
	alive := startReconciliationDaemon(t, d)
	report := awaitReconciliation(t, cleaned)
	if report.AllocationID != *next.AllocationID || !positiveCleanup(report.Cleanup) || attestations.Load() != 1 || w.starts.Load() != 1 {
		t.Fatalf("lost attestation stranded or duplicated ownership: report=%+v releases=%d starts=%d", report, attestations.Load(), w.starts.Load())
	}
	alive()
}

func TestDaemonReconciliationDirtyObservationReplacesCurrentCachedPositiveCleanup(t *testing.T) {
	assignment := testAssignment("/bin/true")
	cleaned := make(chan ReportRequest, 1)
	var cleanupReports atomic.Int32
	var discoveries atomic.Int32
	gate := make(chan struct{})
	close(gate)
	proof := CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}
	launched := &reviewWorkload{waitGate: gate, proof: proof}
	fresh := &reviewWorkload{proof: proof}
	server := reconciliationServer(t, func() PollResponse { return assignment }, func(r ReportRequest, w http.ResponseWriter) {
		switch r.Status {
		case StatusCleanup:
			if cleanupReports.Add(1) == 1 {
				fmt.Fprint(w, `{"accepted":false,"reason":"quarantined","terminal":true}`)
				return
			}
			if fresh.cleanups.Load() != 1 {
				t.Error("fresh dirty observation was bypassed by replaying cached positive cleanup")
			}
			cleaned <- r
			fmt.Fprint(w, `{"accepted":true,"reason":"ok","terminal":true}`)
		case StatusReconcile:
			if r.Reconcile == nil || r.Reconcile.WorkspaceClean || !r.Reconcile.WorkspacePresent {
				t.Errorf("fresh workspace presence did not veto saved proof: %+v", r)
			}
			fmt.Fprint(w, `{"accepted":true,"reason":"reconcile_cleanup_required","terminal":true}`)
		default:
			fmt.Fprintf(w, `{"accepted":true,"reason":"ok","terminal":%t}`, r.Status == StatusSucceeded)
		}
	})
	defer server.Close()
	d, err := NewDaemon(Config{ControllerURL: server.URL, MachineToken: "machine-key", StateDir: t.TempDir(),
		HeartbeatInterval: 10 * time.Millisecond, RetryInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	d.prepare = func(PollResponse) (allocationWorkload, error) { return launched, nil }
	d.discover = func(PollResponse) (allocationWorkload, DiscoveryEvidence, error) {
		discoveries.Add(1)
		return fresh, DiscoveryEvidence{WorkspacePresent: true, CleanupVerified: true, PIDs: []int64{}}, nil
	}
	alive := startReconciliationDaemon(t, d)
	report := awaitReconciliation(t, cleaned)
	if !positiveCleanup(report.Cleanup) || launched.starts.Load() != 1 || launched.cleanups.Load() != 1 || fresh.starts.Load() != 0 || fresh.cleanups.Load() != 1 || discoveries.Load() != 2 {
		t.Fatalf("directed cleanup did not reverify current cached proof: report=%+v discoveries=%d starts=%d prior_cleanup=%d fresh_starts=%d fresh_cleanup=%d",
			report, discoveries.Load(), launched.starts.Load(), launched.cleanups.Load(), fresh.starts.Load(), fresh.cleanups.Load())
	}
	alive()
}

func TestDaemonReconciliationRediscoveryFailureReportsNegativeCleanupBeforeStopping(t *testing.T) {
	dir := t.TempDir()
	assignment := seedRecovery(t, dir, func(a *allocationState) {
		a.Terminal, a.TerminalAcknowledged = StatusSucceeded, true
	})
	failed := make(chan ReportRequest, 1)
	var observations atomic.Int32
	server := reconciliationServer(t, func() PollResponse { return assignment }, func(r ReportRequest, w http.ResponseWriter) {
		switch r.Status {
		case StatusRecovery:
			fmt.Fprint(w, `{"accepted":false,"reason":"quarantined","terminal":true}`)
		case StatusReconcile:
			if r.Reconcile == nil || r.Reconcile.Error != nil || !r.Reconcile.WorkspacePresent {
				t.Errorf("classification must precede the identity failure: %+v", r)
			}
			fmt.Fprint(w, `{"accepted":true,"reason":"reconcile_cleanup_required","terminal":true}`)
		case StatusCleanup:
			failed <- r
			fmt.Fprint(w, `{"accepted":true,"reason":"quarantined","terminal":true}`)
		default:
			t.Errorf("unexpected report during directed cleanup: %s", r.Status)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	defer server.Close()
	d, err := NewDaemon(Config{ControllerURL: server.URL, MachineToken: "machine-key", StateDir: dir,
		HeartbeatInterval: 10 * time.Millisecond, RetryInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w := &reviewWorkload{proof: CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}}
	d.prepare = func(PollResponse) (allocationWorkload, error) {
		t.Error("changed identity must never relaunch execution")
		return w, nil
	}
	identityFailure := errors.New("allocation directory identity changed")
	d.discover = func(PollResponse) (allocationWorkload, DiscoveryEvidence, error) {
		evidence := DiscoveryEvidence{CgroupPresent: true, WorkspacePresent: true, PIDs: []int64{}}
		if observations.Add(1) >= 3 {
			return w, evidence, identityFailure
		}
		return w, evidence, nil
	}
	alive := startReconciliationDaemon(t, d)
	report := awaitReconciliation(t, failed)
	if report.Cleanup == nil || report.Cleanup.Error == nil || *report.Cleanup.Error == "" || report.Cleanup.ExecutionEmpty || report.Cleanup.DescendantsReaped || report.Cleanup.WorkspaceClean {
		t.Fatalf("identity uncertainty was not retained as negative cleanup: %+v", report)
	}
	message := *report.Cleanup.Error
	if len(message) < len(identityFailure.Error()) || message[:len(identityFailure.Error())] != identityFailure.Error() {
		t.Fatalf("cleanup evidence lost the identity failure: %q", message)
	}
	if observations.Load() != 3 || w.cleanups.Load() != 0 || w.starts.Load() != 0 {
		t.Fatalf("unverified cleanup handle was used: observations=%d cleanups=%d starts=%d",
			observations.Load(), w.cleanups.Load(), w.starts.Load())
	}
	alive()
}
