package agent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func seedRecovery(t *testing.T, dir string, change func(*allocationState)) PollResponse {
	t.Helper()
	s, err := openState(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p := testAssignment("/bin/true")
	a := &allocationState{Assignment: p, Seq: 9, Started: true}
	if change != nil {
		change(a)
	}
	next := s.state
	next.Allocation = a
	if err := s.save(next); err != nil {
		t.Fatal(err)
	}
	return p
}

func recoveryDaemon(t *testing.T, endpoint, dir string, discover func(PollResponse) (allocationWorkload, DiscoveryEvidence, error)) (*Daemon, func() error) {
	t.Helper()
	d, err := NewDaemon(Config{ControllerURL: endpoint, MachineToken: "machine-key", StateDir: dir,
		HeartbeatInterval: 10 * time.Millisecond, RetryInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	d.discover = discover
	d.prepare = func(PollResponse) (allocationWorkload, error) {
		t.Error("recovery attempted to prepare a second launch")
		return nil, errors.New("unexpected prepare")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	return d, func() error { cancel(); return <-done }
}

func TestDaemonRecoveryResponseLossAndRepeatedRestart(t *testing.T) {
	dir := t.TempDir()
	p := seedRecovery(t, dir, nil)
	f := &daemonFixture{assignment: p, failReports: true}
	s := f.serve(t)
	defer s.Close()
	var discoveries atomic.Int32
	w := &reviewWorkload{proof: CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}}
	discover := func(PollResponse) (allocationWorkload, DiscoveryEvidence, error) {
		discoveries.Add(1)
		return w, DiscoveryEvidence{CgroupPresent: true, WorkspacePresent: true, PIDs: []int64{101, 102, 103}}, nil
	}
	first, stop := recoveryDaemon(t, s.URL, dir, discover)
	f.wait(t, func() bool { return len(f.reports) >= 2 })
	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if w.cleanups.Load() != 0 || w.starts.Load() != 0 {
		t.Fatal("discovery response loss authorized physical action")
	}
	lastSeq := first.store.state.Allocation.Seq
	f.mu.Lock()
	count := len(f.reports)
	for _, r := range f.reports {
		if r.Status != StatusRecovery || r.Discovery == nil || len(r.Discovery.PIDs) != 3 {
			t.Fatalf("discovery was replaced by launch intent: %+v", r)
		}
	}
	f.failReports = false
	f.mu.Unlock()
	second, stopSecond := recoveryDaemon(t, s.URL, dir, discover)
	f.wait(t, func() bool {
		for _, r := range f.reports[count:] {
			if r.Status == StatusCleanup {
				return true
			}
		}
		return false
	})
	if err := stopSecond(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if discoveries.Load() != 2 || w.cleanups.Load() != 1 || w.starts.Load() != 0 {
		t.Fatalf("discoveries=%d cleanups=%d starts=%d", discoveries.Load(), w.cleanups.Load(), w.starts.Load())
	}
	if second.store.state.Allocation.Terminal != terminalInterrupted || !second.store.state.Allocation.TerminalAcknowledged {
		t.Fatal("controller interruption disposition was not durable")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, r := range f.reports[count:] {
		if r.Seq != lastSeq+1+int64(i) || r.AgentIncarnation != first.Incarnation()+1 || r.RunnerEpoch != *p.RunnerEpoch {
			t.Fatalf("restart reset recovery identity/sequence: %+v", r)
		}
		if r.Status == StatusStarting || r.Status == StatusRunning || r.Status == StatusSucceeded || r.Status == StatusFailed {
			t.Fatalf("recovery fabricated execution: %+v", r)
		}
		if r.Status == StatusCleanup && !positiveCleanup(r.Cleanup) {
			t.Fatal("resolution did not submit physical cleanup proof")
		}
	}
}

func TestDaemonRecoveryRetryPrecedesPollReconciliation(t *testing.T) {
	dir := t.TempDir()
	assignment := seedRecovery(t, dir, nil)
	var requested atomic.Bool
	var recoveries atomic.Int32
	var polls atomic.Int32
	recoverySent := make(chan struct{})
	requestedPoll := make(chan struct{})
	var pollOnce sync.Once
	reconciled := make(chan struct{}, 1)
	server := reconciliationServer(t, func() PollResponse {
		if polls.Add(1) > 1 {
			select {
			case <-recoverySent:
			case <-time.After(2 * time.Second):
				t.Error("initial recovery did not arrive")
			}
		}
		p := assignment
		if requested.Load() {
			v := true
			p.ReconcileRequested = &v
			pollOnce.Do(func() { close(requestedPoll) })
		}
		return p
	}, func(r ReportRequest, w http.ResponseWriter) {
		switch r.Status {
		case StatusRecovery:
			if recoveries.Add(1) == 1 {
				// Queue quarantine reconciliation while the committed recovery's
				// acknowledgment is still in flight, then lose that response.
				requested.Store(true)
				close(recoverySent)
				select {
				case <-requestedPoll:
				case <-time.After(2 * time.Second):
					t.Error("quarantine poll did not arrive during recovery")
				}
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			fmt.Fprint(w, `{"accepted":false,"reason":"quarantined","terminal":true}`)
		case StatusReconcile:
			if recoveries.Load() != 2 {
				t.Errorf("poll replaced recovery before its disposition was learned: recoveries=%d", recoveries.Load())
			}
			data, err := os.ReadFile(filepath.Join(dir, "state.json"))
			if err != nil {
				t.Error(err)
			}
			state, err := decodeState(data)
			if err != nil || state.Allocation == nil || state.Allocation.Terminal != terminalInterrupted || !state.Allocation.TerminalAcknowledged || !positiveCleanup(state.Allocation.Cleanup) || state.Allocation.CleanupIncarnation != r.AgentIncarnation {
				t.Errorf("reconciliation preceded durable recovery disposition and fresh proof: %+v %v", state, err)
			}
			fmt.Fprint(w, `{"accepted":true,"reason":"reconcile_attested","terminal":true}`)
			reconciled <- struct{}{}
		default:
			t.Errorf("unexpected recovery report: %s", r.Status)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	defer server.Close()
	d, err := NewDaemon(Config{ControllerURL: server.URL, MachineToken: "machine-key", StateDir: dir,
		HeartbeatInterval: time.Second, RetryInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w := &reviewWorkload{proof: CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}}
	d.discover = func(PollResponse) (allocationWorkload, DiscoveryEvidence, error) {
		return w, DiscoveryEvidence{CleanupVerified: true, PIDs: []int64{}}, nil
	}
	d.prepare = func(PollResponse) (allocationWorkload, error) {
		t.Error("recovery relaunched execution")
		return w, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	select {
	case <-reconciled:
	case err := <-done:
		t.Fatalf("daemon stopped before reconciliation: %v", err)
	case <-ctx.Done():
		t.Fatal("recovery did not reach reconciliation")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if w.starts.Load() != 0 || w.cleanups.Load() != 0 {
		t.Fatalf("already-clean recovery performed physical work: starts=%d cleanups=%d", w.starts.Load(), w.cleanups.Load())
	}
}

func TestDaemonCleanCheckpointWithoutControllerTerminalRemainsUnresolved(t *testing.T) {
	dir := t.TempDir()
	assignment := seedRecovery(t, dir, nil)
	reconciled := make(chan struct{}, 1)
	server := reconciliationServer(t, func() PollResponse { return assignment }, func(r ReportRequest, w http.ResponseWriter) {
		switch r.Status {
		case StatusRecovery:
			fmt.Fprint(w, `{"accepted":false,"reason":"quarantined","terminal":false}`)
		case StatusReconcile:
			fmt.Fprint(w, `{"accepted":true,"reason":"reconcile_quarantined","terminal":false}`)
			reconciled <- struct{}{}
		case StatusHeartbeat:
			fmt.Fprint(w, `{"accepted":true,"reason":"ok","terminal":false}`)
		default:
			t.Errorf("unknown disposition authorized %s", r.Status)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	defer server.Close()
	w := &reviewWorkload{proof: CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}}
	d, stop := recoveryDaemon(t, server.URL, dir, func(PollResponse) (allocationWorkload, DiscoveryEvidence, error) {
		return w, DiscoveryEvidence{CleanupVerified: true, PIDs: []int64{}}, nil
	})
	var stopOnce sync.Once
	stopDaemon := func() {
		stopOnce.Do(func() {
			if err := stop(); !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		})
	}
	defer stopDaemon()
	select {
	case <-reconciled:
	case <-time.After(3 * time.Second):
		t.Fatal("reconciliation report did not arrive")
	}
	stopDaemon()
	a := d.store.state.Allocation
	if a.Terminal != "" || a.TerminalAcknowledged || a.Cleanup != nil || a.CleanupAcknowledged || w.cleanups.Load() != 0 || w.starts.Load() != 0 {
		t.Fatalf("physical checkpoint fabricated terminal or cleanup authority: state=%+v starts=%d cleanups=%d", a, w.starts.Load(), w.cleanups.Load())
	}
}

func TestDaemonContradictoryDiscoveryNeverCleansOrReplays(t *testing.T) {
	for _, terminal := range []ReportStatus{"", StatusSucceeded, terminalInterrupted} {
		t.Run(string(terminal), func(t *testing.T) {
			dir := t.TempDir()
			p := seedRecovery(t, dir, func(a *allocationState) {
				a.Terminal, a.TerminalAcknowledged = terminal, terminal != ""
			})
			f := &daemonFixture{assignment: p}
			s := f.serve(t)
			defer s.Close()
			w := &reviewWorkload{}
			d, err := NewDaemon(Config{ControllerURL: s.URL, MachineToken: "machine-key", StateDir: dir})
			if err != nil {
				t.Fatal(err)
			}
			d.prepare = func(PollResponse) (allocationWorkload, error) {
				t.Error("contradictory discovery prepared execution")
				return w, nil
			}
			d.discover = func(PollResponse) (allocationWorkload, DiscoveryEvidence, error) {
				return w, DiscoveryEvidence{CgroupPresent: true, WorkspacePresent: true}, errors.New("directory identity changed")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := d.Run(ctx); err == nil || !strings.Contains(err.Error(), "recovery unresolved") {
				t.Fatalf("contradictory evidence was ignored: %v", err)
			}
			if w.starts.Load() != 0 || w.cleanups.Load() != 0 || d.store.state.Allocation.Cleanup != nil {
				t.Fatal("contradictory discovery authorized cleanup or execution")
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if len(f.reports) != 1 || f.reports[0].Status != StatusRecovery || f.reports[0].Discovery.Error == nil || *f.reports[0].Discovery.Error != "directory identity changed" {
				t.Fatalf("discovery failure not reported for quarantine: %+v", f.reports)
			}
		})
	}
}

func TestInterruptedStateRequiresControllerAcknowledgment(t *testing.T) {
	dir := t.TempDir()
	seedRecovery(t, dir, func(a *allocationState) { a.Terminal, a.TerminalAcknowledged = terminalInterrupted, true })
	s, err := openState(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.state.Allocation.Terminal != terminalInterrupted || !s.state.Allocation.TerminalAcknowledged {
		t.Fatal("interrupted disposition did not survive restart")
	}
	next := s.state
	a := *next.Allocation
	next.Allocation = &a
	a.TerminalAcknowledged = false
	if err := s.save(next); err == nil {
		t.Fatal("stored an interruption without controller authority")
	}
}
