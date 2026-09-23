package agent

import (
	"context"
	"errors"
	"strings"
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
