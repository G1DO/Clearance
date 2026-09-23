package agent

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type reviewWorkload struct {
	waitGate         chan struct{}
	waitStarted      chan struct{}
	waitErr          error
	proof            CleanupEvidence
	cleanupErr       error
	starts, cleanups atomic.Int32
}

func (w *reviewWorkload) Start() error { w.starts.Add(1); return nil }
func (w *reviewWorkload) Wait() error {
	if w.waitStarted != nil {
		close(w.waitStarted)
	}
	<-w.waitGate
	return w.waitErr
}
func (w *reviewWorkload) Cleanup() (CleanupEvidence, error) {
	w.cleanups.Add(1)
	return w.proof, w.cleanupErr
}

func TestDaemonNegativeCleanupPreservesResultAndRefusesNextAllocation(t *testing.T) {
	f := &daemonFixture{assignment: testAssignment("/bin/true")}
	server := f.serve(t)
	defer server.Close()
	d, err := NewDaemon(Config{ControllerURL: server.URL, MachineToken: "machine-key", StateDir: t.TempDir(), HeartbeatInterval: 10 * time.Millisecond, RetryInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	close(gate)
	w := &reviewWorkload{waitGate: gate, proof: CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true}, cleanupErr: errors.New("workspace remained mounted")}
	d.prepare = func(PollResponse) (allocationWorkload, error) { return w, nil }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	f.wait(t, func() bool {
		for _, r := range f.reports {
			if r.Status == StatusCleanup {
				return true
			}
		}
		return false
	})
	f.mu.Lock()
	id, epoch := "33333333-3333-3333-3333-333333333333", int64(2)
	f.assignment.AllocationID, f.assignment.RunnerEpoch = &id, &epoch
	f.mu.Unlock()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "verified prior cleanup") {
			t.Fatalf("replacement after negative proof was accepted: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("daemon did not refuse replacement after failed cleanup")
	}
	if got := d.store.state.Allocation.Terminal; got != StatusSucceeded {
		t.Fatalf("cleanup failure rewrote success to %s", got)
	}
	if w.starts.Load() != 1 {
		t.Fatalf("quarantined runner executed %d times", w.starts.Load())
	}
	if positiveCleanup(d.store.state.Allocation.Cleanup) {
		t.Fatal("cleanup error was accepted as positive evidence")
	}
}

func TestDaemonRestartRevalidatesUnacknowledgedCleanupUnderNewIncarnation(t *testing.T) {
	for _, terminalAcknowledged := range []bool{false, true} {
		t.Run(map[bool]string{false: "terminal_ack_lost", true: "cleanup_ack_lost"}[terminalAcknowledged], func(t *testing.T) {
			dir := t.TempDir()
			store, err := openState(dir)
			if err != nil {
				t.Fatal(err)
			}
			assignment := testAssignment("/bin/true")
			previousIncarnation := store.state.Incarnation
			next := store.state
			next.Allocation = &allocationState{Assignment: assignment, Seq: 9, Started: true, Terminal: StatusSucceeded,
				TerminalAcknowledged: terminalAcknowledged, Cleanup: &CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}, CleanupIncarnation: previousIncarnation}
			if err := store.save(next); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			f := &daemonFixture{assignment: assignment}
			server := f.serve(t)
			defer server.Close()
			d, err := NewDaemon(Config{ControllerURL: server.URL, MachineToken: "machine-key", StateDir: dir, HeartbeatInterval: 10 * time.Millisecond, RetryInterval: 10 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			w := &reviewWorkload{cleanupErr: errors.New("restart inspection failed")}
			d.prepare = func(PollResponse) (allocationWorkload, error) {
				t.Error("restart prepared a new execution")
				return w, nil
			}
			d.discover = func(PollResponse) (allocationWorkload, DiscoveryEvidence, error) {
				return w, DiscoveryEvidence{CgroupPresent: true, WorkspacePresent: true, PIDs: []int64{123}}, nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- d.Run(ctx) }()
			f.wait(t, func() bool {
				for _, r := range f.reports {
					if r.Status == StatusCleanup {
						return true
					}
				}
				return false
			})
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if w.starts.Load() != 0 || w.cleanups.Load() != 1 {
				t.Fatalf("restart replayed execution or skipped reinspection: starts=%d cleanups=%d", w.starts.Load(), w.cleanups.Load())
			}
			if d.store.state.Allocation.Terminal != StatusSucceeded || positiveCleanup(d.store.state.Allocation.Cleanup) {
				t.Fatal("reinspection changed result or retained stale positive evidence")
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			for i, report := range f.reports {
				if report.Seq != int64(i+10) || report.AgentIncarnation != previousIncarnation+1 {
					t.Fatalf("restart lost fencing/sequence: %+v", report)
				}
				if report.Status == StatusCleanup && (positiveCleanup(report.Cleanup) || report.Cleanup.Error == nil || !strings.Contains(*report.Cleanup.Error, "inspection")) {
					t.Fatalf("old-incarnation proof reused: %+v", report)
				}
			}
		})
	}
}

func TestDaemonFailedTerminationDoesNotBlockShutdownOrRewriteTimeout(t *testing.T) {
	for _, scenario := range []string{"shutdown", "deadline"} {
		t.Run(scenario, func(t *testing.T) {
			gate := make(chan struct{})
			defer close(gate) // Release the test's deliberately unkillable waiter.
			w := &reviewWorkload{waitGate: gate, cleanupErr: errors.New("SIGKILL failed")}
			d := &Daemon{prepare: func(PollResponse) (allocationWorkload, error) { return w, nil }}
			p := testAssignment("/bin/true")
			ms := int64(20)
			p.WorkloadTimeoutMs = &ms
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			events := make(chan executionEvent, 3)
			done := make(chan struct{})
			go func() { defer close(done); d.execute(ctx, p, make(chan struct{}), events) }()
			select {
			case event := <-events:
				if !event.running {
					t.Fatalf("did not start: %+v", event)
				}
			case <-time.After(time.Second):
				t.Fatal("execution did not start")
			}
			if scenario == "shutdown" {
				cancel()
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("failed termination blocked allocation worker")
			}
			if w.cleanups.Load() != 1 {
				t.Fatalf("cleanup calls=%d", w.cleanups.Load())
			}
			if scenario == "deadline" {
				terminal, cleanup := <-events, <-events
				if terminal.terminal != StatusTimedOut || cleanup.cleanup == nil || positiveCleanup(cleanup.cleanup) || cleanup.cleanup.Error == nil {
					t.Fatalf("cleanup failure rewrote timeout or fabricated proof: terminal=%+v cleanup=%+v", terminal, cleanup)
				}
			}
		})
	}
}

func TestDaemonPrelaunchCancellationSurvivesContainmentFailure(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "no_handle", true: "partial_handle"}[partial], func(t *testing.T) {
			failure := errors.New("cannot establish allocation cgroup")
			w := &reviewWorkload{cleanupErr: failure}
			d := &Daemon{prepare: func(PollResponse) (allocationWorkload, error) {
				if partial {
					return w, failure
				}
				return nil, failure
			}}
			stop := make(chan struct{})
			close(stop)
			events := make(chan executionEvent, 2)
			d.execute(context.Background(), testAssignment("/bin/true"), stop, events)
			terminal, cleanup := <-events, <-events
			if terminal.terminal != StatusCancelled || cleanup.cleanup == nil || positiveCleanup(cleanup.cleanup) || cleanup.cleanup.Error == nil {
				t.Fatalf("containment failure rewrote cancellation or fabricated cleanup: terminal=%+v cleanup=%+v", terminal, cleanup)
			}
			if w.starts.Load() != 0 {
				t.Fatal("cancelled workload started")
			}
			if partial && w.cleanups.Load() != 1 {
				t.Fatal("partial preparation was not cleaned")
			}
		})
	}
}

func TestDaemonExpiredDeadlineWinsReadyCancellation(t *testing.T) {
	for i := 0; i < 10; i++ {
		waitGate := make(chan struct{})
		w := &reviewWorkload{waitGate: waitGate, waitStarted: make(chan struct{}), cleanupErr: errors.New("test termination failure")}
		d := &Daemon{prepare: func(PollResponse) (allocationWorkload, error) { return w, nil }}
		p := testAssignment("/bin/true")
		ms := int64(10)
		p.WorkloadTimeoutMs = &ms
		stop := make(chan struct{})
		events := make(chan executionEvent)
		finished := make(chan struct{})
		go func() { d.execute(context.Background(), p, stop, events); close(finished) }()
		// Hold the RUNNING observation while the independent timer expires, then
		// make cancellation ready too before the worker chooses its stop reason.
		select {
		case <-w.waitStarted:
		case <-time.After(time.Second):
			t.Fatal("workload timer did not start")
		}
		time.Sleep(30 * time.Millisecond)
		close(stop)
		if e := <-events; !e.running {
			t.Fatalf("expected running: %+v", e)
		}
		if e := <-events; e.terminal != StatusTimedOut {
			t.Fatalf("simultaneous stop changed expired deadline to %s", e.terminal)
		}
		<-events
		<-finished
		close(waitGate)
	}
}
