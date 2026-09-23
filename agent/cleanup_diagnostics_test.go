package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestDaemonCleanupDiagnosticsPersistAndReport(t *testing.T) {
	const prefix = "workspace cleanup: "
	boundaryPrefix := prefix + strings.Repeat("x", 2047-len(prefix))
	for _, diagnostic := range []struct{ name, message, want string }{
		{"invalid_utf8", prefix + "bad-\xff-name", prefix + "bad-\ufffd-name"},
		{"multibyte_boundary", boundaryPrefix + "€ trailing diagnostic", boundaryPrefix},
	} {
		for _, scenario := range []struct {
			name                       string
			cancelled, failed, restart bool
			terminal                   ReportStatus
		}{
			{name: "completion", terminal: StatusSucceeded},
			{name: "cancelled_before_launch", cancelled: true, failed: true, terminal: StatusCancelled},
			{name: "failed_prepare", failed: true, terminal: StatusFailed},
			{name: "restart_revalidation", restart: true, terminal: StatusSucceeded},
		} {
			t.Run(scenario.name+"/"+diagnostic.name, func(t *testing.T) {
				dir := t.TempDir()
				assignment := testAssignment("/bin/true")
				if scenario.cancelled {
					cancelled := true
					assignment.CancelRequested = &cancelled
				}
				if scenario.restart {
					store, err := openState(dir)
					if err != nil {
						t.Fatal(err)
					}
					next := store.state
					next.Allocation = &allocationState{Assignment: assignment, Seq: 9, Started: true,
						Terminal: StatusSucceeded, TerminalAcknowledged: true,
						Cleanup:            &CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true},
						CleanupIncarnation: next.Incarnation}
					if err := store.save(next); err != nil {
						_ = store.Close()
						t.Fatal(err)
					}
					if err := store.Close(); err != nil {
						t.Fatal(err)
					}
				}
				f := &daemonFixture{assignment: assignment}
				server := f.serve(t)
				defer server.Close()
				d, err := NewDaemon(Config{ControllerURL: server.URL, MachineToken: "machine-key", StateDir: dir,
					HeartbeatInterval: 10 * time.Millisecond, RetryInterval: 10 * time.Millisecond})
				if err != nil {
					t.Fatal(err)
				}
				gate := make(chan struct{})
				close(gate)
				failure := errors.New(diagnostic.message)
				w := &reviewWorkload{waitGate: gate, cleanupErr: failure,
					proof: CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true}}
				if scenario.restart || (scenario.failed && !scenario.cancelled) {
					previous := "previous cleanup diagnostic"
					w.proof.Error = &previous
				}
				d.prepare = func(PollResponse) (allocationWorkload, error) {
					if scenario.failed {
						return w, failure
					}
					return w, nil
				}
				ctx, cancel := context.WithCancel(context.Background())
				finished := make(chan struct{})
				var runErr error
				go func() { runErr = d.Run(ctx); close(finished) }()
				defer func() {
					cancel()
					select {
					case <-finished:
					case <-time.After(3 * time.Second):
						t.Error("daemon did not stop")
					}
				}()
				var reported *CleanupEvidence
				timer := time.NewTimer(5 * time.Second)
				defer timer.Stop()
				for reported == nil {
					f.mu.Lock()
					for _, report := range f.reports {
						if report.Status == StatusCleanup {
							reported = report.Cleanup
						}
					}
					f.mu.Unlock()
					if reported != nil {
						break
					}
					select {
					case <-f.notify:
					case <-finished:
						t.Fatalf("daemon stopped before reporting cleanup: %v", runErr)
					case <-timer.C:
						t.Fatal("daemon did not report cleanup")
					}
				}
				cancel()
				<-finished
				if !errors.Is(runErr, context.Canceled) {
					t.Fatalf("daemon stopped unexpectedly: %v", runErr)
				}
				store, err := openState(dir)
				if err != nil {
					t.Fatalf("reopen persisted cleanup: %v", err)
				}
				defer store.Close()
				for _, proof := range []*CleanupEvidence{reported, store.state.Allocation.Cleanup} {
					if proof == nil || positiveCleanup(proof) || proof.Error == nil {
						t.Fatalf("cleanup lost negative evidence: %+v", proof)
					}
					if !utf8.ValidString(*proof.Error) || len(*proof.Error) > 2048 || *proof.Error != diagnostic.want {
						t.Fatalf("cleanup diagnostic did not survive persistence/reporting: got %.80q (%d bytes), want %.80q (%d bytes)",
							*proof.Error, len(*proof.Error), diagnostic.want, len(diagnostic.want))
					}
				}
				if got := store.state.Allocation.Terminal; got != scenario.terminal {
					t.Fatalf("cleanup changed execution result: got %s, want %s", got, scenario.terminal)
				}
				wantStarts := int32(0)
				if !scenario.failed && !scenario.restart {
					wantStarts = 1
				}
				if w.starts.Load() != wantStarts || w.cleanups.Load() != 1 {
					t.Fatalf("unexpected execution: starts=%d cleanups=%d", w.starts.Load(), w.cleanups.Load())
				}
			})
		}
	}
}
