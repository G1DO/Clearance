package agent

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDaemonStartsNewerAllocationAfterTerminal(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "executions")
	first := testAssignment("/bin/sh", "-c", `printf '%s' "$2" >> "$1"`, "agent-test", marker, "a")
	second := testAssignment("/bin/sh", "-c", `printf '%s' "$2" >> "$1"`, "agent-test", marker, "b")
	*second.AllocationID = "33333333-3333-3333-3333-333333333333"
	*second.JobID = "44444444-4444-4444-4444-444444444444"
	*second.RunnerEpoch = 2
	f := &daemonFixture{assignment: first}
	server := f.serve(t)
	defer server.Close()
	d, stop := runTestDaemon(t, server.URL, t.TempDir())
	completed := func(id string) bool {
		terminal := false
		for _, report := range f.reports {
			if report.AllocationID == id {
				terminal = terminal || report.Status == StatusSucceeded
				if terminal && report.Status == StatusHeartbeat {
					return true
				}
			}
		}
		return false
	}
	f.wait(t, func() bool { return completed(*first.AllocationID) })
	f.mu.Lock()
	f.assignment = second
	f.mu.Unlock()
	f.wait(t, func() bool { return completed(*second.AllocationID) })
	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	data, err := os.ReadFile(marker)
	if err != nil || string(data) != "ab" {
		t.Fatalf("allocations did not execute once each: %q, %v", data, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := make(map[string]int64)
	for _, report := range f.reports {
		if seen[report.AllocationID] == 0 && (report.Seq != 1 || report.Status != StatusStarting) {
			t.Fatalf("allocation did not start its own sequence at 1: %+v", report)
		}
		if report.Seq <= seen[report.AllocationID] || report.AgentIncarnation != d.Incarnation() {
			t.Fatalf("allocation sequence or incarnation changed incorrectly: %+v", report)
		}
		wantEpoch := int64(1)
		if report.AllocationID == *second.AllocationID {
			wantEpoch = 2
		}
		if report.RunnerEpoch != wantEpoch {
			t.Fatalf("report retained the previous allocation epoch: %+v", report)
		}
		seen[report.AllocationID] = report.Seq
	}
	if len(seen) != 2 || d.store.state.Allocation == nil || *d.store.state.Allocation.Assignment.AllocationID != *second.AllocationID {
		t.Fatal("durable state did not retain only the newer allocation")
	}
}

func TestDaemonLargePollHintDoesNotOverflowAndIsCancelable(t *testing.T) {
	hint := int64(math.MaxInt64)
	f := &daemonFixture{assignment: PollResponse{PollAfterMs: &hint}}
	server := f.serve(t)
	defer server.Close()
	_, stop := runTestDaemon(t, server.URL, t.TempDir())
	f.wait(t, func() bool { return f.polls == 1 })
	// A wrapped duration would fall back to the 20 ms retry interval. The
	// saturated hint must keep the poller asleep while remaining cancelable.
	timer := time.NewTimer(150 * time.Millisecond)
	defer timer.Stop()
	<-timer.C
	f.mu.Lock()
	polls := f.polls
	f.mu.Unlock()
	if polls != 1 {
		t.Fatalf("poll hint overflow caused early retries: %d polls", polls)
	}
	start := time.Now()
	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("shutdown waited for the saturated poll delay")
	}
}
