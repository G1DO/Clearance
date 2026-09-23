package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testAssignment(argv ...string) PollResponse {
	id, job, class, epoch := "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222", "default", int64(1)
	return PollResponse{Assigned: true, AllocationID: &id, JobID: &job, RunnerEpoch: &epoch, RunnerClass: &class, Argv: argv}
}

type daemonFixture struct {
	mu                   sync.Mutex
	assignment           PollResponse
	reports              []ReportRequest
	polls                int
	dropPoll, dropReport bool
	failReports          bool
	reject               string
	notify               chan struct{}
}

func (f *daemonFixture) serve(t *testing.T) *httptest.Server {
	t.Helper()
	f.notify = make(chan struct{}, 1)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		defer func() {
			select {
			case f.notify <- struct{}{}:
			default:
			}
		}()
		if r.Header.Get("Authorization") != "Bearer machine-key" {
			t.Error("missing machine identity")
		}
		if r.URL.Path == "/internal/v1/agents/poll" {
			f.polls++
			if f.dropPoll {
				f.dropPoll = false
				w.WriteHeader(503)
				return
			}
			wire, err := EncodePollResponse(f.assignment)
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			w.Write(wire)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		report, err := ParseReportRequest(body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		f.reports = append(f.reports, report)
		if f.dropReport || f.failReports {
			f.dropReport = false
			w.WriteHeader(503)
			return
		}
		if f.reject != "" {
			fmt.Fprintf(w, `{"accepted":false,"reason":%q,"terminal":false}`, f.reject)
			return
		}
		fmt.Fprint(w, `{"accepted":true,"reason":"ok","terminal":false}`)
	}))
}

func (f *daemonFixture) wait(t *testing.T, condition func() bool) {
	t.Helper()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for {
		f.mu.Lock()
		ok := condition()
		f.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-f.notify:
		case <-timeout.C:
			t.Fatal("timed out waiting for daemon")
		}
	}
}

func runTestDaemon(t *testing.T, url, dir string) (*Daemon, func() error) {
	t.Helper()
	d, err := NewDaemon(Config{ControllerURL: url, MachineToken: "machine-key", StateDir: dir, HeartbeatInterval: 20 * time.Millisecond, RetryInterval: 20 * time.Millisecond, ReportTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	useTestExecution(t, d)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	var once sync.Once
	var result error
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case result = <-done:
			case <-time.After(3 * time.Second):
				t.Error("shutdown did not join workers")
			}
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		})
		return result
	}
	t.Cleanup(func() { stop() })
	return d, stop
}

func TestDaemonDroppedRepliesAndTerminalRestart(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(t.TempDir(), "executions")
	f := &daemonFixture{assignment: testAssignment("/bin/sh", "-c", `printf x >> "$1"`, "agent-test", marker), dropPoll: true, dropReport: true}
	server := f.serve(t)
	defer server.Close()
	d, stop := runTestDaemon(t, server.URL, dir)
	f.wait(t, func() bool {
		terminal := false
		for _, r := range f.reports {
			terminal = terminal || r.Status == StatusSucceeded
		}
		return terminal && f.reports[len(f.reports)-1].Status == StatusCleanup
	})
	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	f.mu.Lock()
	firstCount := len(f.reports)
	if f.polls < 2 {
		t.Error("dropped poll was not retried")
	}
	if f.reports[0].Status != StatusStarting || f.reports[1].Status != StatusStarting {
		t.Error("lost starting ack was not retried before execution")
	}
	f.mu.Unlock()
	second, stopSecond := runTestDaemon(t, server.URL, dir)
	if second.Incarnation() != d.Incarnation()+1 {
		t.Fatal("incarnation reused")
	}
	f.mu.Lock()
	previousPolls := f.polls
	f.mu.Unlock()
	f.wait(t, func() bool { return f.polls >= previousPolls+2 })
	stopSecond()
	data, err := os.ReadFile(marker)
	if err != nil || string(data) != "x" {
		t.Fatalf("duplicate/missing execution: %q %v", data, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	terminalSeen := false
	for i, report := range f.reports {
		if report.Seq != int64(i+1) || report.RunnerEpoch != 1 || report.AllocationID != *f.assignment.AllocationID {
			t.Fatalf("invalid fencing: %+v", report)
		}
		if i < firstCount && report.AgentIncarnation != d.Incarnation() || i >= firstCount && report.AgentIncarnation != second.Incarnation() {
			t.Fatal("stale incarnation")
		}
		if report.Status == StatusSucceeded {
			terminalSeen = true
		}
		if terminalSeen && (report.Status == StatusStarting || report.Status == StatusRunning) {
			t.Fatal("terminal regressed")
		}
		if i >= firstCount && report.Status != StatusCleanup {
			t.Fatal("completed command replayed after restart")
		}
	}
	if !terminalSeen {
		t.Fatal("terminal result missing")
	}
}

func TestDaemonUncertainRestartNeverReexecutes(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(t.TempDir(), "executions")
	f := &daemonFixture{assignment: testAssignment("/bin/sh", "-c", `printf x >> "$1"; exec sleep 60`, "agent-test", marker)}
	server := f.serve(t)
	defer server.Close()
	_, stop := runTestDaemon(t, server.URL, dir)
	f.wait(t, func() bool { data, _ := os.ReadFile(marker); return string(data) == "x" && len(f.reports) >= 3 })
	stop()
	f.mu.Lock()
	count := len(f.reports)
	f.mu.Unlock()
	_, stopSecond := runTestDaemon(t, server.URL, dir)
	f.wait(t, func() bool { return len(f.reports) >= count+2 })
	stopSecond()
	data, err := os.ReadFile(marker)
	if err != nil || string(data) != "x" {
		t.Fatalf("uncertain work replayed: %q %v", data, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, report := range f.reports[count:] {
		if report.Status != StatusHeartbeat {
			t.Fatalf("invented result for uncertain execution: %+v", report)
		}
	}
}

func TestDaemonExecutionFailureAndFencing(t *testing.T) {
	for _, reject := range []string{"", "fenced_rejected", "dropped_stale", "future_reason"} {
		t.Run("reject_"+reject, func(t *testing.T) {
			f := &daemonFixture{assignment: testAssignment("/nonexistent-clearance-command"), reject: reject}
			server := f.serve(t)
			defer server.Close()
			if reject == "" {
				_, stop := runTestDaemon(t, server.URL, t.TempDir())
				f.wait(t, func() bool {
					for _, r := range f.reports {
						if r.Status == StatusFailed {
							return true
						}
					}
					return false
				})
				stop()
			} else {
				d, err := NewDaemon(Config{ControllerURL: server.URL, MachineToken: "machine-key", StateDir: t.TempDir()})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := d.Run(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("rejection ignored: %v", err)
				}
				f.mu.Lock()
				count := len(f.reports)
				f.mu.Unlock()
				if count != 1 {
					t.Fatalf("sent after rejection: %d", count)
				}
			}
		})
	}
}

func TestDaemonShutdownCancelsLongPollAndReport(t *testing.T) {
	for _, blocked := range []string{"poll", "report"} {
		t.Run(blocked, func(t *testing.T) {
			entered := make(chan struct{}, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				if blocked == "report" && r.URL.Path == "/internal/v1/agents/poll" {
					body, _ := EncodePollResponse(testAssignment("/usr/bin/true"))
					w.Write(body)
					return
				}
				select {
				case entered <- struct{}{}:
				default:
				}
				<-r.Context().Done()
			}))
			defer server.Close()
			dir := t.TempDir()
			_, stop := runTestDaemon(t, server.URL, dir)
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("no HTTP call")
			}
			start := time.Now()
			if err := stop(); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if time.Since(start) > time.Second {
				t.Fatal("shutdown did not cancel request promptly")
			}
			// A joined shutdown also releases the durable process lock.
			reopened, err := NewDaemon(Config{ControllerURL: server.URL, MachineToken: "machine-key", StateDir: dir})
			if err != nil {
				t.Fatal(err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDaemonRequiresDurableSequenceBeforeSending(t *testing.T) {
	for _, scenario := range []string{"exhausted", "persistence_failure"} {
		t.Run(scenario, func(t *testing.T) {
			f := &daemonFixture{assignment: testAssignment("/usr/bin/true")}
			server := f.serve(t)
			defer server.Close()
			d, err := NewDaemon(Config{ControllerURL: server.URL, MachineToken: "machine-key", StateDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			next := d.store.state
			next.Allocation = &allocationState{Assignment: f.assignment, Seq: 1, Started: true}
			if scenario == "exhausted" {
				next.Allocation.Seq = math.MaxInt64
			}
			if err := d.store.save(next); err != nil {
				t.Fatal(err)
			}
			if scenario == "persistence_failure" {
				// File data can be written, but the reservation cannot be made durable.
				if err := d.store.directory.Close(); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := d.Run(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("did not fail closed: %v", err)
			}
			f.mu.Lock()
			count := len(f.reports)
			f.mu.Unlock()
			if count != 0 {
				t.Fatal("sent an unreserved/reused sequence")
			}
		})
	}
}

func TestDaemonRejectsChangedUnfinishedAllocation(t *testing.T) {
	for _, scenario := range []string{"argv", "epoch", "new_allocation"} {
		t.Run(scenario, func(t *testing.T) {
			old := testAssignment("/usr/bin/true")
			changed := testAssignment("/usr/bin/true")
			switch scenario {
			case "argv":
				changed.Argv = []string{"/bin/false"}
			case "epoch":
				*changed.RunnerEpoch = 2
			case "new_allocation":
				*changed.AllocationID = "33333333-3333-3333-3333-333333333333"
				*changed.RunnerEpoch = 2
			}
			f := &daemonFixture{assignment: changed}
			server := f.serve(t)
			defer server.Close()
			d, err := NewDaemon(Config{ControllerURL: server.URL, MachineToken: "machine-key", StateDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			next := d.store.state
			next.Allocation = &allocationState{Assignment: old, Seq: 1, Started: true}
			if err := d.store.save(next); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := d.Run(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("accepted changed ownership: %v", err)
			}
			f.mu.Lock()
			count := len(f.reports)
			f.mu.Unlock()
			if count != 0 {
				t.Fatal("reported changed ownership as current")
			}
		})
	}
}

func TestDaemonResendsUnacknowledgedTerminalAfterRestart(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(t.TempDir(), "executions")
	assignment := testAssignment("/bin/sh", "-c", `printf x >> "$1"`, "agent-test", marker)
	terminalSent := make(chan ReportRequest, 1)
	resent := make(chan ReportRequest, 1)
	var mu sync.Mutex
	var heldIncarnation int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		if r.URL.Path == "/internal/v1/agents/poll" {
			body, _ := EncodePollResponse(assignment)
			w.Write(body)
			return
		}
		report, err := ParseReportRequest(data)
		if err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if report.Status == StatusSucceeded {
			mu.Lock()
			if heldIncarnation == 0 {
				heldIncarnation = report.AgentIncarnation
			}
			hold := heldIncarnation == report.AgentIncarnation
			mu.Unlock()
			if hold {
				select {
				case terminalSent <- report:
				default:
				}
				<-r.Context().Done() // Simulate acceptance followed by a lost acknowledgment.
				return
			}
			select {
			case resent <- report:
			default:
			}
		}
		fmt.Fprint(w, `{"accepted":true,"reason":"ok","terminal":true}`)
	}))
	defer server.Close()
	first, stop := runTestDaemon(t, server.URL, dir)
	var original ReportRequest
	select {
	case original = <-terminalSent:
	case <-time.After(3 * time.Second):
		t.Fatal("terminal report missing")
	}
	stop()
	lastReserved := first.store.state.Allocation.Seq
	_, stopSecond := runTestDaemon(t, server.URL, dir)
	select {
	case retried := <-resent:
		if retried.Seq != lastReserved+1 || retried.AgentIncarnation != original.AgentIncarnation+1 || retried.AllocationID != original.AllocationID || retried.RunnerEpoch != original.RunnerEpoch {
			t.Fatalf("terminal retry changed fencing/sequence: %+v -> %+v", original, retried)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("durable terminal result was not resent")
	}
	stopSecond()
	data, err := os.ReadFile(marker)
	if err != nil || string(data) != "x" {
		t.Fatalf("replayed terminal allocation: %q %v", data, err)
	}
}
