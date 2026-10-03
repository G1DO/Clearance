package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func idleTestDaemon(t *testing.T, cgroupRoot, workspaceRoot, stateDir string) *Daemon {
	t.Helper()
	d, err := NewDaemon(Config{
		ControllerURL: "http://localhost",
		MachineToken:  "test-token",
		StateDir:      stateDir,
		CgroupRoot:    cgroupRoot,
		WorkspaceRoot: workspaceRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func TestInspectIdleClean(t *testing.T) {
	cgroup := filepath.Join(t.TempDir(), "cg")
	if err := os.Mkdir(cgroup, 0700); err != nil {
		t.Fatal(err)
	}
	d := idleTestDaemon(t, cgroup, filepath.Join(t.TempDir(), "ws-missing"), t.TempDir())
	evidence, err := d.inspectIdle()
	if err != nil {
		t.Fatal(err)
	}
	if evidence.CgroupPresent || evidence.WorkspacePresent || len(evidence.PIDs) != 0 ||
		!evidence.ExecutionEmpty || !evidence.DescendantsReaped || !evidence.WorkspaceClean ||
		evidence.Error != nil || evidence.ObservedAllocationID != nil {
		t.Fatalf("clean idle inspection not positive: %+v", evidence)
	}
}

func TestInspectIdleDirtyWorkspace(t *testing.T) {
	cgroup := filepath.Join(t.TempDir(), "cg")
	if err := os.Mkdir(cgroup, 0700); err != nil {
		t.Fatal(err)
	}
	workspace := filepath.Join(t.TempDir(), "ws")
	if err := os.Mkdir(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "leftover"), []byte("dirty"), 0600); err != nil {
		t.Fatal(err)
	}
	d := idleTestDaemon(t, cgroup, workspace, t.TempDir())
	evidence, err := d.inspectIdle()
	if err != nil {
		t.Fatal(err)
	}
	if evidence.CgroupPresent || !evidence.WorkspacePresent || evidence.WorkspaceClean || evidence.Error != nil {
		t.Fatalf("dirty workspace not reported: %+v", evidence)
	}
	if !evidence.ExecutionEmpty || !evidence.DescendantsReaped || len(evidence.PIDs) != 0 {
		t.Fatalf("workspace dirt must not invent processes: %+v", evidence)
	}
}

func TestInspectIdleForeignExecution(t *testing.T) {
	cgroup := filepath.Join(t.TempDir(), "cg")
	foreign := "33333333-3333-3333-3333-333333333333-9"
	if err := os.MkdirAll(filepath.Join(cgroup, foreign), 0700); err != nil {
		t.Fatal(err)
	}
	// The test process itself is live; observing it exercises the /proc path.
	if err := os.WriteFile(filepath.Join(cgroup, foreign, "cgroup.procs"),
		[]byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	d := idleTestDaemon(t, cgroup, filepath.Join(t.TempDir(), "ws-missing"), t.TempDir())
	evidence, err := d.inspectIdle()
	if err != nil {
		t.Fatal(err)
	}
	if !evidence.CgroupPresent || len(evidence.PIDs) != 1 || evidence.ExecutionEmpty ||
		evidence.Error != nil || evidence.ObservedAllocationID == nil ||
		*evidence.ObservedAllocationID != "33333333-3333-3333-3333-333333333333" {
		t.Fatalf("foreign execution not reported: %+v", evidence)
	}
}

func TestInspectIdleOwnExecutionHasNoForeignIdentity(t *testing.T) {
	cgroup := filepath.Join(t.TempDir(), "cg")
	own := "11111111-1111-1111-1111-111111111111-3"
	if err := os.MkdirAll(filepath.Join(cgroup, own), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cgroup, own, "cgroup.procs"),
		[]byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	// Non-pattern directories (for example a harness membership dir) are not
	// allocation execution and must not block attestation.
	if err := os.Mkdir(filepath.Join(cgroup, "agent"), 0700); err != nil {
		t.Fatal(err)
	}
	allocID := "11111111-1111-1111-1111-111111111111"
	epoch := int64(3)
	d := idleTestDaemon(t, cgroup, filepath.Join(t.TempDir(), "ws-missing"), t.TempDir())
	d.store.state.Allocation = &allocationState{
		Assignment: PollResponse{Assigned: true, AllocationID: &allocID, RunnerEpoch: &epoch},
	}
	evidence, err := d.inspectIdle()
	if err != nil {
		t.Fatal(err)
	}
	if !evidence.CgroupPresent || len(evidence.PIDs) != 1 || evidence.ObservedAllocationID != nil {
		t.Fatalf("own execution misclassified: %+v", evidence)
	}
}

func TestInspectIdleMissingCgroupRootFailsClosed(t *testing.T) {
	d := idleTestDaemon(t, filepath.Join(t.TempDir(), "no-such-cg"), filepath.Join(t.TempDir(), "ws"), t.TempDir())
	evidence, err := d.inspectIdle()
	if err == nil || evidence.Error == nil {
		t.Fatal("missing cgroup root did not fail closed")
	}
	if evidence.ExecutionEmpty || evidence.DescendantsReaped || evidence.WorkspaceClean {
		t.Fatalf("broken inspection yielded positive claims: %+v", evidence)
	}
}

func TestInspectIdleInvalidProcsFailsClosed(t *testing.T) {
	cgroup := filepath.Join(t.TempDir(), "cg")
	bad := "22222222-2222-2222-2222-222222222222-1"
	if err := os.MkdirAll(filepath.Join(cgroup, bad), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cgroup, bad, "cgroup.procs"), []byte("not-a-pid"), 0600); err != nil {
		t.Fatal(err)
	}
	d := idleTestDaemon(t, cgroup, filepath.Join(t.TempDir(), "ws-missing"), t.TempDir())
	evidence, err := d.inspectIdle()
	if err == nil || evidence.Error == nil {
		t.Fatal("invalid cgroup.procs did not fail closed")
	}
}

func TestStateIdleSeqRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := openState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if s.state.IdleSeq != 0 {
		t.Fatalf("fresh state has idle seq: %+v", s.state)
	}
	if err := s.save(diskState{Incarnation: s.state.Incarnation, IdleSeq: 41}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = openState(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.state.IdleSeq != 41 {
		t.Fatalf("restart lost idle seq: %+v", s.state)
	}
	data, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var snapshot map[string]json.RawMessage
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatal(err)
	}
	if string(snapshot["idle_seq"]) != "41" {
		t.Fatalf("idle seq not persisted: %s", data)
	}
	// Snapshots written before idle_seq existed decode as zero.
	legacy := `{"version":1,"incarnation":9,"allocation":null}`
	state, err := decodeState([]byte(legacy))
	if err != nil || state.IdleSeq != 0 {
		t.Fatalf("legacy snapshot rejected: %+v %v", state, err)
	}
	if _, err := decodeState([]byte(`{"version":1,"incarnation":9,"allocation":null,"idle_seq":-1}`)); err == nil {
		t.Fatal("negative idle seq accepted")
	}
}

// TestDaemonIdleReconcileWire drives a real daemon against an httptest
// controller: idle polls must produce allocation-omitted RECONCILE reports with
// a durable per-runner sequence, rejections must not stop the daemon, and a
// restart must continue (never reset) the sequence.
func TestDaemonIdleReconcileWire(t *testing.T) {
	var mu sync.Mutex
	var reports []ReportRequest
	var wires []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/poll"):
			_, _ = w.Write([]byte(`{"assigned":false}`))
		case strings.HasSuffix(r.URL.Path, "/report"):
			body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
			if err != nil {
				http.Error(w, "read", 500)
				return
			}
			report, err := ParseReportRequest(body)
			if err != nil {
				http.Error(w, "parse: "+err.Error(), 500)
				return
			}
			mu.Lock()
			reports = append(reports, report)
			wires = append(wires, string(body))
			n := len(reports)
			mu.Unlock()
			if n == 1 {
				// The first fencing response must not stop the daemon.
				_, _ = w.Write([]byte(`{"accepted":false,"reason":"fenced_rejected","terminal":false}`))
				return
			}
			_, _ = w.Write([]byte(`{"accepted":true,"reason":"reconcile_attested","terminal":false}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	stateDir := t.TempDir()
	cgroup := filepath.Join(t.TempDir(), "cg")
	if err := os.Mkdir(cgroup, 0700); err != nil {
		t.Fatal(err)
	}
	newDaemon := func() *Daemon {
		d, err := NewDaemon(Config{ControllerURL: server.URL, MachineToken: "idle-key",
			StateDir: stateDir, CgroupRoot: cgroup, WorkspaceRoot: filepath.Join(t.TempDir(), "ws"),
			PollTimeout: time.Second, HeartbeatInterval: 50 * time.Millisecond,
			RetryInterval: 20 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	runBriefly := func(d *Daemon, want int) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- d.Run(ctx) }()
		deadline := time.Now().Add(5 * time.Second)
		for {
			mu.Lock()
			n := len(reports)
			mu.Unlock()
			if n >= want || time.Now().After(deadline) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("daemon stopped: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("daemon did not stop")
		}
	}
	d := newDaemon()
	runBriefly(d, 2)
	mu.Lock()
	if len(reports) < 2 {
		mu.Unlock()
		t.Fatalf("idle daemon sent no reports")
	}
	first, second := reports[0], reports[1]
	mu.Unlock()
	for i, report := range []ReportRequest{first, second} {
		if report.AllocationID != "" || report.RunnerEpoch != 0 || report.Status != StatusReconcile ||
			report.Seq != int64(i+1) || report.RecoveryGeneration != nil || report.Reconcile == nil {
			t.Fatalf("idle report %d malformed: %+v", i, report)
		}
	}
	if !strings.Contains(wires[0], `"status":"RECONCILE"`) || strings.Contains(wires[0], "allocation_id") {
		t.Fatalf("idle wire must omit allocation_id: %s", wires[0])
	}
	data, err := os.ReadFile(filepath.Join(stateDir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := decodeState(data)
	if err != nil || persisted.IdleSeq != second.Seq {
		t.Fatalf("idle seq not durable: %+v %v", persisted, err)
	}
	// A restart must continue the per-runner sequence, never reset it: a reset
	// would be rejected as dropped_stale forever against controller state.
	mu.Lock()
	base := len(reports)
	mu.Unlock()
	d2 := newDaemon()
	runBriefly(d2, base+1)
	mu.Lock()
	defer mu.Unlock()
	if len(reports) < base+1 {
		t.Fatal("restarted idle daemon sent no reports")
	}
	if reports[base].Seq != second.Seq+1 {
		t.Fatalf("restart reset idle seq: got %d want %d", reports[base].Seq, second.Seq+1)
	}
}
