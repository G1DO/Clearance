//go:build integration

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// These drills always start the real executable. In particular, kill kills only
// the daemon PID; it never cancels an in-process context or signals a process group.
type recoveryProcess struct {
	command *exec.Cmd
	done    chan error
	stopped bool
	log     *os.File
}

func startRecoveryProcess(t *testing.T, f lifecycleFixture, endpoint, state string) *recoveryProcess {
	t.Helper()
	binary := os.Getenv("CLEARANCE_INTEGRATION_BINARY")
	if binary == "" {
		t.Fatal("real race-built executable required; run agent/verify-integration.sh")
	}
	log, err := os.CreateTemp(f.EvidenceDir, "daemon-*.log")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "-controller", endpoint, "-state-dir", state,
		"-cgroup-root", os.Getenv("CLEARANCE_CGROUP_ROOT"), "-workspace-root", state+"-workspaces")
	command.Env = append(os.Environ(), "CLEARANCE_MACHINE_TOKEN="+f.Token)
	command.Stdout, command.Stderr = log, log
	if err := command.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	p := &recoveryProcess{command: command, done: make(chan error, 1), log: log}
	go func() { p.done <- command.Wait() }()
	t.Cleanup(func() { p.stop(t, false) })
	return p
}

func (p *recoveryProcess) stop(t *testing.T, kill bool) {
	t.Helper()
	if p.stopped {
		return
	}
	p.stopped = true
	defer p.log.Close()
	signal := syscall.SIGTERM
	if kill {
		signal = syscall.SIGKILL
	}
	if err := p.command.Process.Signal(signal); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("signal daemon: %v", err)
	}
	select {
	case err := <-p.done:
		if kill {
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
				t.Errorf("daemon was not killed by SIGKILL: %v", err)
			}
		} else if err != nil {
			data, _ := os.ReadFile(p.log.Name())
			t.Errorf("daemon exited: %v: %s", err, data)
		}
	case <-time.After(8 * time.Second):
		_ = p.command.Process.Kill()
		<-p.done
		t.Error("daemon exit exceeded eight seconds")
	}
}

type recoveryWireReport struct {
	AllocationID string `json:"allocation_id"`
	RunnerEpoch  int64  `json:"runner_epoch"`
	Incarnation  int64  `json:"agent_incarnation"`
	Seq          int64  `json:"seq"`
	Status       string `json:"status"`
	Discovery    struct {
		CgroupPresent    bool    `json:"cgroup_present"`
		WorkspacePresent bool    `json:"workspace_present"`
		PIDs             []int   `json:"pids"`
		Error            *string `json:"error"`
	} `json:"discovery"`
	Cleanup *CleanupEvidence `json:"cleanup"`
	raw     json.RawMessage
}

type recoveryExchange struct {
	Request  json.RawMessage `json:"request"`
	Response json.RawMessage `json:"response,omitempty"`
	Dropped  bool            `json:"response_dropped"`
}

type recoveryGate struct {
	allocation   string
	holdRecovery atomic.Bool
	holdCleanup  atomic.Bool
	dropRecovery atomic.Bool
	dropCleanup  atomic.Bool
	mu           sync.Mutex
	reports      []recoveryWireReport
	exchanges    []recoveryExchange
}

func newRecoveryGate(t *testing.T, endpoint, allocation string) (*recoveryGate, string) {
	t.Helper()
	g := &recoveryGate{allocation: allocation}
	g.holdRecovery.Store(true)
	g.holdCleanup.Store(true)
	target, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	proxy.Transport = transport
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var report recoveryWireReport
		if strings.HasSuffix(r.URL.Path, "/report") {
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "read report", 500)
				return
			}
			r.Body.Close()
			r.Body = io.NopCloser(bytes.NewReader(data))
			if err := json.Unmarshal(data, &report); err != nil {
				http.Error(w, "decode report", 500)
				return
			}
			parsed, err := ParseReportRequest(data)
			if err != nil {
				http.Error(w, "validate report", 500)
				return
			}
			report.Cleanup = parsed.Cleanup
			report.raw = append(json.RawMessage(nil), data...)
			g.mu.Lock()
			g.reports = append(g.reports, report)
			g.mu.Unlock()
		}
		owned := report.AllocationID == g.allocation
		if owned && ((report.Status == "RECOVERY" && g.holdRecovery.Load()) || (report.Status == "CLEANUP" && g.holdCleanup.Load())) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, r)
		drop := false
		if owned && response.Code == http.StatusOK {
			if report.Status == "RECOVERY" && g.dropRecovery.CompareAndSwap(true, false) {
				// Persist one resolution but prevent the daemon from observing it
				// before the test kills it a second time.
				g.holdRecovery.Store(true)
				drop = true
			}
			if report.Status == "CLEANUP" && g.dropCleanup.CompareAndSwap(true, false) {
				drop = true
			}
			if report.Status == "RECOVERY" || report.Status == "CLEANUP" {
				g.mu.Lock()
				g.exchanges = append(g.exchanges, recoveryExchange{Request: report.raw, Response: append(json.RawMessage(nil), response.Body.Bytes()...), Dropped: drop})
				g.mu.Unlock()
			}
		}
		if drop {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		for k, values := range response.Header() {
			w.Header()[k] = values
		}
		w.WriteHeader(response.Code)
		_, _ = w.Write(response.Body.Bytes())
	}))
	t.Cleanup(func() { server.Close(); transport.CloseIdleConnections() })
	return g, server.URL
}

func (g *recoveryGate) last(status string, incarnationAfter int64) (recoveryWireReport, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for i := len(g.reports) - 1; i >= 0; i-- {
		r := g.reports[i]
		if r.AllocationID == g.allocation && r.Status == status && r.Incarnation > incarnationAfter {
			return r, true
		}
	}
	return recoveryWireReport{}, false
}

func (g *recoveryGate) wait(t *testing.T, status string, incarnationAfter int64) recoveryWireReport {
	t.Helper()
	var report recoveryWireReport
	waitLifecycle(t, "current "+status+" report", func() bool {
		var found bool
		report, found = g.last(status, incarnationAfter)
		return found
	})
	return report
}

func (g *recoveryGate) evidence(t *testing.T, f lifecycleFixture) {
	g.mu.Lock()
	defer g.mu.Unlock()
	writeLifecycleEvidence(t, f, "recovery-exchanges", g.exchanges)
	var reports []json.RawMessage
	for _, report := range g.reports {
		reports = append(reports, report.raw)
	}
	writeLifecycleEvidence(t, f, "all-reports", reports)
}

type recoveryHistory struct {
	Job struct {
		Result *string `json:"result"`
	} `json:"job"`
	Runner struct {
		State  string  `json:"state"`
		Epoch  int64   `json:"epoch"`
		Reason *string `json:"quarantine_reason"`
	} `json:"runner"`
	Attempts []struct {
		ID     string  `json:"attempt_id"`
		Result *string `json:"result"`
		Reason *string `json:"recovery_reason"`
	} `json:"attempts"`
	Allocations []struct {
		ID          string          `json:"allocation_id"`
		AttemptID   string          `json:"attempt_id"`
		RunnerID    string          `json:"runner_id"`
		JobID       string          `json:"job_id"`
		Epoch       int64           `json:"runner_epoch"`
		Incarnation int64           `json:"agent_incarnation"`
		State       string          `json:"state"`
		Recovery    string          `json:"recovery_action"`
		Evidence    json.RawMessage `json:"recovery_evidence"`
		Cleanup     json.RawMessage `json:"cleanup_evidence"`
		Retry       *string         `json:"retry_allocation_id"`
	} `json:"allocations"`
	raw json.RawMessage
}

func readRecoveryHistory(t *testing.T, h lifecycleHarness, f lifecycleFixture) recoveryHistory {
	t.Helper()
	query := fmt.Sprintf(`SELECT json_build_object('job', row_to_json(j), 'runner', row_to_json(r),
 'job_version', j.xmin::text, 'runner_version', r.xmin::text,
 'attempts', (SELECT json_agg(t ORDER BY t.created_at, t.attempt_id) FROM %[1]s.attempts t WHERE t.job_id=j.job_id),
 'attempt_versions', (SELECT json_agg(t.xmin::text ORDER BY t.created_at, t.attempt_id) FROM %[1]s.attempts t WHERE t.job_id=j.job_id),
 'allocations', (SELECT json_agg(a ORDER BY a.runner_epoch) FROM %[1]s.allocations a WHERE a.job_id=j.job_id),
 'allocation_versions', (SELECT json_agg(a.xmin::text ORDER BY a.runner_epoch) FROM %[1]s.allocations a WHERE a.job_id=j.job_id))
 FROM %[1]s.jobs j CROSS JOIN %[1]s.runners r WHERE j.job_id='%[2]s' AND r.runner_id='%[3]s'`, h.Schema, f.JobID, f.RunnerID)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	data, err := exec.CommandContext(ctx, "psql", "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-c", query).CombinedOutput()
	if err != nil {
		t.Fatalf("read recovery history: %v: %s", err, data)
	}
	var history recoveryHistory
	if err := json.Unmarshal(bytes.TrimSpace(data), &history); err != nil {
		t.Fatalf("decode recovery history: %v: %s", err, data)
	}
	history.raw = append(json.RawMessage(nil), bytes.TrimSpace(data)...)
	return history
}

func recoveryPaths(f lifecycleFixture, state string) (string, string) {
	name := strings.ToLower(f.AllocationID) + "-" + strconv.FormatInt(f.RunnerEpoch, 10)
	return filepath.Join(os.Getenv("CLEARANCE_CGROUP_ROOT"), name), filepath.Join(state+"-workspaces", name)
}

func captureRecoveryHost(t *testing.T, f lifecycleFixture, state, stage string) []int {
	t.Helper()
	cg, workspace := recoveryPaths(f, state)
	membership, err := cgroupMembershipPath(cg)
	if err != nil {
		t.Fatal(err)
	}
	processes := map[string]any{}
	var pids []int
	for _, kind := range []string{"direct", "child", "grandchild", "orphan"} {
		data, err := os.ReadFile(filepath.Join(f.EvidenceDir, kind+".pid"))
		if err != nil {
			t.Fatal(err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil || pid <= 0 {
			t.Fatalf("invalid %s PID: %q", kind, data)
		}
		group, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
		if err != nil || !strings.Contains(string(group), "0::"+membership+"\n") {
			t.Fatalf("surviving %s PID %d not in allocation: %s %v", kind, pid, group, err)
		}
		status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
		if err != nil || strings.Contains(string(status), "State:\tZ") {
			t.Fatalf("surviving %s PID %d is not live: %s %v", kind, pid, status, err)
		}
		processes[kind] = map[string]any{"pid": pid, "cgroup": string(group), "status": string(status)}
		pids = append(pids, pid)
	}
	if _, err := os.Stat(filepath.Join(workspace, "nested/deeper/file")); err != nil {
		t.Fatalf("dirty workspace missing: %v", err)
	}
	writeLifecycleEvidence(t, f, stage, map[string]any{"allocation_id": f.AllocationID, "runner_epoch": f.RunnerEpoch,
		"cgroup": cg, "workspace": workspace, "processes": processes})
	return pids
}

// Earlier in-process integration tests make this test runner a subreaper. When
// a CLI child is killed, its descendants can therefore reparent here instead of
// PID 1. Reap only adopted PIDs in this allocation, emulating the normal init
// reaper; the restarted executable still must inspect and prove /proc emptiness.
func reapRecoveryChildren(t *testing.T, cg string) {
	t.Helper()
	membership, err := cgroupMembershipPath(cg)
	if err != nil {
		t.Fatal(err)
	}
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
			entries, _ := os.ReadDir("/proc")
			for _, entry := range entries {
				pid, err := strconv.Atoi(entry.Name())
				if err != nil {
					continue
				}
				data, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "cgroup"))
				if err != nil || !strings.Contains(string(data), "0::"+membership+"\n") {
					continue
				}
				var status syscall.WaitStatus
				_, _ = syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
			}
		}
	}()
	t.Cleanup(func() { close(stop); <-done })
}

func assertRecoveryDiscovered(t *testing.T, report recoveryWireReport, f lifecycleFixture, pids []int) {
	t.Helper()
	if report.AllocationID != f.AllocationID || report.RunnerEpoch != f.RunnerEpoch || !report.Discovery.CgroupPresent || !report.Discovery.WorkspacePresent || report.Discovery.Error != nil {
		t.Fatalf("discovery lacks correlated physical evidence: %s", report.raw)
	}
	for _, pid := range pids {
		found := false
		for _, discovered := range report.Discovery.PIDs {
			if discovered == pid {
				found = true
			}
		}
		if !found {
			t.Fatalf("discovery omitted surviving PID %d: %s", pid, report.raw)
		}
	}
}

func assertRecoveryGone(t *testing.T, f lifecycleFixture, state string, pids []int, workspaceGone bool) {
	t.Helper()
	for _, pid := range pids {
		if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); !os.IsNotExist(err) {
			t.Fatalf("PID %d remains alive or unreaped: %v", pid, err)
		}
	}
	cg, workspace := recoveryPaths(f, state)
	paths := []string{cg}
	if workspaceGone {
		paths = append(paths, workspace)
	}
	for _, path := range paths {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("allocation resource remains: %s: %v", path, err)
		}
	}
	writeLifecycleEvidence(t, f, "physical-cleanup", map[string]any{"absent_pids": pids, "removed_paths": paths})
}

func assertRecoverySingleExecution(t *testing.T, f lifecycleFixture, expected string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(f.EvidenceDir, "executions"))
	if err != nil || string(data) != expected {
		t.Fatalf("unexpected execution count: %q %v", data, err)
	}
}

func assertOldRecoveryRejected(t *testing.T, h lifecycleHarness, f lifecycleFixture, old recoveryWireReport, incarnation int64) {
	t.Helper()
	before := readRecoveryHistory(t, h, f)
	var payload map[string]any
	if err := json.Unmarshal(old.raw, &payload); err != nil {
		t.Fatal(err)
	}
	payload["agent_incarnation"] = incarnation
	payload["seq"] = old.Seq + 100
	for _, status := range []string{"RECOVERY", "CLEANUP"} {
		payload["status"] = status
		if status == "CLEANUP" {
			delete(payload, "discovery")
			payload["cleanup"] = map[string]bool{"execution_empty": true, "descendants_reaped": true, "workspace_clean": true}
		}
		data, code, err := lifecycleHTTP(h.URL+"/internal/v1/agents/report", "Authorization", "Bearer "+f.Token, http.MethodPost, payload)
		var ack ReportResponse
		if err != nil || code != 200 || json.Unmarshal(data, &ack) != nil || ack.Accepted || ack.Reason != "fenced_rejected" {
			t.Fatalf("old %s evidence accepted: HTTP %d %s %v", status, code, data, err)
		}
	}
	if after := readRecoveryHistory(t, h, f); !bytes.Equal(before.raw, after.raw) {
		t.Fatalf("stale recovery changed rows: before=%s after=%s", before.raw, after.raw)
	}
	writeLifecycleEvidence(t, f, "stale-evidence-rejected", before.raw)
}

func startRecoveryWorkload(t *testing.T, h lifecycleHarness, f lifecycleFixture, endpoint, state string) (*recoveryProcess, []int, int64) {
	t.Helper()
	process := startRecoveryProcess(t, f, endpoint, state)
	waitLifecycle(t, "first workload readiness", func() bool { _, err := os.Stat(filepath.Join(f.EvidenceDir, "ready")); return err == nil })
	running := h.waitRows(t, f, func(rows lifecycleRows) bool { return rows.Allocation.Report == "RUNNING" })
	pids := captureRecoveryHost(t, f, state, "before-sigkill")
	process.stop(t, true)
	captureRecoveryHost(t, f, state, "survives-sigkill")
	cg, _ := recoveryPaths(f, state)
	reapRecoveryChildren(t, cg)
	// A failed assertion must not leave a deliberately surviving workload on the
	// host. This test-only fallback runs after the restarted CLI has stopped and
	// before the scoped reaper exits, and never sends controller cleanup proof.
	t.Cleanup(func() {
		if _, err := os.Stat(cg); os.IsNotExist(err) {
			return
		}
		if err := os.WriteFile(filepath.Join(cg, "cgroup.kill"), []byte("1"), 0600); err != nil {
			t.Errorf("remove recovery fixture workload: %v", err)
			return
		}
		deadline := time.Now().Add(3 * time.Second)
		for {
			err := os.Remove(cg)
			if err == nil || os.IsNotExist(err) {
				return
			}
			if time.Now().After(deadline) {
				t.Errorf("remove recovery fixture cgroup: %v", err)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	h.assertClaimRefused(t, f)
	writeLifecycleEvidence(t, f, "agent-sigkill", map[string]any{"agent_pid": process.command.Process.Pid, "signal": "SIGKILL", "agent_incarnation": running.Allocation.Incarnation, "surviving_workload_pids": pids})
	return process, pids, running.Allocation.Incarnation
}

func TestControllerSIGKILLRecoveryRetriesSameJob(t *testing.T) {
	h := loadLifecycleHarness(t)
	f := h.Cases["crash-retry"]
	gate, endpoint := newRecoveryGate(t, h.URL, f.AllocationID)
	defer gate.evidence(t, f)
	state := filepath.Join(f.EvidenceDir, "state")
	_, pids, firstIncarnation := startRecoveryWorkload(t, h, f, endpoint, state)
	second := startRecoveryProcess(t, f, endpoint, state)
	discovery := gate.wait(t, "RECOVERY", firstIncarnation)
	assertRecoveryDiscovered(t, discovery, f, pids)
	h.assertClaimRefused(t, f)
	assertOldRecoveryRejected(t, h, f, discovery, firstIncarnation)
	assertRecoverySingleExecution(t, f, "run\n")
	writeLifecycleEvidence(t, f, "discovery-before-resolution", readRecoveryHistory(t, h, f).raw)

	gate.dropRecovery.Store(true)
	gate.holdRecovery.Store(false)
	waitLifecycle(t, "lost committed recovery response", func() bool {
		gate.mu.Lock()
		defer gate.mu.Unlock()
		for _, exchange := range gate.exchanges {
			if exchange.Dropped {
				return true
			}
		}
		return false
	})
	second.stop(t, true)
	captureRecoveryHost(t, f, state, "survives-second-sigkill")
	interrupted := readRecoveryHistory(t, h, f)
	if len(interrupted.Attempts) != 1 || interrupted.Attempts[0].Result == nil || *interrupted.Attempts[0].Result != "INTERRUPTED" || interrupted.Attempts[0].Reason == nil || interrupted.Job.Result != nil || interrupted.Runner.State != "CLEANING" {
		t.Fatalf("lost response did not preserve pending recovery and unfinished logical job: %s", interrupted.raw)
	}
	writeLifecycleEvidence(t, f, "pending-after-second-sigkill", interrupted.raw)
	h.assertClaimRefused(t, f)

	third := startRecoveryProcess(t, f, endpoint, state)
	repeated := gate.wait(t, "RECOVERY", discovery.Incarnation)
	assertRecoveryDiscovered(t, repeated, f, pids)
	if repeated.Seq <= discovery.Seq {
		t.Fatalf("allocation report sequence reset: %s then %s", discovery.raw, repeated.raw)
	}
	assertOldRecoveryRejected(t, h, f, repeated, discovery.Incarnation)
	gate.holdRecovery.Store(false)
	proof := gate.wait(t, "CLEANUP", discovery.Incarnation)
	if !positiveCleanup(proof.Cleanup) {
		t.Fatalf("recovery cleanup not positive: %s", proof.raw)
	}
	assertRecoveryGone(t, f, state, pids, true)
	h.assertClaimRefused(t, f)
	beforeRelease := readRecoveryHistory(t, h, f)
	if len(beforeRelease.Attempts) != 1 || len(beforeRelease.Allocations) != 1 || beforeRelease.Allocations[0].State != "ACTIVE" || beforeRelease.Job.Result != nil {
		t.Fatalf("retry created before positive proof accepted: %s", beforeRelease.raw)
	}
	writeLifecycleEvidence(t, f, "cleaned-but-held", beforeRelease.raw)
	assertOldRecoveryRejected(t, h, f, repeated, discovery.Incarnation)
	gate.dropCleanup.Store(true)
	gate.holdCleanup.Store(false)
	var completed recoveryHistory
	waitLifecycle(t, "one successful retry and release", func() bool {
		completed = readRecoveryHistory(t, h, f)
		return completed.Job.Result != nil && *completed.Job.Result == "SUCCEEDED" && completed.Runner.State == "AVAILABLE"
	})
	third.stop(t, false)
	if len(completed.Attempts) != 2 || len(completed.Allocations) != 2 || completed.Attempts[0].Result == nil || *completed.Attempts[0].Result != "INTERRUPTED" || completed.Attempts[1].Result == nil || *completed.Attempts[1].Result != "SUCCEEDED" {
		t.Fatalf("retry lost attempt history or duplicated work: %s", completed.raw)
	}
	old, retry := completed.Allocations[0], completed.Allocations[1]
	if old.ID != f.AllocationID || retry.ID == old.ID || retry.AttemptID == old.AttemptID || retry.RunnerID != f.RunnerID || retry.JobID != f.JobID || retry.Epoch <= old.Epoch || old.State != "RELEASED" || retry.State != "RELEASED" || old.Recovery != "TERMINATE" || old.Retry == nil || *old.Retry != retry.ID || old.Incarnation != repeated.Incarnation || retry.Incarnation != repeated.Incarnation {
		t.Fatalf("retry identity, resolution, or cleanup fence invalid: %s", completed.raw)
	}
	if gate.dropCleanup.Load() {
		t.Fatal("test did not drop the accepted current cleanup response")
	}
	gate.mu.Lock()
	resolutions := 0
	for _, exchange := range gate.exchanges {
		var request recoveryWireReport
		var response ReportResponse
		if json.Unmarshal(exchange.Request, &request) == nil && request.Status == "RECOVERY" {
			if json.Unmarshal(exchange.Response, &response) != nil || !response.Accepted || response.Reason != "terminate" || !response.Terminal {
				t.Errorf("controller did not explicitly direct termination: %s", exchange.Response)
			}
			resolutions++
		}
	}
	gate.mu.Unlock()
	if resolutions < 2 {
		t.Fatal("test did not repeat the committed recovery resolution")
	}
	assertRecoverySingleExecution(t, f, "run\nrun\n")
	data, err := os.ReadFile(filepath.Join(f.EvidenceDir, "retry-executions"))
	if err != nil || string(data) != "retry\n" {
		t.Fatalf("retry did not execute exactly once: %q %v", data, err)
	}
	writeLifecycleEvidence(t, f, "completed-history", completed.raw)
	t.Log("SIGKILL left child/grandchild/detached descendants alive; repeated current discovery selected termination; lost recovery/cleanup acknowledgments and second SIGKILL produced exactly one fresh successful attempt of the same job")
}

func TestControllerSIGKILLRecoveryCleanupFailureQuarantines(t *testing.T) {
	h := loadLifecycleHarness(t)
	f := h.Cases["crash-quarantine"]
	gate, endpoint := newRecoveryGate(t, h.URL, f.AllocationID)
	defer gate.evidence(t, f)
	state := filepath.Join(f.EvidenceDir, "state")
	_, pids, firstIncarnation := startRecoveryWorkload(t, h, f, endpoint, state)
	second := startRecoveryProcess(t, f, endpoint, state)
	discovery := gate.wait(t, "RECOVERY", firstIncarnation)
	assertRecoveryDiscovered(t, discovery, f, pids)
	_, workspace := recoveryPaths(f, state)
	original := workspace + "-original"
	// A real directory identity contradiction after discovery exercises the
	// production cleanup guard. Neither the original nor replacement may be erased.
	if err := os.Rename(workspace, original); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(workspace, "unrelated-sentinel")
	if err := os.WriteFile(sentinel, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { second.stop(t, false); _ = os.RemoveAll(workspace); _ = os.RemoveAll(original) })
	gate.holdRecovery.Store(false)
	proof := gate.wait(t, "CLEANUP", firstIncarnation)
	if proof.Cleanup == nil || proof.Cleanup.Error == nil || positiveCleanup(proof.Cleanup) || !strings.Contains(*proof.Cleanup.Error, "directory identity changed") {
		t.Fatalf("physical cleanup failure lacks negative evidence: %s", proof.raw)
	}
	assertRecoveryGone(t, f, state, pids, false)
	for _, path := range []string{sentinel, filepath.Join(original, "nested/deeper/file")} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("cleanup erased mismatched workspace: %s: %v", path, err)
		}
	}
	gate.holdCleanup.Store(false)
	var quarantined recoveryHistory
	waitLifecycle(t, "durable recovery quarantine", func() bool {
		quarantined = readRecoveryHistory(t, h, f)
		return quarantined.Runner.State == "QUARANTINED"
	})
	second.stop(t, false)
	if len(quarantined.Attempts) != 1 || len(quarantined.Allocations) != 1 || quarantined.Allocations[0].State != "ACTIVE" || quarantined.Attempts[0].Result == nil || *quarantined.Attempts[0].Result != "INTERRUPTED" || quarantined.Job.Result != nil || quarantined.Runner.Reason == nil || !strings.Contains(*quarantined.Runner.Reason, "directory identity changed") {
		t.Fatalf("cleanup failure released/retried or lost evidence: %s", quarantined.raw)
	}
	h.assertClaimRefused(t, f)
	assertRecoverySingleExecution(t, f, "run\n")
	writeLifecycleEvidence(t, f, "quarantine-history", quarantined.raw)
	data, code, err := lifecycleHTTP(h.ControlURL+"/restart", "X-Harness-Token", h.ControlToken, http.MethodPost, nil)
	if err != nil || code != 200 {
		t.Fatalf("controller restart: HTTP %d %s %v", code, data, err)
	}
	after := readRecoveryHistory(t, h, f)
	if !bytes.Equal(quarantined.raw, after.raw) {
		t.Fatalf("quarantine history changed across controller restart: before=%s after=%s", quarantined.raw, after.raw)
	}
	h.assertClaimRefused(t, f)
	writeLifecycleEvidence(t, f, "quarantine-after-controller-restart", after.raw)
	t.Log("real workspace identity contradiction preserved both directories, retained interrupted attempt without finalizing job, and durably quarantined runner without retry")
}
