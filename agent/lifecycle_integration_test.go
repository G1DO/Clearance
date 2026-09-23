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
	"testing"
	"time"
)

type lifecycleFixture struct {
	controllerFixture
	AttemptID   string   `json:"attempt_id"`
	EvidenceDir string   `json:"evidence_dir"`
	NextJobID   string   `json:"next_job_id"`
	NextArgv    []string `json:"next_argv"`
}

type lifecycleHarness struct {
	controllerHarness
	SubmitToken  string                      `json:"submit_token"`
	ControlURL   string                      `json:"control_url"`
	ControlToken string                      `json:"control_token"`
	Cases        map[string]lifecycleFixture `json:"fixtures"`
}

type lifecycleClaim struct {
	Assigned     bool   `json:"assigned"`
	AllocationID string `json:"allocation_id"`
	AttemptID    string `json:"attempt_id"`
	JobID        string `json:"job_id"`
	RunnerEpoch  int64  `json:"runner_epoch"`
}

type lifecycleRows struct {
	Runner struct {
		State            string  `json:"state"`
		Epoch            int64   `json:"epoch"`
		QuarantineReason *string `json:"quarantine_reason"`
	} `json:"runner"`
	Allocation struct {
		State       string          `json:"state"`
		Report      string          `json:"report_status"`
		MaxSeq      int64           `json:"max_seq"`
		Incarnation int64           `json:"agent_incarnation"`
		Cleanup     json.RawMessage `json:"cleanup_evidence"`
	} `json:"allocation"`
	Attempt struct {
		ID     string `json:"attempt_id"`
		Result string `json:"result"`
	} `json:"attempt"`
	Job struct {
		Result string `json:"result"`
	} `json:"job"`
	Active int `json:"active"`
	raw    []byte
}

func loadLifecycleHarness(t *testing.T) lifecycleHarness {
	t.Helper()
	base := loadControllerHarness(t)
	data, err := os.ReadFile(os.Getenv("CLEARANCE_INTEGRATION_FIXTURES"))
	if err != nil {
		t.Fatal(err)
	}
	var h lifecycleHarness
	if err := json.Unmarshal(data, &h); err != nil {
		t.Fatal(err)
	}
	h.controllerHarness = base
	if h.ControlURL == "" || h.ControlToken == "" || h.SubmitToken == "" {
		t.Fatal("lifecycle test harness controls and project token required")
	}
	if os.Getenv("CLEARANCE_CGROUP_ROOT") == "" {
		t.Fatal("CLEARANCE_CGROUP_ROOT must identify a writable delegated cgroup v2 root")
	}
	return h
}

func lifecycleHTTP(endpoint, tokenHeader, token, method string, body any) ([]byte, int, error) {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return nil, 0, err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(data))
	if err != nil {
		return nil, 0, err
	}
	request.Header.Set(tokenHeader, token)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	data, err = io.ReadAll(io.LimitReader(response.Body, 64<<10))
	return data, response.StatusCode, err
}

func (h lifecycleHarness) claim(f lifecycleFixture) (lifecycleClaim, error) {
	data, status, err := lifecycleHTTP(h.ControlURL+"/claim", "X-Harness-Token", h.ControlToken,
		http.MethodPost, map[string]string{"runner_id": f.RunnerID, "job_id": f.NextJobID})
	if err != nil || status != 200 {
		return lifecycleClaim{}, fmt.Errorf("claim status=%d body=%s: %w", status, data, err)
	}
	var claim lifecycleClaim
	err = json.Unmarshal(data, &claim)
	return claim, err
}

func (h lifecycleHarness) assertClaimRefused(t *testing.T, f lifecycleFixture) {
	t.Helper()
	claim, err := h.claim(f)
	if err != nil || claim.Assigned {
		t.Fatalf("unsafe runner accepted another claim: %+v error=%v", claim, err)
	}
}

func (h lifecycleHarness) job(t *testing.T, f lifecycleFixture, method, suffix string) string {
	t.Helper()
	data, status, err := lifecycleHTTP(h.URL+"/api/v1/jobs/"+f.JobID+suffix,
		"Authorization", "Bearer "+h.SubmitToken, method, nil)
	if err != nil || status != 200 {
		t.Fatalf("job API status=%d body=%s error=%v", status, data, err)
	}
	var job struct {
		Result string `json:"result"`
	}
	if err := json.Unmarshal(data, &job); err != nil {
		t.Fatal(err)
	}
	return job.Result
}

func (h lifecycleHarness) rows(t *testing.T, f lifecycleFixture) lifecycleRows {
	t.Helper()
	if !isValidUUID(f.AllocationID) {
		t.Fatal("invalid allocation identity")
	}
	query := fmt.Sprintf(`SELECT json_build_object(
 'runner', row_to_json(r), 'allocation', row_to_json(a), 'attempt', row_to_json(t), 'job', row_to_json(j),
 'runner_version', r.xmin::text, 'allocation_version', a.xmin::text,
 'attempt_version', t.xmin::text, 'job_version', j.xmin::text,
 'active', (SELECT count(*) FROM %[1]s.allocations WHERE runner_id=r.runner_id AND state='ACTIVE'))
 FROM %[1]s.allocations a JOIN %[1]s.runners r USING (runner_id)
 JOIN %[1]s.attempts t USING (attempt_id) JOIN %[1]s.jobs j ON j.job_id=a.job_id
 WHERE a.allocation_id='%[2]s'`, h.Schema, f.AllocationID)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	data, err := exec.CommandContext(ctx, "psql", "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-c", query).CombinedOutput()
	if err != nil {
		t.Fatalf("read lifecycle PostgreSQL evidence: %v: %s", err, data)
	}
	var rows lifecycleRows
	if err := json.Unmarshal(bytes.TrimSpace(data), &rows); err != nil {
		t.Fatalf("decode lifecycle rows: %v: %s", err, data)
	}
	rows.raw = data
	return rows
}

func (h lifecycleHarness) waitRows(t *testing.T, f lifecycleFixture, predicate func(lifecycleRows) bool) lifecycleRows {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		rows := h.rows(t, f)
		if predicate(rows) {
			return rows
		}
		if time.Now().After(deadline) {
			t.Fatalf("lifecycle rows did not converge: %s", rows.raw)
		}
		time.Sleep(30 * time.Millisecond)
	}
}

func writeLifecycleEvidence(t *testing.T, f lifecycleFixture, name string, evidence any) {
	t.Helper()
	if report, ok := evidence.(ReportRequest); ok {
		wire, err := EncodeReportRequest(report)
		if err != nil {
			t.Fatal(err)
		}
		evidence = json.RawMessage(wire)
	}
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.EvidenceDir, name+".json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

// The proxy can withhold a physical attestation before ingestion, then lose its
// first committed acknowledgment. It never fabricates positive cleanup evidence.
type cleanupGate struct {
	hold      atomic.Bool
	dropAck   atomic.Bool
	forwarded atomic.Int32
	seen      chan struct{}
	once      sync.Once
	mu        sync.Mutex
	reports   []ReportRequest
}

func newCleanupGate(t *testing.T, endpoint string) (*cleanupGate, string) {
	t.Helper()
	g := &cleanupGate{seen: make(chan struct{})}
	g.hold.Store(true)
	target, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cleanup := false
		if strings.HasSuffix(r.URL.Path, "/report") {
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "read report", 500)
				return
			}
			r.Body.Close()
			r.Body = io.NopCloser(bytes.NewReader(data))
			if report, err := ParseReportRequest(data); err == nil && report.Status == StatusCleanup {
				cleanup = true
				g.mu.Lock()
				g.reports = append(g.reports, report)
				g.mu.Unlock()
				g.once.Do(func() { close(g.seen) })
				if g.hold.Load() {
					w.WriteHeader(503)
					return
				}
			}
		}
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, r)
		if cleanup && response.Code == 200 {
			g.forwarded.Add(1)
			if g.dropAck.CompareAndSwap(true, false) {
				w.WriteHeader(503)
				return
			}
		}
		for k, values := range response.Header() {
			w.Header()[k] = values
		}
		w.WriteHeader(response.Code)
		_, _ = w.Write(response.Body.Bytes())
	}))
	t.Cleanup(server.Close)
	return g, server.URL
}

func (g *cleanupGate) last() ReportRequest {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.reports[len(g.reports)-1]
}

func waitLifecycle(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for " + what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func lifecycleDaemon(t *testing.T, h lifecycleHarness, f lifecycleFixture, endpoint, fault string) (*Daemon, <-chan *containedWorkload, <-chan time.Time) {
	t.Helper()
	state := filepath.Join(t.TempDir(), "state")
	d, err := NewDaemon(Config{ControllerURL: endpoint, MachineToken: f.Token, StateDir: state,
		CgroupRoot: os.Getenv("CLEARANCE_CGROUP_ROOT"), WorkspaceRoot: filepath.Join(filepath.Dir(state), "workspaces"),
		PollTimeout: time.Second, HeartbeatInterval: 100 * time.Millisecond, RetryInterval: 50 * time.Millisecond,
		GracePeriod: 200 * time.Millisecond, KillTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	prepared := make(chan *containedWorkload, 4)
	forced := make(chan time.Time, 4)
	original := d.prepare
	d.prepare = func(p PollResponse) (allocationWorkload, error) {
		work, err := original(p)
		if work != nil {
			w := work.(*containedWorkload)
			w.testKill = func() error {
				forced <- time.Now()
				if fault == "kill" {
					return errors.New("injected lifecycle termination failure")
				}
				return nil
			}
			if fault == "inspect" {
				w.testInspect = func() error { return errors.New("injected lifecycle inspection failure") }
			}
			if fault == "scrub" {
				w.testScrub = func() error { return errors.New("injected lifecycle workspace scrub failure") }
			}
			prepared <- w
		}
		return work, err
	}
	return d, prepared, forced
}

func captureLifecycleProcesses(t *testing.T, f lifecycleFixture, w *containedWorkload) []int {
	t.Helper()
	waitLifecycle(t, "workload process tree", func() bool {
		_, err := os.Stat(filepath.Join(f.EvidenceDir, "ready"))
		return err == nil
	})
	pids := make([]int, 0, 4)
	processes := map[string]any{}
	for _, kind := range []string{"direct", "child", "grandchild", "orphan"} {
		data, err := os.ReadFile(filepath.Join(f.EvidenceDir, kind+".pid"))
		if err != nil {
			t.Fatal(err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil || pid <= 0 {
			t.Fatalf("invalid %s pid %s", kind, data)
		}
		owned, err := w.ownsPID(pid)
		if err != nil || !owned {
			t.Fatalf("%s pid %d escaped allocation containment: %v", kind, pid, err)
		}
		membership, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
		if err != nil {
			t.Fatal(err)
		}
		status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
		if err != nil {
			t.Fatal(err)
		}
		processes[kind] = map[string]any{"pid": pid, "cgroup": string(membership), "status": string(status)}
		pids = append(pids, pid)
	}
	if _, err := os.Stat(filepath.Join(w.workspacePath, "nested/deeper/file")); err != nil {
		t.Fatalf("workload did not write isolated workspace: %v", err)
	}
	writeLifecycleEvidence(t, f, "before-cleanup", map[string]any{"allocation_id": f.AllocationID,
		"runner_epoch": f.RunnerEpoch, "cgroup": w.cgroupPath, "workspace": w.workspacePath, "processes": processes})
	return pids
}

func assertLifecycleGone(t *testing.T, f lifecycleFixture, w *containedWorkload, pids []int) {
	t.Helper()
	for _, pid := range pids {
		if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); !os.IsNotExist(err) {
			t.Fatalf("allocation descendant %d remains live or unreaped: %v", pid, err)
		}
	}
	for _, path := range []string{w.cgroupPath, w.workspacePath} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("allocation resource still exists after proof: %s: %v", path, err)
		}
	}
	writeLifecycleEvidence(t, f, "after-cleanup", map[string]any{"reaped_pids": pids,
		"cgroup_removed": w.cgroupPath, "workspace_removed": w.workspacePath})
}

func lifecycleNext(f lifecycleFixture, claim lifecycleClaim) lifecycleFixture {
	next := f
	next.AllocationID, next.JobID, next.AttemptID, next.RunnerEpoch = claim.AllocationID, claim.JobID, claim.AttemptID, claim.RunnerEpoch
	next.Argv = f.NextArgv
	return next
}

func TestControllerLinuxLifecycleAndReuse(t *testing.T) {
	h := loadLifecycleHarness(t)
	results := map[string]string{"success": "SUCCEEDED", "failure": "FAILED", "cancel": "CANCELLED", "timeout": "TIMED_OUT"}
	var completed []lifecycleFixture
	for _, kind := range []string{"success", "failure", "cancel", "timeout"} {
		t.Run(kind, func(t *testing.T) {
			f := h.Cases["lifecycle-"+kind]
			gate, endpoint := newCleanupGate(t, h.URL)
			d, prepared, forced := lifecycleDaemon(t, h, f, endpoint, "")
			stop := startIntegrationDaemon(t, d)
			var workload *containedWorkload
			select {
			case workload = <-prepared:
			case <-time.After(5 * time.Second):
				t.Fatal("allocation containment was not prepared")
			}
			pids := captureLifecycleProcesses(t, f, workload)
			h.assertClaimRefused(t, f)
			beganCleanup := time.Now()
			switch kind {
			case "cancel":
				h.job(t, f, http.MethodPost, "/cancel")
				h.job(t, f, http.MethodPost, "/cancel") // Authorized retries stay idempotent.
			case "success", "failure":
				if err := os.WriteFile(filepath.Join(f.EvidenceDir, "finish"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-gate.seen:
			case <-time.After(12 * time.Second):
				t.Fatal("agent did not submit physical cleanup proof")
			}
			proof := gate.last()
			if !positiveCleanup(proof.Cleanup) {
				t.Fatalf("cleanup was not positive: %+v", proof.Cleanup)
			}
			select {
			case forcedAt := <-forced:
				if forcedAt.Sub(beganCleanup) < 180*time.Millisecond {
					t.Fatal("SIGKILL escalation occurred before the graceful bound")
				}
				writeLifecycleEvidence(t, f, "forced-termination", map[string]any{"observed_at": forcedAt,
					"grace_period_ms": 200, "kill_timeout_ms": 2000})
			default:
				t.Fatal("TERM-ignoring detached descendant did not require forced termination")
			}
			assertLifecycleGone(t, f, workload, pids)
			cleaning := h.rows(t, f)
			if cleaning.Runner.State != "CLEANING" || cleaning.Allocation.State != "ACTIVE" || cleaning.Active != 1 ||
				cleaning.Job.Result != results[kind] || cleaning.Attempt.Result != results[kind] {
				t.Fatalf("terminal result or no-premature-release invariant failed: %s", cleaning.raw)
			}
			writeLifecycleEvidence(t, f, "cleaning-database", json.RawMessage(cleaning.raw))
			client := integrationClient(t, h.URL, f.Token)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			missing := proof
			missing.Cleanup = nil
			ack, err := client.Report(ctx, missing)
			assertIntegrationRejected(t, ack, err, "cleanup_required")
			h.assertClaimRefused(t, f)
			wrong := proof
			wrong.AgentIncarnation--
			ack, err = client.Report(ctx, wrong)
			assertIntegrationRejected(t, ack, err, "fenced_rejected")
			wrong = proof
			wrong.AllocationID = "00000000-0000-0000-0000-000000000001"
			ack, err = client.Report(ctx, wrong)
			assertIntegrationRejected(t, ack, err, "fenced_rejected")
			wrong = proof
			wrong.Seq = cleaning.Allocation.MaxSeq
			ack, err = client.Report(ctx, wrong)
			assertIntegrationRejected(t, ack, err, "dropped_stale")
			if after := h.rows(t, f); !bytes.Equal(after.raw, cleaning.raw) {
				t.Fatalf("rejected cleanup evidence mutated ownership: before=%s after=%s", cleaning.raw, after.raw)
			}
			var claim lifecycleClaim
			if kind == "success" {
				gate.dropAck.Store(true)
				gate.hold.Store(false)
				waitLifecycle(t, "cleanup acknowledgment retry", func() bool { return gate.forwarded.Load() >= 2 })
				claim, err = h.claim(f)
			} else {
				// Race the real scheduler's conditional claim against release's transaction.
				claimDone := make(chan struct {
					claim lifecycleClaim
					err   error
				}, 1)
				go func() {
					c, e := h.claim(f)
					claimDone <- struct {
						claim lifecycleClaim
						err   error
					}{c, e}
				}()
				gate.hold.Store(false)
				result := <-claimDone
				claim, err = result.claim, result.err
			}
			if err != nil {
				t.Fatal(err)
			}
			released := h.waitRows(t, f, func(rows lifecycleRows) bool { return rows.Allocation.State == "RELEASED" })
			writeLifecycleEvidence(t, f, "released-database", json.RawMessage(released.raw))
			writeLifecycleEvidence(t, f, "cleanup-report", proof)
			if !claim.Assigned {
				claim, err = h.claim(f)
			}
			if err != nil || !claim.Assigned || claim.RunnerEpoch <= f.RunnerEpoch || claim.AllocationID == f.AllocationID || claim.AttemptID == f.AttemptID || claim.JobID != f.NextJobID {
				t.Fatalf("new committed allocation identities invalid: %+v error=%v", claim, err)
			}
			next := lifecycleNext(f, claim)
			nextRows := h.waitRows(t, next, func(rows lifecycleRows) bool { return rows.Allocation.State == "RELEASED" })
			if nextRows.Job.Result != "SUCCEEDED" || nextRows.Attempt.Result != "SUCCEEDED" || nextRows.Runner.State != "AVAILABLE" || nextRows.Active != 0 {
				t.Fatalf("subsequent real execution did not release safely: %s", nextRows.raw)
			}
			data, err := os.ReadFile(filepath.Join(f.EvidenceDir, "next-executed"))
			if err != nil || string(data) != "next\n" {
				t.Fatalf("subsequent workload did not execute: %q %v", data, err)
			}
			stop()
			beforeReplay := h.rows(t, next)
			ack, err = client.Report(context.Background(), proof)
			assertIntegrationRejected(t, ack, err, "fenced_rejected")
			if after := h.rows(t, next); !bytes.Equal(after.raw, beforeReplay.raw) {
				t.Fatalf("old proof mutated subsequent allocation: %s", after.raw)
			}
			if got := h.job(t, f, http.MethodPost, "/cancel"); got != results[kind] {
				t.Fatalf("late cancellation regressed terminal result to %s", got)
			}
			writeLifecycleEvidence(t, f, "next-allocation", json.RawMessage(beforeReplay.raw))
			completed = append(completed, f, next)
		})
	}
	data, status, err := lifecycleHTTP(h.ControlURL+"/restart", "X-Harness-Token", h.ControlToken, http.MethodPost, nil)
	if err != nil || status != 200 {
		t.Fatalf("restart actual controller: status=%d body=%s error=%v", status, data, err)
	}
	for _, f := range completed {
		rows := h.rows(t, f)
		if result := h.job(t, f, http.MethodGet, ""); result == "" || result != rows.Job.Result || result != rows.Attempt.Result {
			t.Fatalf("job/attempt execution result did not survive controller restart: API=%s database=%s", result, rows.raw)
		}
		writeLifecycleEvidence(t, f, "restart-"+f.AllocationID, json.RawMessage(rows.raw))
	}
}

func TestControllerLinuxCleanupFailuresQuarantine(t *testing.T) {
	h := loadLifecycleHarness(t)
	var failed []lifecycleFixture
	for _, fault := range []string{"kill", "inspect", "scrub"} {
		t.Run(fault, func(t *testing.T) {
			f := h.Cases["lifecycle-"+fault+"-fault"]
			gate, endpoint := newCleanupGate(t, h.URL)
			d, prepared, _ := lifecycleDaemon(t, h, f, endpoint, fault)
			stop := startIntegrationDaemon(t, d)
			var workload *containedWorkload
			select {
			case workload = <-prepared:
			case <-time.After(5 * time.Second):
				t.Fatal("fault workload was not prepared")
			}
			// Fault cleanup is deliberately incomplete. After assertions, restore physical
			// host resources without submitting new evidence or clearing database quarantine.
			t.Cleanup(func() {
				stop()
				workload.testKill, workload.testInspect, workload.testScrub = nil, nil, nil
				var err error
				if _, statErr := os.Stat(workload.cgroupPath); os.IsNotExist(statErr) {
					err = workload.scrubWorkspace()
				} else {
					_, err = workload.cleanup()
				}
				if err != nil {
					t.Errorf("remove fault-test resources: %v", err)
				}
			})
			captureLifecycleProcesses(t, f, workload)
			if err := os.WriteFile(filepath.Join(f.EvidenceDir, "finish"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			select {
			case <-gate.seen:
			case <-time.After(8 * time.Second):
				t.Fatal("fault did not produce cleanup attestation")
			}
			proof := gate.last()
			if positiveCleanup(proof.Cleanup) || proof.Cleanup == nil || proof.Cleanup.Error == nil || !strings.Contains(*proof.Cleanup.Error, "injected lifecycle") {
				t.Fatalf("fault did not produce inspectable negative proof: %+v", proof.Cleanup)
			}
			gate.hold.Store(false)
			quarantined := h.waitRows(t, f, func(rows lifecycleRows) bool { return rows.Runner.State == "QUARANTINED" })
			if quarantined.Allocation.State != "ACTIVE" || quarantined.Active != 1 || quarantined.Job.Result != "SUCCEEDED" || quarantined.Attempt.Result != "SUCCEEDED" || quarantined.Runner.QuarantineReason == nil || !strings.Contains(*quarantined.Runner.QuarantineReason, "injected lifecycle") {
				t.Fatalf("cleanup fault changed execution result or lost diagnostic evidence: %s", quarantined.raw)
			}
			h.assertClaimRefused(t, f)
			stop()
			client := integrationClient(t, h.URL, f.Token)
			proof.Seq = h.rows(t, f).Allocation.MaxSeq + 1
			proof.Status, proof.Cleanup = StatusHeartbeat, nil
			ack, err := client.Report(context.Background(), proof)
			if err != nil || !ack.Accepted {
				t.Fatalf("heartbeat after quarantine failed unexpectedly: %+v %v", ack, err)
			}
			if after := h.rows(t, f); after.Runner.State != "QUARANTINED" || after.Job.Result != "SUCCEEDED" {
				t.Fatalf("heartbeat cleared quarantine or changed result: %s", after.raw)
			}
			h.assertClaimRefused(t, f)
			writeLifecycleEvidence(t, f, "quarantine-database", json.RawMessage(quarantined.raw))
			writeLifecycleEvidence(t, f, "negative-proof", gate.last())
			failed = append(failed, f)
		})
	}
	data, status, err := lifecycleHTTP(h.ControlURL+"/restart", "X-Harness-Token", h.ControlToken, http.MethodPost, nil)
	if err != nil || status != 200 {
		t.Fatalf("restart quarantined controller: status=%d body=%s error=%v", status, data, err)
	}
	for _, f := range failed {
		rows := h.rows(t, f)
		if rows.Runner.State != "QUARANTINED" || rows.Runner.QuarantineReason == nil || h.job(t, f, http.MethodGet, "") != "SUCCEEDED" {
			t.Fatalf("quarantine and independent result did not survive restart: %s", rows.raw)
		}
		h.assertClaimRefused(t, f)
		writeLifecycleEvidence(t, f, "quarantine-after-restart", json.RawMessage(rows.raw))
	}
}
