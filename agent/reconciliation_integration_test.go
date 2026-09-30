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

// Every report here is observed at the actual HTTP boundary. Blocked requests
// are retained too, so host facts can be correlated with what PostgreSQL knew.
type reconciliationGate struct {
	partition, hideRunning, loseStart, holdCleanup, loseCleanup, loseResolution atomic.Bool
	delayStart                                                                  atomic.Bool
	startHeld, startRelease                                                     chan struct{}
	mu                                                                          sync.Mutex
	exchanges                                                                   []recoveryExchange
}

func newReconciliationGate(t *testing.T, endpoint string) (*reconciliationGate, string) {
	t.Helper()
	g := &reconciliationGate{startHeld: make(chan struct{}), startRelease: make(chan struct{})}
	target, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	proxy.Transport = transport
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var report ReportRequest
		var data []byte
		var err error
		if strings.HasSuffix(r.URL.Path, "/report") {
			data, err = io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "read report", 500)
				return
			}
			r.Body.Close()
			r.Body = io.NopCloser(bytes.NewReader(data))
			report, err = ParseReportRequest(data)
			if err != nil {
				http.Error(w, "parse report", 500)
				return
			}
		}
		exchange := recoveryExchange{Request: append(json.RawMessage(nil), data...)}
		defer func() {
			if len(data) == 0 {
				return
			}
			g.mu.Lock()
			defer g.mu.Unlock()
			// A bounded trace suffices for this short drill and cannot grow with retries.
			if len(g.exchanges) < 512 {
				g.exchanges = append(g.exchanges, exchange)
			}
		}()
		if g.partition.Load() || (report.Status == StatusRunning && g.hideRunning.Load()) || (report.Status == StatusCleanup && g.holdCleanup.Load()) {
			exchange.Dropped = true
			http.Error(w, "injected partition", 503)
			return
		}
		recorder := httptest.NewRecorder()
		proxy.ServeHTTP(recorder, r)
		exchange.Response = append(json.RawMessage(nil), recorder.Body.Bytes()...)
		if recorder.Code == 200 && report.Status == StatusStarting && g.delayStart.CompareAndSwap(true, false) {
			close(g.startHeld)
			select {
			case <-g.startRelease:
			case <-r.Context().Done():
				return
			}
		}
		if recorder.Code == 200 && ((report.Status == StatusStarting && g.loseStart.CompareAndSwap(true, false)) ||
			(report.Status == StatusCleanup && g.loseCleanup.CompareAndSwap(true, false)) ||
			(report.Status == StatusReconcile && g.loseResolution.CompareAndSwap(true, false))) {
			exchange.Dropped = true
			if report.Status == StatusStarting || report.Status == StatusReconcile {
				g.partition.Store(true)
			}
			http.Error(w, "lost committed acknowledgment", 503)
			return
		}
		for key, values := range recorder.Header() {
			w.Header()[key] = values
		}
		w.WriteHeader(recorder.Code)
		_, _ = w.Write(recorder.Body.Bytes())
	}))
	t.Cleanup(func() { server.Close(); transport.CloseIdleConnections() })
	return g, server.URL
}

func (g *reconciliationGate) trace(t *testing.T, f lifecycleFixture) {
	t.Helper()
	g.mu.Lock()
	defer g.mu.Unlock()
	writeLifecycleEvidence(t, f, "reconciliation-http", g.exchanges)
}

func (g *reconciliationGate) saw(status ReportStatus, reason string, dropped bool) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, exchange := range g.exchanges {
		report, err := ParseReportRequest(exchange.Request)
		if err != nil || report.Status != status || exchange.Dropped != dropped {
			continue
		}
		if reason == "" {
			return true
		}
		response, err := ParseReportResponse(exchange.Response)
		if err == nil && response.Reason == reason {
			return true
		}
	}
	return false
}

func requestReconciliation(t *testing.T, h lifecycleHarness, f lifecycleFixture) {
	t.Helper()
	data, code, err := lifecycleHTTP(h.ControlURL+"/reconcile", "X-Harness-Token", h.ControlToken,
		http.MethodPost, map[string]string{"allocation_id": f.AllocationID})
	if err != nil || code != 200 {
		t.Fatalf("request real reconciliation pass: %d %s %v", code, data, err)
	}
}

func armReconciliation(t *testing.T, h lifecycleHarness, f lifecycleFixture) {
	t.Helper()
	data, code, err := lifecycleHTTP(h.ControlURL+"/arm", "X-Harness-Token", h.ControlToken,
		http.MethodPost, map[string]string{"allocation_id": f.AllocationID})
	if err != nil || code != 200 || !bytes.Contains(data, []byte(`"armed":true`)) {
		t.Fatalf("arm unobserved physical fixture: %d %s %v", code, data, err)
	}
}

func reconciliationFields(t *testing.T, rows lifecycleRows) (string, string, json.RawMessage) {
	t.Helper()
	var payload struct {
		Allocation struct {
			Classification string          `json:"reconcile_classification"`
			Action         string          `json:"reconcile_action"`
			Evidence       json.RawMessage `json:"reconcile_evidence"`
		} `json:"allocation"`
	}
	if err := json.Unmarshal(rows.raw, &payload); err != nil {
		t.Fatal(err)
	}
	return payload.Allocation.Classification, payload.Allocation.Action, payload.Allocation.Evidence
}

func waitClassification(t *testing.T, h lifecycleHarness, f lifecycleFixture, classification, action string) lifecycleRows {
	t.Helper()
	rows := h.waitRows(t, f, func(rows lifecycleRows) bool {
		c, a, _ := reconciliationFields(t, rows)
		return c == classification && a == action
	})
	_, _, evidence := reconciliationFields(t, rows)
	if len(evidence) == 0 || bytes.Equal(evidence, []byte("null")) {
		t.Fatal("classification has no durable supporting evidence")
	}
	var durable struct {
		AllocationID   string `json:"allocation_id"`
		Epoch          int64  `json:"runner_epoch"`
		Incarnation    int64  `json:"agent_incarnation"`
		Seq            int64  `json:"seq"`
		Classification string `json:"classification"`
		Action         string `json:"action"`
	}
	if err := json.Unmarshal(evidence, &durable); err != nil || durable.AllocationID != f.AllocationID || durable.Epoch != f.RunnerEpoch || durable.Incarnation != rows.Allocation.Incarnation || durable.Seq <= 0 || durable.Seq > rows.Allocation.MaxSeq || durable.Classification != classification || durable.Action != action {
		t.Fatalf("classification lost correlated identity, sequence, or action: %s", evidence)
	}
	writeLifecycleEvidence(t, f, "classification-"+classification, json.RawMessage(rows.raw))
	return rows
}

func finishReconciliationWork(t *testing.T, f lifecycleFixture) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(f.EvidenceDir, "finish"), nil, 0600); err != nil {
		t.Fatal(err)
	}
}

func assertReconciliationTimeout(t *testing.T, h lifecycleHarness, f lifecycleFixture) lifecycleRows {
	t.Helper()
	rows := h.waitRows(t, f, func(rows lifecycleRows) bool { return rows.Runner.State == "QUARANTINED" })
	if rows.Runner.QuarantineReason == nil || !strings.Contains(*rows.Runner.QuarantineReason, "heartbeat timeout:") ||
		rows.Active != 1 || rows.Allocation.State != "ACTIVE" || rows.Job.Result != "" || rows.Attempt.Result != "" {
		t.Fatalf("timeout invented a terminal result or released ownership: %s", rows.raw)
	}
	h.assertClaimRefused(t, f)
	writeLifecycleEvidence(t, f, "partition-timeout", json.RawMessage(rows.raw))
	return rows
}

func assertBeforePartition(t *testing.T, h lifecycleHarness, f lifecycleFixture, status string) {
	t.Helper()
	rows := h.waitRows(t, f, func(rows lifecycleRows) bool { return rows.Allocation.Report == status })
	if rows.Runner.State != "ASSIGNED" || rows.Allocation.State != "ACTIVE" || rows.Active != 1 || rows.Job.Result != "" {
		t.Fatalf("fixture was not healthy before contact loss: %s", rows.raw)
	}
	writeLifecycleEvidence(t, f, "before-partition-database", json.RawMessage(rows.raw))
}

func assertReconciliationNext(t *testing.T, h lifecycleHarness, f lifecycleFixture) lifecycleFixture {
	t.Helper()
	claim, err := h.claim(f)
	if err != nil || !claim.Assigned || claim.RunnerEpoch <= f.RunnerEpoch || claim.AllocationID == f.AllocationID {
		t.Fatalf("safe subsequent claim: %+v %v", claim, err)
	}
	next := lifecycleNext(f, claim)
	rows := h.waitRows(t, next, func(r lifecycleRows) bool { return r.Allocation.State == "RELEASED" })
	if rows.Runner.State != "AVAILABLE" || rows.Active != 0 || rows.Job.Result != "SUCCEEDED" {
		t.Fatalf("subsequent physical execution failed: %s", rows.raw)
	}
	data, err := os.ReadFile(filepath.Join(f.EvidenceDir, "next-executed"))
	if err != nil || string(data) != "next\n" {
		t.Fatalf("next workload did not execute once: %q %v", data, err)
	}
	writeLifecycleEvidence(t, f, "subsequent-allocation", json.RawMessage(rows.raw))
	return next
}

func TestControllerPhysicalPartitionAndReturn(t *testing.T) {
	h := loadLifecycleHarness(t)
	for _, kind := range []string{"running", "finished", "lost-report", "lost-start"} {
		t.Run(kind, func(t *testing.T) {
			f := h.Cases["reconcile-"+kind]
			armReconciliation(t, h, f)
			gate, endpoint := newReconciliationGate(t, h.URL)
			defer gate.trace(t, f)
			gate.hideRunning.Store(kind == "lost-report")
			gate.loseStart.Store(kind == "lost-start")
			d, prepared, _ := lifecycleDaemon(t, h, f, endpoint, "")
			stop := startIntegrationDaemon(t, d)
			if kind == "lost-start" {
				waitLifecycle(t, "lost committed START acknowledgment", func() bool { return gate.saw(StatusStarting, "ok", true) })
				assertBeforePartition(t, h, f, "STARTING")
				assertReconciliationTimeout(t, h, f)
				select {
				case <-prepared:
					t.Fatal("launch occurred without START acknowledgment")
				default:
				}
				if _, err := os.Stat(filepath.Join(f.EvidenceDir, "executions")); !os.IsNotExist(err) {
					t.Fatal("lost START acknowledgment executed workload")
				}
				requestReconciliation(t, h, f)
				// Poll-triggered inspection must not replace an actionable, unacknowledged
				// START merely because its physical paths do not exist yet.
				gate.partition.Store(false)
			}
			var work *containedWorkload
			select {
			case work = <-prepared:
			case <-time.After(8 * time.Second):
				t.Fatal("no physical launch")
			}
			pids := captureLifecycleProcesses(t, f, work)
			if kind != "lost-start" {
				status := "RUNNING"
				if kind == "lost-report" {
					status = "STARTING"
				}
				assertBeforePartition(t, h, f, status)
			}
			if kind != "lost-start" {
				gate.partition.Store(true)
				timeout := assertReconciliationTimeout(t, h, f)
				if kind == "lost-report" && timeout.Allocation.Report != "STARTING" {
					t.Fatalf("hidden report leaked to controller: %s", timeout.raw)
				}
				captureLifecycleProcesses(t, f, work)
				if kind == "finished" {
					finishReconciliationWork(t, f)
					waitLifecycle(t, "autonomous cleanup during partition", func() bool {
						_, cgroupErr := os.Lstat(work.cgroupPath)
						_, workspaceErr := os.Lstat(work.workspacePath)
						return os.IsNotExist(cgroupErr) && os.IsNotExist(workspaceErr)
					})
					assertLifecycleGone(t, f, work, pids)
					still := h.rows(t, f)
					if still.Job.Result != "" || still.Runner.State != "QUARANTINED" {
						t.Fatalf("disconnected physical finish invented controller result: %s", still.raw)
					}
				}
				requestReconciliation(t, h, f)
				gate.hideRunning.Store(false)
				gate.partition.Store(false)
			}
			if kind != "finished" {
				rows := waitClassification(t, h, f, "STILL_RUNNING", "KEEP")
				if rows.Job.Result != "" || rows.Runner.State != "QUARANTINED" {
					t.Fatalf("still-running classification resolved execution: %s", rows.raw)
				}
				h.assertClaimRefused(t, f)
				captureLifecycleProcesses(t, f, work)
				finishReconciliationWork(t, f)
			}
			released := h.waitRows(t, f, func(r lifecycleRows) bool { return r.Allocation.State == "RELEASED" })
			if released.Job.Result != "SUCCEEDED" || released.Attempt.Result != "SUCCEEDED" || released.Active != 0 || released.Runner.State != "AVAILABLE" {
				t.Fatalf("reconciliation release inconsistent: %s", released.raw)
			}
			c, a, _ := reconciliationFields(t, released)
			if c != "ALREADY_CLEAN" || a != "ATTEST" {
				t.Fatalf("physically clean return did not attest: %s", released.raw)
			}
			assertLifecycleGone(t, f, work, pids)
			assertRecoverySingleExecution(t, f, "run\n")
			writeLifecycleEvidence(t, f, "released", json.RawMessage(released.raw))
			assertReconciliationNext(t, h, f)
			stop()
		})
	}
}

// A protocol driver controls the interval between physical completion and local
// cleanup. It uses the production containment and HTTP client, with no database
// mutation and no fabricated terminal result, PID, or positive cleanup proof.
type physicalReconciliation struct {
	h          lifecycleHarness
	f          lifecycleFixture
	client     *Client
	work       *containedWorkload
	assignment PollResponse
	seq        int64
	pids       []int
}

func startPhysicalReconciliation(t *testing.T, h lifecycleHarness, f lifecycleFixture) *physicalReconciliation {
	t.Helper()
	armReconciliation(t, h, f)
	client := integrationClient(t, h.URL, f.Token)
	assignment, err := client.Poll(context.Background(), 1, 1)
	if err != nil || !assignment.Assigned {
		t.Fatalf("poll physical fixture: %+v %v", assignment, err)
	}
	p := &physicalReconciliation{h: h, f: f, client: client, assignment: assignment}
	p.send(t, StatusStarting, nil, nil, "ok")
	state := filepath.Join(f.EvidenceDir, "physical-state")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	containment, err := newContainment(containmentConfig{CgroupRoot: os.Getenv("CLEARANCE_CGROUP_ROOT"), WorkspaceRoot: state + "-workspaces", StateDir: state, GracePeriod: 200 * time.Millisecond, KillTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	p.work, err = containment.Prepare(assignment)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		p.work.testKill, p.work.testInspect, p.work.testScrub = nil, nil, nil
		if _, err := os.Stat(p.work.cgroupPath); os.IsNotExist(err) {
			_ = p.work.scrubWorkspace()
		} else if _, err := p.work.cleanup(); err != nil {
			t.Errorf("remove physical drill fixture: %v", err)
		}
	})
	if err := p.work.Start(); err != nil {
		t.Fatal(err)
	}
	p.send(t, StatusRunning, nil, nil, "ok")
	p.pids = captureLifecycleProcesses(t, f, p.work)
	assertBeforePartition(t, h, f, "RUNNING")
	assertReconciliationTimeout(t, h, f)
	requestReconciliation(t, h, f)
	return p
}

func (p *physicalReconciliation) send(t *testing.T, status ReportStatus, evidence *ReconcileEvidence, proof *CleanupEvidence, reason string) ReportRequest {
	t.Helper()
	p.seq++
	report := integrationReport(p.f.controllerFixture, 1, p.seq, status)
	report.Reconcile, report.Cleanup = evidence, proof
	ack, err := p.client.Report(context.Background(), report)
	if err != nil || ack.Reason != reason || (reason != "quarantined" && !ack.Accepted) {
		t.Fatalf("%s expected %s: %+v %v", status, reason, ack, err)
	}
	writeLifecycleEvidence(t, p.f, fmt.Sprintf("report-%02d-%s", p.seq, status), report)
	return report
}

func (p *physicalReconciliation) finish(t *testing.T) {
	t.Helper()
	finishReconciliationWork(t, p.f)
	if err := p.work.Wait(); err != nil {
		t.Fatalf("actual workload did not succeed: %v", err)
	}
	p.send(t, StatusSucceeded, nil, nil, "ok")
}

func (p *physicalReconciliation) observation(t *testing.T) ReconcileEvidence {
	t.Helper()
	_, discovery, err := p.work.owner.Discover(p.assignment)
	if err != nil {
		t.Fatal(err)
	}
	return BuildReconcileEvidence(discovery, nil)
}

func assertReconciliationReplay(t *testing.T, p *physicalReconciliation, report ReportRequest) {
	t.Helper()
	before := p.h.rows(t, p.f)
	stale := report
	variants := []ReportRequest{stale}
	stale.Seq += 100
	stale.RunnerEpoch++
	variants = append(variants, stale)
	stale = report
	stale.Seq += 100
	stale.AgentIncarnation--
	variants = append(variants, stale)
	for _, replay := range variants {
		ack, err := p.client.Report(context.Background(), replay)
		if err != nil || ack.Accepted || (ack.Reason != "dropped_stale" && ack.Reason != "fenced_rejected") {
			t.Fatalf("replay accepted: %+v %v", ack, err)
		}
		if after := p.h.rows(t, p.f); !bytes.Equal(before.raw, after.raw) {
			t.Fatalf("replay changed rows: before=%s after=%s", before.raw, after.raw)
		}
	}
	writeLifecycleEvidence(t, p.f, "replay-unchanged-rows", json.RawMessage(before.raw))
}

func TestControllerPhysicalReconciliationClassification(t *testing.T) {
	h := loadLifecycleHarness(t)
	for _, kind := range []string{"stale", "orphaned", "contradictory", "insufficient"} {
		t.Run(kind, func(t *testing.T) {
			f := h.Cases["reconcile-"+kind]
			p := startPhysicalReconciliation(t, h, f)
			evidence := p.observation(t)
			classification := ""
			switch kind {
			case "stale":
				// The durable controller assignment differs from the actual owned cgroup
				// identity: report the independently observed physical allocation identity.
				foreign := h.Cases["reconcile-orphaned"].AllocationID
				foreignAssignment := p.assignment
				foreignAssignment.AllocationID = &foreign
				foreignAssignment.Argv = []string{"sleep", "60"}
				other, err := p.work.owner.Prepare(foreignAssignment)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _, _ = other.cleanup() })
				if err := other.Start(); err != nil {
					t.Fatal(err)
				}
				_, discovery, err := p.work.owner.Discover(foreignAssignment)
				if err != nil || len(discovery.PIDs) == 0 {
					t.Fatalf("foreign physical discovery: %+v %v", discovery, err)
				}
				evidence = BuildReconcileEvidence(discovery, nil)
				evidence.ObservedAllocationID = &foreign
				classification = "STALE_EXECUTION"
			case "orphaned", "contradictory":
				// Move only this fixture's descendants to a new owned cgroup, then remove
				// their original group. /proc supplies the surviving orphan identities.
				// This explicit harness fault simulates damaged containment; production
				// workloads have no authority to perform such migration.
				orphan := p.work.cgroupPath + "-orphan"
				if err := os.Mkdir(orphan, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(orphan, "cgroup.freeze"), []byte("1"), 0600); err != nil {
					t.Fatal(err)
				}
				original := p.work.cgroupPath
				t.Cleanup(func() {
					_ = os.WriteFile(filepath.Join(orphan, "cgroup.kill"), []byte("1"), 0600)
					_ = os.Remove(orphan)
				})
				// Freeze prevents descendants being forked between membership reads.
				if err := os.WriteFile(filepath.Join(original, "cgroup.freeze"), []byte("1"), 0600); err != nil {
					t.Fatal(err)
				}
				waitLifecycle(t, "frozen owned process tree", func() bool {
					data, _ := os.ReadFile(filepath.Join(original, "cgroup.events"))
					return strings.Contains(string(data), "frozen 1")
				})
				members, err := os.ReadFile(filepath.Join(original, "cgroup.procs"))
				if err != nil {
					t.Fatal(err)
				}
				evidence.PIDs = nil
				for _, raw := range strings.Fields(string(members)) {
					pid, err := strconv.ParseInt(raw, 10, 64)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(orphan, "cgroup.procs"), []byte(raw), 0600); err != nil {
						t.Fatal(err)
					}
					evidence.PIDs = append(evidence.PIDs, pid)
				}
				if err := os.Remove(original); err != nil {
					t.Fatal(err)
				}
				// Restore the original membership before the production cleanup fallback.
				t.Cleanup(func() {
					_ = os.Mkdir(original, 0700)
					for _, pid := range evidence.PIDs {
						_ = os.WriteFile(filepath.Join(original, "cgroup.procs"), []byte(strconv.FormatInt(pid, 10)), 0600)
					}
					_ = os.Remove(orphan)
					p.work.cgroupInfo, _ = os.Stat(original)
				})
				evidence.CgroupPresent = false
				evidence.ExecutionEmpty = kind == "orphaned" // original group is absent; surviving PIDs are explicitly foreign
				if kind == "orphaned" {
					foreign := h.Cases["reconcile-stale"].AllocationID
					evidence.ObservedAllocationID = &foreign
					classification = "ORPHANED_EXECUTION"
				} else {
					classification = "CONTRADICTORY"
				}
				writeLifecycleEvidence(t, f, "orphan-host", map[string]any{"former_cgroup": original, "surviving_cgroup": orphan, "pids": evidence.PIDs})
			case "insufficient":
				// Destroy only the fixture's journal; Discover must report uncertainty
				// without touching its still-live descendants or dirty workspace.
				if err := os.Rename(filepath.Join(p.work.owner.cfg.StateDir, "containment.json"), filepath.Join(p.work.owner.cfg.StateDir, "containment.saved")); err != nil {
					t.Fatal(err)
				}
				_, discovery, err := p.work.owner.Discover(p.assignment)
				if err == nil || discovery.Error == nil {
					t.Fatal("missing durable journal did not fail discovery")
				}
				evidence = BuildReconcileEvidence(discovery, nil)
				classification = "INSUFFICIENT_EVIDENCE"
			}
			report := p.send(t, StatusReconcile, &evidence, nil, "reconcile_quarantined")
			rows := waitClassification(t, h, f, classification, "KEEP")
			if rows.Runner.State != "QUARANTINED" || rows.Allocation.State != "ACTIVE" || rows.Job.Result != "" {
				t.Fatalf("uncertain observation released ownership: %s", rows.raw)
			}
			h.assertClaimRefused(t, f)
			for _, pid := range p.pids {
				if err := syscall.Kill(pid, 0); err != nil {
					t.Fatalf("classification killed unrelated/uncertain PID %d: %v", pid, err)
				}
			}
			assertReconciliationReplay(t, p, report)
		})
	}
}

func TestControllerPhysicalReconciliationCleanupAndFailure(t *testing.T) {
	h := loadLifecycleHarness(t)
	var failed []lifecycleFixture
	for _, kind := range []string{"dirty", "terminate", "kill-fault", "inspect-fault", "reap-fault", "scrub-fault"} {
		t.Run(kind, func(t *testing.T) {
			f := h.Cases["reconcile-"+kind]
			p := startPhysicalReconciliation(t, h, f)
			// An unrelated host process and workspace are deliberately outside the
			// allocation. Assertions cover both destructive cleanup and failure paths.
			sentinel := exec.Command("sleep", "60")
			if err := sentinel.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = sentinel.Process.Kill(); _ = sentinel.Wait() })
			external := filepath.Join(t.TempDir(), "unrelated")
			if err := os.WriteFile(external, []byte("preserve"), 0600); err != nil {
				t.Fatal(err)
			}
			p.finish(t)
			var forced atomic.Int32
			p.work.testKill = func() error {
				forced.Add(1)
				if kind == "kill-fault" {
					return errors.New("injected reconciliation termination failure")
				}
				return nil
			}
			if kind == "dirty" {
				// Establish fresh execution/reaping facts while intentionally leaving the
				// workload's workspace dirty; no negative proof is sent to the controller.
				if err := p.work.terminate(); err != nil {
					t.Fatal(err)
				}
				if err := p.work.reap(time.Now().Add(2 * time.Second)); err != nil {
					t.Fatal(err)
				}
				if err := p.work.authorizeRemoval(); err != nil {
					t.Fatal(err)
				}
				if err := p.work.removeCgroup(); err != nil {
					t.Fatal(err)
				}
			}
			evidence := p.observation(t)
			if kind == "dirty" {
				if evidence.CgroupPresent || len(evidence.PIDs) != 0 || !evidence.WorkspacePresent ||
					!evidence.ExecutionEmpty || !evidence.DescendantsReaped || evidence.WorkspaceClean {
					t.Fatalf("dirty-only setup: %+v", evidence)
				}
			}
			report := p.send(t, StatusReconcile, &evidence, nil, "reconcile_cleanup_required")
			rows := waitClassification(t, h, f, "FINISHED_NEEDS_CLEANUP", "TERMINATE_CLEANUP")
			if rows.Allocation.State != "ACTIVE" || rows.Runner.State != "QUARANTINED" || rows.Job.Result != "SUCCEEDED" {
				t.Fatalf("cleanup authorized before durable terminal resolution: %s", rows.raw)
			}
			h.assertClaimRefused(t, f)
			assertReconciliationReplay(t, p, report)
			if kind == "inspect-fault" {
				p.work.testInspect = func() error { return errors.New("injected reconciliation reaping inspection failure") }
			}
			if kind == "scrub-fault" {
				p.work.testScrub = func() error { return errors.New("injected reconciliation workspace scrub failure") }
			}
			diagnostic := "injected reconciliation"
			if kind == "reap-fault" {
				// Wait already established the factual successful direct-child exit.
				// Withhold its local completion indication to exercise the production
				// reaping deadline: empty cgroup membership cannot substitute for a
				// joined direct child. Restore it only for test resource disposal.
				joined := p.work.done
				p.work.done = make(chan struct{})
				t.Cleanup(func() { p.work.done = joined })
				diagnostic = "allocation descendant reaping deadline exceeded"
			}
			// Dirty-only has removed its group: use fresh production discovery for the
			// cleanup handle, as the real daemon would after restart.
			cleanup := p.work
			if kind == "dirty" {
				var err error
				cleanup, _, err = p.work.owner.Discover(p.assignment)
				if err != nil {
					t.Fatal(err)
				}
			}
			proof, cleanupErr := cleanup.Cleanup()
			if kind == "dirty" || kind == "terminate" {
				if cleanupErr != nil || !positiveCleanup(&proof) || forced.Load() != 1 {
					t.Fatalf("dirty workspace cleanup/SIGKILL proof: %+v %v forced=%d", proof, cleanupErr, forced.Load())
				}
				if kind == "terminate" {
					gate, endpoint := newReconciliationGate(t, h.URL)
					defer gate.trace(t, f)
					gate.loseCleanup.Store(true)
					client := integrationClient(t, endpoint, f.Token)
					p.seq++
					cleanupReport := integrationReport(f.controllerFixture, 1, p.seq, StatusCleanup)
					cleanupReport.Cleanup = &proof
					if _, err := client.Report(context.Background(), cleanupReport); err == nil {
						t.Fatal("cleanup acknowledgment was not lost")
					}
					before := h.rows(t, f)
					if before.Allocation.State != "RELEASED" || before.Runner.State != "AVAILABLE" {
						t.Fatalf("current cleanup did not release: %s", before.raw)
					}
					ack, err := client.Report(context.Background(), cleanupReport)
					if err != nil || !ack.Accepted {
						t.Fatalf("lost cleanup acknowledgment retry: %+v %v", ack, err)
					}
					if after := h.rows(t, f); !bytes.Equal(before.raw, after.raw) {
						t.Fatalf("cleanup acknowledgment replay mutated rows: %s", after.raw)
					}
					writeLifecycleEvidence(t, f, "cleanup-acknowledgment-replay", json.RawMessage(before.raw))
				} else {
					p.send(t, StatusCleanup, nil, &proof, "ok")
				}
				assertLifecycleGone(t, f, p.work, p.pids)
			} else {
				if cleanupErr == nil || positiveCleanup(&proof) || proof.Error == nil {
					t.Fatalf("cleanup fault lost negative evidence: %+v %v", proof, cleanupErr)
				}
				if kind == "reap-fault" && (proof.DescendantsReaped || !proof.ExecutionEmpty || !strings.Contains(*proof.Error, diagnostic)) {
					t.Fatalf("reaping failure lost its independent negative evidence: %+v", proof)
				}
				if (kind == "scrub-fault" || kind == "kill-fault") && forced.Load() != 1 {
					t.Fatalf("TERM-ignoring descendant did not require directed SIGKILL: %d", forced.Load())
				}
				p.send(t, StatusCleanup, nil, &proof, "quarantined")
				before := h.rows(t, f)
				if before.Runner.QuarantineReason == nil || !strings.Contains(*before.Runner.QuarantineReason, diagnostic) || before.Allocation.State != "ACTIVE" || before.Job.Result != "SUCCEEDED" {
					t.Fatalf("negative cleanup did not preserve ownership/result: %s", before.raw)
				}
				// No later fresh-looking observation or unsolicited positive proof can
				// erase the accepted negative evidence, even with a higher sequence.
				positive := CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}
				p.send(t, StatusCleanup, nil, &positive, "quarantined")
				p.send(t, StatusReconcile, &evidence, nil, "quarantined")
				if after := h.rows(t, f); !bytes.Equal(before.raw, after.raw) {
					t.Fatalf("later proof changed failure quarantine: before=%s after=%s", before.raw, after.raw)
				}
				p.send(t, StatusHeartbeat, nil, nil, "ok")
				h.assertClaimRefused(t, f)
				writeLifecycleEvidence(t, f, "negative-cleanup-quarantine", json.RawMessage(h.rows(t, f).raw))
				failed = append(failed, f)
			}
			if err := sentinel.Process.Signal(syscall.Signal(0)); err != nil {
				t.Fatalf("unrelated host process changed: %v", err)
			}
			status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", sentinel.Process.Pid))
			if err != nil || strings.Contains(string(status), "State:\tZ") {
				t.Fatalf("unrelated host process is absent or a zombie: %s %v", status, err)
			}
			if data, err := os.ReadFile(external); err != nil || string(data) != "preserve" {
				t.Fatalf("unrelated workspace changed: %q %v", data, err)
			}
			assertRecoverySingleExecution(t, f, "run\n")
		})
	}
	data, code, err := lifecycleHTTP(h.ControlURL+"/restart", "X-Harness-Token", h.ControlToken, http.MethodPost, nil)
	if err != nil || code != 200 {
		t.Fatalf("restart after failed reconciliation: %d %s %v", code, data, err)
	}
	for _, f := range failed {
		rows := h.rows(t, f)
		negativeRetained := bytes.Contains(rows.Allocation.Cleanup, []byte("injected reconciliation")) || bytes.Contains(rows.Allocation.Cleanup, []byte("allocation descendant reaping deadline exceeded"))
		if rows.Runner.State != "QUARANTINED" || rows.Allocation.State != "ACTIVE" || rows.Job.Result != "SUCCEEDED" || !negativeRetained {
			t.Fatalf("restart lost failure quarantine: %s", rows.raw)
		}
		h.assertClaimRefused(t, f)
		writeLifecycleEvidence(t, f, "negative-cleanup-after-restart", json.RawMessage(rows.raw))
	}
}

func TestControllerPhysicalReconciliationLostResolutionAndRestart(t *testing.T) {
	h := loadLifecycleHarness(t)
	f := h.Cases["reconcile-restart"]
	armReconciliation(t, h, f)
	gate, endpoint := newReconciliationGate(t, h.URL)
	defer gate.trace(t, f)
	state := filepath.Join(f.EvidenceDir, "state")
	first := startRecoveryProcess(t, f, endpoint, state)
	waitLifecycle(t, "physical restart workload readiness", func() bool { _, err := os.Stat(filepath.Join(f.EvidenceDir, "ready")); return err == nil })
	assertBeforePartition(t, h, f, "RUNNING")
	pids := captureRecoveryHost(t, f, state, "before-partition")
	gate.partition.Store(true)
	assertReconciliationTimeout(t, h, f)
	finishReconciliationWork(t, f)
	cg, workspace := recoveryPaths(f, state)
	waitLifecycle(t, "autonomous physical cleanup before restart", func() bool {
		_, cgroupErr := os.Lstat(cg)
		_, workspaceErr := os.Lstat(workspace)
		return os.IsNotExist(cgroupErr) && os.IsNotExist(workspaceErr)
	})
	assertRecoveryGone(t, f, state, pids, true)
	requestReconciliation(t, h, f)
	gate.loseResolution.Store(true)
	gate.partition.Store(false)
	waitLifecycle(t, "lost committed attest-only resolution", func() bool { return gate.saw(StatusReconcile, "reconcile_attested", true) })
	released := waitClassification(t, h, f, "ALREADY_CLEAN", "ATTEST")
	if released.Allocation.State != "RELEASED" || released.Runner.State != "AVAILABLE" || released.Job.Result != "SUCCEEDED" {
		t.Fatalf("lost resolution acknowledgment changed release: %s", released.raw)
	}
	// Freeze traffic after the lost reply, then kill only the real daemon. Its
	// durable state must still lack the cleanup acknowledgment.
	first.stop(t, true)
	saved, err := os.ReadFile(filepath.Join(state, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var snapshot struct {
		Allocation struct {
			Assignment struct {
				AllocationID string `json:"allocation_id"`
			} `json:"assignment"`
			CleanupAcknowledged bool `json:"cleanup_acknowledged"`
		} `json:"allocation"`
	}
	if err := json.Unmarshal(saved, &snapshot); err != nil || snapshot.Allocation.Assignment.AllocationID != f.AllocationID || snapshot.Allocation.CleanupAcknowledged {
		t.Fatalf("lost resolution did not retain unacknowledged current allocation: %s %v", saved, err)
	}
	writeLifecycleEvidence(t, f, "lost-resolution-agent-state", json.RawMessage(saved))
	data, code, err := lifecycleHTTP(h.ControlURL+"/restart", "X-Harness-Token", h.ControlToken, http.MethodPost, nil)
	if err != nil || code != 200 {
		t.Fatalf("restart during reconciliation: %d %s %v", code, data, err)
	}
	after := h.rows(t, f)
	if !bytes.Equal(released.raw, after.raw) {
		t.Fatalf("controller restart mutated release: before=%s after=%s", released.raw, after.raw)
	}
	gate.partition.Store(false)
	second := startRecoveryProcess(t, f, endpoint, state)
	next := assertReconciliationNext(t, h, f)
	second.stop(t, false)
	assertRecoverySingleExecution(t, f, "run\n")
	history := readRecoveryHistory(t, h, f)
	if len(history.Attempts) != 1 || len(history.Allocations) != 1 || history.Job.Result == nil || *history.Job.Result != "SUCCEEDED" {
		t.Fatalf("restart created duplicate retry or lost result: %s", history.raw)
	}
	// Old action/evidence arriving after newer ownership must mutate no row.
	before := h.rows(t, next)
	prior := h.rows(t, f)
	gate.mu.Lock()
	var old ReportRequest
	for _, exchange := range gate.exchanges {
		report, err := ParseReportRequest(exchange.Request)
		if err == nil && report.Status == StatusReconcile {
			old = report
			break
		}
	}
	gate.mu.Unlock()
	if old.Status != StatusReconcile {
		t.Fatal("missing old physical resolution")
	}
	ack, err := integrationClient(t, h.URL, f.Token).Report(context.Background(), old)
	assertIntegrationRejected(t, ack, err, "fenced_rejected")
	if after := h.rows(t, next); !bytes.Equal(before.raw, after.raw) {
		t.Fatalf("delayed resolution changed newer ownership: %s", after.raw)
	}
	if after := h.rows(t, f); !bytes.Equal(prior.raw, after.raw) {
		t.Fatalf("delayed resolution changed prior ownership: %s", after.raw)
	}
	writeLifecycleEvidence(t, f, "restarted-history", history.raw)
	writeLifecycleEvidence(t, f, "delayed-resolution-unchanged", json.RawMessage(before.raw))
}

func TestControllerPhysicalDelayedStartReconciliation(t *testing.T) {
	h := loadLifecycleHarness(t)
	f := h.Cases["reconcile-delayed-start"]
	armReconciliation(t, h, f)
	gate, endpoint := newReconciliationGate(t, h.URL)
	defer gate.trace(t, f)
	gate.delayStart.Store(true)
	client := integrationClient(t, endpoint, f.Token)
	assignment, err := client.Poll(context.Background(), 1, 1)
	if err != nil || !assignment.Assigned {
		t.Fatalf("delayed START poll: %+v %v", assignment, err)
	}
	// Reserve sequence 1 before dispatch. A test protocol driver then reserves 2
	// for reconciliation while the already-committed START reply is still in
	// flight. This orders the reports without resetting or reusing a sequence.
	started := make(chan struct {
		ack ReportResponse
		err error
	}, 1)
	request := integrationReport(f.controllerFixture, 1, 1, StatusStarting)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	go func() {
		ack, err := client.Report(ctx, request)
		started <- struct {
			ack ReportResponse
			err error
		}{ack, err}
	}()
	select {
	case <-gate.startHeld:
	case <-time.After(5 * time.Second):
		t.Fatal("START reply not delayed")
	}
	assertBeforePartition(t, h, f, "STARTING")
	assertReconciliationTimeout(t, h, f)
	requestReconciliation(t, h, f)
	state := filepath.Join(f.EvidenceDir, "physical-state")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	owner, err := newContainment(containmentConfig{CgroupRoot: os.Getenv("CLEARANCE_CGROUP_ROOT"), WorkspaceRoot: state + "-workspaces", StateDir: state, GracePeriod: 200 * time.Millisecond, KillTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	name := strings.ToLower(f.AllocationID) + "-" + strconv.FormatInt(f.RunnerEpoch, 10)
	for _, path := range []string{filepath.Join(owner.cfg.CgroupRoot, name), filepath.Join(owner.cfg.WorkspaceRoot, name)} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("START reply still held but physical path exists: %s %v", path, err)
		}
	}
	// Even the strongest apparently-empty observation cannot supply a terminal
	// disposition or cancel the still-actionable accepted START exchange.
	empty := ReconcileEvidence{PIDs: []int64{}, ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}
	p := &physicalReconciliation{h: h, f: f, client: integrationClient(t, h.URL, f.Token), assignment: assignment, seq: 1}
	p.send(t, StatusReconcile, &empty, nil, "reconcile_quarantined")
	rows := waitClassification(t, h, f, "INSUFFICIENT_EVIDENCE", "KEEP")
	if rows.Allocation.State != "ACTIVE" || rows.Job.Result != "" {
		t.Fatalf("empty observation released pending launch: %s", rows.raw)
	}
	h.assertClaimRefused(t, f)
	close(gate.startRelease)
	result := <-started
	if result.err != nil || !result.ack.Accepted || result.ack.Terminal {
		t.Fatalf("delayed START response changed: %+v %v", result.ack, result.err)
	}
	p.work, err = owner.Prepare(assignment)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := os.Stat(p.work.cgroupPath); !os.IsNotExist(err) {
			_, _ = p.work.cleanup()
		}
	})
	if err := p.work.Start(); err != nil {
		t.Fatal(err)
	}
	p.pids = captureLifecycleProcesses(t, f, p.work)
	p.send(t, StatusRunning, nil, nil, "ok")
	live := p.observation(t)
	p.send(t, StatusReconcile, &live, nil, "reconcile_still_running")
	waitClassification(t, h, f, "STILL_RUNNING", "KEEP")
	p.finish(t)
	live = p.observation(t)
	p.send(t, StatusReconcile, &live, nil, "reconcile_cleanup_required")
	waitClassification(t, h, f, "FINISHED_NEEDS_CLEANUP", "TERMINATE_CLEANUP")
	proof, err := p.work.Cleanup()
	if err != nil || !positiveCleanup(&proof) {
		t.Fatalf("delayed launch cleanup: %+v %v", proof, err)
	}
	p.send(t, StatusCleanup, nil, &proof, "ok")
	assertLifecycleGone(t, f, p.work, p.pids)
	assertRecoverySingleExecution(t, f, "run\n")
	writeLifecycleEvidence(t, f, "delayed-start-released", json.RawMessage(h.rows(t, f).raw))
}
