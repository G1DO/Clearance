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
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type controllerFixture struct {
	RunnerID      string   `json:"runner_id"`
	Token         string   `json:"token"`
	AllocationID  string   `json:"allocation_id"`
	JobID         string   `json:"job_id"`
	RunnerEpoch   int64    `json:"runner_epoch"`
	Argv          []string `json:"argv"`
	Marker        string   `json:"marker"`
	NextJobID     string   `json:"next_job_id"`
	NextArgv      []string `json:"next_argv"`
	SpareRunnerID string   `json:"spare_runner_id"`
}

type controllerHarness struct {
	URL           string                       `json:"url"`
	Schema        string                       `json:"schema"`
	Fixtures      map[string]controllerFixture `json:"fixtures"`
	ControlURL    string                       `json:"control_url"`
	ControlToken  string                       `json:"control_token"`
	SpareRunnerID string                       `json:"spare_runner_id"`
}

type controllerSnapshot struct {
	Runner struct {
		Epoch            int64   `json:"epoch"`
		Incarnation      int64   `json:"agent_incarnation"`
		State            string  `json:"state"`
		QuarantineReason *string `json:"quarantine_reason"`
	} `json:"runner"`
	Allocation struct {
		MaxSeq       int64   `json:"max_seq"`
		Incarnation  int64   `json:"agent_incarnation"`
		ReportStatus *string `json:"report_status"`
		State        string  `json:"state"`
	} `json:"allocation"`
	Job struct {
		Result *string `json:"result"`
	} `json:"job"`
	Attempt struct {
		Result *string `json:"result"`
	} `json:"attempt"`
	Attempts int `json:"attempts"`
	Active   int `json:"active"`
	raw      string
}

func loadControllerHarness(t *testing.T) controllerHarness {
	t.Helper()
	path := os.Getenv("CLEARANCE_INTEGRATION_FIXTURES")
	if path == "" {
		t.Fatal("real controller fixtures are required; run bash agent/verify-integration.sh")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var harness controllerHarness
	if err := json.Unmarshal(data, &harness); err != nil {
		t.Fatal(err)
	}
	// Suffixed schemas isolate suites that enter real recovery mode (fleet
	// quarantine plus authority) from suites asserting pre-recovery behavior.
	if !regexp.MustCompile(`^agent_integration_[0-9]+_[0-9]+(_[a-z]+)?$`).MatchString(harness.Schema) {
		t.Fatal("unexpected integration schema")
	}
	for _, fixture := range harness.Fixtures {
		if !isValidUUID(fixture.RunnerID) || !isValidUUID(fixture.AllocationID) || !isValidUUID(fixture.JobID) {
			t.Fatal("invalid fixture identity")
		}
	}
	return harness
}

func (h controllerHarness) snapshot(t *testing.T, fixture controllerFixture) controllerSnapshot {
	t.Helper()
	// Include PostgreSQL row versions so rejected requests cannot silently rewrite rows.
	query := fmt.Sprintf(`SELECT json_build_object(
 'runner', row_to_json(r), 'allocation', row_to_json(a),
 'job', (SELECT row_to_json(j) FROM %[1]s.jobs j WHERE j.job_id = '%[3]s'),
 'attempt', (SELECT row_to_json(att) FROM %[1]s.attempts att WHERE att.attempt_id = a.attempt_id),
 'runner_version', r.xmin::text, 'allocation_version', a.xmin::text,
 'attempts', (SELECT count(*) FROM %[1]s.attempts WHERE job_id = '%[3]s'),
 'active', (SELECT count(*) FROM %[1]s.allocations WHERE runner_id = '%[2]s' AND state = 'ACTIVE'))
 FROM %[1]s.runners r JOIN %[1]s.allocations a USING (runner_id)
 WHERE r.runner_id = '%[2]s' AND a.allocation_id = '%[4]s'`, h.Schema, fixture.RunnerID, fixture.JobID, fixture.AllocationID)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	data, err := exec.CommandContext(ctx, "psql", "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-c", query).CombinedOutput()
	if err != nil {
		t.Fatalf("read controller PostgreSQL state: %v: %s", err, data)
	}
	var result controllerSnapshot
	if err := json.Unmarshal(bytes.TrimSpace(data), &result); err != nil {
		t.Fatalf("decode controller PostgreSQL state: %v: %s", err, data)
	}
	result.raw = string(data)
	return result
}

func (h controllerHarness) waitSnapshot(t *testing.T, fixture controllerFixture, predicate func(controllerSnapshot) bool) controllerSnapshot {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		state := h.snapshot(t, fixture)
		if predicate(state) {
			return state
		}
		if time.Now().After(deadline) {
			t.Fatalf("controller state did not converge: %s", state.raw)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func integrationClient(t *testing.T, endpoint, token string) *Client {
	t.Helper()
	client, err := NewClient(endpoint, token)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func integrationDaemon(t *testing.T, harness controllerHarness, fixture controllerFixture, stateDir string) *Daemon {
	t.Helper()
	d, err := NewDaemon(Config{
		ControllerURL: harness.URL,
		MachineToken:  fixture.Token,
		StateDir:      stateDir,
		CgroupRoot:    os.Getenv("CLEARANCE_CGROUP_ROOT"),
		WorkspaceRoot: stateDir + "-workspaces",
		RetryInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func startIntegrationDaemon(t *testing.T, d *Daemon) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	stopped := false
	stop := func() {
		t.Helper()
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("daemon shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("daemon shutdown exceeded 5 seconds")
			return
		}
		if err := d.Close(); err != nil {
			t.Errorf("close daemon: %v", err)
		}
	}
	t.Cleanup(stop)
	return stop
}

func startIntegrationProcess(t *testing.T, harness controllerHarness, fixture controllerFixture, stateDir string) func() {
	t.Helper()
	binary := os.Getenv("CLEARANCE_INTEGRATION_BINARY")
	if binary == "" {
		t.Fatal("race-built daemon executable is required; run bash agent/verify-integration.sh")
	}
	log, err := os.CreateTemp(filepath.Dir(os.Getenv("CLEARANCE_INTEGRATION_FIXTURES")), "cli-*.log")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "-controller", harness.URL, "-state-dir", stateDir, "-cgroup-root", os.Getenv("CLEARANCE_CGROUP_ROOT"), "-workspace-root", stateDir+"-workspaces")
	command.Env = append(os.Environ(), "CLEARANCE_MACHINE_TOKEN="+fixture.Token)
	command.Stdout, command.Stderr = log, log
	if err := command.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	stopped := false
	stop := func() {
		t.Helper()
		if stopped {
			return
		}
		stopped = true
		defer log.Close()
		if err := command.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Errorf("signal daemon process: %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				output, _ := os.ReadFile(log.Name())
				t.Errorf("daemon process exit: %v: %s", err, output)
			}
		case <-time.After(5 * time.Second):
			_ = command.Process.Kill()
			<-done
			t.Error("daemon process SIGTERM shutdown exceeded 5 seconds")
		}
	}
	t.Cleanup(stop)
	return stop
}

func configureIntegrationFault(t *testing.T, harness controllerHarness, fixture controllerFixture, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, harness.URL+"/internal/v1/agents/test/faults", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+fixture.Token)
	req.Header.Set("Content-Type", "application/json")
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	response, err := (&http.Client{Transport: transport}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("configure fault: status=%d body=%s error=%v", response.StatusCode, data, err)
	}
}

func assertSingleIntegrationExecution(t *testing.T, fixture controllerFixture) {
	t.Helper()
	data, err := os.ReadFile(fixture.Marker)
	if err != nil || string(data) != "run\n" {
		t.Fatalf("allocation must execute exactly once: marker=%q error=%v", data, err)
	}
}

func integrationReport(fixture controllerFixture, incarnation, seq int64, status ReportStatus) ReportRequest {
	return ReportRequest{
		AllocationID: fixture.AllocationID, RunnerEpoch: fixture.RunnerEpoch,
		AgentIncarnation: incarnation, Seq: seq, Status: status, Ts: time.Now().UTC(),
	}
}

func assertIntegrationRejected(t *testing.T, ack ReportResponse, err error, reason string) {
	t.Helper()
	if err != nil || ack.Accepted || ack.Reason != reason {
		t.Fatalf("expected %s rejection: ack=%+v error=%v", reason, ack, err)
	}
}

func assertIntegrationHTTPError(t *testing.T, err error, status int, code string) {
	t.Helper()
	var response *HTTPError
	if !errors.As(err, &response) || response.StatusCode != status || (code != "" && response.Body.Error != code) {
		t.Fatalf("expected HTTP %d %s: %v", status, code, err)
	}
}

func TestControllerDropRecoveryAndRestartFencing(t *testing.T) {
	harness := loadControllerHarness(t)
	fixture := harness.Fixtures["recovery"]
	client := integrationClient(t, harness.URL, fixture.Token)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	stateDir := t.TempDir()
	first := integrationDaemon(t, harness, fixture, stateDir)
	firstIncarnation := first.Incarnation()
	configureIntegrationFault(t, harness, fixture, `{"drop_next_poll":true}`)
	_, err := client.Poll(ctx, firstIncarnation, 0)
	assertIntegrationHTTPError(t, err, http.StatusServiceUnavailable, "")
	afterDrop := harness.snapshot(t, fixture)
	if afterDrop.Attempts != 1 || afterDrop.Active != 1 || afterDrop.Allocation.MaxSeq != 0 {
		t.Fatalf("dropped response changed the committed claim: %s", afterDrop.raw)
	}
	if _, err := os.Stat(fixture.Marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("work ran without a delivered allocation: %v", err)
	}
	assignment, err := client.Poll(ctx, firstIncarnation, 0)
	if err != nil || !assignment.Assigned || *assignment.AllocationID != fixture.AllocationID ||
		*assignment.JobID != fixture.JobID || *assignment.RunnerEpoch != fixture.RunnerEpoch ||
		!reflect.DeepEqual(assignment.Argv, fixture.Argv) {
		t.Fatalf("retry did not recover identical committed work: %+v, %v", assignment, err)
	}
	if after := harness.snapshot(t, fixture); after.raw != afterDrop.raw {
		t.Fatalf("retry rewrote committed ownership: before=%s after=%s", afterDrop.raw, after.raw)
	}
	// Inject again to prove the production daemon itself retries the lost response.
	configureIntegrationFault(t, harness, fixture, `{"drop_next_poll":true}`)
	stopFirst := startIntegrationDaemon(t, first)
	harness.waitSnapshot(t, fixture, func(state controllerSnapshot) bool {
		return state.Allocation.MaxSeq >= 3 && state.Allocation.ReportStatus != nil && *state.Allocation.ReportStatus == "RUNNING"
	})
	assertSingleIntegrationExecution(t, fixture)
	stopFirst()
	beforeRestart := harness.snapshot(t, fixture)

	// Graceful shutdown physically stopped this still-nonterminal attempt. A new
	// incarnation must reconcile that evidence before creating the job's retry.
	stopRestarted := startIntegrationProcess(t, harness, fixture, stateDir)
	historyHarness := loadLifecycleHarness(t)
	historyFixture := historyHarness.Cases["recovery"]
	var history recoveryHistory
	waitLifecycle(t, "recovery creates one new committed attempt", func() bool {
		history = readRecoveryHistory(t, historyHarness, historyFixture)
		return len(history.Allocations) == 2 && history.Allocations[0].State == "RELEASED"
	})
	retry := history.Allocations[1]
	if retry.ID == fixture.AllocationID || retry.AttemptID == history.Allocations[0].AttemptID || retry.Epoch <= fixture.RunnerEpoch {
		t.Fatalf("restart did not create distinct retry ownership: %s", history.raw)
	}
	retryFixture := fixture
	retryFixture.AllocationID, retryFixture.RunnerEpoch = retry.ID, retry.Epoch
	afterRestart := harness.waitSnapshot(t, retryFixture, func(state controllerSnapshot) bool {
		return state.Allocation.Incarnation > firstIncarnation && state.Allocation.ReportStatus != nil && *state.Allocation.ReportStatus == "RUNNING"
	})
	stopRestarted()
	beforeRejected := readRecoveryHistory(t, historyHarness, historyFixture)
	for _, oldIncarnation := range []int64{firstIncarnation, afterRestart.Allocation.Incarnation} {
		ack, err := client.Report(ctx, integrationReport(fixture, oldIncarnation, beforeRestart.Allocation.MaxSeq+100, StatusSucceeded))
		assertIntegrationRejected(t, ack, err, "fenced_rejected")
	}
	_, err = client.Poll(ctx, firstIncarnation, 0)
	assertIntegrationHTTPError(t, err, http.StatusConflict, "fenced_rejected")
	if after := readRecoveryHistory(t, historyHarness, historyFixture); !bytes.Equal(after.raw, beforeRejected.raw) {
		t.Fatalf("old allocation/incarnation changed PostgreSQL rows: before=%s after=%s", beforeRejected.raw, after.raw)
	}
	if len(beforeRejected.Attempts) != 2 || beforeRejected.Attempts[0].Result == nil || *beforeRejected.Attempts[0].Result != "INTERRUPTED" || beforeRejected.Attempts[0].Reason == nil || beforeRejected.Attempts[1].Result != nil || beforeRejected.Job.Result != nil || afterRestart.Active != 1 {
		t.Fatalf("restart lost attempt history or prematurely finalized job: %s", beforeRejected.raw)
	}
	data, err := os.ReadFile(fixture.Marker)
	if err != nil || string(data) != "run\nrun\n" {
		t.Fatalf("original and retry must each execute exactly once: %q %v", data, err)
	}
	t.Logf("lost poll response retained one committed launch; restart preserved interrupted attempt and same logical job with one fresh attempt at epoch %d; stale reports made no writes; CLI SIGTERM joined within 5s", retry.Epoch)
}

func TestControllerHeartbeatLiveness(t *testing.T) {
	harness := loadControllerHarness(t)
	fixture := harness.Fixtures["heartbeat"]
	d := integrationDaemon(t, harness, fixture, t.TempDir())
	stop := startIntegrationDaemon(t, d)
	running := harness.waitSnapshot(t, fixture, func(state controllerSnapshot) bool {
		return state.Allocation.ReportStatus != nil && *state.Allocation.ReportStatus == "RUNNING"
	})
	start := time.Now()
	alive := harness.waitSnapshot(t, fixture, func(state controllerSnapshot) bool {
		return state.Allocation.MaxSeq >= running.Allocation.MaxSeq+2
	})
	stop()
	if alive.Allocation.ReportStatus == nil || *alive.Allocation.ReportStatus != "RUNNING" ||
		alive.Runner.State != running.Runner.State || alive.Runner.Epoch != running.Runner.Epoch ||
		alive.Allocation.State != "ACTIVE" || alive.Allocation.Incarnation != d.Incarnation() {
		t.Fatalf("heartbeat changed ownership or progress: before=%s after=%s", running.raw, alive.raw)
	}
	assertSingleIntegrationExecution(t, fixture)
	t.Logf("default 1s heartbeat persisted seq %d -> %d in %s without changing RUNNING or ACTIVE ownership",
		running.Allocation.MaxSeq, alive.Allocation.MaxSeq, time.Since(start).Round(time.Millisecond))
}

func TestControllerReorderedReportsRemainTerminal(t *testing.T) {
	harness := loadControllerHarness(t)
	fixture := harness.Fixtures["reorder"]
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client := integrationClient(t, harness.URL, fixture.Token)
	if _, err := client.Poll(ctx, 1, 0); err != nil {
		t.Fatal(err)
	}
	ack, err := client.Report(ctx, integrationReport(fixture, 1, 1, StatusRunning))
	if err != nil || !ack.Accepted {
		t.Fatalf("initial progress: %+v, %v", ack, err)
	}

	target, err := url.Parse(harness.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	proxy.Transport = transport
	defer transport.CloseIdleConnections()
	delayed, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseDelayed := func() { releaseOnce.Do(func() { close(release) }) }
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		r.Body.Close()
		r.Body = io.NopCloser(bytes.NewReader(data))
		report, err := ParseReportRequest(data)
		if err == nil && report.Seq == 2 {
			close(delayed)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		proxy.ServeHTTP(w, r)
	}))
	defer server.Close()
	defer releaseDelayed()
	reordered := integrationClient(t, server.URL, fixture.Token)
	type result struct {
		ack ReportResponse
		err error
	}
	late := make(chan result, 1)
	go func() {
		ack, err := reordered.Report(ctx, integrationReport(fixture, 1, 2, StatusRunning))
		late <- result{ack, err}
	}()
	select {
	case <-delayed:
	case <-ctx.Done():
		t.Fatal("earlier report did not reach the delay proxy")
	}
	ack, err = reordered.Report(ctx, integrationReport(fixture, 1, 3, StatusSucceeded))
	if err != nil || !ack.Accepted || !ack.Terminal {
		t.Fatalf("reordered terminal: %+v, %v", ack, err)
	}
	terminal := harness.snapshot(t, fixture)
	releaseDelayed()
	select {
	case result := <-late:
		assertIntegrationRejected(t, result.ack, result.err, "dropped_stale")
	case <-ctx.Done():
		t.Fatal("delayed report did not complete")
	}
	ack, err = client.Report(ctx, integrationReport(fixture, 1, 4, StatusRunning))
	assertIntegrationRejected(t, ack, err, "terminal_sticky")
	if after := harness.snapshot(t, fixture); after.raw != terminal.raw {
		t.Fatalf("reordered progress rewrote terminal rows: before=%s after=%s", terminal.raw, after.raw)
	}
	ack, err = client.Report(ctx, integrationReport(fixture, 1, 5, StatusHeartbeat))
	if err != nil || !ack.Accepted || !ack.Terminal {
		t.Fatalf("post-terminal heartbeat: %+v, %v", ack, err)
	}
	after := harness.snapshot(t, fixture)
	if after.Allocation.MaxSeq != 5 || after.Allocation.ReportStatus == nil || *after.Allocation.ReportStatus != "SUCCEEDED" ||
		after.Runner.State != "CLEANING" || after.Allocation.State != "ACTIVE" || after.Runner.Epoch != fixture.RunnerEpoch {
		t.Fatalf("terminal or ownership regressed: %s", after.raw)
	}
	t.Log("real HTTP reorder: seq 3 SUCCEEDED arrived before seq 2 RUNNING; stale/progress rejected without writes; seq 5 heartbeat preserved terminal")
}

func TestControllerInvalidMachineIdentity(t *testing.T) {
	harness := loadControllerHarness(t)
	fixture := harness.Fixtures["identity"]
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	before := harness.snapshot(t, fixture)
	for _, token := range []string{"invalid-machine-token", "test-key-alpha"} {
		client := integrationClient(t, harness.URL, token)
		_, err := client.Poll(ctx, 1, 0)
		assertIntegrationHTTPError(t, err, http.StatusUnauthorized, "unauthorized")
		_, err = client.Report(ctx, integrationReport(fixture, 1, 1, StatusRunning))
		assertIntegrationHTTPError(t, err, http.StatusUnauthorized, "unauthorized")
	}
	other := harness.Fixtures["reorder"]
	client := integrationClient(t, harness.URL, other.Token)
	_, err := client.Report(ctx, integrationReport(fixture, 1, 1, StatusRunning))
	assertIntegrationHTTPError(t, err, http.StatusForbidden, "fenced_rejected")
	if after := harness.snapshot(t, fixture); after.raw != before.raw {
		t.Fatalf("invalid identity changed PostgreSQL rows: before=%s after=%s", before.raw, after.raw)
	}
	t.Log("invalid machine token, submit-only token, and cross-runner report rejected server-side with unchanged PostgreSQL rows")
}

type harnessClaim struct {
	Assigned     bool   `json:"assigned"`
	AllocationID string `json:"allocation_id"`
	AttemptID    string `json:"attempt_id"`
	JobID        string `json:"job_id"`
	RunnerEpoch  int64  `json:"runner_epoch"`
}

func claimViaControl(controlURL, controlToken, runnerID, jobID string) (harnessClaim, error) {
	body, err := json.Marshal(map[string]string{"runner_id": runnerID, "job_id": jobID})
	if err != nil {
		return harnessClaim{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, controlURL+"/claim", bytes.NewReader(body))
	if err != nil {
		return harnessClaim{}, err
	}
	req.Header.Set("X-Harness-Token", controlToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return harnessClaim{}, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return harnessClaim{}, err
	}
	var claim harnessClaim
	return claim, json.Unmarshal(data, &claim)
}

func TestControllerHeartbeatLossQuarantinesRunnerWhileWorkloadAlive(t *testing.T) {
	harness := loadControllerHarness(t)
	fixture, ok := harness.Fixtures["heartbeat-loss"]
	if !ok {
		t.Fatal("heartbeat-loss fixture required")
	}

	target, err := url.Parse(harness.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	proxy.Transport = transport
	defer transport.CloseIdleConnections()

	var severed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if severed.Load() {
			http.Error(w, "network partition", http.StatusServiceUnavailable)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	defer server.Close()

	d := integrationDaemon(t, harness, controllerFixture{
		RunnerID:     fixture.RunnerID,
		Token:        fixture.Token,
		AllocationID: fixture.AllocationID,
		JobID:        fixture.JobID,
		RunnerEpoch:  fixture.RunnerEpoch,
		Argv:         fixture.Argv,
		Marker:       fixture.Marker,
	}, t.TempDir())
	client, err := NewClient(server.URL, fixture.Token)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	d.client = client

	stop := startIntegrationDaemon(t, d)
	defer stop()

	// Wait for allocation to reach RUNNING
	running := harness.waitSnapshot(t, fixture, func(state controllerSnapshot) bool {
		return state.Allocation.ReportStatus != nil && *state.Allocation.ReportStatus == "RUNNING"
	})

	// Verify workload started
	data, err := os.ReadFile(fixture.Marker)
	if err != nil || string(data) != "run\n" {
		t.Fatalf("workload did not run before partition: %q %v", data, err)
	}

	// 1. Cut controller-agent communication!
	severed.Store(true)

	// 2. Independently verify the workload remains alive while communication is cut
	time.Sleep(300 * time.Millisecond)
	name := strings.ToLower(fixture.AllocationID) + "-" + strconv.FormatInt(fixture.RunnerEpoch, 10)
	cgroupProcs := filepath.Join(os.Getenv("CLEARANCE_CGROUP_ROOT"), name, "cgroup.procs")
	procsData, err := os.ReadFile(cgroupProcs)
	if err != nil {
		t.Fatalf("failed to read workload cgroup.procs: %v", err)
	}
	pids := strings.Fields(string(procsData))
	if len(pids) == 0 {
		t.Fatalf("no processes in workload cgroup: %s", cgroupProcs)
	}
	pid, err := strconv.Atoi(pids[0])
	if err != nil {
		t.Fatalf("invalid PID %q in cgroup.procs: %v", pids[0], err)
	}
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("workload process %d is not alive during communication loss: %v", pid, err)
	}
	if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); err != nil {
		t.Fatalf("workload proc directory /proc/%d missing: %v", pid, err)
	}

	// 3. Observe timeout-driven quarantine
	quarantined := harness.waitSnapshot(t, fixture, func(state controllerSnapshot) bool {
		return state.Runner.State == "QUARANTINED"
	})

	if quarantined.Runner.State != "QUARANTINED" {
		t.Fatalf("expected QUARANTINED runner state, got: %s", quarantined.Runner.State)
	}
	if quarantined.Runner.QuarantineReason == nil || !strings.Contains(*quarantined.Runner.QuarantineReason, "heartbeat timeout: ") {
		t.Fatalf("expected heartbeat timeout quarantine reason, got: %v", quarantined.Runner.QuarantineReason)
	}
	if !strings.Contains(*quarantined.Runner.QuarantineReason, fixture.AllocationID) {
		t.Fatalf("expected quarantine reason to include allocation ID %s, got: %v", fixture.AllocationID, quarantined.Runner.QuarantineReason)
	}

	// Process remains alive: quarantine preserves active ownership without killing or resolving execution
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("workload process %d terminated prematurely after quarantine: %v", pid, err)
	}

	// 4. Retained active ownership and unresolved job/attempt result
	if quarantined.Allocation.State != "ACTIVE" {
		t.Fatalf("expected allocation to remain ACTIVE, got: %s", quarantined.Allocation.State)
	}
	if quarantined.Allocation.ReportStatus == nil || *quarantined.Allocation.ReportStatus != "RUNNING" {
		t.Fatalf("expected report status to remain RUNNING, got: %v", quarantined.Allocation.ReportStatus)
	}
	if quarantined.Job.Result != nil {
		t.Fatalf("expected job result to remain unresolved (nil), got: %v", *quarantined.Job.Result)
	}
	if quarantined.Attempt.Result != nil {
		t.Fatalf("expected attempt result to remain unresolved (nil), got: %v", *quarantined.Attempt.Result)
	}

	// 5. Refused claims:
	// Claiming this quarantined runner for another job must be refused!
	spareRunner := fixture.SpareRunnerID
	if spareRunner == "" {
		spareRunner = harness.SpareRunnerID
	}
	if harness.ControlURL == "" || harness.ControlToken == "" || fixture.NextJobID == "" || spareRunner == "" {
		t.Fatal("control server, next job ID, and spare runner required to verify refused claims")
	}
	claimResp, err := claimViaControl(harness.ControlURL, harness.ControlToken, fixture.RunnerID, fixture.NextJobID)
	if err != nil || claimResp.Assigned {
		t.Fatalf("quarantined runner accepted claim for new job: %+v, %v", claimResp, err)
	}
	// Claiming this unresolved job on another available runner must be refused!
	claimResp, err = claimViaControl(harness.ControlURL, harness.ControlToken, spareRunner, fixture.JobID)
	if err != nil || claimResp.Assigned {
		t.Fatalf("unresolved job accepted claim on another runner: %+v, %v", claimResp, err)
	}

	// 6. Restored connectivity: reports cannot clear quarantine
	severed.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Direct client report with next sequence
	ack, err := client.Report(ctx, integrationReport(fixture, d.Incarnation(), running.Allocation.MaxSeq+10, StatusHeartbeat))
	if err != nil {
		t.Fatalf("restored report failed: %v", err)
	}
	if !ack.Accepted {
		t.Fatalf("heartbeat report should be accepted: %+v", ack)
	}
	afterReport := harness.snapshot(t, fixture)
	if afterReport.Runner.State != "QUARANTINED" {
		t.Fatalf("heartbeat report cleared quarantine: %s", afterReport.raw)
	}
	if afterReport.Allocation.State != "ACTIVE" {
		t.Fatalf("heartbeat report changed allocation state: %s", afterReport.raw)
	}

	t.Logf("heartbeat loss cleanly quarantined runner with inspectable reason, preserved active ownership and unresolved job, refused claims, and restored traffic could not clear quarantine")
}
