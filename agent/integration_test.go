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
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type controllerFixture struct {
	RunnerID     string   `json:"runner_id"`
	Token        string   `json:"token"`
	AllocationID string   `json:"allocation_id"`
	JobID        string   `json:"job_id"`
	RunnerEpoch  int64    `json:"runner_epoch"`
	Argv         []string `json:"argv"`
	Marker       string   `json:"marker"`
}

type controllerHarness struct {
	URL      string                       `json:"url"`
	Schema   string                       `json:"schema"`
	Fixtures map[string]controllerFixture `json:"fixtures"`
}

type controllerSnapshot struct {
	Runner struct {
		Epoch       int64  `json:"epoch"`
		Incarnation int64  `json:"agent_incarnation"`
		State       string `json:"state"`
	} `json:"runner"`
	Allocation struct {
		MaxSeq       int64   `json:"max_seq"`
		Incarnation  int64   `json:"agent_incarnation"`
		ReportStatus *string `json:"report_status"`
		State        string  `json:"state"`
	} `json:"allocation"`
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
	if !regexp.MustCompile(`^agent_integration_[0-9]+_[0-9]+$`).MatchString(harness.Schema) {
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

	// Exercise the standalone executable and OS signal shutdown, then boot it again.
	stopRestarted := startIntegrationProcess(t, harness, fixture, stateDir)
	afterRestart := harness.waitSnapshot(t, fixture, func(state controllerSnapshot) bool {
		return state.Allocation.Incarnation > firstIncarnation && state.Allocation.MaxSeq > beforeRestart.Allocation.MaxSeq
	})
	stopRestarted()
	beforeSecondBoot := harness.snapshot(t, fixture)
	stopSecondProcess := startIntegrationProcess(t, harness, fixture, stateDir)
	afterSecondBoot := harness.waitSnapshot(t, fixture, func(state controllerSnapshot) bool {
		return state.Allocation.Incarnation > afterRestart.Allocation.Incarnation && state.Allocation.MaxSeq > beforeSecondBoot.Allocation.MaxSeq
	})
	stopSecondProcess()
	assertSingleIntegrationExecution(t, fixture)
	beforeRejected := harness.snapshot(t, fixture)
	for _, oldIncarnation := range []int64{firstIncarnation, afterRestart.Allocation.Incarnation} {
		ack, err := client.Report(ctx, integrationReport(fixture, oldIncarnation, beforeRejected.Allocation.MaxSeq+1, StatusSucceeded))
		assertIntegrationRejected(t, ack, err, "fenced_rejected")
		_, err = client.Poll(ctx, oldIncarnation, 0)
		assertIntegrationHTTPError(t, err, http.StatusConflict, "fenced_rejected")
	}
	if after := harness.snapshot(t, fixture); after.raw != beforeRejected.raw {
		t.Fatalf("old incarnation changed PostgreSQL rows: before=%s after=%s", beforeRejected.raw, after.raw)
	}
	if afterSecondBoot.Attempts != 1 || afterSecondBoot.Active != 1 || afterSecondBoot.Runner.Epoch != fixture.RunnerEpoch || afterSecondBoot.Runner.State != "ASSIGNED" {
		t.Fatalf("restart changed allocation ownership: %s", afterSecondBoot.raw)
	}
	t.Logf("PostgreSQL proof: one execution/allocation/attempt, incarnation %d -> %d -> %d, seq %d -> %d -> %d; old incarnations rejected with unchanged rows; two CLI SIGTERM shutdowns within 5s",
		firstIncarnation, afterRestart.Allocation.Incarnation, afterSecondBoot.Allocation.Incarnation,
		beforeRestart.Allocation.MaxSeq, afterRestart.Allocation.MaxSeq, afterSecondBoot.Allocation.MaxSeq)
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
