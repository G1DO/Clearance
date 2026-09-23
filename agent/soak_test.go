package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"
)

// This is a defined-load hygiene check, not a capacity benchmark. Run separately
// from other tests so unrelated goroutines do not contaminate the measurements.
func TestDaemonHygieneSoak(t *testing.T) {
	if os.Getenv("AGENT_SOAK") != "1" {
		t.Skip("set AGENT_SOAK=1 for the five-agent, five-minute hygiene soak")
	}
	duration := 5 * time.Minute
	if value := os.Getenv("AGENT_SOAK_DURATION"); value != "" {
		var err error
		duration, err = time.ParseDuration(value)
		if err != nil || duration < 5*time.Second || duration > 5*time.Minute {
			t.Fatal("AGENT_SOAK_DURATION must be between 5s and 5m; shorter runs are smoke checks only")
		}
	}
	dir := filepath.Join("target", "hygiene-soak")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	command, err := exec.LookPath("sleep")
	if err != nil {
		t.Fatal("the hygiene fixture requires the Linux sleep executable: ", err)
	}
	fixture := &soakController{command: command, connections: make(map[net.Conn]bool), failure: make(chan error, 1)}
	server := httptest.NewUnstartedServer(http.HandlerFunc(fixture.serveHTTP))
	server.Config.ConnState = fixture.connectionState
	server.Start()
	defer server.Close()
	baseline := runtime.NumGoroutine()
	summary := soakSummary{Agents: 5, BaselineGoroutines: baseline}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type runningDaemon struct {
		daemon *Daemon
		done   chan error
	}
	daemons := make([]runningDaemon, 0, 5)
	defer func() {
		cancel()
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		for _, running := range daemons {
			select {
			case err := <-running.done:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("daemon shutdown: %v", err)
				}
			case <-shutdown.Done():
				t.Error("daemon did not stop within 5s")
			}
			if err := running.daemon.Close(); err != nil {
				t.Errorf("daemon close: %v", err)
			}
		}
		// Keep the fixture listener alive until after this sample, matching baseline.
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			if fixture.snapshot().Connections == 0 && runtime.NumGoroutine() <= baseline+3 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		after := writeSoakProfiles(t, dir, "shutdown")
		summary.ShutdownGoroutines = after
		summary.ShutdownConnections = fixture.snapshot().Connections
		if after-baseline > 3 {
			t.Errorf("shutdown goroutine growth %d exceeds budget 3 (before %d, after %d)", after-baseline, baseline, after)
		}
		if summary.ShutdownConnections != 0 {
			t.Errorf("shutdown left %d HTTP connections open", summary.ShutdownConnections)
		}
		data, err := json.MarshalIndent(summary, "", "  ")
		if err != nil {
			t.Error(err)
		} else if err := os.WriteFile(filepath.Join(dir, "summary.json"), append(data, '\n'), 0600); err != nil {
			t.Error(err)
		}
		t.Logf("hygiene evidence: %s; duration=%s polls=%d reports=%d goroutine_delta=%d peak_connections=%d shutdown_delta=%d",
			dir, summary.MeasuredDuration, summary.Polls, summary.Reports, summary.AfterGoroutines-summary.BeforeGoroutines,
			summary.PeakConnections, after-baseline)
	}()
	for i := 0; i < 5; i++ {
		daemon, err := NewDaemon(Config{
			ControllerURL: server.URL, MachineToken: fmt.Sprintf("soak-%d", i), StateDir: t.TempDir(),
			PollTimeout: 10 * time.Second, HeartbeatInterval: time.Second,
			RetryInterval: time.Second, ReportTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		useTestExecution(t, daemon)
		running := runningDaemon{daemon: daemon, done: make(chan error, 1)}
		daemons = append(daemons, running)
		go func() {
			err := running.daemon.Run(ctx)
			running.done <- err
			if ctx.Err() == nil {
				select {
				case fixture.failure <- fmt.Errorf("daemon stopped before cancellation: %v", err):
				default:
				}
			}
		}()
		// Keep all five agents active while avoiding synchronized measurement noise.
		time.Sleep(100 * time.Millisecond)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		ready := true
		for _, agent := range fixture.snapshot().Agents {
			ready = ready && agent.Polls >= 3 && agent.Heartbeats >= 2 && agent.Status == StatusRunning
		}
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("agents did not reach steady poll/running-heartbeat load during warm-up")
		}
		select {
		case err := <-fixture.failure:
			t.Fatal(err)
		case <-time.After(20 * time.Millisecond):
		}
	}
	summary.BeforeGoroutines = writeSoakProfiles(t, dir, "before")
	before := fixture.snapshot()
	started := time.Now()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case err := <-fixture.failure:
		t.Error(err)
	case <-timer.C:
	}
	summary.MeasuredDuration = time.Since(started).String()
	summary.FullSoak = duration == 5*time.Minute && time.Since(started) >= duration
	summary.AfterGoroutines = writeSoakProfiles(t, dir, "after")
	after := fixture.snapshot()
	summary.BeforeConnections = before.Connections
	summary.AfterConnections = after.Connections
	summary.PeakConnections = after.PeakConnections
	minimum := int64(duration/time.Second) * 95 / 100
	maximum := int64(duration/time.Second)*105/100 + 2
	for i, agent := range after.Agents {
		polls := agent.Polls - before.Agents[i].Polls
		reports := agent.Reports - before.Agents[i].Reports
		summary.Polls += polls
		summary.Reports += reports
		if polls < minimum || polls > maximum || reports < minimum || reports > maximum {
			t.Errorf("agent %d did not sustain defined load: polls=%d reports=%d, expected each in [%d,%d]", i, polls, reports, minimum, maximum)
		}
	}
	if delta := summary.AfterGoroutines - summary.BeforeGoroutines; delta > 3 {
		t.Errorf("steady-state goroutine growth %d exceeds budget 3", delta)
	}
	if summary.PeakConnections > 10 {
		t.Errorf("peak HTTP connections %d exceeds budget 10", summary.PeakConnections)
	}
}

type soakSummary struct {
	Agents              int    `json:"agents"`
	FullSoak            bool   `json:"full_five_minute_soak"`
	MeasuredDuration    string `json:"measured_duration"`
	Polls               int64  `json:"polls"`
	Reports             int64  `json:"reports"`
	BaselineGoroutines  int    `json:"baseline_goroutines"`
	BeforeGoroutines    int    `json:"before_goroutines"`
	AfterGoroutines     int    `json:"after_goroutines"`
	ShutdownGoroutines  int    `json:"shutdown_goroutines"`
	BeforeConnections   int    `json:"before_connections"`
	AfterConnections    int    `json:"after_connections"`
	PeakConnections     int    `json:"peak_connections"`
	ShutdownConnections int    `json:"shutdown_connections"`
}

type soakAgentCounts struct {
	Polls       int64
	Reports     int64
	Heartbeats  int64
	Incarnation int64
	Seq         int64
	Status      ReportStatus
}

type soakCounts struct {
	Agents          [5]soakAgentCounts
	Connections     int
	PeakConnections int
}

type soakController struct {
	mu          sync.Mutex
	command     string
	connections map[net.Conn]bool
	counts      soakCounts
	failure     chan error
}

func (s *soakController) snapshot() soakCounts {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts
}

func (s *soakController) connectionState(conn net.Conn, state http.ConnState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch state {
	case http.StateNew:
		s.connections[conn] = true
	case http.StateClosed, http.StateHijacked:
		delete(s.connections, conn)
	}
	// Count each accepted TCP connection once, regardless of active/idle transitions.
	s.counts.Connections = len(s.connections)
	if s.counts.Connections > s.counts.PeakConnections {
		s.counts.PeakConnections = s.counts.Connections
	}
}

func (s *soakController) serveHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fail := func(format string, args ...any) {
		err := fmt.Errorf(format, args...)
		select {
		case s.failure <- err:
		default:
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	index := -1
	for i := range s.counts.Agents {
		if r.Header.Get("Authorization") == fmt.Sprintf("Bearer soak-%d", i) {
			index = i
		}
	}
	if index < 0 || r.Method != http.MethodPost {
		fail("missing machine identity or wrong HTTP method")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 16<<10))
	if err != nil {
		fail("read request: %v", err)
		return
	}
	if strings.Contains(string(body), "recoveryGeneration") {
		fail("reserved recoveryGeneration sent on v1")
		return
	}
	agent := &s.counts.Agents[index]
	allocationID := fmt.Sprintf("11111111-1111-1111-1111-%012d", index+1)
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/internal/v1/agents/poll":
		var request struct {
			Incarnation *int64 `json:"agent_incarnation"`
			Timeout     *int   `json:"timeout_s"`
		}
		if err := json.Unmarshal(body, &request); err != nil || request.Incarnation == nil || request.Timeout == nil || *request.Timeout != 10 {
			fail("invalid poll identity or long-poll timeout: %s", body)
			return
		}
		if agent.Polls > 0 && agent.Incarnation != *request.Incarnation {
			fail("incarnation changed within a daemon lifetime")
			return
		}
		agent.Incarnation = *request.Incarnation
		agent.Polls++
		jobID, epoch, class, pause := "22222222-2222-2222-2222-222222222222", int64(1), "default", int64(1000)
		response, err := EncodePollResponse(PollResponse{
			Assigned: true, AllocationID: &allocationID, JobID: &jobID, RunnerEpoch: &epoch,
			Argv: []string{s.command, "3600"}, RunnerClass: &class, PollAfterMs: &pause,
		})
		if err != nil {
			fail("encode fixture: %v", err)
			return
		}
		_, _ = w.Write(response)
	case "/internal/v1/agents/report":
		report, err := ParseReportRequest(body)
		if err != nil {
			fail("invalid report: %v", err)
			return
		}
		if agent.Polls == 0 || report.AllocationID != allocationID || report.RunnerEpoch != 1 || report.AgentIncarnation != agent.Incarnation || report.Seq != agent.Seq+1 {
			fail("report changed fencing or skipped/reused sequence: %+v", report)
			return
		}
		agent.Seq = report.Seq
		agent.Reports++
		if report.Status == StatusHeartbeat {
			agent.Heartbeats++
		} else {
			if agent.Status == StatusSucceeded || report.Status == StatusFailed {
				fail("fixture command reran or failed: status=%s previous=%s", report.Status, agent.Status)
				return
			}
			agent.Status = report.Status
		}
		_, _ = fmt.Fprintf(w, `{"accepted":true,"reason":"ok","terminal":%t}`, agent.Status == StatusSucceeded)
	default:
		fail("unexpected agent endpoint %s", r.URL.Path)
	}
}

func writeSoakProfiles(t *testing.T, dir, phase string) int {
	t.Helper()
	runtime.GC()
	count := runtime.NumGoroutine()
	for _, name := range []string{"heap", "goroutine"} {
		file, err := os.OpenFile(filepath.Join(dir, phase+"-"+name+".pprof"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		if err != nil {
			t.Error(err)
			continue
		}
		if err := pprof.Lookup(name).WriteTo(file, 0); err != nil {
			t.Error(err)
		}
		if err := file.Close(); err != nil {
			t.Error(err)
		}
	}
	return count
}
