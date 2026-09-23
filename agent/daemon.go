package agent

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type Config struct {
	ControllerURL, MachineToken, StateDir                        string
	CgroupRoot, WorkspaceRoot                                    string
	GracePeriod, KillTimeout                                     time.Duration
	PollTimeout, HeartbeatInterval, RetryInterval, ReportTimeout time.Duration
}

// Daemon runs one allocation at a time. Durable launch intent prevents replay
// across crashes; an uncertain pre-restart workload is discovered and resolved
// by the controller without relaunch. Physical cleanup and its acknowledgment are
// separate from the sticky execution result.
type Daemon struct {
	cfg                   Config
	client                *Client
	store                 *stateStore
	incarnation           int64
	mu                    sync.Mutex
	used, running, closed bool
	prepare               func(PollResponse) (allocationWorkload, error)
	discover              func(PollResponse) (allocationWorkload, DiscoveryEvidence, error)
}

func NewDaemon(cfg Config) (*Daemon, error) {
	if cfg.PollTimeout == 0 {
		cfg.PollTimeout = 10 * time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = time.Second
	}
	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = time.Second
	}
	if cfg.ReportTimeout == 0 {
		cfg.ReportTimeout = 5 * time.Second
	}
	if cfg.GracePeriod < 0 || cfg.GracePeriod > 5*time.Second || cfg.KillTimeout < 0 || cfg.KillTimeout > 5*time.Second || cfg.StateDir == "" || cfg.PollTimeout < time.Second || cfg.PollTimeout > 30*time.Second || cfg.PollTimeout%time.Second != 0 || cfg.HeartbeatInterval <= 0 || cfg.RetryInterval <= 0 || cfg.ReportTimeout <= 0 || cfg.ReportTimeout > 5*time.Second {
		return nil, errors.New("state directory and positive durations required; poll must be whole seconds in 1..30, report and cleanup phases at most 5s")
	}
	client, err := NewClient(cfg.ControllerURL, cfg.MachineToken)
	if err != nil {
		return nil, err
	}
	store, err := openState(cfg.StateDir)
	if err != nil {
		client.Close()
		return nil, err
	}
	if cfg.CgroupRoot == "" {
		cfg.CgroupRoot = "/sys/fs/cgroup/clearance"
	}
	if cfg.WorkspaceRoot == "" {
		cfg.WorkspaceRoot = filepath.Clean(cfg.StateDir) + "-workspaces"
	}
	if cfg.GracePeriod == 0 {
		cfg.GracePeriod = time.Second
	}
	if cfg.KillTimeout == 0 {
		cfg.KillTimeout = 2 * time.Second
	}
	d := &Daemon{cfg: cfg, client: client, store: store, incarnation: store.state.Incarnation}
	d.prepare = func(p PollResponse) (allocationWorkload, error) {
		c, err := newContainment(containmentConfig{CgroupRoot: cfg.CgroupRoot, WorkspaceRoot: cfg.WorkspaceRoot,
			StateDir: cfg.StateDir, GracePeriod: cfg.GracePeriod, KillTimeout: cfg.KillTimeout})
		if err != nil {
			return nil, err
		}
		w, err := c.Prepare(p)
		if w == nil {
			return nil, err
		}
		return w, err
	}
	d.discover = func(p PollResponse) (allocationWorkload, DiscoveryEvidence, error) {
		c, err := newContainment(containmentConfig{CgroupRoot: cfg.CgroupRoot, WorkspaceRoot: cfg.WorkspaceRoot,
			StateDir: cfg.StateDir, GracePeriod: cfg.GracePeriod, KillTimeout: cfg.KillTimeout})
		if err != nil {
			return nil, DiscoveryEvidence{}, err
		}
		w, evidence, err := c.Discover(p)
		if w == nil {
			return nil, evidence, err
		}
		return w, evidence, err
	}
	return d, nil
}

func (d *Daemon) Incarnation() int64 { return d.incarnation }

// Close releases an unstarted daemon. Run closes its own resources after joining
// workers; cancel Run's context to stop a running daemon.
func (d *Daemon) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.running {
		return errors.New("cancel Run before closing the daemon")
	}
	if d.closed {
		return nil
	}
	d.closed = true
	d.client.Close()
	return d.store.Close()
}

type pollResult struct {
	assignment PollResponse
	err        error
}

func (d *Daemon) poll(ctx context.Context, output chan<- pollResult) {
	for {
		assignment, err := d.client.Poll(ctx, d.incarnation, int(d.cfg.PollTimeout/time.Second))
		if ctx.Err() != nil {
			return
		}
		if err == nil || !retryable(err) {
			select {
			case output <- pollResult{assignment, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
		// Assigned and immediate-idle replies must not create a busy poll loop.
		delay := d.cfg.RetryInterval
		if err == nil && assignment.PollAfterMs != nil {
			// Saturate before duration conversion (v1 allows MaxInt64 milliseconds).
			ms := *assignment.PollAfterMs
			if ms > int64((30*time.Second)/time.Millisecond) {
				ms = 30000
			}
			if hint := time.Duration(ms) * time.Millisecond; hint > delay {
				delay = hint
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}

// allocationWorkload keeps the event loop independent of blocking Linux cleanup.
// Production always uses cgroup containment; test doubles exist only in _test.go.
type allocationWorkload interface {
	Start() error
	Wait() error
	Cleanup() (CleanupEvidence, error)
}

type executionEvent struct {
	running  bool
	terminal ReportStatus
	cleanup  *CleanupEvidence
}

func positiveCleanup(p *CleanupEvidence) bool {
	return p != nil && p.ExecutionEmpty && p.DescendantsReaped && p.WorkspaceClean && p.Error == nil
}

// Linux filenames may contain arbitrary bytes. Keep cleanup diagnostics valid
// for durable state and wire encoding, even when the byte limit cuts a rune.
func cleanupErrorMessage(err error) string {
	message := strings.ToValidUTF8(err.Error(), "\ufffd")
	const limit = 2048
	if len(message) > limit {
		end := limit
		for !utf8.RuneStart(message[end]) {
			end--
		}
		message = message[:end]
	}
	return message
}

// execute owns the workload deadline independently of HTTP/report latency. When
// completion is already observable it wins a concurrent stop request. Otherwise
// an expired deadline wins over cancellation, including when both are ready.
// Cleanup never changes the execution result.
func (d *Daemon) execute(ctx context.Context, p PollResponse, stop <-chan struct{}, events chan<- executionEvent) {
	emit := func(e executionEvent) {
		select {
		case events <- e:
		case <-ctx.Done():
		}
	}
	w, err := d.prepare(p)
	if ctx.Err() != nil {
		if w != nil {
			_, _ = w.Cleanup()
		}
		return
	}
	cancelledBeforeLaunch := false
	select {
	case <-stop:
		cancelledBeforeLaunch = true
	default:
	}
	if err == nil && !cancelledBeforeLaunch {
		err = w.Start()
	}
	result := StatusFailed
	if cancelledBeforeLaunch {
		emit(executionEvent{terminal: StatusCancelled})
		proof := CleanupEvidence{}
		cleanupErr := err
		if w != nil {
			proof, cleanupErr = w.Cleanup()
		}
		if cleanupErr != nil && proof.Error == nil {
			message := cleanupErrorMessage(cleanupErr)
			proof.Error = &message
		}
		emit(executionEvent{cleanup: &proof})
		return
	}
	if err == nil {
		timeout := time.Hour
		if p.WorkloadTimeoutMs != nil {
			timeout = time.Duration(*p.WorkloadTimeoutMs) * time.Millisecond
		}
		deadline := time.Now().Add(timeout)
		timer := time.NewTimer(timeout)
		done := make(chan error, 1)
		go func() { done <- w.Wait() }()
		emit(executionEvent{running: true})
		select {
		case err = <-done:
			if err == nil {
				result = StatusSucceeded
			}
		case <-stop:
			select {
			case err = <-done:
				if err == nil {
					result = StatusSucceeded
				}
			default:
				if time.Now().Before(deadline) {
					result = StatusCancelled
				} else {
					result = StatusTimedOut
				}
			}
		case <-timer.C:
			select {
			case err = <-done:
				if err == nil {
					result = StatusSucceeded
				}
			default:
				result = StatusTimedOut
			}
		case <-ctx.Done():
			timer.Stop()
			_, cleanupErr := w.Cleanup()
			if cleanupErr == nil {
				<-done
			}
			return
		}
		timer.Stop()
		emit(executionEvent{terminal: result})
		proof, cleanupErr := w.Cleanup()
		// Wait is reusable; cleanup also joins the direct child. This waiter must not
		// outlive the daemon even when the terminal reason was cancellation/timeout.
		if cleanupErr == nil && (result == StatusCancelled || result == StatusTimedOut) {
			<-done
		}
		if cleanupErr != nil && proof.Error == nil {
			message := cleanupErrorMessage(cleanupErr)
			proof.Error = &message
		}
		emit(executionEvent{cleanup: &proof})
		return
	}
	emit(executionEvent{terminal: result})
	proof := CleanupEvidence{}
	if w != nil {
		proof, err = w.Cleanup()
	} // A failure to establish/inspect containment is never positive cleanup proof.
	if err != nil {
		message := cleanupErrorMessage(err)
		proof.Error = &message
	}
	emit(executionEvent{cleanup: &proof})
}

// Run has one poller, one allocation worker, and a serial report sender. The
// allocation worker enforces timeout and physical cleanup even during HTTP loss.
func (d *Daemon) Run(parent context.Context) (runErr error) {
	d.mu.Lock()
	if d.used || d.closed {
		d.mu.Unlock()
		return errors.New("daemon Run may only be called once")
	}
	d.used, d.running = true, true
	d.mu.Unlock()
	ctx, cancel := context.WithCancel(parent)
	var workers sync.WaitGroup
	defer func() {
		cancel()
		workers.Wait()
		d.mu.Lock()
		d.running = false
		d.mu.Unlock()
		runErr = errors.Join(runErr, d.Close())
	}()
	polls := make(chan pollResult, 1)
	workers.Add(1)
	go func() { defer workers.Done(); d.poll(ctx, polls) }()
	ticker := time.NewTicker(d.cfg.HeartbeatInterval)
	defer ticker.Stop()
	events := make(chan executionEvent, 2)
	var stop chan struct{}
	var stopOnce sync.Once
	var pending ReportStatus
	bound, executing := false, false
	var recoveryPending bool
	var discovery *DiscoveryEvidence
	var recovered allocationWorkload
	update := func(change func(*allocationState)) error {
		next := d.store.state
		allocation := *next.Allocation
		change(&allocation)
		next.Allocation = &allocation
		return d.store.save(next)
	}
	requestStop := func() {
		if stop != nil {
			stopOnce.Do(func() { close(stop) })
		}
	}
	start := func() {
		stop = make(chan struct{})
		stopOnce = sync.Once{}
		executing = true
		p := d.store.state.Allocation.Assignment
		if p.CancelRequested != nil && *p.CancelRequested {
			requestStop()
		}
		workers.Add(1)
		go func() { defer workers.Done(); d.execute(ctx, p, stop, events) }()
	}
	resolve := func() {
		executing = true
		workers.Add(1)
		go func() {
			defer workers.Done()
			proof, err := recovered.Cleanup()
			if err != nil {
				message := cleanupErrorMessage(err)
				proof.Error = &message
			}
			select {
			case events <- executionEvent{cleanup: &proof}:
			case <-ctx.Done():
			}
		}()
	}
	send := func() error {
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			a := d.store.state.Allocation
			if a.CleanupAcknowledged {
				return nil
			}
			status := pending
			if status == "" {
				status = StatusHeartbeat
			}
			if a.Seq == math.MaxInt64 {
				return errors.New("allocation sequence exhausted")
			}
			if err := update(func(a *allocationState) { a.Seq++ }); err != nil {
				return err
			}
			a = d.store.state.Allocation
			report := ReportRequest{AllocationID: *a.Assignment.AllocationID, RunnerEpoch: *a.Assignment.RunnerEpoch,
				AgentIncarnation: d.incarnation, Seq: a.Seq, Status: status, Ts: time.Now().UTC()}
			if status == StatusCleanup {
				report.Cleanup = a.Cleanup
			}
			if status == StatusRecovery {
				report.Discovery = discovery
			}
			reportCtx, reportCancel := context.WithTimeout(ctx, d.cfg.ReportTimeout)
			ack, err := d.client.Report(reportCtx, report)
			reportCancel()
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if retryable(err) {
					return nil
				}
				return err
			}
			if !ack.Accepted {
				// A newer claim can overtake a lost cleanup acknowledgment. Keep
				// polling for the authoritative next assignment; never reinterpret
				// this rejection as accepted proof or restart the previous command.
				if status == StatusCleanup && positiveCleanup(a.Cleanup) && ack.Reason == "fenced_rejected" {
					return nil
				}
				return fmt.Errorf("report rejected: %s", ack.Reason)
			}
			switch status {
			case StatusRecovery:
				if ack.Reason != "terminate" || !ack.Terminal || recovered == nil || discovery.Error != nil {
					return fmt.Errorf("recovery unresolved: %s", ack.Reason)
				}
				if err := update(func(a *allocationState) {
					if a.Terminal == "" {
						a.Terminal = terminalInterrupted
					}
					a.TerminalAcknowledged = true
					a.Cleanup, a.CleanupAcknowledged = nil, false
				}); err != nil {
					return err
				}
				recoveryPending = false
				resolve()
			case StatusStarting:
				if err := update(func(a *allocationState) { a.Started = true }); err != nil {
					return err
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				start()
			case StatusSucceeded, StatusFailed, StatusCancelled, StatusTimedOut:
				if err := update(func(a *allocationState) { a.TerminalAcknowledged = true }); err != nil {
					return err
				}
				if recoveryPending {
					pending = StatusRecovery
					continue
				}
				if d.store.state.Allocation.Cleanup != nil {
					pending = StatusCleanup
					continue
				}
			case StatusCleanup:
				if err := update(func(a *allocationState) { a.CleanupAcknowledged = true }); err != nil {
					return err
				}
			}
			pending = ""
			return nil
		}
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-polls:
			if result.err != nil {
				return result.err
			}
			p := result.assignment
			previous := d.store.state.Allocation
			if !p.Assigned {
				// An idle poll may overtake the cleanup acknowledgment. Continue its
				// retry; only a fenced accepted proof marks local cleanup acknowledged.
				if bound && previous != nil && !positiveCleanup(previous.Cleanup) {
					return errors.New("current allocation disappeared; stopping uncertain work")
				}

				continue
			}
			p.PollAfterMs = nil
			var handoffFailure error
			if previous != nil {
				comparable := p
				comparable.CancelRequested = previous.Assignment.CancelRequested
				if !reflect.DeepEqual(previous.Assignment, comparable) {
					if *p.RunnerEpoch <= *previous.Assignment.RunnerEpoch || *p.AllocationID == *previous.Assignment.AllocationID || !positiveCleanup(previous.Cleanup) || !previous.TerminalAcknowledged || executing {
						return errors.New("allocation changed without verified prior cleanup and newer epoch")
					}
					if previous.CleanupIncarnation != d.incarnation {
						// Release/next claim can overtake a lost reply and another boot.
						// Reinspect physical state before replacing the old local identity.
						w, evidence, err := d.discover(previous.Assignment)
						if err == nil && evidence.Error != nil {
							err = errors.New(*evidence.Error)
						}
						if err != nil || w == nil {
							handoffFailure = errors.Join(err, errors.New("discovery incomplete"))
						} else {
							proof, err := w.Cleanup()
							if err == nil && proof.Error != nil {
								err = errors.New(*proof.Error)
							}
							if err != nil || !positiveCleanup(&proof) {
								handoffFailure = errors.Join(err, errors.New("cleanup incomplete"))
							}
						}
					}
					bound = false
				}
			}
			if bound {
				if p.CancelRequested != nil && *p.CancelRequested {
					if err := update(func(a *allocationState) { a.Assignment.CancelRequested = p.CancelRequested }); err != nil {
						return err
					}
					requestStop()
				}
				continue
			}
			if previous == nil || *previous.Assignment.AllocationID != *p.AllocationID {
				next := d.store.state
				next.Allocation = &allocationState{Assignment: p}
				if handoffFailure != nil {
					// Ownership already advanced at the controller. Reserve a never-
					// launch marker under that identity so negative discovery can
					// quarantine it, even if another restart interrupts this report.
					// The old physical identity remains in containment.json.
					next.Allocation.Started, next.Allocation.Seq = true, 1
				}
				if err := d.store.save(next); err != nil {
					return err
				}
			}
			bound = true
			a := d.store.state.Allocation
			recoveryPending = a.Started && !a.CleanupAcknowledged
			if handoffFailure != nil {
				message := cleanupErrorMessage(fmt.Errorf("prior allocation %s at runner epoch %d cleanup could not be reverified: %w",
					*previous.Assignment.AllocationID, *previous.Assignment.RunnerEpoch, handoffFailure))
				recovered = nil
				discovery = &DiscoveryEvidence{PIDs: []int64{}, Error: &message}
			} else if recoveryPending {
				w, evidence, err := d.discover(a.Assignment)
				if err != nil {
					message := cleanupErrorMessage(err)
					evidence.Error = &message
				}
				recovered, discovery = w, &evidence
			}
			if !a.Started {
				if err := update(func(a *allocationState) { a.Assignment.CancelRequested = p.CancelRequested }); err != nil {
					return err
				}
				pending = StatusStarting
			} else if a.Terminal != "" && !a.TerminalAcknowledged {
				pending = a.Terminal
			} else if recoveryPending {
				pending = StatusRecovery
			} else if a.Cleanup != nil && !a.CleanupAcknowledged {
				pending = StatusCleanup
			}
			if err := send(); err != nil {
				return err
			}
		case event := <-events:
			if event.running {
				pending = StatusRunning
			} else if event.terminal != "" {
				if err := update(func(a *allocationState) { a.Terminal = event.terminal; a.TerminalAcknowledged = false }); err != nil {
					return err
				}
				pending = event.terminal
			} else {
				executing = false
				if err := update(func(a *allocationState) { a.Cleanup = event.cleanup; a.CleanupIncarnation = d.incarnation }); err != nil {
					return err
				}
				if d.store.state.Allocation.TerminalAcknowledged {
					pending = StatusCleanup
				}
			}
			if err := send(); err != nil {
				return err
			}
		case <-ticker.C:
			if bound {
				if err := send(); err != nil {
					return err
				}
			}
		}
	}
}
