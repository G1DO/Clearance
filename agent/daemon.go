package agent

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os/exec"
	"reflect"
	"sync"
	"time"
)

type Config struct {
	ControllerURL, MachineToken, StateDir                        string
	PollTimeout, HeartbeatInterval, RetryInterval, ReportTimeout time.Duration
}

// Daemon runs one allocation at a time. Durable launch intent prevents replay
// across crashes; an uncertain pre-restart workload is only heartbeated, never
// rediscovered or executed again. Cleanup and runner release are not implemented.
type Daemon struct {
	cfg                   Config
	client                *Client
	store                 *stateStore
	incarnation           int64
	mu                    sync.Mutex
	used, running, closed bool
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
	if cfg.StateDir == "" || cfg.PollTimeout < time.Second || cfg.PollTimeout > 30*time.Second || cfg.PollTimeout%time.Second != 0 || cfg.HeartbeatInterval <= 0 || cfg.RetryInterval <= 0 || cfg.ReportTimeout <= 0 || cfg.ReportTimeout > 5*time.Second {
		return nil, errors.New("state directory and positive durations required; poll must be whole seconds in 1..30, report at most 5s")
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
	return &Daemon{cfg: cfg, client: client, store: store, incarnation: store.state.Incarnation}, nil
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

// Run has one poll worker, at most one process waiter, and a single report
// sender (this event loop). Channels retain at most one item, and tickers coalesce.
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
	var completion <-chan error
	var pending ReportStatus
	bound := false // Re-poll first on every boot before any report or execution.

	// Each write copies the allocation so failed persistence cannot mutate memory.
	update := func(change func(*allocationState)) error {
		next := d.store.state
		allocation := *next.Allocation
		change(&allocation)
		next.Allocation = &allocation
		return d.store.save(next)
	}
	finish := func(err error) error {
		pending = StatusSucceeded
		if err != nil {
			pending = StatusFailed
		}
		return update(func(a *allocationState) { a.Terminal = pending; a.TerminalAcknowledged = false })
	}
	send := func() error {
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			status := pending
			if status == "" {
				status = StatusHeartbeat
			}
			if d.store.state.Allocation.Seq == math.MaxInt64 {
				return errors.New("allocation sequence exhausted")
			}
			if err := update(func(a *allocationState) { a.Seq++ }); err != nil {
				return err
			}
			a := d.store.state.Allocation
			report := ReportRequest{AllocationID: *a.Assignment.AllocationID, RunnerEpoch: *a.Assignment.RunnerEpoch,
				AgentIncarnation: d.incarnation, Seq: a.Seq, Status: status, Ts: time.Now().UTC()}
			reportCtx, reportCancel := context.WithTimeout(ctx, d.cfg.ReportTimeout)
			ack, err := d.client.Report(reportCtx, report)
			reportCancel()
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if retryable(err) {
					return nil
				} // Retry the observation with the next seq on a tick.
				return err
			}
			if !ack.Accepted {
				return fmt.Errorf("report rejected: %s", ack.Reason)
			}
			switch status {
			case StatusStarting:
				if err := update(func(a *allocationState) { a.Started = true }); err != nil {
					return err
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				argv := a.Assignment.Argv
				command := exec.CommandContext(ctx, argv[0], argv[1:]...)
				command.WaitDelay = time.Second
				// No output pipes or retained workload output. Direct-process lifetime
				// only: process-tree containment and cleanup are explicitly deferred.
				if err := command.Start(); err != nil {
					if err := finish(err); err != nil {
						return err
					}
					continue
				}
				done := make(chan error, 1)
				completion = done
				workers.Add(1)
				go func() { defer workers.Done(); done <- command.Wait() }()
				pending = StatusRunning
				continue
			case StatusSucceeded, StatusFailed:
				if err := update(func(a *allocationState) { a.TerminalAcknowledged = true }); err != nil {
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
			if !p.Assigned {
				if bound {
					return errors.New("current allocation disappeared; stopping uncertain work")
				}
				continue
			}
			// poll_after_ms is a transport hint, not allocation identity.
			p.PollAfterMs = nil
			previous := d.store.state.Allocation
			if previous != nil && !reflect.DeepEqual(previous.Assignment, p) {
				if *p.RunnerEpoch <= *previous.Assignment.RunnerEpoch || *p.AllocationID == *previous.Assignment.AllocationID || previous.Terminal == "" || completion != nil {
					return errors.New("allocation changed without a completed prior workload and newer epoch")
				}
				bound = false
			}
			if bound {
				continue
			}
			if previous == nil || *previous.Assignment.AllocationID != *p.AllocationID {
				next := d.store.state
				next.Allocation = &allocationState{Assignment: p}
				if err := d.store.save(next); err != nil {
					return err
				}
			}
			bound = true
			a := d.store.state.Allocation
			if !a.Started {
				pending = StatusStarting
			} else if a.Terminal != "" && !a.TerminalAcknowledged {
				pending = a.Terminal
			}
			if err := send(); err != nil {
				return err
			}
		case err := <-completion:
			completion = nil
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := finish(err); err != nil {
				return err
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
