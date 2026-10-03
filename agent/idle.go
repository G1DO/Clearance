package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Idle quarantine reconciliation observes the agent-managed roots without an
// allocation identity. Allocation execution always lives in an
// `<allocation-uuid>-<runner-epoch>` hierarchy (see Prepare); anything else
// directly under the cgroup root (for example the harness's own `agent`
// membership directory) is not allocation execution and is ignored. The
// workspace root is private to this agent's state directory, so any entry
// there is this runner's leftover.
//
// Every boolean below derives from a fresh scan performed immediately before
// transmission. Liveness is established through cgroup.procs plus /proc (which
// also observes zombies); if /proc is unavailable while member PIDs were
// found, the observation fails closed instead of inventing absence.

func parseIdleCgroupName(name string) (string, bool) {
	if len(name) < 38 || name[36] != '-' {
		return "", false
	}
	id := name[:36]
	if !isValidUUID(id) {
		return "", false
	}
	epoch, err := strconv.ParseInt(name[37:], 10, 64)
	if err != nil || epoch < 0 {
		return "", false
	}
	return strings.ToLower(id), true
}

func idlePIDLive(pid int) (bool, error) {
	_, err := os.Lstat(fmt.Sprintf("/proc/%d", pid))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("inspect idle pid %d: %w", pid, err)
}

// inspectIdle performs one fresh, non-destructive scan of the agent-managed
// roots for idle quarantine reconciliation. It never creates paths, starts
// workloads, or mutates durable state: absence is established by observation,
// and any inspection failure is reported as negative evidence so it can never
// authorize release through a broken handle.
func (d *Daemon) inspectIdle() (ReconcileEvidence, error) {
	evidence := ReconcileEvidence{PIDs: []int64{}}
	fail := func(err error) (ReconcileEvidence, error) {
		message := cleanupErrorMessage(err)
		evidence.Error = &message
		evidence.ExecutionEmpty, evidence.DescendantsReaped, evidence.WorkspaceClean = false, false, false
		return evidence, err
	}
	var ownID string
	if a := d.store.state.Allocation; a != nil && a.Assignment.AllocationID != nil {
		ownID = strings.ToLower(*a.Assignment.AllocationID)
	}
	entries, err := os.ReadDir(d.cfg.CgroupRoot)
	if err != nil {
		return fail(fmt.Errorf("inspect idle cgroup root: %w", err))
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	var observed *string
	seen := make(map[int64]bool)
	probedProc := false
	for _, name := range names {
		id, ok := parseIdleCgroupName(name)
		if !ok {
			continue
		}
		evidence.CgroupPresent = true
		if observed == nil && (ownID == "" || id != ownID) {
			foreign := id
			observed = &foreign
		}
		data, err := os.ReadFile(filepath.Join(d.cfg.CgroupRoot, name, "cgroup.procs"))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fail(fmt.Errorf("inspect idle cgroup processes: %w", err))
		}
		for _, word := range strings.Fields(string(data)) {
			pid, err := strconv.Atoi(word)
			if err != nil || pid <= 0 {
				return fail(errors.New("invalid PID in idle cgroup.procs"))
			}
			if !probedProc {
				if _, err := os.Lstat("/proc"); err != nil {
					return fail(fmt.Errorf("inspect idle process table: %w", err))
				}
				probedProc = true
			}
			live, err := idlePIDLive(pid)
			if err != nil {
				return fail(err)
			}
			if !live || seen[int64(pid)] {
				continue
			}
			if len(evidence.PIDs) == 4096 {
				return fail(errors.New("idle discovery exceeds 4096 PIDs; evidence is incomplete"))
			}
			seen[int64(pid)] = true
			evidence.PIDs = append(evidence.PIDs, int64(pid))
		}
	}
	sort.Slice(evidence.PIDs, func(i, j int) bool { return evidence.PIDs[i] < evidence.PIDs[j] })
	workspace, err := os.ReadDir(d.cfg.WorkspaceRoot)
	if err != nil {
		if !os.IsNotExist(err) {
			return fail(fmt.Errorf("inspect idle workspace root: %w", err))
		}
	} else {
		evidence.WorkspacePresent = len(workspace) > 0
	}
	evidence.ObservedAllocationID = observed
	// Absence established by this fresh scan is the idle-cleanliness evidence:
	// no member processes means no execution and nothing left to reap, and a
	// missing workspace is clean. Any present error above already returned.
	evidence.ExecutionEmpty = len(evidence.PIDs) == 0
	evidence.DescendantsReaped = len(evidence.PIDs) == 0
	evidence.WorkspaceClean = !evidence.WorkspacePresent
	return evidence, nil
}
