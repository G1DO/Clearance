package agent

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Containment is for trusted workloads. The delegated cgroup and workspace roots
// must be exclusively managed by the agent; workloads must not migrate themselves
// out of their cgroup or change the agent's namespaces or filesystem mounts.
type containmentConfig struct {
	CgroupRoot, WorkspaceRoot, StateDir string
	GracePeriod, KillTimeout            time.Duration
}

type containment struct {
	cfg                           containmentConfig
	cgroupRootInfo, workspaceInfo fs.FileInfo
	membershipRoot                string
}

type containedWorkload struct {
	owner                     *containment
	assignment                PollResponse
	cgroupPath, workspacePath string
	membership                string
	cgroupInfo, workspaceInfo fs.FileInfo
	command                   *exec.Cmd
	done                      chan struct{}
	waitErr                   error
	cleanupOnce               sync.Once
	cleanupEvidence           CleanupEvidence
	cleanupErr                error
	recoveryRecord            *containmentRecord
	adopted                   bool
	discoveryErr              error
	// Fault injection stays private to package tests and cannot be configured by
	// workload input, daemon flags, or environment variables.
	testInspect, testKill, testScrub func() error
}

func newContainment(cfg containmentConfig) (*containment, error) {
	if cfg.CgroupRoot == "" || cfg.WorkspaceRoot == "" || cfg.StateDir == "" || cfg.GracePeriod <= 0 || cfg.KillTimeout <= 0 {
		return nil, errors.New("cgroup, workspace and state roots and positive cleanup durations required")
	}
	var err error
	cfg.CgroupRoot, err = filepath.EvalSymlinks(cfg.CgroupRoot)
	if err != nil {
		return nil, fmt.Errorf("cgroup root: %w", err)
	}
	cfg.CgroupRoot, err = filepath.Abs(cfg.CgroupRoot)
	if err != nil {
		return nil, err
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(cfg.CgroupRoot, &stat); err != nil || stat.Type != 0x63677270 {
		return nil, fmt.Errorf("cgroup root must be a cgroup v2 filesystem: %w", errors.Join(err, errors.New("cgroup v2 required")))
	}
	state, err := filepath.EvalSymlinks(cfg.StateDir)
	if err != nil {
		return nil, fmt.Errorf("state root: %w", err)
	}
	state, err = filepath.Abs(state)
	if err != nil {
		return nil, err
	}
	cfg.StateDir = state
	// Resolve existing parents before creation so an alias cannot place the
	// workspace inside durable state. Recheck the resulting canonical path.
	workspace, err := canonicalCreationPath(cfg.WorkspaceRoot)
	if err != nil {
		return nil, err
	}
	if pathsOverlap(workspace, state) || pathsOverlap(workspace, cfg.CgroupRoot) {
		return nil, errors.New("workspace, cgroup and durable state roots must be separate")
	}
	if err := os.MkdirAll(workspace, 0700); err != nil {
		return nil, fmt.Errorf("workspace root: %w", err)
	}
	cfg.WorkspaceRoot, err = filepath.EvalSymlinks(workspace)
	if err != nil {
		return nil, err
	}
	if cfg.WorkspaceRoot != workspace {
		return nil, errors.New("workspace root changed during preparation")
	}
	cgroupInfo, err := os.Stat(cfg.CgroupRoot)
	if err != nil || !cgroupInfo.IsDir() {
		return nil, errors.New("cgroup root is not a directory")
	}
	workspaceInfo, err := os.Stat(cfg.WorkspaceRoot)
	if err != nil || !workspaceInfo.IsDir() {
		return nil, errors.New("workspace root is not a directory")
	}
	membershipRoot, err := cgroupMembershipPath(cfg.CgroupRoot)
	if err != nil {
		return nil, err
	}
	// All allocation orphans become this process's children. Reaping below is
	// per PID after checking cgroup ownership, never a process-wide wait(-1).
	if _, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, 36 /* PR_SET_CHILD_SUBREAPER */, 1, 0, 0, 0, 0); errno != 0 {
		return nil, fmt.Errorf("enable child subreaper: %w", errno)
	}
	return &containment{cfg: cfg, cgroupRootInfo: cgroupInfo, workspaceInfo: workspaceInfo, membershipRoot: membershipRoot}, nil
}

func canonicalCreationPath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err == nil {
		return resolved, nil
	}
	if !os.IsNotExist(err) {
		return "", err
	}
	parent, err := canonicalCreationPath(filepath.Dir(absolute))
	if err != nil {
		return "", err
	}
	return filepath.Join(parent, filepath.Base(absolute)), nil
}

func withinPath(path, root string) bool {
	return path == root || (root == string(os.PathSeparator) && filepath.IsAbs(path)) || strings.HasPrefix(path, root+string(os.PathSeparator))
}

func pathsOverlap(a, b string) bool { return withinPath(a, b) || withinPath(b, a) }

func sameDirectory(path string, expected fs.FileInfo) error {
	actual, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if expected == nil || !actual.IsDir() || !os.SameFile(expected, actual) {
		return fmt.Errorf("directory identity changed: %s", path)
	}
	return nil
}

// Prepare returns a handle even after partial preparation so the caller can
// attest cleanup of a failed launch. Existing allocation directories are never
// adopted, erased, or used to start a second workload.
func (c *containment) Prepare(assignment PollResponse) (*containedWorkload, error) {
	if assignment.AllocationID == nil || !isValidUUID(*assignment.AllocationID) || assignment.RunnerEpoch == nil || *assignment.RunnerEpoch <= 0 || len(assignment.Argv) == 0 {
		return nil, errors.New("valid allocation identity and argv required for containment")
	}
	name := strings.ToLower(*assignment.AllocationID) + "-" + strconv.FormatInt(*assignment.RunnerEpoch, 10)
	w := &containedWorkload{owner: c, assignment: assignment,
		cgroupPath: filepath.Join(c.cfg.CgroupRoot, name), workspacePath: filepath.Join(c.cfg.WorkspaceRoot, name),
		membership: filepath.Join(c.membershipRoot, name)}
	if err := sameDirectory(c.cfg.CgroupRoot, c.cgroupRootInfo); err != nil {
		return w, err
	}
	if err := sameDirectory(c.cfg.WorkspaceRoot, c.workspaceInfo); err != nil {
		return w, err
	}
	if err := os.Mkdir(w.cgroupPath, 0700); err != nil {
		return w, fmt.Errorf("create allocation cgroup: %w", err)
	}
	var err error
	w.cgroupInfo, err = os.Lstat(w.cgroupPath)
	if err != nil {
		return w, err
	}
	// Verify the mandatory atomic kill interface before any executable starts.
	kill, err := os.OpenFile(filepath.Join(w.cgroupPath, "cgroup.kill"), os.O_WRONLY, 0)
	if err != nil {
		return w, fmt.Errorf("allocation cgroup.kill unavailable: %w", err)
	}
	if err := kill.Close(); err != nil {
		return w, err
	}
	if err := os.Mkdir(w.workspacePath, 0700); err != nil {
		return w, fmt.Errorf("create allocation workspace: %w", err)
	}
	w.workspaceInfo, err = os.Lstat(w.workspacePath)
	if err != nil {
		return w, err
	}
	if err := syncDirectory(c.cfg.WorkspaceRoot); err != nil {
		return w, fmt.Errorf("persist allocation workspace: %w", err)
	}
	return w, w.recordPreparation()
}

func (w *containedWorkload) Start() error {
	if w.adopted || w.recoveryRecord == nil {
		return errors.New("only a durably prepared new allocation may start")
	}
	if w.command != nil || w.done != nil {
		return errors.New("allocation command may only be started once")
	}
	if err := sameDirectory(w.cgroupPath, w.cgroupInfo); err != nil {
		return fmt.Errorf("allocation containment unavailable: %w", err)
	}
	if err := sameDirectory(w.workspacePath, w.workspaceInfo); err != nil {
		return fmt.Errorf("allocation workspace unavailable: %w", err)
	}
	fd, err := os.Open(w.cgroupPath)
	if err != nil {
		return err
	}
	defer fd.Close()
	argv := w.assignment.Argv
	w.command = exec.Command(argv[0], argv[1:]...)
	w.command.Dir = w.workspacePath
	w.command.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: int(fd.Fd())}
	// CLONE_INTO_CGROUP establishes membership before the first instruction.
	// There is deliberately no fallback to start-then-migrate or process groups.
	if err := w.command.Start(); err != nil {
		return fmt.Errorf("start command inside allocation cgroup: %w", err)
	}
	w.done = make(chan struct{})
	go func() {
		w.waitErr = w.command.Wait()
		close(w.done)
	}()
	return nil
}

func (w *containedWorkload) Wait() error {
	if w.done == nil {
		return errors.New("allocation command did not start")
	}
	<-w.done
	return w.waitErr
}

func (w *containedWorkload) Cleanup() (CleanupEvidence, error) {
	w.cleanupOnce.Do(func() { w.cleanupEvidence, w.cleanupErr = w.cleanup() })
	return w.cleanupEvidence, w.cleanupErr
}

func (w *containedWorkload) cleanup() (CleanupEvidence, error) {
	var proof CleanupEvidence
	if w.adopted && w.discoveryErr == nil {
		// Resolution may arrive after discovery. In particular, missing child
		// paths only establish absence while their verified roots still match.
		w.discoveryErr = errors.Join(
			sameDirectory(w.owner.cfg.CgroupRoot, w.owner.cgroupRootInfo),
			sameDirectory(w.owner.cfg.WorkspaceRoot, w.owner.workspaceInfo),
		)
	}
	if w.discoveryErr != nil {
		message := cleanupErrorMessage(w.discoveryErr)
		proof.Error = &message
		return proof, w.discoveryErr
	}
	var failures []error
	record := func(err error) {
		if err != nil {
			failures = append(failures, err)
		}
	}
	if w.cgroupInfo == nil {
		// Nothing launched, but an unexpected existing directory must not be
		// mistaken for a clean allocation or removed on its owner's behalf.
		if _, err := os.Lstat(w.cgroupPath); os.IsNotExist(err) {
			proof.ExecutionEmpty, proof.DescendantsReaped = true, true
			if w.adopted {
				remaining, inspectErr := w.allocationPIDs()
				record(inspectErr)
				proof.ExecutionEmpty = inspectErr == nil && len(remaining) == 0
				proof.DescendantsReaped = proof.ExecutionEmpty
			}
		} else {
			record(errors.New("cannot verify an allocation cgroup that was not created by this launch"))
		}
	} else {
		record(w.terminate())
		empty, err := w.executionEmpty()
		record(err)
		proof.ExecutionEmpty = empty && err == nil
		if proof.ExecutionEmpty {
			record(w.reap(time.Now().Add(w.owner.cfg.KillTimeout)))
			remaining, err := w.allocationPIDs()
			record(err)
			proof.DescendantsReaped = err == nil && len(remaining) == 0 && w.directWaitFinished()
			if !proof.DescendantsReaped {
				record(errors.New("allocation descendants remain unreaped"))
			}
		}
		if proof.ExecutionEmpty && proof.DescendantsReaped {
			if err := w.authorizeRemoval(); err != nil {
				record(err)
				message := cleanupErrorMessage(errors.Join(failures...))
				proof.Error = &message
				return proof, errors.Join(failures...)
			}
			record(w.removeCgroup())
		}
	}
	if proof.ExecutionEmpty && proof.DescendantsReaped {
		if w.workspaceInfo == nil {
			if _, err := os.Lstat(w.workspacePath); os.IsNotExist(err) {
				proof.WorkspaceClean = true
			} else {
				record(errors.New("cannot scrub a workspace that was not created by this launch"))
			}
		} else {
			err := w.scrubWorkspace()
			record(err)
			proof.WorkspaceClean = err == nil
		}
	}
	err := errors.Join(failures...)
	if !proof.ExecutionEmpty || !proof.DescendantsReaped || !proof.WorkspaceClean {
		err = errors.Join(err, errors.New("allocation cleanup could not establish all required evidence"))
	}
	if err != nil {
		message := cleanupErrorMessage(err)
		proof.Error = &message
	}
	return proof, err
}

func (w *containedWorkload) terminate() error {
	if err := sameDirectory(w.owner.cfg.CgroupRoot, w.owner.cgroupRootInfo); err != nil {
		return err
	}
	if err := sameDirectory(w.cgroupPath, w.cgroupInfo); err != nil {
		return err
	}
	pids, err := w.livePIDs()
	if err != nil {
		return err
	}
	var failures []error
	for _, pid := range pids {
		// FindProcess holds a pidfd on supported Go/Linux kernels. Checking
		// membership after opening it avoids signaling a recycled unrelated PID.
		process, err := os.FindProcess(pid)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		owned, inspectErr := w.ownsPID(pid)
		if inspectErr != nil {
			failures = append(failures, inspectErr)
		}
		if owned {
			if err := process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
				failures = append(failures, fmt.Errorf("graceful termination of pid %d: %w", pid, err))
			}
		}
		_ = process.Release()
	}
	deadline := time.Now().Add(w.owner.cfg.GracePeriod)
	for time.Now().Before(deadline) {
		empty, err := w.executionEmpty()
		if err != nil {
			return errors.Join(append(failures, err)...)
		}
		if empty {
			return errors.Join(failures...)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if w.testKill != nil {
		if err := w.testKill(); err != nil {
			return errors.Join(append(failures, err)...)
		}
	}
	if err := os.WriteFile(filepath.Join(w.cgroupPath, "cgroup.kill"), []byte("1"), 0600); err != nil {
		return errors.Join(append(failures, fmt.Errorf("forced allocation termination: %w", err))...)
	}
	deadline = time.Now().Add(w.owner.cfg.KillTimeout)
	for {
		empty, err := w.executionEmpty()
		if err != nil {
			return errors.Join(append(failures, err)...)
		}
		if empty {
			return errors.Join(failures...)
		}
		if !time.Now().Before(deadline) {
			return errors.Join(append(failures, errors.New("allocation remained populated after SIGKILL deadline"))...)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (w *containedWorkload) cgroupDirectories() ([]string, error) {
	if err := sameDirectory(w.cgroupPath, w.cgroupInfo); err != nil {
		return nil, err
	}
	var directories []string
	err := filepath.WalkDir(w.cgroupPath, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("unexpected symlink in cgroup hierarchy: %s", path)
		}
		if entry.IsDir() {
			directories = append(directories, path)
		}
		return nil
	})
	return directories, err
}

func (w *containedWorkload) livePIDs() ([]int, error) {
	directories, err := w.cgroupDirectories()
	if err != nil {
		return nil, err
	}
	seen := make(map[int]bool)
	var pids []int
	for _, directory := range directories {
		data, err := os.ReadFile(filepath.Join(directory, "cgroup.procs"))
		if err != nil {
			return nil, fmt.Errorf("inspect allocation processes: %w", err)
		}
		for _, word := range strings.Fields(string(data)) {
			pid, err := strconv.Atoi(word)
			if err != nil || pid <= 0 {
				return nil, errors.New("invalid PID in cgroup.procs")
			}
			if !seen[pid] {
				seen[pid] = true
				pids = append(pids, pid)
			}
		}
	}
	return pids, nil
}

func (w *containedWorkload) executionEmpty() (bool, error) {
	if w.testInspect != nil {
		if err := w.testInspect(); err != nil {
			return false, err
		}
	}
	pids, err := w.livePIDs()
	if err != nil {
		return false, err
	}
	data, err := os.ReadFile(filepath.Join(w.cgroupPath, "cgroup.events"))
	if err != nil {
		return false, fmt.Errorf("inspect cgroup population: %w", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if line == "populated 1" {
			return false, nil
		}
		if line == "populated 0" {
			return len(pids) == 0, nil
		}
	}
	return false, errors.New("cgroup.events has no valid populated field")
}

func (w *containedWorkload) ownsPID(pid int) (bool, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if os.IsNotExist(err) || errors.Is(err, syscall.ESRCH) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect cgroup of pid %d: %w", pid, err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "0::") {
			return withinPath(strings.TrimPrefix(line, "0::"), w.membership), nil
		}
	}
	return false, errors.New("process has no cgroup v2 membership")
}

// Zombies retain their cgroup membership until reaped, even though cgroup.procs
// omits them. Inspect only our direct/adopted children, including every Go thread;
// never consume another allocation's child or exec.Cmd's direct child status.
func (w *containedWorkload) ownedChildren() ([]int, error) {
	tasks, err := os.ReadDir("/proc/self/task")
	if err != nil {
		return nil, err
	}
	seen := make(map[int]bool)
	var owned []int
	for _, task := range tasks {
		data, err := os.ReadFile(filepath.Join("/proc/self/task", task.Name(), "children"))
		if os.IsNotExist(err) || errors.Is(err, syscall.ESRCH) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("inspect adopted descendants: %w", err)
		}
		for _, word := range strings.Fields(string(data)) {
			pid, err := strconv.Atoi(word)
			if err != nil || pid <= 0 {
				return nil, errors.New("invalid PID in /proc task children")
			}
			if seen[pid] {
				continue
			}
			seen[pid] = true
			if w.command != nil && w.command.Process != nil && pid == w.command.Process.Pid {
				continue
			}
			matches, err := w.ownsPID(pid)
			if err != nil {
				return nil, err
			}
			if matches {
				owned = append(owned, pid)
			}
		}
	}
	return owned, nil
}

func (w *containedWorkload) directWaitFinished() bool {
	if w.done == nil {
		return true
	}
	select {
	case <-w.done:
		return true
	default:
		return false
	}
}

// allocationPIDs includes zombies and descendants still being reparented. An
// empty cgroup alone is insufficient because cgroup.procs excludes zombies.
func (w *containedWorkload) allocationPIDs() ([]int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	var found []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		owned, err := w.ownsPID(pid)
		if err != nil {
			return nil, err
		}
		if owned {
			found = append(found, pid)
		}
	}
	return found, nil
}

func (w *containedWorkload) reap(deadline time.Time) error {
	for {
		remaining, err := w.allocationPIDs()
		if err != nil {
			return err
		}
		if len(remaining) == 0 && w.directWaitFinished() {
			return nil
		}
		children, err := w.ownedChildren()
		if err != nil {
			return err
		}
		for _, pid := range children {
			var status syscall.WaitStatus
			_, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
			if err != nil && !errors.Is(err, syscall.ECHILD) && !errors.Is(err, syscall.EINTR) {
				return fmt.Errorf("reap allocation pid %d: %w", pid, err)
			}
		}
		if !time.Now().Before(deadline) {
			return errors.New("allocation descendant reaping deadline exceeded")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (w *containedWorkload) removeCgroup() error {
	directories, err := w.cgroupDirectories()
	if err != nil {
		return err
	}
	for i := len(directories) - 1; i >= 0; i-- {
		if err := os.Remove(directories[i]); err != nil {
			return fmt.Errorf("remove empty allocation cgroup: %w", err)
		}
	}
	return nil
}

func (w *containedWorkload) scrubWorkspace() error {
	if w.testScrub != nil {
		if err := w.testScrub(); err != nil {
			return err
		}
	}
	if err := sameDirectory(w.owner.cfg.WorkspaceRoot, w.owner.workspaceInfo); err != nil {
		return err
	}
	if err := sameDirectory(w.workspacePath, w.workspaceInfo); err != nil {
		return err
	}
	if err := w.inspectWorkspaceMounts(); err != nil {
		return err
	}
	// RemoveAll unlinks symlinks; it does not follow workspace links into another
	// allocation or durable state. No allocation processes remain at this point.
	if err := os.RemoveAll(w.workspacePath); err != nil {
		return fmt.Errorf("scrub allocation workspace: %w", err)
	}
	if _, err := os.Lstat(w.workspacePath); !os.IsNotExist(err) {
		return errors.New("workspace removal could not be verified")
	}
	return syncDirectory(w.owner.cfg.WorkspaceRoot)
}

func (w *containedWorkload) inspectWorkspaceMounts() error {
	mounts, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return fmt.Errorf("inspect workspace mounts: %w", err)
	}
	for _, line := range strings.Split(string(mounts), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 5 && withinPath(unescapeMount(fields[4]), w.workspacePath) {
			return errors.New("refusing to scrub a mounted allocation workspace subtree")
		}
	}
	return nil
}

func unescapeMount(value string) string {
	return strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`).Replace(value)
}

func cgroupMembershipPath(path string) (string, error) {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return "", err
	}
	longest, membership := "", ""
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.SplitN(line, " - ", 2)
		if len(parts) != 2 || !strings.HasPrefix(parts[1], "cgroup2 ") {
			continue
		}
		fields := strings.Fields(parts[0])
		if len(fields) < 5 {
			return "", errors.New("malformed cgroup mount information")
		}
		mount := unescapeMount(fields[4])
		if (mount == "/" || withinPath(path, mount)) && len(mount) > len(longest) {
			relative, err := filepath.Rel(mount, path)
			if err != nil {
				return "", err
			}
			longest, membership = mount, filepath.Join(unescapeMount(fields[3]), relative)
		}
	}
	if longest == "" {
		return "", errors.New("cannot determine cgroup v2 membership path")
	}
	return membership, nil
}
