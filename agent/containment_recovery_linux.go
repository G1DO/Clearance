package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

// This bounded journal is protected by the daemon's existing state-directory
// lock. Its physical identities must be committed before the first instruction
// runs. Launch intent by itself never authorizes adoption or deletion.
type containmentRecord struct {
	Version           int               `json:"version"`
	AllocationID      string            `json:"allocation_id"`
	RunnerEpoch       int64             `json:"runner_epoch"`
	CgroupRoot        directoryIdentity `json:"cgroup_root"`
	WorkspaceRoot     directoryIdentity `json:"workspace_root"`
	Cgroup            directoryIdentity `json:"cgroup"`
	Workspace         directoryIdentity `json:"workspace"`
	RemovalAuthorized bool              `json:"removal_authorized"`
}

type directoryIdentity struct {
	Path   string `json:"path"`
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
}

func identifyDirectory(path string, info fs.FileInfo) (directoryIdentity, error) {
	if info == nil || !info.IsDir() {
		return directoryIdentity{}, fmt.Errorf("directory identity unavailable: %s", path)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return directoryIdentity{}, fmt.Errorf("Linux directory identity unavailable: %s", path)
	}
	return directoryIdentity{Path: path, Device: uint64(stat.Dev), Inode: stat.Ino}, nil
}

func (d directoryIdentity) inspect(path string, allowAbsent bool) (fs.FileInfo, error) {
	if d.Path != path || d.Inode == 0 {
		return nil, fmt.Errorf("durable directory identity contradicts configured path: %s", path)
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) && allowAbsent {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	actual, err := identifyDirectory(path, info)
	if err != nil || actual != d {
		return nil, fmt.Errorf("durable directory identity changed: %s", path)
	}
	return info, nil
}

func (w *containedWorkload) recordPreparation() error {
	record := containmentRecord{Version: 1, AllocationID: strings.ToLower(*w.assignment.AllocationID), RunnerEpoch: *w.assignment.RunnerEpoch}
	for _, item := range []struct {
		path string
		info fs.FileInfo
		dst  *directoryIdentity
	}{
		{w.owner.cfg.CgroupRoot, w.owner.cgroupRootInfo, &record.CgroupRoot},
		{w.owner.cfg.WorkspaceRoot, w.owner.workspaceInfo, &record.WorkspaceRoot},
		{w.cgroupPath, w.cgroupInfo, &record.Cgroup},
		{w.workspacePath, w.workspaceInfo, &record.Workspace},
	} {
		identity, err := identifyDirectory(item.path, item.info)
		if err != nil {
			return err
		}
		*item.dst = identity
	}
	if err := w.owner.saveContainmentRecord(record); err != nil {
		return err
	}
	w.recoveryRecord = &record
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func (c *containment) saveContainmentRecord(record containmentRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(c.cfg.StateDir, "containment.tmp"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("create containment identity: %w", err)
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("persist containment identity: %w", err)
	}
	if err := os.Rename(file.Name(), filepath.Join(c.cfg.StateDir, "containment.json")); err != nil {
		return fmt.Errorf("replace containment identity: %w", err)
	}
	if err := syncDirectory(c.cfg.StateDir); err != nil {
		return fmt.Errorf("sync containment identity: %w", err)
	}
	return nil
}

func (c *containment) loadContainmentRecord() (containmentRecord, error) {
	var record containmentRecord
	path := filepath.Join(c.cfg.StateDir, "containment.json")
	info, err := os.Lstat(path)
	if err != nil {
		return record, fmt.Errorf("load durable containment identity: %w", err)
	}
	if !info.Mode().IsRegular() {
		return record, errors.New("durable containment identity is not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return record, err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxStateBytes+1))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return record, err
	}
	if len(data) > maxStateBytes {
		return record, errors.New("durable containment identity exceeds size limit")
	}
	if err := decodeSnapshot(data, &record); err != nil {
		return record, fmt.Errorf("invalid durable containment identity: %w", err)
	}
	return record, nil
}

// authorizeRemoval runs only after cgroup and /proc verification established
// that execution and zombies are gone. A crash between directory removals can
// then be recovered without mistaking unexplained absence for cleanup proof.
func (w *containedWorkload) authorizeRemoval() error {
	if w.recoveryRecord == nil || w.recoveryRecord.RemovalAuthorized {
		return nil
	}
	record := *w.recoveryRecord
	record.RemovalAuthorized = true
	if err := w.owner.saveContainmentRecord(record); err != nil {
		return err
	}
	w.recoveryRecord = &record
	return nil
}

// Discover never creates allocation paths or starts a command. It correlates
// the durable allocation identity with fresh Linux directory and /proc evidence.
// On contradictory evidence the returned handle is unusable for cleanup too.
func (c *containment) Discover(assignment PollResponse) (*containedWorkload, DiscoveryEvidence, error) {
	proof := DiscoveryEvidence{PIDs: []int64{}}
	w := &containedWorkload{owner: c, assignment: assignment, adopted: true}
	fail := func(err error) (*containedWorkload, DiscoveryEvidence, error) {
		w.discoveryErr = err
		message := cleanupErrorMessage(err)
		proof.Error = &message
		return w, proof, err
	}
	if assignment.AllocationID == nil || !isValidUUID(*assignment.AllocationID) || assignment.RunnerEpoch == nil || *assignment.RunnerEpoch <= 0 {
		return fail(errors.New("valid allocation identity required for discovery"))
	}
	name := strings.ToLower(*assignment.AllocationID) + "-" + strconv.FormatInt(*assignment.RunnerEpoch, 10)
	w.cgroupPath, w.workspacePath = filepath.Join(c.cfg.CgroupRoot, name), filepath.Join(c.cfg.WorkspaceRoot, name)
	w.membership = filepath.Join(c.membershipRoot, name)
	// Presence is an observation, not an ownership assertion. Symlinks and
	// replaced directories are reported as present but fail identity validation.
	if _, err := os.Lstat(w.cgroupPath); err == nil {
		proof.CgroupPresent = true
	} else if !os.IsNotExist(err) {
		return fail(err)
	}
	if _, err := os.Lstat(w.workspacePath); err == nil {
		proof.WorkspacePresent = true
	} else if !os.IsNotExist(err) {
		return fail(err)
	}
	record, err := c.loadContainmentRecord()
	if err != nil {
		return fail(err)
	}
	if record.Version != 1 || record.AllocationID != strings.ToLower(*assignment.AllocationID) || record.RunnerEpoch != *assignment.RunnerEpoch {
		return fail(errors.New("durable containment allocation identity contradicts assignment"))
	}
	for _, item := range []struct {
		path     string
		identity directoryIdentity
	}{
		{c.cfg.CgroupRoot, record.CgroupRoot},
		{c.cfg.WorkspaceRoot, record.WorkspaceRoot},
	} {
		if _, err := item.identity.inspect(item.path, false); err != nil {
			return fail(err)
		}
	}
	w.cgroupInfo, err = record.Cgroup.inspect(w.cgroupPath, record.RemovalAuthorized)
	if err != nil {
		return fail(err)
	}
	w.workspaceInfo, err = record.Workspace.inspect(w.workspacePath, record.RemovalAuthorized)
	if err != nil {
		return fail(err)
	}
	if w.workspaceInfo != nil {
		if err := w.inspectWorkspaceMounts(); err != nil {
			return fail(err)
		}
	}
	w.recoveryRecord = &record
	pids, err := w.allocationPIDs()
	if err != nil {
		return fail(err)
	}
	sort.Ints(pids)
	for _, pid := range pids {
		if len(proof.PIDs) == 4096 {
			return fail(errors.New("allocation discovery exceeds 4096 PIDs; evidence is incomplete"))
		}
		proof.PIDs = append(proof.PIDs, int64(pid))
	}
	if record.RemovalAuthorized && len(pids) != 0 {
		return fail(errors.New("allocation processes contradict verified removal authorization"))
	}
	if w.cgroupInfo != nil {
		empty, err := w.executionEmpty()
		if err != nil {
			return fail(err)
		}
		if record.RemovalAuthorized && !empty {
			return fail(errors.New("populated allocation cgroup contradicts verified removal authorization"))
		}
	}
	proof.CleanupVerified = record.RemovalAuthorized
	return w, proof, nil
}
