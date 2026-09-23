package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"syscall"
)

const maxStateBytes = 4 << 20

type allocationState struct {
	Assignment           PollResponse
	Seq                  int64
	Started              bool
	Terminal             ReportStatus
	TerminalAcknowledged bool
}

type diskState struct {
	Incarnation int64
	Allocation  *allocationState
}

// stateStore has one event-loop owner. The separate lock file is never replaced:
// locking state.json itself would lose exclusion when its inode is replaced.
type stateStore struct {
	state     diskState
	dir       string
	lock      *os.File
	directory *os.File
}

type stateSnapshot struct {
	Version     *int            `json:"version"`
	Incarnation *int64          `json:"incarnation"`
	Allocation  json.RawMessage `json:"allocation"`
}

type allocationSnapshot struct {
	Assignment           json.RawMessage `json:"assignment"`
	Seq                  *int64          `json:"seq"`
	Started              *bool           `json:"started"`
	Terminal             *ReportStatus   `json:"terminal"`
	TerminalAcknowledged *bool           `json:"terminal_acknowledged"`
}

// openState durably reserves a new incarnation before the caller can send it.
// Losing or corrupting an initialized snapshot fails closed. An interrupted
// first bootstrap also fails closed rather than guessing whether an incarnation
// was previously used; operators must preserve this directory across restarts.
func openState(dir string) (_ *stateStore, err error) {
	if dir == "" {
		return nil, fmt.Errorf("state directory is required")
	}
	if err := createStateDirectory(dir); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	lockPath := filepath.Join(dir, "state.lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	fresh := err == nil
	if errors.Is(err, os.ErrExist) {
		lock, err = os.OpenFile(lockPath, os.O_RDWR, 0600)
	}
	if err != nil {
		return nil, fmt.Errorf("open state lock: %w", err)
	}
	s := &stateStore{dir: dir, lock: lock}
	defer func() {
		if err != nil {
			_ = s.Close()
		}
	}()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("lock state directory: %w", err)
	}
	s.directory, err = os.Open(dir)
	if err != nil {
		return nil, fmt.Errorf("open state directory: %w", err)
	}
	file, err := os.Open(filepath.Join(dir, "state.json"))
	if errors.Is(err, os.ErrNotExist) && fresh {
		s.state = diskState{}
	} else if err != nil {
		return nil, fmt.Errorf("load initialized state: %w", err)
	} else {
		data, readErr := io.ReadAll(io.LimitReader(file, maxStateBytes+1))
		closeErr := file.Close()
		if readErr != nil || closeErr != nil {
			return nil, fmt.Errorf("read state: %w", errors.Join(readErr, closeErr))
		}
		if len(data) > maxStateBytes {
			return nil, fmt.Errorf("state snapshot exceeds %d bytes", maxStateBytes)
		}
		s.state, err = decodeState(data)
		if err != nil {
			return nil, fmt.Errorf("invalid state snapshot: %w", err)
		}
	}
	if s.state.Incarnation == math.MaxInt64 {
		return nil, fmt.Errorf("agent incarnation exhausted")
	}
	next := s.state
	next.Incarnation++
	if err := s.save(next); err != nil {
		return nil, err
	}
	return s, nil
}

// Sync newly created directory entries as well as the snapshot itself. Syncing
// only the leaf directory could lose its parent entry after a power failure.
func createStateDirectory(dir string) error {
	var missing []string
	for path := filepath.Clean(dir); ; path = filepath.Dir(path) {
		info, err := os.Stat(path)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("%s is not a directory", path)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		missing = append(missing, path)
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	for i := len(missing) - 1; i >= 0; i-- {
		parent, err := os.Open(filepath.Dir(missing[i]))
		if err != nil {
			return err
		}
		err = parent.Sync()
		if closeErr := parent.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func decodeSnapshot(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("expected exactly one state JSON object")
	}
	return nil
}

func decodeState(data []byte) (diskState, error) {
	var snapshot stateSnapshot
	if err := decodeSnapshot(data, &snapshot); err != nil {
		return diskState{}, err
	}
	if snapshot.Version == nil || *snapshot.Version != 1 || snapshot.Incarnation == nil || *snapshot.Incarnation < 1 || len(snapshot.Allocation) == 0 {
		return diskState{}, fmt.Errorf("version, positive incarnation, and allocation are required")
	}
	state := diskState{Incarnation: *snapshot.Incarnation}
	if bytes.Equal(bytes.TrimSpace(snapshot.Allocation), []byte("null")) {
		return state, nil
	}
	var allocation allocationSnapshot
	if err := decodeSnapshot(snapshot.Allocation, &allocation); err != nil {
		return diskState{}, err
	}
	if allocation.Seq == nil || *allocation.Seq < 0 || allocation.Started == nil || allocation.Terminal == nil || allocation.TerminalAcknowledged == nil {
		return diskState{}, fmt.Errorf("allocation requires nonnegative seq, started, terminal, and terminal_acknowledged")
	}
	assignment, err := ParsePollResponse(allocation.Assignment)
	if err != nil || !assignment.Assigned {
		return diskState{}, fmt.Errorf("allocation requires a valid assigned poll response: %v", err)
	}
	if terminal := *allocation.Terminal; terminal != "" && terminal != StatusSucceeded && terminal != StatusFailed {
		return diskState{}, fmt.Errorf("invalid terminal status %q", terminal)
	}
	if *allocation.Started && *allocation.Seq == 0 {
		return diskState{}, fmt.Errorf("started allocation requires a reserved report sequence")
	}
	if *allocation.Terminal != "" && !*allocation.Started {
		return diskState{}, fmt.Errorf("terminal allocation requires durable launch intent")
	}
	if *allocation.TerminalAcknowledged && (*allocation.Terminal == "" || *allocation.Seq == 0) {
		return diskState{}, fmt.Errorf("acknowledged terminal requires a terminal report sequence")
	}
	state.Allocation = &allocationState{
		Assignment: assignment, Seq: *allocation.Seq, Started: *allocation.Started,
		Terminal: *allocation.Terminal, TerminalAcknowledged: *allocation.TerminalAcknowledged,
	}
	return state, nil
}

func encodeState(state diskState) ([]byte, error) {
	version := 1
	snapshot := stateSnapshot{Version: &version, Incarnation: &state.Incarnation, Allocation: json.RawMessage("null")}
	if a := state.Allocation; a != nil {
		assignment, err := EncodePollResponse(a.Assignment)
		if err != nil {
			return nil, err
		}
		snapshot.Allocation, err = json.Marshal(allocationSnapshot{
			Assignment: assignment, Seq: &a.Seq, Started: &a.Started,
			Terminal: &a.Terminal, TerminalAcknowledged: &a.TerminalAcknowledged,
		})
		if err != nil {
			return nil, err
		}
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return nil, err
	}
	if len(data) > maxStateBytes {
		return nil, fmt.Errorf("state snapshot exceeds %d bytes", maxStateBytes)
	}
	return data, nil
}

// save replaces the complete bounded snapshot, syncing both the data and the
// rename. A durability error is fatal to the caller: no report may be sent using
// an incarnation or sequence whose reservation has failed.
func (s *stateStore) save(next diskState) error {
	if s.lock == nil || s.directory == nil {
		return fmt.Errorf("state store is closed")
	}
	data, err := encodeState(next)
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	canonical, err := decodeState(data)
	if err != nil {
		return fmt.Errorf("validate state: %w", err)
	}
	// The exclusive lock permits one reusable staging name. Repeated crashes
	// during writes cannot accumulate an unbounded set of temporary snapshots.
	file, err := os.OpenFile(filepath.Join(s.dir, "state.tmp"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("create state snapshot: %w", err)
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("persist state snapshot: %w", err)
	}
	if err := os.Rename(file.Name(), filepath.Join(s.dir, "state.json")); err != nil {
		return fmt.Errorf("replace state snapshot: %w", err)
	}
	if err := s.directory.Sync(); err != nil {
		return fmt.Errorf("sync state directory: %w", err)
	}
	s.state = canonical
	return nil
}

func (s *stateStore) Close() error {
	var err error
	if s.directory != nil {
		err = s.directory.Close()
		s.directory = nil
	}
	if s.lock != nil {
		err = errors.Join(err, s.lock.Close())
		s.lock = nil
	}
	return err
}
