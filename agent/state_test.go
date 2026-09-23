package agent

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func stateAssignment(t *testing.T) PollResponse {
	t.Helper()
	assignment, err := ParsePollResponse([]byte(`{"assigned":true,"allocation_id":"AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA","job_id":"22222222-2222-2222-2222-222222222222","runner_epoch":3,"argv":["echo","hello"],"runner_class":"default"}`))
	if err != nil {
		t.Fatal(err)
	}
	return assignment
}

func TestStateRestartReservesIncarnationAndPreservesAllocation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "state")
	s, err := openState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if s.state.Incarnation != 1 || s.state.Allocation != nil {
		t.Fatalf("unexpected initial state: %+v", s.state)
	}
	allocation := &allocationState{
		Assignment: stateAssignment(t), Seq: 37, Started: true,
		Terminal: StatusSucceeded, TerminalAcknowledged: true,
	}
	if err := s.save(diskState{Incarnation: 1, Allocation: allocation}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for wantIncarnation := int64(2); wantIncarnation <= 3; wantIncarnation++ {
		s, err = openState(dir)
		if err != nil {
			t.Fatal(err)
		}
		if s.state.Incarnation != wantIncarnation || !reflect.DeepEqual(s.state.Allocation, allocation) {
			t.Fatalf("restart lost durable state: %+v allocation=%+v", s.state, s.state.Allocation)
		}
		data, err := os.ReadFile(filepath.Join(dir, "state.json"))
		if err != nil {
			t.Fatal(err)
		}
		persisted, err := decodeState(data)
		if err != nil || persisted.Incarnation != wantIncarnation {
			t.Fatalf("incarnation was not persisted before open returned: %+v, %v", persisted, err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStateExclusiveLockSurvivesSnapshotReplacement(t *testing.T) {
	dir := t.TempDir()
	s, err := openState(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 3; i++ {
		if err := s.save(s.state); err != nil {
			t.Fatal(err)
		}
		other, err := openState(dir)
		if err == nil {
			other.Close()
			t.Fatal("simultaneous agent acquired the state lock")
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	other, err := openState(dir)
	if err != nil {
		t.Fatalf("lock did not release: %v", err)
	}
	defer other.Close()
	if other.state.Incarnation != 2 {
		t.Fatalf("failed opens changed incarnation: %d", other.state.Incarnation)
	}
}

func TestStateMissingSnapshotFailsClosed(t *testing.T) {
	dir := t.TempDir()
	s, err := openState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "state.json")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if s, err := openState(dir); err == nil {
			s.Close()
			t.Fatal("missing initialized snapshot silently reset incarnation")
		}
	}
}

func TestStateInterruptedBootstrapFailsClosed(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "state.lock"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if s, err := openState(dir); err == nil {
		s.Close()
		t.Fatal("interrupted bootstrap silently reused an incarnation")
	}
}

func TestStateCorruptSnapshotsFailClosed(t *testing.T) {
	valid, err := encodeState(diskState{Incarnation: 9, Allocation: &allocationState{Assignment: stateAssignment(t)}})
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"malformed":               "{",
		"null":                    "null",
		"missing incarnation":     `{"version":1,"allocation":null}`,
		"null incarnation":        `{"version":1,"incarnation":null,"allocation":null}`,
		"zero incarnation":        `{"version":1,"incarnation":0,"allocation":null}`,
		"overflow incarnation":    `{"version":1,"incarnation":9223372036854775808,"allocation":null}`,
		"fractional incarnation":  `{"version":1,"incarnation":1.0,"allocation":null}`,
		"unsupported version":     `{"version":2,"incarnation":9,"allocation":null}`,
		"missing allocation":      `{"version":1,"incarnation":9}`,
		"trailing object":         `{"version":1,"incarnation":9,"allocation":null}{}`,
		"negative seq":            strings.Replace(string(valid), `"seq":0`, `"seq":-1`, 1),
		"missing seq":             strings.Replace(string(valid), `"seq":0,`, "", 1),
		"null started":            strings.Replace(string(valid), `"started":false`, `"started":null`, 1),
		"invalid terminal":        strings.Replace(string(valid), `"terminal":""`, `"terminal":"RUNNING"`, 1),
		"terminal without launch": strings.Replace(string(valid), `"terminal":""`, `"terminal":"FAILED"`, 1),
		"started without seq":     strings.Replace(string(valid), `"started":false`, `"started":true`, 1),
		"false terminal ack":      strings.Replace(string(valid), `"terminal_acknowledged":false`, `"terminal_acknowledged":true`, 1),
		"invalid assignment":      strings.Replace(string(valid), `"runner_epoch":3`, `"runner_epoch":0`, 1),
		"idle assignment":         strings.Replace(string(valid), `"assigned":true`, `"assigned":false`, 1),
		"oversize":                strings.Repeat(" ", maxStateBytes+1),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "state.json")
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if s, err := openState(dir); err == nil {
					s.Close()
					t.Fatal("corrupt snapshot accepted")
				}
			}
			actual, err := os.ReadFile(path)
			if err != nil || string(actual) != data {
				t.Fatalf("corrupt snapshot modified: %v", err)
			}
		})
	}
}

func TestStateIncarnationExhaustionDoesNotWrap(t *testing.T) {
	dir := t.TempDir()
	s, err := openState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.save(diskState{Incarnation: math.MaxInt64}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err := openState(dir); err == nil {
		s.Close()
		t.Fatal("exhausted incarnation accepted")
	}
	data, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := decodeState(data)
	if err != nil || persisted.Incarnation != math.MaxInt64 {
		t.Fatalf("exhausted incarnation changed: %+v, %v", persisted, err)
	}
}

func TestStatePersistenceFailureDoesNotAdvanceMemory(t *testing.T) {
	for _, failure := range []string{"create", "rename", "directory sync"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			s, err := openState(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			switch failure {
			case "create":
				s.dir = filepath.Join(dir, "missing")
			case "rename":
				if err := os.Remove(filepath.Join(dir, "state.json")); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(filepath.Join(dir, "state.json"), 0700); err != nil {
					t.Fatal(err)
				}
			case "directory sync":
				if err := s.directory.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.save(diskState{Incarnation: 2}); err == nil {
				t.Fatal("persistence failure ignored")
			}
			if s.state.Incarnation != 1 {
				t.Fatal("failed persistence advanced memory")
			}
			if _, err := os.Stat(filepath.Join(dir, "state.tmp")); !os.IsNotExist(err) {
				t.Fatalf("temporary snapshot leaked: %v", err)
			}
		})
	}
}

func TestStateInvalidSavePreservesSnapshot(t *testing.T) {
	dir := t.TempDir()
	s, err := openState(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	before, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []diskState{
		{Incarnation: 0},
		{Incarnation: 1, Allocation: &allocationState{Assignment: stateAssignment(t), Seq: -1}},
		{Incarnation: 1, Allocation: &allocationState{Assignment: stateAssignment(t), Terminal: StatusRunning}},
		{Incarnation: 1, Allocation: &allocationState{Assignment: stateAssignment(t), Terminal: StatusFailed}},
		{Incarnation: 1, Allocation: &allocationState{Assignment: stateAssignment(t), Started: true}},
		{Incarnation: 1, Allocation: &allocationState{Assignment: PollResponse{Assigned: true}}},
	} {
		if err := s.save(invalid); err == nil {
			t.Fatal("invalid state persisted")
		}
	}
	after, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil || string(before) != string(after) || s.state.Incarnation != 1 {
		t.Fatalf("invalid save changed state: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.save(diskState{Incarnation: 2}); err == nil {
		t.Fatal("closed state store accepted a write")
	}
}

func TestStateRestartReusesInterruptedStagingFile(t *testing.T) {
	dir := t.TempDir()
	for incarnation := int64(1); incarnation <= 3; incarnation++ {
		s, err := openState(dir)
		if err != nil {
			t.Fatal(err)
		}
		if s.state.Incarnation != incarnation {
			t.Fatalf("staged data affected incarnation: got %d, want %d", s.state.Incarnation, incarnation)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 2 || entries[0].Name() != "state.json" || entries[1].Name() != "state.lock" {
			t.Fatalf("unexpected retained state files: %v", entries)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		// Model a process killed midway through writing a replacement. The next
		// boot must trust state.json and truncate the incomplete staging file.
		if err := os.WriteFile(filepath.Join(dir, "state.tmp"), []byte(strings.Repeat("partial", 4096)), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStateAcceptsLargestEscapedAssignment(t *testing.T) {
	dir := t.TempDir()
	s, err := openState(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	assignment := stateAssignment(t)
	assignment.Argv = make([]string, 128)
	for i := range assignment.Argv {
		assignment.Argv[i] = strings.Repeat("\x01", 4096)
	}
	want := diskState{Incarnation: 1, Allocation: &allocationState{Assignment: assignment, Seq: math.MaxInt64}}
	if err := s.save(want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(s.state, want) {
		t.Fatal("maximum-size legal assignment did not round trip")
	}
	data, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil || len(data) > maxStateBytes || !json.Valid(data) {
		t.Fatalf("invalid maximum-size snapshot: bytes=%d, %v", len(data), err)
	}
}

func TestStateRejectsIncompleteOrInconsistentCleanupEvidence(t *testing.T) {
	proof := &CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true}
	state := diskState{Incarnation: 2, Allocation: &allocationState{Assignment: stateAssignment(t), Seq: 9, Started: true,
		Terminal: StatusSucceeded, TerminalAcknowledged: true, Cleanup: proof, CleanupIncarnation: 2, CleanupAcknowledged: true}}
	data, err := encodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeState(data)
	if err != nil || !reflect.DeepEqual(decoded, state) {
		t.Fatalf("cleanup state did not round trip: %+v %v", decoded, err)
	}
	for name, corrupt := range map[string]string{
		"missing_boolean":        strings.Replace(string(data), `"workspace_clean":true`, `"future_field":true`, 1),
		"null_boolean":           strings.Replace(string(data), `"execution_empty":true`, `"execution_empty":null`, 1),
		"unreserved_incarnation": strings.Replace(string(data), `"cleanup_incarnation":2`, `"cleanup_incarnation":3`, 1),
		"missing_incarnation":    strings.Replace(string(data), `"cleanup_incarnation":2`, `"cleanup_incarnation":0`, 1),
		"no_terminal_ack":        strings.Replace(string(data), `"terminal_acknowledged":true`, `"terminal_acknowledged":false`, 1),
		"no_terminal_result":     strings.Replace(string(data), `"terminal":"SUCCEEDED"`, `"terminal":""`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeState([]byte(corrupt)); err == nil {
				t.Fatal("unsafe persisted evidence accepted")
			}
		})
	}
}
