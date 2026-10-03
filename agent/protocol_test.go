package agent

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

type fixtureEnv struct {
	Name                string          `json:"name"`
	Kind                string          `json:"kind"`
	Wire                json.RawMessage `json:"wire"`
	ExpectValid         bool            `json:"expect_valid"`
	Expect              json.RawMessage `json:"expect"`
	ExpectErrorContains *string         `json:"expect_error_contains"`
}

func fixturesDir(t *testing.T) string {
	t.Helper()
	cands := []string{
		"../contracts/agent-v1/fixtures",
		"contracts/agent-v1/fixtures",
	}
	for _, c := range cands {
		if st, err := os.Stat(c); err == nil && st.IsDir() {
			return c
		}
	}
	t.Fatalf("fixtures dir not found; tried %v", cands)
	return ""
}

func loadFixtures(t *testing.T) []fixtureEnv {
	t.Helper()
	dir := fixturesDir(t)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read fixtures %s: %v", dir, err)
	}
	var out []fixtureEnv
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		var env fixtureEnv
		if err := json.Unmarshal(data, &env); err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		out = append(out, env)
	}
	if len(out) == 0 {
		t.Fatalf("no fixtures in %s", dir)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

type exchange struct {
	Wire    json.RawMessage `json:"wire"`
	Encoded json.RawMessage `json:"encoded,omitempty"`
}

func TestMatrix(t *testing.T) {
	for _, fx := range loadFixtures(t) {
		t.Run("fixture/"+fx.Name, func(t *testing.T) {
			checkFixture(t, fx)
			if dir := os.Getenv("WIRE_EXPORT_DIR"); dir != "" {
				peer := exchange{Wire: fx.Wire}
				if fx.ExpectValid {
					var err error
					peer.Encoded, err = encodeFixture(fx)
					if err != nil {
						t.Fatal(err)
					}
				}
				data, err := json.Marshal(peer)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(dir, 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, fx.Name+".json"), data, 0644); err != nil {
					t.Fatal(err)
				}
			}
		})
		if dir := os.Getenv("WIRE_PEER_DIR"); dir != "" {
			t.Run("java-to-go/"+fx.Name, func(t *testing.T) {
				data, err := os.ReadFile(filepath.Join(dir, fx.Name+".json"))
				if err != nil {
					t.Fatal(err)
				}
				var peer exchange
				if err := json.Unmarshal(data, &peer); err != nil {
					t.Fatal(err)
				}
				if len(peer.Wire) == 0 {
					t.Fatal("missing peer wire")
				}
				fx.Wire = peer.Wire
				checkFixture(t, fx)
				if fx.ExpectValid {
					if len(peer.Encoded) == 0 {
						t.Fatal("missing Java codec output")
					}
					fx.Wire = peer.Encoded
					checkFixture(t, fx)
				}
			})
		}
	}
}

func checkFixture(t *testing.T, fx fixtureEnv) {
	t.Helper()
	if !fx.ExpectValid && (fx.ExpectErrorContains == nil || *fx.ExpectErrorContains == "") {
		t.Fatal("invalid fixture needs expect_error_contains")
	}
	switch fx.Kind {
	case "report_request":
		checkReport(t, fx)
	case "poll_response":
		checkPoll(t, fx)
	case "report_response":
		checkAck(t, fx)
	case "error":
		checkError(t, fx)
	default:
		t.Fatalf("case %s: unknown kind %s", fx.Name, fx.Kind)
	}
	if fx.ExpectValid {
		encoded, err := encodeFixture(fx)
		if err != nil {
			t.Fatal(err)
		}
		// Compare exact keys and values against independent expected data, preserving int64.
		var actual, expected map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &actual); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(fx.Expect, &expected); err != nil {
			t.Fatal(err)
		}
		for field, raw := range expected {
			if string(raw) == "null" {
				delete(expected, field)
				continue
			}
			if fx.Kind == "report_request" && field == "ts" {
				var value string
				if err := json.Unmarshal(raw, &value); err != nil {
					t.Fatal(err)
				}
				instant, err := time.Parse(time.RFC3339Nano, value)
				if err != nil {
					t.Fatal(err)
				}
				expected[field], err = json.Marshal(instant.UTC().Format(time.RFC3339Nano))
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		if len(actual) != len(expected) {
			t.Fatalf("%s: encoded keys got %v want %v", fx.Name, actual, expected)
		}
		for field, want := range expected {
			// Decode scalars/arrays with UseNumber to compare formatting-independent JSON.
			var gotValue, wantValue any
			decoder := json.NewDecoder(strings.NewReader(string(actual[field])))
			decoder.UseNumber()
			if err := decoder.Decode(&gotValue); err != nil {
				t.Fatalf("%s: field %s: %v", fx.Name, field, err)
			}
			decoder = json.NewDecoder(strings.NewReader(string(want)))
			decoder.UseNumber()
			if err := decoder.Decode(&wantValue); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotValue, wantValue) {
				t.Fatalf("%s: encoded field %s got %v want %v", fx.Name, field, gotValue, wantValue)
			}
		}
	}
}

func encodeFixture(fx fixtureEnv) ([]byte, error) {
	switch fx.Kind {
	case "report_request":
		value, err := ParseReportRequest(fx.Wire)
		if err != nil {
			return nil, err
		}
		return EncodeReportRequest(value)
	case "poll_response":
		value, err := ParsePollResponse(fx.Wire)
		if err != nil {
			return nil, err
		}
		return EncodePollResponse(value)
	case "report_response":
		value, err := ParseReportResponse(fx.Wire)
		if err != nil {
			return nil, err
		}
		return EncodeReportResponse(value)
	case "error":
		value, err := ParseErrorBody(fx.Wire)
		if err != nil {
			return nil, err
		}
		return EncodeErrorBody(value)
	default:
		return nil, fmt.Errorf("unknown kind %s", fx.Kind)
	}
}

func mustExpect(t *testing.T, fx fixtureEnv) map[string]json.RawMessage {
	t.Helper()
	if len(fx.Expect) == 0 {
		t.Fatalf("case %s: missing expect", fx.Name)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(fx.Expect, &m); err != nil {
		t.Fatalf("case %s: bad expect: %v", fx.Name, err)
	}
	return m
}

func expectString(t *testing.T, kase, field string, m map[string]json.RawMessage) *string {
	t.Helper()
	raw, ok := m[field]
	if !ok || string(raw) == "null" {
		return nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("case %s: expect field %s not a string: %v", kase, field, err)
	}
	return &s
}

func checkReport(t *testing.T, fx fixtureEnv) {
	if !fx.ExpectValid {
		// Go-encode -> Java-decode direction is covered by Java side; here Go must reject too.
		if _, err := ParseReportRequest(fx.Wire); err == nil {
			t.Fatalf("case %s: expected invalid but decoded ok (field hint '%v')", fx.Name, strOrEmpty(fx.ExpectErrorContains))
		} else if fx.ExpectErrorContains != nil && !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(*fx.ExpectErrorContains)) {
			t.Fatalf("case %s: error '%s' must mention field '%s'", fx.Name, err.Error(), *fx.ExpectErrorContains)
		}
		return
	}
	// Decode either the shared fixture or real peer output.
	decoded, err := ParseReportRequest(fx.Wire)
	if err != nil {
		t.Fatalf("case %s: valid wire rejected: %v", fx.Name, err)
	}
	exp := mustExpect(t, fx)
	var expAlloc, expStatus, expTs string
	if exp["allocation_id"] != nil && !rawIsNull(exp["allocation_id"]) {
		_ = json.Unmarshal(exp["allocation_id"], &expAlloc)
		if decoded.AllocationID != expAlloc {
			t.Fatalf("case %s: field allocation_id got %s want %s", fx.Name, decoded.AllocationID, expAlloc)
		}
	} else if decoded.AllocationID != "" {
		t.Fatalf("case %s: field allocation_id got %s want empty", fx.Name, decoded.AllocationID)
	}
	var expEpoch, expInc, expSeq int64
	_ = json.Unmarshal(exp["runner_epoch"], &expEpoch)
	_ = json.Unmarshal(exp["agent_incarnation"], &expInc)
	_ = json.Unmarshal(exp["seq"], &expSeq)
	if decoded.RunnerEpoch != expEpoch {
		t.Fatalf("case %s: field runner_epoch got %d want %d", fx.Name, decoded.RunnerEpoch, expEpoch)
	}
	if decoded.AgentIncarnation != expInc {
		t.Fatalf("case %s: field agent_incarnation got %d want %d", fx.Name, decoded.AgentIncarnation, expInc)
	}
	if decoded.Seq != expSeq {
		t.Fatalf("case %s: field seq got %d want %d", fx.Name, decoded.Seq, expSeq)
	}
	_ = json.Unmarshal(exp["status"], &expStatus)
	if string(decoded.Status) != expStatus {
		t.Fatalf("case %s: field status got %s want %s", fx.Name, decoded.Status, expStatus)
	}
	_ = json.Unmarshal(exp["ts"], &expTs)
	wantTs, err := time.Parse(time.RFC3339Nano, expTs)
	if err != nil {
		t.Fatalf("case %s: bad expect ts: %v", fx.Name, err)
	}
	if !decoded.Ts.Equal(wantTs) {
		t.Fatalf("case %s: field ts instant got %s want %s", fx.Name, decoded.Ts.UTC().Format(time.RFC3339Nano), wantTs.UTC().Format(time.RFC3339Nano))
	}
	if want, got := expectString(t, fx.Name, "detail", exp), decoded.Detail; !strPtrEq(want, got) {
		t.Fatalf("case %s: field detail got %v want %v", fx.Name, strOrEmpty(got), strOrEmpty(want))
	}
	if want, got := expectString(t, fx.Name, "error", exp), decoded.Error; !strPtrEq(want, got) {
		t.Fatalf("case %s: field error got %v want %v", fx.Name, strOrEmpty(got), strOrEmpty(want))
	}
	if exp["recovery_generation"] != nil && !rawIsNull(exp["recovery_generation"]) {
		var want string
		_ = json.Unmarshal(exp["recovery_generation"], &want)
		if decoded.RecoveryGeneration == nil || *decoded.RecoveryGeneration != strings.ToLower(want) {
			t.Fatalf("case %s: field recovery_generation got %v want %s", fx.Name, decoded.RecoveryGeneration, want)
		}
	} else if decoded.RecoveryGeneration != nil {
		t.Fatalf("case %s: field recovery_generation got %v want nil", fx.Name, *decoded.RecoveryGeneration)
	}
	if rawIsNull(exp["cleanup"]) {
		if decoded.Cleanup != nil {
			t.Fatalf("case %s: cleanup must be nil", fx.Name)
		}
	} else {
		var want struct {
			ExecutionEmpty    bool    `json:"execution_empty"`
			DescendantsReaped bool    `json:"descendants_reaped"`
			WorkspaceClean    bool    `json:"workspace_clean"`
			Error             *string `json:"error"`
		}
		if err := json.Unmarshal(exp["cleanup"], &want); err != nil {
			t.Fatal(err)
		}
		if got := decoded.Cleanup; got == nil || got.ExecutionEmpty != want.ExecutionEmpty || got.DescendantsReaped != want.DescendantsReaped || got.WorkspaceClean != want.WorkspaceClean || !strPtrEq(got.Error, want.Error) {
			t.Fatalf("case %s: cleanup got %+v want %+v", fx.Name, got, want)
		}
	}
	if rawIsNull(exp["reconcile"]) {
		if decoded.Reconcile != nil {
			t.Fatalf("case %s: reconcile must be nil", fx.Name)
		}
	} else {
		var want struct {
			CgroupPresent        bool     `json:"cgroup_present"`
			WorkspacePresent     bool     `json:"workspace_present"`
			PIDs                 []int64  `json:"pids"`
			ExecutionEmpty       bool     `json:"execution_empty"`
			DescendantsReaped    bool     `json:"descendants_reaped"`
			WorkspaceClean       bool     `json:"workspace_clean"`
			ObservedAllocationID *string  `json:"observed_allocation_id"`
			Error                *string  `json:"error"`
		}
		if err := json.Unmarshal(exp["reconcile"], &want); err != nil {
			t.Fatal(err)
		}
		got := decoded.Reconcile
		if got == nil || got.CgroupPresent != want.CgroupPresent || got.WorkspacePresent != want.WorkspacePresent || got.ExecutionEmpty != want.ExecutionEmpty || got.DescendantsReaped != want.DescendantsReaped || got.WorkspaceClean != want.WorkspaceClean || !strPtrEq(got.Error, want.Error) || !strPtrEq(got.ObservedAllocationID, want.ObservedAllocationID) || !reflect.DeepEqual(got.PIDs, want.PIDs) {
			t.Fatalf("case %s: reconcile got %+v want %+v", fx.Name, got, want)
		}
	}

	// Check local encoding; the exchange runner passes it to Java.
	enc, err := EncodeReportRequest(decoded)
	if err != nil {
		t.Fatalf("case %s: encode failed: %v", fx.Name, err)
	}
	var rew map[string]json.RawMessage
	if err := json.Unmarshal(enc, &rew); err != nil {
		t.Fatalf("case %s: encoded not JSON: %v", fx.Name, err)
	}
	requiredFields := []string{"runner_epoch", "agent_incarnation", "seq", "status", "ts"}
	if decoded.AllocationID != "" {
		requiredFields = append(requiredFields, "allocation_id")
	}
	for _, f := range requiredFields {
		raw, ok := rew[f]
		if !ok || string(raw) == "null" {
			t.Fatalf("case %s: go-encoded wire missing required field %s", fx.Name, f)
		}
	}
	if _, ok := rew["recoveryGeneration"]; ok {
		t.Fatalf("case %s: go-encoded wire must never contain recoveryGeneration", fx.Name)
	}
	if decoded.RecoveryGeneration == nil {
		if _, ok := rew["recovery_generation"]; ok {
			t.Fatalf("case %s: absent recovery_generation must be omitted", fx.Name)
		}
	} else {
		if _, ok := rew["recovery_generation"]; !ok {
			t.Fatalf("case %s: present recovery_generation must be included", fx.Name)
		}
	}
	var tsOut string
	_ = json.Unmarshal(rew["ts"], &tsOut)
	if !strings.HasSuffix(tsOut, "Z") {
		t.Fatalf("case %s: go-encoded ts must be UTC Z, got %s", fx.Name, tsOut)
	}
	rt, err := ParseReportRequest(enc)
	if err != nil {
		t.Fatalf("case %s: Go codec round-trip re-decode failed: %v", fx.Name, err)
	}
	if !rt.Ts.Equal(decoded.Ts) {
		t.Fatalf("case %s: timestamp round-trip instant changed", fx.Name)
	}
	if rt.AllocationID != decoded.AllocationID || rt.RunnerEpoch != decoded.RunnerEpoch || rt.Seq != decoded.Seq || rt.AgentIncarnation != decoded.AgentIncarnation || rt.Status != decoded.Status || !strPtrEq(rt.Detail, decoded.Detail) || !strPtrEq(rt.Error, decoded.Error) || !reflect.DeepEqual(rt.Cleanup, decoded.Cleanup) || !reflect.DeepEqual(rt.Reconcile, decoded.Reconcile) || !strPtrEq(rt.RecoveryGeneration, decoded.RecoveryGeneration) {
		t.Fatalf("case %s: Go codec round-trip changed fencing/enum fields", fx.Name)
	}
}

func checkPoll(t *testing.T, fx fixtureEnv) {
	if !fx.ExpectValid {
		if _, err := ParsePollResponse(fx.Wire); err == nil {
			t.Fatalf("case %s: expected invalid but decoded ok", fx.Name)
		} else if fx.ExpectErrorContains != nil && !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(*fx.ExpectErrorContains)) {
			t.Fatalf("case %s: error '%s' must mention field '%s'", fx.Name, err.Error(), *fx.ExpectErrorContains)
		}
		return
	}
	decoded, err := ParsePollResponse(fx.Wire)
	if err != nil {
		t.Fatalf("case %s: valid poll wire rejected: %v", fx.Name, err)
	}
	exp := mustExpect(t, fx)
	var expAssigned bool
	_ = json.Unmarshal(exp["assigned"], &expAssigned)
	if decoded.Assigned != expAssigned {
		t.Fatalf("case %s: field assigned got %v want %v", fx.Name, decoded.Assigned, expAssigned)
	}
	if expAssigned {
		var a, j, rc string
		_ = json.Unmarshal(exp["allocation_id"], &a)
		_ = json.Unmarshal(exp["job_id"], &j)
		_ = json.Unmarshal(exp["runner_class"], &rc)
		if decoded.AllocationID == nil || *decoded.AllocationID != a {
			t.Fatalf("case %s: field allocation_id mismatch", fx.Name)
		}
		if decoded.JobID == nil || *decoded.JobID != j {
			t.Fatalf("case %s: field job_id mismatch", fx.Name)
		}
		var e int64
		_ = json.Unmarshal(exp["runner_epoch"], &e)
		if decoded.RunnerEpoch == nil || *decoded.RunnerEpoch != e {
			t.Fatalf("case %s: field runner_epoch mismatch", fx.Name)
		}
		var argv []string
		_ = json.Unmarshal(exp["argv"], &argv)
		if len(decoded.Argv) != len(argv) {
			t.Fatalf("case %s: field argv length got %d want %d", fx.Name, len(decoded.Argv), len(argv))
		}
		for i := range argv {
			if decoded.Argv[i] != argv[i] {
				t.Fatalf("case %s: field argv[%d] mismatch", fx.Name, i)
			}
		}
		if decoded.RunnerClass == nil || *decoded.RunnerClass != rc {
			t.Fatalf("case %s: field runner_class mismatch", fx.Name)
		}
	}
	if !decoded.Assigned && (decoded.AllocationID != nil || decoded.JobID != nil || decoded.RunnerEpoch != nil || decoded.Argv != nil || decoded.RunnerClass != nil || decoded.CancelRequested != nil || decoded.WorkloadTimeoutMs != nil || decoded.ReconcileRequested != nil) {
		t.Fatal("idle poll retained allocation fields")
	}
	rawPam, hasPam := exp["poll_after_ms"]
	wantNil := !hasPam || string(rawPam) == "null"
	if wantNil && decoded.PollAfterMs != nil {
		t.Fatalf("case %s: field poll_after_ms must be nil", fx.Name)
	}
	if !wantNil {
		var v int64
		_ = json.Unmarshal(rawPam, &v)
		if decoded.PollAfterMs == nil || *decoded.PollAfterMs != v {
			t.Fatalf("case %s: field poll_after_ms mismatch", fx.Name)
		}
	}
	var wantCancel *bool
	if raw := exp["cancel_requested"]; !rawIsNull(raw) {
		if err := json.Unmarshal(raw, &wantCancel); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(wantCancel, decoded.CancelRequested) {
		t.Fatalf("case %s: field cancel_requested mismatch", fx.Name)
	}
	var wantTimeout *int64
	if raw := exp["workload_timeout_ms"]; !rawIsNull(raw) {
		if err := json.Unmarshal(raw, &wantTimeout); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(wantTimeout, decoded.WorkloadTimeoutMs) {
		t.Fatalf("case %s: field workload_timeout_ms mismatch", fx.Name)
	}
	var wantReconcile *bool
	if raw := exp["reconcile_requested"]; !rawIsNull(raw) {
		if err := json.Unmarshal(raw, &wantReconcile); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(wantReconcile, decoded.ReconcileRequested) {
		t.Fatalf("case %s: field reconcile_requested mismatch", fx.Name)
	}
	enc, err := EncodePollResponse(decoded)
	if err != nil {
		t.Fatalf("case %s: encode failed: %v", fx.Name, err)
	}
	var rew map[string]json.RawMessage
	_ = json.Unmarshal(enc, &rew)
	if _, ok := rew["recoveryGeneration"]; ok {
		t.Fatalf("case %s: go-encoded poll must never contain recoveryGeneration", fx.Name)
	}
	rt, err := ParsePollResponse(enc)
	if err != nil {
		t.Fatalf("case %s: Go codec round-trip failed: %v", fx.Name, err)
	}
	if !reflect.DeepEqual(rt, decoded) {
		t.Fatalf("case %s: round-trip poll changed", fx.Name)
	}
}

func checkAck(t *testing.T, fx fixtureEnv) {
	if !fx.ExpectValid {
		if _, err := ParseReportResponse(fx.Wire); err == nil {
			t.Fatalf("case %s: expected invalid but decoded ok", fx.Name)
		} else if fx.ExpectErrorContains != nil && !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(*fx.ExpectErrorContains)) {
			t.Fatalf("case %s: error '%s' must mention field '%s'", fx.Name, err.Error(), *fx.ExpectErrorContains)
		}
		return
	}
	decoded, err := ParseReportResponse(fx.Wire)
	if err != nil {
		t.Fatalf("case %s: valid ack wire rejected: %v", fx.Name, err)
	}
	exp := mustExpect(t, fx)
	var a bool
	var r string
	var term bool
	_ = json.Unmarshal(exp["accepted"], &a)
	_ = json.Unmarshal(exp["reason"], &r)
	_ = json.Unmarshal(exp["terminal"], &term)
	if decoded.Accepted != a {
		t.Fatalf("case %s: field accepted mismatch", fx.Name)
	}
	if decoded.Reason != r {
		t.Fatalf("case %s: field reason mismatch", fx.Name)
	}
	if decoded.Terminal != term {
		t.Fatalf("case %s: field terminal mismatch", fx.Name)
	}
	enc, err := EncodeReportResponse(decoded)
	if err != nil {
		t.Fatal(err)
	}
	rt, err := ParseReportResponse(enc)
	if err != nil {
		t.Fatalf("case %s: round-trip failed: %v", fx.Name, err)
	}
	if rt != decoded {
		t.Fatalf("case %s: round-trip changed value", fx.Name)
	}
}

func checkError(t *testing.T, fx fixtureEnv) {
	if !fx.ExpectValid {
		if _, err := ParseErrorBody(fx.Wire); err == nil {
			t.Fatalf("case %s: expected invalid but decoded ok", fx.Name)
		} else if fx.ExpectErrorContains != nil && !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(*fx.ExpectErrorContains)) {
			t.Fatalf("case %s: error '%s' must mention field '%s'", fx.Name, err.Error(), *fx.ExpectErrorContains)
		}
		return
	}
	decoded, err := ParseErrorBody(fx.Wire)
	if err != nil {
		t.Fatalf("case %s: valid error wire rejected: %v", fx.Name, err)
	}
	exp := mustExpect(t, fx)
	var e, m string
	_ = json.Unmarshal(exp["error"], &e)
	_ = json.Unmarshal(exp["message"], &m)
	if decoded.Error != e {
		t.Fatalf("case %s: field error got %s want %s", fx.Name, decoded.Error, e)
	}
	if decoded.Message != m {
		t.Fatalf("case %s: field message got %s want %s", fx.Name, decoded.Message, m)
	}
	enc, err := EncodeErrorBody(decoded)
	if err != nil {
		t.Fatal(err)
	}
	rt, err := ParseErrorBody(enc)
	if err != nil {
		t.Fatalf("case %s: round-trip failed: %v", fx.Name, err)
	}
	if rt != decoded {
		t.Fatalf("case %s: round-trip changed value", fx.Name)
	}
}

func strPtrEq(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func strOrEmpty(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

func TestRejectMalformedBodies(t *testing.T) {
	decoders := map[string]func([]byte) error{
		"report": func(data []byte) error { _, err := ParseReportRequest(data); return err },
		"poll":   func(data []byte) error { _, err := ParsePollResponse(data); return err },
		"ack":    func(data []byte) error { _, err := ParseReportResponse(data); return err },
		"error":  func(data []byte) error { _, err := ParseErrorBody(data); return err },
	}
	for name, decode := range decoders {
		t.Run(name, func(t *testing.T) {
			for _, body := range []string{"", "null", "[]", "true", "{", "{} {}"} {
				if err := decode([]byte(body)); err == nil {
					t.Fatalf("accepted malformed body %q", body)
				}
			}
		})
	}
	if _, err := ParsePollResponse([]byte(`{"assigned":false} {}`)); err == nil {
		t.Fatal("accepted trailing JSON")
	}
}

func TestEncodersRejectInvalidTypedValues(t *testing.T) {
	report := ReportRequest{AllocationID: "11111111-1111-1111-1111-111111111111", RunnerEpoch: 1, Seq: 0, Status: StatusRunning, Ts: time.Now()}
	if _, err := EncodeReportRequest(report); err == nil {
		t.Fatal("accepted seq zero")
	}
	backoff := int64(-1)
	if _, err := EncodePollResponse(PollResponse{PollAfterMs: &backoff}); err == nil {
		t.Fatal("accepted negative backoff")
	}
	if _, err := EncodeReportResponse(ReportResponse{Accepted: true}); err == nil {
		t.Fatal("accepted empty reason")
	}
	if _, err := EncodeErrorBody(ErrorBody{Error: "bad_request"}); err == nil {
		t.Fatal("accepted empty message")
	}
	report.Seq = 1
	invalid := string([]byte{0xff})
	report.Detail = &invalid
	if _, err := EncodeReportRequest(report); err == nil {
		t.Fatal("silently replaced invalid UTF-8")
	}
	report.Detail = nil
	report.Status = StatusCleanup
	report.Cleanup = &CleanupEvidence{ExecutionEmpty: true, DescendantsReaped: true, WorkspaceClean: true, Error: &invalid}
	if _, err := EncodeReportRequest(report); err == nil {
		t.Fatal("silently replaced invalid cleanup error UTF-8")
	}
	poll, err := ParsePollResponse([]byte(`{"assigned":true,"allocation_id":"11111111-1111-1111-1111-111111111111","job_id":"22222222-2222-2222-2222-222222222222","runner_epoch":1,"argv":["true"],"runner_class":"default"}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, timeout := range []int64{0, -1, 86400001} {
		poll.WorkloadTimeoutMs = &timeout
		if _, err := EncodePollResponse(poll); err == nil {
			t.Fatalf("accepted invalid workload timeout %d", timeout)
		}
	}
}

func TestDiscoveryPIDCountBoundary(t *testing.T) {
	for _, count := range []int{4096, 4097} {
		pids := make([]int64, count)
		for i := range pids {
			pids[i] = int64(i + 1)
		}
		r := ReportRequest{AllocationID: "11111111-1111-1111-1111-111111111111", RunnerEpoch: 1,
			AgentIncarnation: 2, Seq: 3, Status: StatusRecovery, Ts: time.Now(),
			Discovery: &DiscoveryEvidence{CgroupPresent: true, WorkspacePresent: true, PIDs: pids}}
		wire, err := EncodeReportRequest(r)
		if count == 4097 {
			if err == nil || !strings.Contains(err.Error(), "discovery.pids") {
				t.Fatalf("oversized discovery encoded: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := ParseReportRequest(wire)
		if err != nil || !reflect.DeepEqual(decoded.Discovery.PIDs, pids) {
			t.Fatalf("bounded discovery did not round-trip: %v", err)
		}
	}
}

func TestIdleReconcileEpochZeroAndGeneration(t *testing.T) {
	// Never-claimed idle runners remain epoch 0: idle RECONCILE without
	// allocation_id must accept 0, while allocated reports still require >= 1.
	idle := `{"runner_epoch":0,"agent_incarnation":7,"seq":1,"status":"RECONCILE",` +
		`"ts":"2026-09-22T13:00:00Z",` +
		`"reconcile":{"cgroup_present":false,"workspace_present":false,"pids":[],` +
		`"execution_empty":true,"descendants_reaped":true,"workspace_clean":true}}`
	decoded, err := ParseReportRequest([]byte(idle))
	if err != nil || decoded.RunnerEpoch != 0 || decoded.AllocationID != "" {
		t.Fatalf("idle epoch 0 rejected: %+v %v", decoded, err)
	}
	if _, err := EncodeReportRequest(decoded); err != nil {
		t.Fatalf("idle epoch 0 encode failed: %v", err)
	}
	allocatedZero := `{"allocation_id":"11111111-1111-1111-1111-111111111111",` +
		`"runner_epoch":0,"agent_incarnation":7,"seq":1,"status":"RUNNING",` +
		`"ts":"2026-09-22T13:00:00Z"}`
	if _, err := ParseReportRequest([]byte(allocatedZero)); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "runner_epoch") {
		t.Fatalf("allocated epoch 0 must be rejected, got %v", err)
	}
	// recovery_generation round-trips when present and is rejected when malformed.
	gen := "123e4567-e89b-12d3-a456-426614174000"
	// Unknown top-level fields are ignored; generation is preserved.
	decoded, err = ParseReportRequest([]byte(
		`{"allocation_id":"11111111-1111-1111-1111-111111111111",` +
			`"runner_epoch":3,"agent_incarnation":7,"seq":9,"status":"RUNNING",` +
			`"ts":"2026-09-22T13:00:00Z","recovery_generation":"` + gen + `","future_top":1}`))
	if err != nil || decoded.RecoveryGeneration == nil || *decoded.RecoveryGeneration != gen {
		t.Fatalf("valid generation not preserved: %+v %v", decoded, err)
	}
	if _, err := ParseReportRequest([]byte(
		`{"allocation_id":"11111111-1111-1111-1111-111111111111",` +
			`"runner_epoch":3,"agent_incarnation":7,"seq":9,"status":"RUNNING",` +
			`"ts":"2026-09-22T13:00:00Z","recovery_generation":"not-a-uuid"}`)); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "recovery_generation") {
		t.Fatalf("bad generation must be rejected, got %v", err)
	}
}
