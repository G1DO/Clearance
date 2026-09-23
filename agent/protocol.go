// Package agent implements the Go codec and standalone runtime for agent wire contract v1.
//
// See contracts/agent-v1/README.md for the normative shapes. Unknown fields —
// including the reserved recoveryGeneration — are ignored, never rejected, and
// never alter ownership interpretation. This codec never reads recoveryGeneration.
//
// Fencing fields allocation_id, runner_epoch, agent_incarnation, seq are required.
// seq is per allocation_id, starts at 1, sender-increments by 1 (presence and range
// are validated here; the controller enforces cross-report sequencing in PostgreSQL).
// Execution results are sticky; only current positive CLEANUP evidence releases
// ownership, as documented in the contract and enforced by the controller.
package agent

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type ReportStatus string

const (
	StatusStarting  ReportStatus = "STARTING"
	StatusRunning   ReportStatus = "RUNNING"
	StatusSucceeded ReportStatus = "SUCCEEDED"
	StatusFailed    ReportStatus = "FAILED"
	StatusCancelled ReportStatus = "CANCELLED"
	StatusTimedOut  ReportStatus = "TIMED_OUT"
	StatusCleanup   ReportStatus = "CLEANUP"
	StatusHeartbeat ReportStatus = "HEARTBEAT"
	StatusRecovery  ReportStatus = "RECOVERY"
)

// Discovery is a physical observation, not launch intent or a resumption proof.
type DiscoveryEvidence struct {
	CgroupPresent    bool
	WorkspacePresent bool
	CleanupVerified  bool
	PIDs             []int64
	Error            *string
}

type CleanupEvidence struct {
	ExecutionEmpty    bool
	DescendantsReaped bool
	WorkspaceClean    bool
	Error             *string
}

type ReportRequest struct {
	AllocationID     string
	RunnerEpoch      int64
	AgentIncarnation int64
	Seq              int64
	Status           ReportStatus
	Ts               time.Time
	Detail           *string
	Error            *string
	Cleanup          *CleanupEvidence
	Discovery        *DiscoveryEvidence
}

type PollResponse struct {
	Assigned          bool
	AllocationID      *string
	JobID             *string
	RunnerEpoch       *int64
	Argv              []string
	RunnerClass       *string
	PollAfterMs       *int64
	CancelRequested   *bool
	WorkloadTimeoutMs *int64
}

type ReportResponse struct {
	Accepted bool
	Reason   string
	Terminal bool
}

type ErrorBody struct {
	Error   string
	Message string
}

var (
	runnerClassRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
	codeRe        = regexp.MustCompile(`^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$`)
	timestampRe   = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt](?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\.[0-9]{1,9})?(?:[Zz]|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])$`)
)

func isValidUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
				return false
			}
		}
	}
	return true
}

func rawIsNull(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return true
	}
	return strings.TrimSpace(string(raw)) == "null"
}

// encoding/json replaces malformed Unicode. Check original bytes of known
// strings so Java and Go preserve identical Unicode scalar values.
func unmarshalString(raw json.RawMessage, value *string) error {
	if !utf8.Valid(raw) {
		return fmt.Errorf("string must contain valid UTF-8")
	}
	if err := json.Unmarshal(raw, value); err != nil {
		return err
	}
	// JSON syntax is valid, so each Unicode escape has four hexadecimal digits.
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		if raw[i+1] != 'u' {
			i++ // Skip escaped backslashes and other simple escapes.
			continue
		}
		code, _ := strconv.ParseUint(string(raw[i+2:i+6]), 16, 16)
		if code >= 0xd800 && code <= 0xdbff {
			if i+12 > len(raw) || raw[i+6] != '\\' || raw[i+7] != 'u' {
				return fmt.Errorf("string contains an unpaired surrogate")
			}
			low, err := strconv.ParseUint(string(raw[i+8:i+12]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return fmt.Errorf("string contains an unpaired surrogate")
			}
			i += 11
		} else if code >= 0xdc00 && code <= 0xdfff {
			return fmt.Errorf("string contains an unpaired surrogate")
		} else {
			i += 5
		}
	}
	return nil
}

// Validate typed strings before json.Marshal can replace invalid UTF-8.
func marshalWireObject(fields map[string]any) ([]byte, error) {
	for name, value := range fields {
		switch v := value.(type) {
		case string:
			if !utf8.ValidString(v) {
				return nil, fmt.Errorf("%s must contain valid UTF-8", name)
			}
		case []string:
			for i, s := range v {
				if !utf8.ValidString(s) {
					return nil, fmt.Errorf("%s[%d] must contain valid UTF-8", name, i)
				}
			}
		}
	}
	return json.Marshal(fields)
}

func requiredUUID(m map[string]json.RawMessage, name string) (string, error) {
	raw, ok := m[name]
	if !ok || rawIsNull(raw) {
		return "", fmt.Errorf("%s is required and must be a UUID string", name)
	}
	var s string
	if err := unmarshalString(raw, &s); err != nil || s == "" {
		return "", fmt.Errorf("%s is required and must be a UUID string", name)
	}
	if !isValidUUID(s) {
		return "", fmt.Errorf("%s must be a UUID string", name)
	}
	return strings.ToLower(s), nil
}

func requiredIntMin(m map[string]json.RawMessage, name string, min int64) (int64, error) {
	raw, ok := m[name]
	if !ok || rawIsNull(raw) {
		return 0, fmt.Errorf("%s is required and must be an integer", name)
	}
	// Strict integers only: reject floats/exponents even when numerically integral.
	if strings.ContainsAny(strings.TrimSpace(string(raw)), ".eE") {
		return 0, fmt.Errorf("%s is required and must be an integer", name)
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err != nil {
		return 0, fmt.Errorf("%s is required and must be an integer", name)
	}
	if n < min {
		return 0, fmt.Errorf("%s must be >= %d", name, min)
	}
	return n, nil
}

func optionalIntMin(m map[string]json.RawMessage, name string, min int64) (*int64, error) {
	raw, ok := m[name]
	if !ok || rawIsNull(raw) {
		return nil, nil
	}
	n, err := requiredIntMin(m, name, min)
	if err != nil {
		// requiredIntMin mentions required; reword to "when present" for optionals.
		return nil, fmt.Errorf("%s must be an integer when present (>= %d)", name, min)
	}
	return &n, nil
}

func requiredBool(m map[string]json.RawMessage, name string) (bool, error) {
	raw, ok := m[name]
	if !ok || rawIsNull(raw) {
		return false, fmt.Errorf("%s is required and must be a boolean", name)
	}
	var b bool
	if err := json.Unmarshal(raw, &b); err != nil {
		return false, fmt.Errorf("%s is required and must be a boolean", name)
	}
	return b, nil
}

func optionalBool(m map[string]json.RawMessage, name string) (*bool, error) {
	if rawIsNull(m[name]) {
		return nil, nil
	}
	b, err := requiredBool(m, name)
	if err != nil {
		return nil, err
	}
	return &b, nil
}

func optionalCleanup(m map[string]json.RawMessage) (*CleanupEvidence, error) {
	if rawIsNull(m["cleanup"]) {
		return nil, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(m["cleanup"], &fields); err != nil {
		return nil, fmt.Errorf("cleanup must be an object when present")
	}
	executionEmpty, err := requiredBool(fields, "execution_empty")
	if err != nil {
		return nil, fmt.Errorf("cleanup.%w", err)
	}
	descendantsReaped, err := requiredBool(fields, "descendants_reaped")
	if err != nil {
		return nil, fmt.Errorf("cleanup.%w", err)
	}
	workspaceClean, err := requiredBool(fields, "workspace_clean")
	if err != nil {
		return nil, fmt.Errorf("cleanup.%w", err)
	}
	errStr, err := optionalString(fields, "error")
	if err != nil {
		return nil, fmt.Errorf("cleanup.%w", err)
	}
	return &CleanupEvidence{executionEmpty, descendantsReaped, workspaceClean, errStr}, nil
}

func optionalDiscovery(m map[string]json.RawMessage) (*DiscoveryEvidence, error) {
	if rawIsNull(m["discovery"]) {
		return nil, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(m["discovery"], &fields); err != nil {
		return nil, fmt.Errorf("discovery must be an object when present")
	}
	var d DiscoveryEvidence
	for name, target := range map[string]*bool{"cgroup_present": &d.CgroupPresent, "workspace_present": &d.WorkspacePresent, "cleanup_verified": &d.CleanupVerified} {
		v, err := requiredBool(fields, name)
		if err != nil {
			return nil, fmt.Errorf("discovery.%w", err)
		}
		*target = v
	}
	var pids []json.RawMessage
	if rawIsNull(fields["pids"]) || json.Unmarshal(fields["pids"], &pids) != nil || len(pids) > 4096 {
		return nil, fmt.Errorf("discovery.pids must be an array of at most 4096 positive integers")
	}
	d.PIDs = make([]int64, 0, len(pids))
	for _, raw := range pids {
		pid, err := requiredIntMin(map[string]json.RawMessage{"pids": raw}, "pids", 1)
		if err != nil {
			return nil, fmt.Errorf("discovery.%w", err)
		}
		d.PIDs = append(d.PIDs, pid)
	}
	var err error
	d.Error, err = optionalString(fields, "error")
	if err != nil {
		return nil, fmt.Errorf("discovery.%w", err)
	}
	return &d, nil
}

func requiredNonEmptyString(m map[string]json.RawMessage, name string) (string, error) {
	raw, ok := m[name]
	if !ok || rawIsNull(raw) {
		return "", fmt.Errorf("%s is required and must be a non-empty string", name)
	}
	var s string
	if err := unmarshalString(raw, &s); err != nil || s == "" {
		return "", fmt.Errorf("%s is required and must be a non-empty string", name)
	}
	return s, nil
}

func requiredCode(m map[string]json.RawMessage, name string) (string, error) {
	s, err := requiredNonEmptyString(m, name)
	if err != nil {
		return "", err
	}
	if !codeRe.MatchString(s) {
		return "", fmt.Errorf("%s must be a snake_case code", name)
	}
	return s, nil
}

func optionalString(m map[string]json.RawMessage, name string) (*string, error) {
	raw, ok := m[name]
	if !ok || rawIsNull(raw) {
		return nil, nil
	}
	var s string
	if err := unmarshalString(raw, &s); err != nil {
		return nil, fmt.Errorf("%s must be a string when present", name)
	}
	v := s
	return &v, nil
}

func requiredStatus(m map[string]json.RawMessage, name string) (ReportStatus, error) {
	raw, ok := m[name]
	if !ok || rawIsNull(raw) {
		return "", fmt.Errorf("%s is required and must be one of STARTING,RUNNING,SUCCEEDED,FAILED,CANCELLED,TIMED_OUT,CLEANUP,HEARTBEAT,RECOVERY", name)
	}
	var s string
	if err := unmarshalString(raw, &s); err != nil {
		return "", fmt.Errorf("%s is required and must be one of STARTING,RUNNING,SUCCEEDED,FAILED,CANCELLED,TIMED_OUT,CLEANUP,HEARTBEAT,RECOVERY", name)
	}
	switch ReportStatus(s) {
	case StatusStarting, StatusRunning, StatusSucceeded, StatusFailed, StatusCancelled, StatusTimedOut, StatusCleanup, StatusHeartbeat, StatusRecovery:
		return ReportStatus(s), nil
	default:
		return "", fmt.Errorf("%s must be one of STARTING,RUNNING,SUCCEEDED,FAILED,CANCELLED,TIMED_OUT,CLEANUP,HEARTBEAT,RECOVERY", name)
	}
}

func requiredTime(m map[string]json.RawMessage, name string) (time.Time, error) {
	raw, ok := m[name]
	if !ok || rawIsNull(raw) {
		return time.Time{}, fmt.Errorf("%s is required and must be an RFC 3339 timestamp", name)
	}
	var s string
	if err := unmarshalString(raw, &s); err != nil || s == "" {
		return time.Time{}, fmt.Errorf("%s is required and must be an RFC 3339 timestamp", name)
	}
	if !timestampRe.MatchString(s) {
		return time.Time{}, fmt.Errorf("%s must be an RFC 3339 timestamp with at most 9 fractional digits", name)
	}
	t, err := time.Parse(time.RFC3339Nano, strings.ToUpper(s))
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be an RFC 3339 timestamp", name)
	}
	t = t.UTC()
	if t.Year() < 0 || t.Year() > 9999 {
		return time.Time{}, fmt.Errorf("%s UTC year must be between 0000 and 9999", name)
	}
	return t, nil
}

// ParseReportRequest decodes wire JSON; unknown fields (incl. recoveryGeneration) ignored.
func ParseReportRequest(data []byte) (ReportRequest, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return ReportRequest{}, fmt.Errorf("malformed JSON body: %v", err)
	}
	if m == nil {
		return ReportRequest{}, fmt.Errorf("report: expected JSON object")
	}
	allocationID, err := requiredUUID(m, "allocation_id")
	if err != nil {
		return ReportRequest{}, err
	}
	runnerEpoch, err := requiredIntMin(m, "runner_epoch", 1)
	if err != nil {
		return ReportRequest{}, err
	}
	incarnation, err := requiredIntMin(m, "agent_incarnation", 0)
	if err != nil {
		return ReportRequest{}, err
	}
	seq, err := requiredIntMin(m, "seq", 1)
	if err != nil {
		return ReportRequest{}, err
	}
	status, err := requiredStatus(m, "status")
	if err != nil {
		return ReportRequest{}, err
	}
	ts, err := requiredTime(m, "ts")
	if err != nil {
		return ReportRequest{}, err
	}
	detail, err := optionalString(m, "detail")
	if err != nil {
		return ReportRequest{}, err
	}
	errStr, err := optionalString(m, "error")
	if err != nil {
		return ReportRequest{}, err
	}
	cleanup, err := optionalCleanup(m)
	if err != nil {
		return ReportRequest{}, err
	}
	discovery, err := optionalDiscovery(m)
	if err != nil {
		return ReportRequest{}, err
	}
	return ReportRequest{
		AllocationID:     allocationID,
		RunnerEpoch:      runnerEpoch,
		AgentIncarnation: incarnation,
		Seq:              seq,
		Status:           status,
		Ts:               ts,
		Detail:           detail,
		Error:            errStr,
		Cleanup:          cleanup,
		Discovery:        discovery,
	}, nil
}

// EncodeReportRequest emits canonical wire JSON; absent optionals omitted, never recoveryGeneration.
func EncodeReportRequest(r ReportRequest) ([]byte, error) {
	m := map[string]any{
		"allocation_id":     strings.ToLower(r.AllocationID),
		"runner_epoch":      r.RunnerEpoch,
		"agent_incarnation": r.AgentIncarnation,
		"seq":               r.Seq,
		"status":            string(r.Status),
		"ts":                r.Ts.UTC().Format(time.RFC3339Nano),
	}
	if r.Detail != nil {
		m["detail"] = *r.Detail
	}
	if r.Error != nil {
		m["error"] = *r.Error
	}
	if r.Cleanup != nil {
		cleanup := map[string]any{
			"execution_empty":    r.Cleanup.ExecutionEmpty,
			"descendants_reaped": r.Cleanup.DescendantsReaped,
			"workspace_clean":    r.Cleanup.WorkspaceClean,
		}
		if r.Cleanup.Error != nil {
			cleanup["error"] = *r.Cleanup.Error
		}
		encoded, err := marshalWireObject(cleanup)
		if err != nil {
			return nil, fmt.Errorf("cleanup.%w", err)
		}
		m["cleanup"] = json.RawMessage(encoded)
	}
	if r.Discovery != nil {
		pids := r.Discovery.PIDs
		if pids == nil {
			pids = []int64{}
		}
		fields := map[string]any{"cgroup_present": r.Discovery.CgroupPresent, "workspace_present": r.Discovery.WorkspacePresent,
			"cleanup_verified": r.Discovery.CleanupVerified, "pids": pids}
		if r.Discovery.Error != nil {
			fields["error"] = *r.Discovery.Error
		}
		encoded, err := marshalWireObject(fields)
		if err != nil {
			return nil, fmt.Errorf("discovery.%w", err)
		}
		m["discovery"] = json.RawMessage(encoded)
	}
	data, err := marshalWireObject(m)
	if err != nil {
		return nil, err
	}
	if _, err := ParseReportRequest(data); err != nil {
		return nil, err
	}
	return data, nil
}

// Job intake measures Java String length in UTF-16 code units.
func utf16Length(s string) int {
	n := 0
	for _, r := range s {
		n++
		if r > 0xffff {
			n++
		}
	}
	return n
}

func requiredArgv(m map[string]json.RawMessage) ([]string, error) {
	raw, ok := m["argv"]
	if !ok || rawIsNull(raw) {
		return nil, fmt.Errorf("argv is required and must be an array of 1..128 strings")
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil || len(arr) == 0 || len(arr) > 128 {
		return nil, fmt.Errorf("argv is required and must be an array of 1..128 strings")
	}
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		var s string
		if err := unmarshalString(e, &s); err != nil || s == "" || utf16Length(s) > 4096 {
			return nil, fmt.Errorf("argv elements must be strings of 1..4096 UTF-16 code units")
		}
		out = append(out, s)
	}
	return out, nil
}

func requiredRunnerClass(m map[string]json.RawMessage) (string, error) {
	s, err := requiredNonEmptyString(m, "runner_class")
	if err != nil {
		return "", fmt.Errorf("runner_class is required")
	}
	if !runnerClassRe.MatchString(s) {
		return "", fmt.Errorf("runner_class must match [A-Za-z0-9._-]{1,128}")
	}
	return s, nil
}

// ParsePollResponse decodes poll wire JSON; when assigned==false allocation fields ignored.
func ParsePollResponse(data []byte) (PollResponse, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return PollResponse{}, fmt.Errorf("malformed JSON body: %v", err)
	}
	if m == nil {
		return PollResponse{}, fmt.Errorf("poll: expected JSON object")
	}
	assigned, err := requiredBool(m, "assigned")
	if err != nil {
		return PollResponse{}, err
	}
	pollAfter, err := optionalIntMin(m, "poll_after_ms", 0)
	if err != nil {
		return PollResponse{}, err
	}
	if !assigned {
		return PollResponse{Assigned: false, PollAfterMs: pollAfter}, nil
	}
	alloc, err := requiredUUID(m, "allocation_id")
	if err != nil {
		return PollResponse{}, err
	}
	job, err := requiredUUID(m, "job_id")
	if err != nil {
		return PollResponse{}, err
	}
	epoch, err := requiredIntMin(m, "runner_epoch", 1)
	if err != nil {
		return PollResponse{}, err
	}
	argv, err := requiredArgv(m)
	if err != nil {
		return PollResponse{}, err
	}
	rc, err := requiredRunnerClass(m)
	if err != nil {
		return PollResponse{}, err
	}
	cancelRequested, err := optionalBool(m, "cancel_requested")
	if err != nil {
		return PollResponse{}, err
	}
	workloadTimeout, err := optionalIntMin(m, "workload_timeout_ms", 1)
	if err != nil {
		return PollResponse{}, err
	}
	if workloadTimeout != nil && *workloadTimeout > 86400000 {
		return PollResponse{}, fmt.Errorf("workload_timeout_ms must be <= 86400000")
	}
	return PollResponse{
		Assigned:          true,
		AllocationID:      &alloc,
		JobID:             &job,
		RunnerEpoch:       &epoch,
		Argv:              argv,
		RunnerClass:       &rc,
		PollAfterMs:       pollAfter,
		CancelRequested:   cancelRequested,
		WorkloadTimeoutMs: workloadTimeout,
	}, nil
}

// EncodePollResponse emits canonical poll JSON; idle omits allocation fields.
func EncodePollResponse(p PollResponse) ([]byte, error) {
	m := map[string]any{"assigned": p.Assigned}
	if p.PollAfterMs != nil {
		m["poll_after_ms"] = *p.PollAfterMs
	}
	if p.Assigned {
		if p.AllocationID == nil || p.JobID == nil || p.RunnerEpoch == nil || p.Argv == nil || p.RunnerClass == nil {
			return nil, fmt.Errorf("assigned poll requires allocation_id, job_id, runner_epoch, argv, runner_class")
		}
		m["allocation_id"] = strings.ToLower(*p.AllocationID)
		m["job_id"] = strings.ToLower(*p.JobID)
		m["runner_epoch"] = *p.RunnerEpoch
		m["argv"] = p.Argv
		m["runner_class"] = *p.RunnerClass
		if p.CancelRequested != nil {
			m["cancel_requested"] = *p.CancelRequested
		}
		if p.WorkloadTimeoutMs != nil {
			m["workload_timeout_ms"] = *p.WorkloadTimeoutMs
		}
	}
	data, err := marshalWireObject(m)
	if err != nil {
		return nil, err
	}
	if _, err := ParsePollResponse(data); err != nil {
		return nil, err
	}
	return data, nil
}

// ParseReportResponse decodes ack JSON.
func ParseReportResponse(data []byte) (ReportResponse, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return ReportResponse{}, fmt.Errorf("malformed JSON body: %v", err)
	}
	if m == nil {
		return ReportResponse{}, fmt.Errorf("ack: expected JSON object")
	}
	accepted, err := requiredBool(m, "accepted")
	if err != nil {
		return ReportResponse{}, err
	}
	reason, err := requiredCode(m, "reason")
	if err != nil {
		return ReportResponse{}, err
	}
	terminal, err := requiredBool(m, "terminal")
	if err != nil {
		return ReportResponse{}, err
	}
	return ReportResponse{Accepted: accepted, Reason: reason, Terminal: terminal}, nil
}

// EncodeReportResponse emits canonical ack JSON.
func EncodeReportResponse(r ReportResponse) ([]byte, error) {
	data, err := marshalWireObject(map[string]any{
		"accepted": r.Accepted,
		"reason":   r.Reason,
		"terminal": r.Terminal,
	})
	if err != nil {
		return nil, err
	}
	if _, err := ParseReportResponse(data); err != nil {
		return nil, err
	}
	return data, nil
}

// ParseErrorBody decodes error JSON.
func ParseErrorBody(data []byte) (ErrorBody, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return ErrorBody{}, fmt.Errorf("malformed JSON body: %v", err)
	}
	if m == nil {
		return ErrorBody{}, fmt.Errorf("error: expected JSON object")
	}
	code, err := requiredCode(m, "error")
	if err != nil {
		return ErrorBody{}, err
	}
	msg, err := requiredNonEmptyString(m, "message")
	if err != nil {
		return ErrorBody{}, err
	}
	return ErrorBody{Error: code, Message: msg}, nil
}

// EncodeErrorBody emits canonical error JSON.
func EncodeErrorBody(e ErrorBody) ([]byte, error) {
	data, err := marshalWireObject(map[string]any{
		"error":   e.Error,
		"message": e.Message,
	})
	if err != nil {
		return nil, err
	}
	if _, err := ParseErrorBody(data); err != nil {
		return nil, err
	}
	return data, nil
}
