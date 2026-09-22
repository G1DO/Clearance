# Agent wire contract v1 (Java ↔ Go)

Version: `v1`. Status: normative codecs from issue #11; internal transport and ingestion from issue #12.

Single versioned contract between controller (Java) and runner agent (thin standalone
Go process). Proves both codecs honor identical field names, enums, timestamps,
optional/null handling, unknown-field tolerance, and error interpretation so allocation
delivery and fenced reporting cannot diverge.

Transport (retained, deferred choice): outbound HTTP long-polling with `application/json`.
No gRPC, no Python service, no second backend. Trusted internal workloads only; this is
not a hostile sandbox boundary. HTTP/JSON retention vs gRPC/protobuf stays deferred;
no schema codegen is locked in.

Controller polling and reporting are implemented under `/internal/v1/agents/*`; see
[agent-api.md](../../docs/design/specifications/agent-api.md) for authentication, transactions,
and test faults. Go daemon logic, cgroup execution, cleanup verification, timeout-to-quarantine,
recovery-generation issuance, operator UI, and quantitative overload bounds remain out of scope.

## Common rules

- JSON objects, UTF-8, `application/json`. Field names are exact `snake_case` as listed.
- Integers use signed 64-bit JSON integer tokens (maximum `9223372036854775807`);
  quoted numbers, fractional/exponent forms and overflow are rejected, never coerced.
- UUIDs use the full `8-4-4-4-12` hexadecimal form. Input is case-insensitive;
  decoded values and output use lowercase.
- Known string fields contain Unicode scalar values; unpaired surrogate escapes are
  rejected. `argv` length is measured in UTF-16 code units to preserve the existing
  controller `JobRequest` validation (a supplementary character counts as two).
- Error codes and acknowledgment reasons match `[a-z][a-z0-9]*(?:_[a-z0-9]+)*`.
  Human error messages are non-empty strings; optional report strings may be empty.
- Unknown fields: consumers MUST ignore, MUST NOT reject, MUST NOT alter ownership
  interpretation. This protects `(allocation_id, runner_epoch, agent_incarnation, seq)`
  against future fields.
- `recoveryGeneration` (exact camelCase) is reserved and excluded from wire v1.
  If present it MUST be ignored under unknown-field rules. Controller MUST NOT
  read, persist, or branch on it in this Outcome.
- Optional vs null: required fields MUST be present and non-null; missing or explicit
  null on a required field is `bad_request`. Optional fields MAY be absent; explicit
  null is equivalent to absent and MUST be accepted and treated as absent. Optional
  presence MUST NOT affect fencing or ownership. Producers SHOULD omit absent
  optionals rather than emitting null.
- Timestamps: RFC 3339. Producers MUST emit UTC `Z`
  (e.g. `2026-09-22T12:34:56.123456789Z`). Consumers MUST accept any RFC 3339 offset
  (`Z` or `±hh:mm`) with optional fractional seconds up to nanos and normalize to the
  same UTC instant. Timestamp MUST round-trip the instant. The v1 profile requires date, hours,
  minutes and seconds, a dot before 1..9 fractional digits when present, and a zone
  (`Z`/`z` or an offset with hours `00..23` and minutes `00..59`). Lowercase `t`/`z`
  is accepted. Calendar dates must be valid; leap seconds, offset seconds and
  precision above nanos are rejected. The normalized UTC year is `0000..9999`.
- Error shape for 4xx/5xx on both poll and report: `{"error": "<snake_case>", "message": "<human>"}`.
  Both fields required. Unknown fields ignored.

## Poll

Agent long-polls; controller delivers at-most-one committed allocation. Delivery MUST
never precede the PostgreSQL commit owned by `SchedulerService.claim`.

Request (`POST /internal/v1/agents/poll`, JSON body):

- `agent_incarnation`: integer `>= 0`, required. One lifetime of the agent process.
- `runner_id`: UUID string, optional assertion; absent/null is allowed. Identity is derived
  from the separate machine token. A mismatching assertion returns 403 without writes.
- `timeout_s`: integer `>= 0`, optional long-poll hint. Absent/null defaults to 20 seconds,
  values above 30 are capped at 30, and 0 checks immediately.

The client must persist and increase its incarnation on every restart, never reuse or wrap it.
The first poll binds an incarnation; a greater incarnation supersedes the previous process.
A delayed older-incarnation poll returns 409 `fenced_rejected` without writes. Polling never
creates an allocation, attempt, or runner epoch; retries deliver the same committed assignment.

Response JSON (`PollResponse`):

| field | required | type | rule |
|---|---|---|---|
| `assigned` | yes | bool | `true` means allocation delivered; `false` means idle. |
| `allocation_id` | iff `assigned==true` | string UUID | Ownership identity from `allocations.allocation_id`. |
| `job_id` | iff `assigned==true` | string UUID | Logical job from `jobs.job_id`. |
| `runner_epoch` | iff `assigned==true` | int `>= 1` | Post-claim `runners.epoch`; equals `allocations.runner_epoch`. |
| `argv` | iff `assigned==true` | array 1..128 of string 1..4096 | Process vector (not shell-parsed). |
| `runner_class` | iff `assigned==true` | string `[A-Za-z0-9._-]{1,128}` | Exact-match compatibility input (job-intake rule). |
| `poll_after_ms` | no | int `>= 0` | Idle backoff hint. Absent/null means no hint. |
| unknown | — | — | MUST be ignored. When `assigned==false`, allocation fields if present MUST be ignored. |

Idle is `{"assigned": false}` plus optional `poll_after_ms`.
Assigned example:

```json
{
  "assigned": true,
  "allocation_id": "11111111-1111-1111-1111-111111111111",
  "job_id": "22222222-2222-2222-2222-222222222222",
  "runner_epoch": 3,
  "argv": ["echo", "hi"],
  "runner_class": "default"
}
```

## Report / heartbeat

Request (`ReportRequest`, `POST /internal/v1/agents/report`):

| field | required | type | rule |
|---|---|---|---|
| `runner_id` | no | string UUID | Optional identity assertion, as for poll; mismatch returns 403 without writes. |
| `allocation_id` | yes | string UUID | Fencing: exact match with authoritative owner. |
| `runner_epoch` | yes | int `>= 1` | Fencing: exact match with `runners.epoch`. Stale MUST NOT mutate. |
| `agent_incarnation` | yes | int `>= 0` | Fencing: exact match with owner incarnation. Stale MUST NOT mutate. |
| `seq` | yes | int `>= 1` | Per `allocation_id`, starts at 1, sender-increments by 1. Fenced like epoch. |
| `status` | yes | enum | `STARTING`, `RUNNING`, `SUCCEEDED`, `FAILED`, `HEARTBEAT` (exact uppercase). |
| `ts` | yes | RFC 3339 string | Observation time, see timestamp rules. |
| `detail` | no | string | Human detail. Absent/null equivalent, MUST NOT affect fencing. |
| `error` | no | string | Machine/human error hint (e.g. for `FAILED`). Absent/null equivalent. |
| unknown incl. `recoveryGeneration` | — | — | MUST be ignored, never alter ownership. |

Fencing is exact-match on `(allocation_id, runner_epoch, agent_incarnation)` plus
monotonic `seq` per allocation. PostgreSQL persists `max_seq` transactionally with acceptance;
`seq` must be greater, and incarnation rotation never resets it. A restarted client must
continue the allocation sequence. Stale or mismatched fencing MUST NOT mutate current
ownership, consistent with `clearance/model.py` and `runner-ownership-semantics.md`.
This contract makes F07 testable without unifying report states with runner lifecycle
`AVAILABLE -> ASSIGNED -> STARTING -> RUNNING -> CLEANING`.

Report vocabulary (O3):

- `STARTING`, `RUNNING` are non-terminal progress.
- `SUCCEEDED`, `FAILED` are terminal reports and sticky. Once terminal is accepted for
  an allocation, late non-terminal progress (`STARTING`/`RUNNING`) MUST be dropped
  (no state transition, no regression). A later conflicting terminal report cannot
  replace the first accepted terminal result either. Those rejected reports make no writes.
  A higher-sequence repeat of the same terminal result is accepted and advances only `max_seq`.
- `HEARTBEAT` carries `seq`, is fenced the same way, and causes no state transition.
  It only proves liveness under current ownership. Higher-sequence heartbeats remain acceptable
  after terminal, advancing only `max_seq` while preserving the terminal result.
- Terminal report does NOT release the runner. Release still requires cleanup proof
  owned by later work (`CLEANING -> AVAILABLE` via current proof; quarantine paths
  unchanged). All reports leave runner state/epoch and allocation `ACTIVE` ownership unchanged.
- Report progress is stored separately from runner lifecycle. A higher-sequence `STARTING`
  after `RUNNING` advances `max_seq` while retaining `RUNNING`.

Example:

```json
{
  "allocation_id": "11111111-1111-1111-1111-111111111111",
  "runner_epoch": 3,
  "agent_incarnation": 7,
  "seq": 1,
  "status": "RUNNING",
  "ts": "2026-09-22T12:34:56.123456789Z",
  "detail": "step 1/2"
}
```

Response ack (`ReportResponse`):

| field | required | type | rule |
|---|---|---|---|
| `accepted` | yes | bool | Whether the report was accepted and its `seq` recorded; acceptance need not change report status. |
| `reason` | yes | string snake_case | e.g. `ok`, `dropped_stale`, `fenced_rejected`, `terminal_sticky`. |
| `terminal` | yes | bool | Whether the allocation is now terminal-sticky (`SUCCEEDED`/`FAILED` seen). |

Error example (both endpoints):

```json
{"error": "bad_request", "message": "seq is required"}
```

Defined v1 error codes: `bad_request`, `unauthorized`, `not_found`, `fenced_rejected`,
`terminal_sticky`, `internal`. Unknown codes MUST be tolerated by agents (treat as
non-ok without branching on unknown semantics). Missing/invalid machine credentials
return 401 `unauthorized`; cross-runner access and
missing seeded inventory return 403 `fenced_rejected`. A stale poll incarnation returns 409
`fenced_rejected`; fenced or stale reports return a 200 acknowledgment with `accepted: false`.
Existing job API error responses remain unchanged; runner credentials cannot authorize job APIs.
The test-only deliberate poll-response drop returns an empty 503 instead of the normal error
shape, as described in [agent-api.md](../../docs/design/specifications/agent-api.md).

## Compatibility fixtures and matrix

Shared fixtures live in `contracts/agent-v1/fixtures/*.json`. Each file is one case:

```json
{
  "name": "report-normal",
  "kind": "report_request | poll_response | report_response | error",
  "wire": { ... exact wire JSON ... },
  "expect_valid": true,
  "expect": { ... expected decoded fields ... }
}
```

Invalid cases use `"expect_valid": false` plus `"expect_error_contains": "<field>"`.
The matrix covers: normal, optional-absent, explicit-null, unknown-fields,
error cases, timestamp round-trip, and `recoveryGeneration present-but-ignored`.

Run the complete, database-free exchange from the repository root:

```sh
bash contracts/agent-v1/verify.sh
```

The runner performs three steps with actual files produced during the run:

1. Java decodes every shared fixture and exports its JSON serializer output and,
   for valid cases, the `AgentProtocol` encoder output.
2. Go independently checks every fixture, consumes both Java outputs against the
   checked-in expectations, then exports its own JSON serializer and codec outputs.
3. Java consumes both Go outputs against the same checked-in expectations.

The raw serializer outputs retain explicit nulls, unknown fields and invalid inputs
that a typed producer cannot generate; typed codec outputs prove field names,
normalization, optional omission and removal of unknown fields. Both consumers check
invalid cases and name the offending field. Expectations always come from the
checked-in fixtures, never from peer-generated files. A missing peer case fails.
These are codec checks. PostgreSQL-backed agent integration tests separately exercise
sequencing, sticky terminal processing, database fencing, committed delivery, and no-release
behavior; see [agent-api.md](../../docs/design/specifications/agent-api.md).

Standalone checks: `cd agent && go test ./... -v`, or
`cd controller && bash ./mvnw -B -ntp -Dtest=AgentWireContractTest test`.
The test exchange uses `WIRE_EXPORT_DIR` and `WIRE_PEER_DIR`; the runner supplies them.

Each run preserves direction-specific logs and exchanged JSON below
`controller/target/agent-wire-contract/run.*/`. The CI `agent-contract` job runs the
complete exchange on every PR and main push and uploads `agent-v1-compatibility`
(including Surefire reports), even on failure, with 30-day retention. Fixture changes
therefore require both sides to re-pass. Full mixed-version rollout and migration
remain later work.
