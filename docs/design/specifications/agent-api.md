# Committed agent delivery and fenced reports (v1)

Issue #12 implements the controller side of the [agent wire contract](../../../contracts/agent-v1/README.md).
PostgreSQL is the durable authority. Polling delivers an existing scheduler claim; reports
record execution results separately from physical cleanup and runner release.

## Authentication and inventory

`clearance.auth.runner-keys` maps separate machine tokens to seeded runner UUIDs and defaults
to an empty map. Both `Authorization: Bearer <token>` and `X-API-Key: <token>` are accepted.
Inventory is inserted directly as described in [runner-claim.md](runner-claim.md); there is
no registration endpoint. A configured machine identity without an inventory row returns 403.

The authenticated UUID determines authority. Optional body `runner_id` is an assertion only;
a mismatch or a report targeting another runner's allocation returns 403 `fenced_rejected`
before writes. Missing/invalid machine credentials return 401 `unauthorized`. Submit tokens
cannot call internal routes; runner tokens cannot call `/api/**` or internal operator actions.
A token configured in both maps is rejected by both APIs. Runner inventory has no project
mapping: machine access is restricted to its own allocations, independent of job submit keys.

## Poll and incarnation

`POST /internal/v1/agents/poll` takes JSON:

```json
{"agent_incarnation": 7, "timeout_s": 0}
```

`agent_incarnation` is required, a signed 64-bit integer at least zero. The client must persist
and increase it for each process restart; values must never be reused or wrap. The first poll
records the incarnation. A greater value atomically supersedes the runner and active
allocation's previous incarnation; a smaller value returns 409 `fenced_rejected` without
writes. Equal-incarnation retries preserve stored values. The [Go daemon](../../../agent/README.md)
persists an incremented incarnation before polling on each boot, and preserves per-allocation
sequence and execution intent across restarts.

`timeout_s` is optional: absent/null means 20 seconds, values above 30 are capped at 30, and
0 checks immediately. Between observations the controller waits up to 100 ms outside any
transaction. It returns the committed assigned response, or `{"assigned":false}` at the
deadline. The wait hint does not impose a database lock-wait or request-latency bound.

Poll never invokes the scheduler, creates attempts/allocations, or advances the runner epoch.
`SchedulerService.claim` commits before its assignment can be delivered. Retrying after a lost
response returns the same serialized allocation with the same identities, epoch, and argv.
Incarnation rotation preserves the allocation identity, sequence maximum, and terminal result.

## Report acceptance

`POST /internal/v1/agents/report` uses the existing wire report fields and optional
`runner_id` assertion. Flyway V4 adds nullable `agent_incarnation` to runners and allocations,
and adds `max_seq` (initially 0) and nullable `report_status` to allocations. A report must match
the authenticated allocation owner, current runner/allocation epoch and incarnation, and have
`seq > max_seq`. An incarnation must have been established by polling first. Unknown fields
are ignored; the controller never reads, persists, or branches on `recoveryGeneration`.

Acceptance atomically persists `max_seq` and the report status:

- `STARTING` and `RUNNING` record progress. A newer `STARTING` after `RUNNING` consumes its
  sequence while retaining `RUNNING`.
- The first `SUCCEEDED`, `FAILED`, `CANCELLED`, or `TIMED_OUT` is sticky. Later progress or a conflicting terminal result
  returns `terminal_sticky` with no writes. A newer repeat of the same terminal result advances
  only `max_seq`.
- `HEARTBEAT` advances only `max_seq`, including after terminal. It neither changes report
  status nor interprets heartbeat loss, timeout, or quarantine.
- Stale/duplicate sequences return `dropped_stale`; mismatched fencing returns
  `fenced_rejected`. These acknowledgments have `accepted: false` and perform no writes.

The maximum sequence belongs to the allocation and never resets on incarnation rotation.
A restarted sender must continue that allocation's sequence. Report timestamps and outer optional
detail/error strings do not determine acceptance and are not persisted. Structured cleanup
and discovery error text is retained with evidence and the quarantine reason.
The first terminal report also writes the job and attempt result and moves the runner to
`CLEANING`, preserving the epoch and active allocation. A heartbeat or repeated result
cannot release ownership or clear quarantine. Flyway V5 adds these durable results,
cancellation state, per-allocation timeout, cleanup evidence, release time, and quarantine reason.

A `CLEANUP` report uses the same identity and allocation-wide sequence fields, plus
`cleanup: {execution_empty, descendants_reaped, workspace_clean, error?}`. All three
booleans must be present. Release requires all true and absent/null `error`, a terminal
allocation disposition (an execution result or `INTERRUPTED`), and the exact current
authenticated ownership. Missing evidence is rejected; negative current evidence records
`QUARANTINED` and a durable reason, leaving
the active allocation in place. The allocation's `cleanup_evidence` retains diagnostic flags
and error text. Stale or mismatched identity is checked before interpreting proof and makes
no writes, including no quarantine.

For valid positive proof after completed execution, one PostgreSQL transaction writes the
evidence, marks the allocation `RELEASED`, and makes the runner `AVAILABLE`. Interrupted
attempts instead follow the [atomic cleanup-and-retry path](#agent-crash-discovery-and-recovery).
The same positive cleanup can be acknowledged again after a lost acknowledgment, without
writes, while the same epoch and incarnation are
still current and the retry sequence is at least the accepted sequence. A later claim or
incarnation change fences that proof. Accepted terminal execution results remain unchanged.
Quarantine is sticky: another proof, terminal report, or heartbeat cannot clear it.

## Agent-crash discovery and recovery

The restarted Go agent discovers allocation-owned Linux state before sending `RECOVERY`
under its newly polled incarnation and the existing allocation-wide sequence. Its optional
`discovery` object is required for recovery acceptance and has this shape:

```json
{"cgroup_present":true,"workspace_present":true,"cleanup_verified":false,"pids":[101,202,303]}
```

The booleans and `pids` are required whenever the object is present. PIDs are positive signed
64-bit integers, with at most 4096 entries; `error` is an optional string. These observations
come from allocation-owned cgroups, workspace identity, and Linux processes, not the persisted
launch marker. `cleanup_verified` is true only when the current agent has independently
verified a durable removal checkpoint against physical state. Absent resources without that
proof, nonempty PIDs with that proof, or any error (including an empty error string) record
`QUARANTINED` and diagnostic evidence. The controller does not authorize continuation.

For consistent discovery the controller commits `recovery_action = 'TERMINATE'` and returns
`{"accepted":true,"reason":"terminate","terminal":true}`. If no execution result was
already accepted, the old attempt and allocation become `INTERRUPTED`, with durable attempt
`recovery_reason = 'agent_restart_without_resume_proof'`; the logical job result stays null.
`INTERRUPTED` is a database disposition, not a wire report status. Already accepted terminal
results are preserved and need cleanup only. In both cases the runner remains `CLEANING`,
with active ownership. The terminal acknowledgment does not assert that Linux processes have
stopped: the command requires the agent to terminate surviving execution and perform cleanup.

Flyway V6 adds recovery evidence, action, current discovery incarnation, pending discovery
state, and a link to the retry allocation, plus the attempt disposition and reason. Each accepted
discovery retains its current incarnation and sequence with the evidence. Repeating `RECOVERY`
with a newer sequence returns the same termination decision; stale sequences and old
incarnations are rejected before any write. Heartbeats cannot resolve recovery, normal progress
or conflicting terminal reports cannot replace `INTERRUPTED`, and quarantine remains sticky.
If the agent restarts during resolution, `CLEANUP` returns `recovery_required` until discovery
has been accepted under that new incarnation. A restart before durable launch intent retains
the normal launch/report contract; incarnation rotation alone does not infer interrupted work.

After termination, the existing `CLEANUP` report is the only release path. Negative cleanup
evidence quarantines the runner and preserves the allocation and interrupted history. Current
positive cleanup releases old ownership, then calls `SchedulerService.claim` for the same job
and runner in the same PostgreSQL transaction. This commits exactly one fresh attempt and
allocation with a higher epoch; `retry_allocation_id` links the history. The interrupted attempt
never writes the retry's job result. Cancellation suppresses the retry and completes the job as
`CANCELLED` after cleanup. Recovery of an already accepted terminal result does not retry.

There is no intermediate visible `AVAILABLE` state during successful recovery retry. A lost
cleanup response is resolved by polling the committed new allocation; old proof is fenced by
the advanced epoch and cannot create another retry. The agent verifies physical cleanup of its
old local allocation before accepting that different assignment. A missing response or locally
remembered terminal result never replaces physical cleanup. See the [agent recovery contract
and drill](../../../agent/README.md) for the Linux discovery and checkpoint rules.

Assigned polls include `cancel_requested` and `workload_timeout_ms`. The latter snapshots
`clearance.workload-timeout-ms` at claim (default 3,600,000; positive values capped at 86,400,000 milliseconds).
It is a local agent execution deadline, not a heartbeat-loss inference. Cancellation is
requested through the project-authorized [job interface](job-intake.md).

## Transactions and contention

`AgentService.poll` and `AgentService.report` declare `READ_COMMITTED` transactions with
explicit `JdbcTemplate` SQL. Both lock the runner row first (`SELECT ... FOR UPDATE`) and then
the allocation row, serializing against the scheduler's runner lock. Terminal ingestion
also locks/updates the job after ownership locks; claim locks runner then job. Cancellation
locks only the job, so it cannot invert ownership lock order. Reports
validate authority, sequence, terminal rules, and applicable proof before ownership writes. Poll/report return
through the Spring transaction proxy after commit. Long-poll waits and injected transport
delays hold no transaction or database connection.

The existing partial unique index `uq_allocations_runner_active` remains the independent
at-most-one-active-allocation backstop. No process-local ownership store or additional
coordination system is introduced. This slice establishes correctness under tested contention;
quantitative overload, shutdown bounds, general reconciliation, and recovery generations are deferred.

## Test transport faults and verification

Only with the `test` Spring profile, authenticated runners can configure their own one-shot
faults using `POST /internal/v1/agents/test/faults`:

```json
{"drop_next_poll": true, "delay_next_poll_ms": 0, "delay_next_report_ms": 0}
```

Each field is optional; absent/null means false or zero. Delays are integer milliseconds in
`0..5000`. Configuration replaces pending faults for that runner and returns `{}`. Poll
faults are consumed only by assigned responses after commit; a drop returns an empty 503.
Report delay runs before ingestion opens its transaction. Faults never change ownership and
are absent outside `test`, where the fault route returns 404. Never enable `test` in deployment.

The controller integration suite covers committed/lost delivery, restart/incarnation fencing,
stale epochs and sequences with unchanged-table comparisons, terminal stickiness, heartbeat,
credential separation, production fault absence, and poll/report/claim contention. These tests
use PostgreSQL through the existing integration-test mechanism. Run with PostgreSQL available:

```sh
cd controller
./mvnw -B -ntp test
```

The separate database-free codec exchange remains `bash contracts/agent-v1/verify.sh`.
