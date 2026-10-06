# Committed agent delivery and fenced reports (v1)

The controller implements the [agent wire contract](../../contracts/agent-v1/README.md).
PostgreSQL is the durable authority. Polling delivers an existing scheduler claim; reports
record execution results separately from physical cleanup and runner release.

## Authentication and inventory

`clearance.auth.runner-keys` maps separate machine tokens to seeded runner UUIDs and defaults
to an empty map. Both `Authorization: Bearer <token>` and `X-API-Key: <token>` are accepted.
Inventory is inserted directly as described in [runner-claim.md](../architecture/runner-claim.md); there is
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
writes. Equal-incarnation retries preserve stored values. The [Go daemon](../operations/agent.md)
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
`seq > max_seq`. An incarnation must have been established by polling first. Unknown wire fields
are ignored; the v1 wire field `recoveryGeneration` (exact camelCase) is never read, persisted,
or branched on. Server-side recovery-generation fencing (Flyway V9, issue #32) is separate:
when a current generation exists, only allocations tagged with it are current, and superseded
evidence is rejected with zero mutation.

Acceptance atomically persists `max_seq` and the report status:

- `STARTING` and `RUNNING` record progress. A newer `STARTING` after `RUNNING` consumes its
  sequence while retaining `RUNNING`.
- The first `SUCCEEDED`, `FAILED`, `CANCELLED`, or `TIMED_OUT` is sticky. Later progress or a conflicting terminal result
  returns `terminal_sticky` with no writes. A newer repeat of the same terminal result advances
  only `max_seq`.
- `HEARTBEAT` advances `max_seq` and refreshes contact time (`last_contact_at = now()`),
  including after terminal. It does not change report status.
- Stale/duplicate sequences return `dropped_stale`; mismatched fencing returns
  `fenced_rejected`. These acknowledgments have `accepted: false` and perform no writes
  (contact metadata and row versions are completely untouched).

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
has been accepted under that new incarnation, or quarantine reconciliation has committed a
current `TERMINATE_CLEANUP` binding from fresh discovery. A restart before durable launch intent retains
the normal launch/report contract; incarnation rotation alone does not infer interrupted work.

After termination, positive cleanup is the release gate, submitted through `CLEANUP`
or attest-only [quarantine reconciliation](#quarantine-reconciliation). Negative cleanup
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
remembered terminal result never replaces physical cleanup. See the [agent recovery contract](../operations/agent.md#durable-state-and-failure-behavior)
and [verification drill](../development/agent-verification.md#reproducible-agent-sigkill-recovery-drill) for the Linux discovery and checkpoint rules.

Assigned polls include `cancel_requested` and `workload_timeout_ms`. The latter snapshots
`clearance.workload-timeout-ms` at claim (default 3,600,000; positive values capped at 86,400,000 milliseconds).
It is a local agent execution deadline, not a heartbeat-loss inference. Cancellation is
requested through the project-authorized [job interface](job-intake.md).

## Heartbeat timeout and quarantine

Flyway V7 adds `last_contact_at TIMESTAMPTZ NOT NULL DEFAULT now()` and `heartbeat_timeout_ms BIGINT NOT NULL DEFAULT 15000` to `allocations`, with partial index `ix_allocations_active_contact` on `(last_contact_at) WHERE state = 'ACTIVE'`.

At claim time, `SchedulerService.claim` snapshots `clearance.heartbeat-timeout-ms` (default 15,000 ms, range 1 to 86,400,000 ms) into `allocations.heartbeat_timeout_ms` and initializes `last_contact_at = now()`. Every accepted report (`STARTING`, `RUNNING`, terminal, `HEARTBEAT`, `CLEANUP`, `RECOVERY`) refreshes `allocations.last_contact_at = now()`. Fenced, stale, or duplicate reports perform no writes, leaving contact metadata untouched.

A background evaluator (`HeartbeatEvaluatorService`) runs periodically (`clearance.heartbeat-evaluator-interval-ms`, default 1,000 ms, batch size `clearance.heartbeat-evaluator-batch-size`, default 50). It identifies active allocations whose heartbeat timeout has elapsed:

```sql
SELECT a.allocation_id
FROM allocations a
JOIN runners r ON r.runner_id = a.runner_id
WHERE a.state = 'ACTIVE'
  AND r.state NOT IN ('AVAILABLE', 'QUARANTINED')
  AND (a.last_contact_at + interval '1 millisecond' * a.heartbeat_timeout_ms) <= now()
ORDER BY a.last_contact_at ASC, a.allocation_id ASC
LIMIT ?;
```

For each candidate, the evaluator opens a transaction and acquires row locks in the repository lock order: runner row first (`SELECT ... FOR UPDATE`), then allocation row. Under row locks, it re-verifies that the runner remains assigned/cleaning, the allocation remains `ACTIVE`, and the expiration condition still holds against database `now()`. If verified, it updates the runner:

```sql
UPDATE runners
SET state = 'QUARANTINED', quarantine_reason = ?, updated_at = now()
WHERE runner_id = ? AND state NOT IN ('AVAILABLE', 'QUARANTINED') AND epoch = ?;
```

The durable `quarantine_reason` records an inspectable payload with allocation ID, runner epoch, agent incarnation, sequence, contact timestamp, timeout duration, and last report status.

Crucially:
- Active ownership is retained (`allocations.state = 'ACTIVE'`).
- Unfinished execution results are not resolved (`jobs.result = NULL`, `attempts.result = NULL`).
- No retries are scheduled.
- The quarantined runner rejects subsequent claims (`state != 'AVAILABLE'`), and the unresolved job cannot be claimed on another runner (`EXISTS (SELECT 1 FROM allocations WHERE job_id = ? AND state = 'ACTIVE')`).
- Subsequent agent reports, heartbeats, or polling cannot clear quarantine or release ownership.
- Controller restarts do not clear quarantine or reset the timeout baseline (evaluated against durable `last_contact_at` in PostgreSQL).

## Quarantine reconciliation

Flyway V8 adds `reconcile_requested`, `reconcile_classification`, `reconcile_action`,
`reconcile_evidence`, `reconcile_incarnation`, `reconcile_seq`, and
`reconcile_updated_at` to `allocations`, with partial index
`ix_allocations_reconcile_pending` on `(reconcile_requested) WHERE state = 'ACTIVE'`.

A background service (`ReconciliationService`) runs periodically
(`clearance.reconciliation-interval-ms`, default 1,000 ms, batch size
`clearance.reconciliation-batch-size`, default 50). It flags `QUARANTINED` runners
holding `ACTIVE` work for fresh observation (`reconcile_requested = true`) under
runner-then-allocation row locks, re-verifying quarantine, active ownership, and
epoch match. It never inspects host state, releases ownership, or clears
quarantine. Assigned polls deliver the flag as `reconcile_requested`; it is
ignored on idle responses. The same agent (same incarnation) or a restarted agent
can then send `RECONCILE` without manual database edits or a restart merely to
trigger discovery.

`POST /internal/v1/agents/report` with `status: RECONCILE` carries a fresh
`reconcile` observation (`cgroup_present`, `workspace_present`, `pids` up to 4096,
`execution_empty`, `descendants_reaped`, `workspace_clean`, optional
`observed_allocation_id`, optional `error`). Fencing is exact-match on
`(allocation_id, runner_epoch, agent_incarnation)` plus `seq > max_seq`, checked
before any writes: stale/mismatched envelopes return `fenced_rejected` or
`dropped_stale` with zero mutation, including no quarantine. Missing evidence
returns `reconcile_required`; non-quarantined runners return
`reconcile_not_required`; both perform no writes.

On valid current evidence the controller classifies against allocation identity
and terminal disposition (`clearance/reconcile.py` mirrors the rule):

- `STILL_RUNNING` (current live execution, no terminal yet) is recognized without
  relaunch, invented outcome, termination, or release (`reconcile_still_running`).
- `FINISHED_NEEDS_CLEANUP` (terminal with remaining processes or dirty workspace)
  directs termination/scrub via the existing cleanup lifecycle
  (`reconcile_cleanup_required`, action `TERMINATE_CLEANUP`).
- `ALREADY_CLEAN` (terminal plus fully positive, consistent evidence under matching
  identity) releases attest-only (`reconcile_attested`, action `ATTEST`).
- `STALE_EXECUTION`, `ORPHANED_EXECUTION`, `CONTRADICTORY`, `INSUFFICIENT_EVIDENCE`
  (mismatched identity, internal contradiction, any present error including empty,
  or empty resources without a terminal) preserve quarantine
  (`reconcile_quarantined`, action `KEEP`) without destructive directives.

Each resolution durably records classification, supporting evidence, and intended
action (`reconcile_*` plus `max_seq`/`last_contact_at`) before authorizing cleanup
or release, and updates `quarantine_reason` when quarantine is preserved. Empty
resources, launch markers, heartbeats, or database rows alone never establish
completion. Uncertain identity never authorizes termination or release. Accepted
execution results remain immutable; lost reports and empty cgroups never imply
success or failure.

Heartbeat-loss quarantine exits only through this path. Ordinary heartbeats and terminal
reports retain their normal sequence/contact/result behavior but cannot release ownership
or clear quarantine. Unsolicited `CLEANUP` cannot bypass the reconciliation gate.
The agent reinspects physical state for every `RECONCILE` transmission, including a
retry after a lost response; cached cleanup alone never establishes current safety.
Reconciled `CLEANUP` releases only with a current
`TERMINATE_CLEANUP` binding (matching incarnation, higher sequence) plus positive
evidence. Negative cleanup, including reconciliation-directed cleanup, is a durable stop:
later `RECONCILE`, `RECOVERY`, or `CLEANUP` cannot replace its negative evidence or
quarantine reason, release ownership, or schedule a retry. Incarnation rotation and
controller restart do not clear this failure. Fencing and stale-sequence checks still
precede this rejection and perform zero mutation.
Attest-only release and reconciled cleanup reuse the existing release transaction
(including atomic `INTERRUPTED` retry without a visible `AVAILABLE` interlude).
A lost attest acknowledgment is idempotent while ownership is still current.
Incarnation rotation invalidates prior bindings; repeated reconciliation safely
supersedes via fresh sequences. Every accepted `RECONCILE`/`CLEANUP` refreshes
`last_contact_at`; fenced/stale reports leave contact untouched.

The agent preserves a pending START exchange and sends an unacknowledged terminal
result before reconciliation. Empty physical state while START can still authorize a
launch does not imply a terminal result or permit reuse. Classification does not
disable autonomous deadlines, cancellation, or terminal cleanup. A restarted agent
executes directed cleanup only through a freshly rediscovered allocation-owned handle;
it never replays the launch.

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
coordination system is introduced. The integration tests exercise correctness under contention;
quantitative overload, shutdown bounds, and per-runner generation advancement are deferred. Reconciliation
uses bounded passes (batch, per-allocation isolation, single-threaded scheduling) and reuses
the existing containment and cleanup/release lifecycle. The separate
[physical verification harness](../development/agent-verification.md#partition-and-return-reconciliation-drills)
exercises reconciliation against real Linux execution and records correlated host,
protocol, and PostgreSQL evidence.

## Recovery generation authority

Flyway V9 adds `recovery_authority(singleton, current_generation, updated_at)`,
`runners.reconciled_generation`, and `allocations.recovery_generation`, with indexes on both
generation columns. V9 comments claiming random UUIDv4 are superseded by
`V11__recovery_generation_evidence.sql` (no-op, preserves history) and
`RecoveryGenerationSource`: time-ordered UUIDv7 plus an external append-only log;
PostgreSQL remains the sole gating authority.

Booting with `clearance.recovery-mode=true` issues one fresh time-ordered UUIDv7 from the
external monotonic source (external wall-clock plus a durable append-only log on persistent
storage outside PostgreSQL, default `/var/lib/clearance/recovery-generations.log`, fenced
against repeats; optionally operator-supplied UUIDv7 via `clearance.recovery-generation`),
persists it as the
single current generation, and quarantines every runner with an inspectable reason naming that
generation (`recovery quarantine generation <uuid>: post-restore unsafe, physical
reconciliation required`), including idle runners with no active allocation. Monotonic
ordering plus the rewind-surviving log guarantees the new value is strictly newer than every
pre-rewind generation even when the
restore rewinds the authority table. Normal boots preserve the authority untouched.

While an authority exists, `SchedulerService.claim` additionally requires
`reconciled_generation` to equal the current generation, so restored `AVAILABLE` rows without
current reconciliation are refused with nothing written, and new allocations are tagged with the
current generation. `AgentService.report` rejects allocations whose `recovery_generation` differs
from current for normal progress, unsolicited cleanup, and released replays with `fenced_rejected`
and zero mutation (no state, allocation, contact, or generation change).

Runners quarantined under a new recovery generation advance `runners.reconciled_generation` to the
current recovery generation and return to `AVAILABLE` only via verified physical reconciliation
(issue #33):
- **Attest-only release**: for unreleased allocations with terminal disposition and fully positive,
  clean physical state (`ALREADY_CLEAN`, `ATTEST`), `allocations.state = 'RELEASED'`,
  `allocations.recovery_generation = currentGeneration`, `runners.state = 'AVAILABLE'`, and
  `runners.reconciled_generation` advances to the current generation atomically.
- **Directed cleanup**: if physical state requires cleanup (`FINISHED_NEEDS_CLEANUP`,
  `TERMINATE_CLEANUP`), `allocations.recovery_generation` is tagged with the current generation,
  and the runner remains `QUARANTINED`. Subsequent positive verified `CLEANUP` atomically releases
  the allocation, sets `runners.state = 'AVAILABLE'`, and advances `runners.reconciled_generation`.
  Failed cleanup remains a durable stop, preserving quarantine without advancing generation.
- **Still running and uncertain identity**: live work (`STILL_RUNNING`) or mismatched/uncertain
  observations (`STALE_EXECUTION`, `ORPHANED_EXECUTION`, `CONTRADICTORY`, `INSUFFICIENT_EVIDENCE`)
  preserve quarantine with inspectable reasons; `reconciled_generation` is not advanced.
- **Idle-at-backup runners**: quarantined idle runners submit `status: RECONCILE` with `allocation_id`
  omitted and `runner_epoch` matching `runners.epoch` (`0` when never claimed). Fully clean physical state advances `runners.reconciled_generation` to current authority
  and returns the runner to `AVAILABLE`. Leftover execution or resources keep the runner `QUARANTINED`
  without destructive directives or generation advancement. Idle `RECONCILE` enforces per-runner monotonic `seq` (`idle_reconcile_seq`, Flyway V10): duplicates/reorders return `dropped_stale` with zero mutation.
- **Wire evolution**: `recovery_generation` (UUID, snake_case) is optional on report requests.
  Unknown field `recoveryGeneration` (camelCase) remains ignored under v1 rules. Reports presenting a
  stale non-null `recovery_generation`, stale epoch, stale incarnation, or stale sequence are rejected
  with zero mutation.

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
quarantine reconciliation (fencing zero-mutation, classification, attest-only, reconciled
cleanup binding, flag-via-poll, and schema), credential separation, production fault absence,
and poll/report/claim contention. These tests use PostgreSQL through the existing
integration-test mechanism. Run with PostgreSQL available:

```sh
cd controller
./mvnw -B -ntp test
```

The separate database-free codec exchange remains `bash contracts/agent-v1/verify.sh`.
