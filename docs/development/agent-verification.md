# Agent verification and diagnostics

Use [CONTRIBUTING](../../CONTRIBUTING.md#toolchain-and-local-setup) for toolchain and
database setup, and [agent operation](../operations/agent.md) for runtime prerequisites.
Run commands from the repository root unless a command changes directory.

```sh
cd agent
go vet ./...
go test -race ./...
cd ..
bash contracts/agent-v1/verify.sh
# Run inside a delegated child cgroup; see setup below.
AGENT_CGROUP_TEST_ROOT=/sys/fs/cgroup/clearance-test bash agent/verify-containment.sh
CLEARANCE_CGROUP_ROOT=/sys/fs/cgroup/clearance-test bash agent/verify-integration.sh
```

The last command requires Java 25 and PostgreSQL (default localhost:5544,
database/user/password `clearance`, overridable with standard `PG*` variables).
Its test-only Java launcher uses Flyway in a private schema and the real job and
scheduler services. The idle recovery suite enters real recovery mode, which
quarantines the whole fleet, so the script runs it in a second private schema
isolated from the main controller suite. The destructive rewind drill also
enters recovery mode twice with a logical full-schema copy/restore (logic-only
precursor, not physical PITR), so the script runs it in a
third private schema isolated from both. Go drives success, cancellation, failure, and timeout with real
descendants; missing/replayed/dropped cleanup proof; quarantine faults; controller
restart; and a subsequent real allocation. The SIGKILL recovery drill below adds surviving-workload
discovery and same-job retries. Existing delivery, daemon restart,
incarnation, heartbeat, ordering, and authentication checks remain. Host process,
cgroup, protocol and database evidence is retained in the run directory. Evidence is saved under `controller/target/agent-integration/run.*/` and the
private schema is removed. Run this separately from the existing controller suite:
some existing schema checks count indexes across the entire database. Local evidence
directories can be removed after inspection. The
[CI workflow](../../.github/workflows/ci.yml) uploads `agent-controller-integration`
with 30-day retention; download that artifact from the relevant
[CI run](https://github.com/G1DO/Clearance/actions/workflows/ci.yml), rather than
copying logs into an issue. A workflow definition is not evidence that a particular
revision passed: retain the run link with the tested commit.

Quarantine reconciliation also has controller, codec, and agent-unit coverage:
PostgreSQL-backed `AgentApiIntegrationTest` exercises fencing zero-mutation,
classification (still-running, finished-needs-cleanup, already-clean, stale,
orphaned, contradictory, insufficient), attest-only release, reconciled cleanup
binding, retained negative cleanup, flag-via-poll without restart, and schema V8;
`AgentWireContractTest` plus
`bash contracts/agent-v1/verify.sh` cover `RECONCILE`/`reconcile_requested` codecs
via shared fixtures; `clearance/reconcile.py` (`tests/test_reconcile.py`) checks the
deterministic classifier; `agent/reconcile_test.go` checks fresh-observation
construction and rejects cached proof that conflicts with current physical state.

## Partition-and-return reconciliation drills

The integration target uses real Go containment, the Spring controller, PostgreSQL,
and Linux cgroups. Test transport gates partition delivery while the allocation's
real process tree continues running. Heartbeat timeout quarantines the runner;
the harness invokes a bounded pass of the real `ReconciliationService` to request
fresh observations. Its periodic scheduling is disabled in this harness to keep
unrelated unchanged-row assertions deterministic. No production route exposes
this test control.

The tests in [reconciliation_integration_test.go](../../agent/reconciliation_integration_test.go)
cover these physical cases:

- `TestControllerPhysicalPartitionAndReturn`: still-running and
  finished-while-disconnected returns, lost START acknowledgment with a pending
  launch, and a hidden RUNNING report after proven launch. It checks attest-only
  release after autonomous cleanup, one execution, and a subsequent safe claim.
- `TestControllerPhysicalReconciliationClassification`: stale, orphaned,
  contradictory, and insufficient observations against the controller. The
  harness measures real cgroups/PIDs and constructs the foreign-identity and
  contradictory report envelopes; uncertain execution remains alive and ownership
  stays held. The missing-journal case uses production discovery error evidence.
- `TestControllerPhysicalReconciliationCleanupAndFailure`: dirty workspace alone
  refuses attest; a TERM-ignoring descendant requires SIGKILL; termination,
  inspection, reaping, and scrub faults retain negative proof through further
  reports and controller restart. Unrelated processes/workspaces survive, and
  epoch/incarnation/sequence replays compare PostgreSQL rows including row versions.
  The successful termination case drops the committed cleanup acknowledgment and
  proves an exact retry leaves release unchanged. The reaping fault withholds the
  direct-child wait completion indication after its factual exit, exercising the
  production reaping deadline independently of cgroup emptiness.
- `TestControllerPhysicalReconciliationLostResolutionAndRestart`: drops a
  committed attest acknowledgment, SIGKILLs the real agent, restarts both
  controller and agent, and executes a subsequent allocation without retrying the
  completed job. Delayed old reconciliation evidence leaves newer ownership rows
  unchanged.
- `TestControllerPhysicalDelayedStartReconciliation`: a protocol driver holds a
  committed START response while submitting an observation of physically absent
  resources. `INSUFFICIENT_EVIDENCE` keeps ownership held until the delayed response
  permits one launch; current classification then directs cleanup after completion.

Evidence is retained under
`controller/target/agent-integration/run.*/reconcile-*/`, beside `go-test.log`,
`controller.log`, and the harness manifest. Per-case files include
`reconciliation-http.json`, `classification-*.json`, `report-*-*.json`, host process
snapshots, `before-partition-database.json`, `partition-timeout.json`, and
`negative-cleanup-after-restart.json`.
The executable restart case adds `lost-resolution-agent-state.json`,
`restarted-history.json`, and `delayed-resolution-unchanged.json`.
Cleanup-response and delayed-START cases add `cleanup-acknowledgment-replay.json`
and `delayed-start-released.json`.
Correlate allocation ID, epoch,
incarnation, sequence, and observed PIDs across host snapshots, protocol exchanges,
and database snapshots. Use the `agent-controller-integration` artifact from the
[CI run](https://github.com/G1DO/Clearance/actions/workflows/ci.yml) for shared
evidence; the same artifact includes `agent/target/linux-containment/`.

These drills require delegated cgroup v2, atomic cgroup launch, `cgroup.kill`,
readable `/proc`, and functioning child reaping. Skipped tests or a failed
prerequisite check do not prove physical reconciliation. The scope remains trusted
internal Linux workloads with intact durable local identity: lost/rolled-back
agent state, hostile-workload isolation, mixed-version rollout, and fleet
capacity are not established by this harness. Database PITR generation safety is
established only by the destructive rewind drill below, not by the partition
matrix. The production daemon does not
populate `observed_allocation_id` from auto-discovery: invalid physical identity
produces an error and `INSUFFICIENT_EVIDENCE`. The matrix proves the controller's
classification/action rules against measured physical scenarios, not automatic
daemon emission of every classification. All identity uncertainty fails closed.

## Reproducible agent SIGKILL recovery drill

Use the same Java 25, Go, PostgreSQL, delegated cgroup v2, `/proc`, and child-reaping
prerequisites as the integration target below. From the delegated shell run:

```sh
CLEARANCE_CGROUP_ROOT="$cg" bash agent/verify-integration.sh
```

`TestControllerSIGKILLRecoveryRetriesSameJob` runs the built real `clearance-agent` executable,
kills only that process with SIGKILL, proves the direct child, child/grandchild and
detached orphan remain alive in the allocation cgroup, then restarts against the
same durable directory and dirty workspace. Test proxies gate discovery/cleanup,
lose a committed discovery response, kill the recovering agent again, replay old
incarnation evidence, and lose the cleanup acknowledgment. The drill checks blocked
claims while unresolved, the conservative termination decision, current proof,
exactly two attempt records for one job, distinct allocations and increasing epoch,
and exactly one retry execution. A separate case replaces the workspace after
discovery to prove cleanup failure quarantines without deleting unrelated contents.
Host membership, reports, state snapshots, and PostgreSQL attempt/allocation history
are retained in the integration run directory. The test harness reaps only known
allocation descendants adopted by its own subreaper; the production agent still
requires independent `/proc` emptiness before positive proof.

## Destructive rewind drill (logical precursor, not physical PITR)

`TestControllerPitrRewindDrill` (isolated `pitr` suite in
`bash agent/verify-integration.sh`) proves safe recovery with a logical
full-schema copy, controller stop with negative claim proof before each rewind,
new-OS/JVM-process recovery boot with retained boot logs, and real Linux
cgroup/workspace runners. The controller-side logic is additionally pinned by
`PitrRewindDrillIntegrationTest` (`cd controller && ./mvnw -B -ntp
-Dtest=PitrRewindDrillIntegrationTest test`), which uses a logical full-schema
backup schema, single-transaction logical restore, controller stop before each rewind
with a claim-must-fail negative check, `RecoveryBootRunner` boots in a new OS/JVM child
process (boot logs retained), service assertions against a normal-mode replacement
context, and artifacts under `controller/target/pitr-drill/`.

Both drills are logic-only precursors, not physical PITR (Issue #40 AC5): the backup
is `CREATE TABLE AS TABLE WITH DATA` for every table (no `pg_basebackup`/`pg_dump`
base backup, no WAL replay, no timeline-history check), recorded
`pg_current_wal_lsn()` values and timeline IDs are informational markers only and never
select a restore point, and DDL, sequences, non-table state, and crash-consistent timeline
branching are unexercised. Physical base-backup + WAL replay with timeline validation
remains unproven: issue #40 stays open, do not close it on this change. The physical drill takes a logical full-schema copy at T0 (backup schema via
`CREATE TABLE AS TABLE WITH DATA` for every table in the harness schema, with
informational `pg_current_wal_lsn()` + timeline markers), executes jobs on real Linux runners at T1
with live `cgroup.procs` plus `/proc` plus workspace evidence, rewinds the
logic-copy database to T0 (single-transaction logical restore via `DELETE FROM` plus `INSERT SELECT`
with `session_replication_role='replica'`, host execution untouched), stops serving, and boots
a new OS/JVM child process with `clearance.recovery-mode=true`
exercising `RecoveryBootRunner` (`POST /recovery-boot-process`: the harness spawns the child
against the restored schema, asserts freshness and quarantine from its retained boot log,
destroys it, then recreates serving in normal mode with the stashed credentials — the control
plane stays up throughout). The Java
drill stops its serving controller before each rewind (negative-checked: a claim attempt
through the stopped instance fails, so no unreconciled runner receives work while the
authority is empty) and boots a new OS/JVM child process with `clearance.recovery-mode=true`
exercising `RecoveryBootRunner` against the restored database; freshness and quarantine are
asserted from its retained boot logs, and service-level assertions run against a normal-mode
replacement context over the same database. Both drills stop serving before each rewind
with a negative-checked claim refusal, so nobody serves claims while the authority is empty;
a live controller would grant legacy-availability claims on rewound rows, so stopping it is
what closes the window. Physical base-backup plus WAL replay with timeline validation remains
#40 work: issue #40 stays open, do not close it on this change. After rewind the fleet is quarantined
regardless of restored rows claiming idle, scheduling stays disabled for
unreconciled runners, stale-generation evidence makes zero writes, and reuse
without positive cleanup does not occur. Runners with surviving T1 execution
reconnect, reconcile physical reality (`STILL_RUNNING` stays quarantined,
`FINISHED_NEEDS_CLEANUP` directs `TERMINATE_CLEANUP`), terminate with bounded
graceful shutdown then `cgroup.kill` SIGKILL with reaping and workspace scrub,
submit verified cleanup, advance to the new generation, and only then become
available for claims. Freshness shows the post-rewind generation is strictly newer than the pre-rewind value
(time-ordered UUIDv7 from the external monotonic source plus a rewind-surviving durable log,
with fenced boot plus authority persistence as durable evidence); repeating the T0 rewind
issues a still-new generation.

Artifacts are retained under `controller/target/agent-integration/run.*/pitr-drill/`
(backup and rewind markers with informational LSNs + timeline IDs, pre/post-restore snapshots, recovery-boot
records, generations, claim-refusal proof, stale zero-mutation proof, desired-versus-observed
evidence, forced-termination timings with reaped PIDs, cleanup attestations,
repeated-rewind generations) beside `go-test.log`, `controller.log`, and the
harness manifest, plus per-fixture evidence dirs. The same `agent-controller-integration`
CI artifact carries them with 30-day retention. Trusted test workloads only with
isolated schemas, state directories, and credentials; the drill never contacts
production or mutates real fleets.

## Hygiene soak and profiles

The defined hygiene soak uses five daemon event loops with a test-only execution
backend and an instrumented HTTP fixture for five minutes, with 10-second poll hints and one-second heartbeat/re-poll
intervals (about 1500 polls and 1500 reports):

```sh
cd agent
AGENT_SOAK=1 go test -race -run '^TestDaemonHygieneSoak$' -count=1 -timeout=7m -v ./...
```

It fails if warmed goroutine growth exceeds 3, peak connections exceed 10, the
defined load is missed, or shutdown leaves connections or goroutine growth beyond
budget. Heap and goroutine profiles before/after/shutdown and `summary.json` are
written to fixed filenames under `agent/target/hygiene-soak/`. Profiles are not
collected continuously, no per-allocation metric labels/history are retained, and
each transport permits at most two connections. The runtime has one poll worker,
one serial report sender, one allocation worker plus command waiters, and single-item channels.

A shorter `AGENT_SOAK_DURATION=5s` run is a smoke check only. For a bounded execution
trace, add `-trace=target/hygiene-soak/trace.out` to that short run (create the directory
first). Inspect profiles with `go tool pprof` and traces with `go tool trace`.
CI uploads the fixed profile set with 30-day retention. These checks establish
resource hygiene at the defined load, **not throughput or fleet capacity**.

## Isolated delegated Linux verification target

Physical verification is required for lifecycle claims; the ordinary unit suite's
explicit cgroup-test skips do not establish it. An administrator can prepare a fresh
root (example uses sudo; choose a dedicated unused path):

```sh
cg=/sys/fs/cgroup/clearance-test
sudo mkdir "$cg" "$cg/agent"
sudo chown "$(id -u):$(id -g)" "$cg" "$cg/cgroup.procs" \
  "$cg/cgroup.threads" "$cg/cgroup.subtree_control"
# In a disposable shell, move only that shell into the delegated child:
sudo sh -c 'echo "$1" > "$2/agent/cgroup.procs"' -- "$$" "$cg"
export AGENT_CGROUP_TEST_ROOT="$cg" CLEARANCE_CGROUP_ROOT="$cg"
bash agent/verify-containment.sh
bash agent/verify-integration.sh
# Exit this shell; an administrator can then rmdir "$cg/agent" and "$cg".
```

`verify-containment.sh` requires a cgroup target and fails if physical tests cannot
run. Its retained evidence is under `agent/target/linux-containment/`. Test fault
hooks are private Go test functions; the launcher/control server and transport proxies
exist only in the integration harness, never in the production agent binary. CI
prepares its own delegated root and retains both physical and integration artifacts.
