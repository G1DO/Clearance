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
isolated from the main controller suite. Go drives success, cancellation, failure, and timeout with real
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
state, database PITR, hostile-workload isolation, mixed-version rollout, and fleet
capacity are not established by this harness. The production daemon does not
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
