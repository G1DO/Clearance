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
scheduler services. Go drives success, cancellation, failure, and timeout with real
descendants; missing/replayed/dropped cleanup proof; quarantine faults; controller
restart; and a subsequent real allocation. The SIGKILL recovery drill below adds surviving-workload
discovery and same-job retries. Existing delivery, daemon restart,
incarnation, heartbeat, ordering, and authentication checks remain. Host process,
cgroup, protocol and database evidence is retained in the run directory. Evidence is saved under `controller/target/agent-integration/run.*/` and the
private schema is removed. Run this separately from the existing controller suite:
some existing schema checks count indexes across the entire database. Local evidence
directories can be removed after inspection; CI retains artifacts for 30 days.

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
