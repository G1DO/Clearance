# Standalone runner agent

`cmd/clearance-agent` is the Linux Go daemon for the existing
[v1 wire contract](../contracts/agent-v1/README.md) and
[fenced controller API](../docs/design/specifications/agent-api.md). It uses only
the Go standard library. Go 1.23+ is required.

Build and run after seeding runner inventory and configuring its separate machine
key in the controller:

```sh
cd agent
go build -o target/clearance-agent ./cmd/clearance-agent
# Set CLEARANCE_MACHINE_TOKEN through your environment/secret configuration.
./target/clearance-agent -controller http://localhost:8080 \
  -state-dir /durable/runner-state \
  -cgroup-root /sys/fs/cgroup/clearance \
  -workspace-root /var/lib/clearance-workspaces
```

Every call presents the machine token. Redirects are rejected. The daemon polls
with `timeout_s: 10`, sends heartbeats every second, and paces repeated assignments
and transport retries at one second. Poll HTTP deadlines allow five additional
seconds for transport; report requests have a five-second deadline. A slow request
coalesces heartbeat ticks rather than queuing reports. Valid `poll_after_ms` hints
increase the poll delay, capped at 30 seconds without integer overflow.

Only a fully decoded successful poll can authorize execution. The daemon directly
executes the supplied `argv`, without adding a shell, and reports `STARTING`,
`RUNNING`, then `SUCCEEDED`, `FAILED`, `CANCELLED`, or `TIMED_OUT`. A command-start or containment failure reports `FAILED`.
Standard input/output/error are disconnected; workload log capture is not provided.
Repeated delivery of the same allocation never starts a second command. Reports
and heartbeats use the allocation's exact epoch and this boot's incarnation.

## Linux containment and cleanup

Requires Linux cgroup v2, `clone3(CLONE_INTO_CGROUP)` (Linux 5.7+), `cgroup.kill`
(Linux 5.14+), readable `/proc` including process cgroup membership and task children,
and permission to enable child subreaping. The agent uses Go's atomic cgroup launch;
there is no start-then-migrate fallback. Seccomp must permit these calls.

An administrator must delegate a dedicated cgroup ancestor to the agent user,
including its `cgroup.procs`, `cgroup.threads`, and `cgroup.subtree_control`. The
agent process must itself run in a child of that ancestor: moving a process from
outside the delegation can fail even if the target directory is writable. Use a
service manager with cgroup delegation, or the isolated test setup below. The
agent never moves unrelated processes or changes global cgroup permissions.

Each allocation gets a new `<allocation_id>-<runner_epoch>` cgroup and workspace.
Workloads start with the workspace as their current directory and inherit the
allocation cgroup across fork, double-fork, `setsid`, and parent exit. Existing
allocation directories are not adopted for launch. The workspace root must be
separate from durable state and cgroups (including aliases); cleanup checks directory
identity, refuses mounted workspace subtrees, and unlinks symlinks without following
them. This is containment for trusted workloads, not a hostile filesystem, mount,
credential, network, or syscall sandbox. Workloads must not migrate out of cgroups
or alter the agent's namespaces, roots, or mounts.

Every observed terminal path cleans the entire allocation hierarchy, including
remaining descendants after a successful direct-child exit. Cleanup sends SIGTERM,
waits up to one second, then uses `cgroup.kill` (SIGKILL) with a two-second empty-state
wait. Reaping has a further two-second bound. The subreaper waits only for adopted
PIDs identified as belonging to this allocation; it does not consume other commands'
exit statuses. Empty cgroup state alone is insufficient because zombies are absent
from `cgroup.procs`: `/proc` membership must also be empty after reaping. Only then
are the empty hierarchy and workspace removed and their removal verified. Filesystem
inspection/scrubbing cost depends on the trusted workload's file/process population;
this does not establish a filesystem I/O or workload-size bound.

The agent submits `CLEANUP` with the allocation ID, runner epoch, current incarnation,
next durable sequence, and structured `execution_empty`, `descendants_reaped`, and
`workspace_clean` evidence. Failure retains negative evidence and an inspectable error;
the controller quarantines the runner without rewriting the execution result. Positive
proof is sent only after all cleanup succeeds. The controller transaction makes release
and availability visible together. Losing the acknowledgment retries proof safely;
a newer committed assignment can establish that accepted release overtook the reply.

Project-authorized callers request cancellation with `POST /api/v1/jobs/{jobId}/cancel`.
Polling delivers the durable request. Cancellation observed before launch prevents the
command from starting; after launch it stops the whole allocation. The controller
snapshots `clearance.workload-timeout-ms` on claim (default one hour, positive milliseconds, capped at
24 hours). The agent starts a monotonic timer at successful launch; the timer stops
execution independently of heartbeat/report delivery. Already observable completion
wins a racing stop. Otherwise an expired deadline takes precedence over cancellation;
cancellation observed before the deadline produces `CANCELLED`. Accepted results never regress. Network report delay may postpone
observation by the controller, but not the local execution deadline.

## Durable state and failure behavior

Dedicate one durable local directory to one controller/runner identity and preserve
it across process restarts. The controller's incarnation is an increasing signed
64-bit integer, so the daemon reserves a new number on each boot; it is not a random
UUID. `state.lock` excludes simultaneous processes using that directory. `state.json`
contains the incarnation and at most one allocation's assignment, sequence, launch
intent, terminal acknowledgment, and cleanup evidence/acknowledgment. `containment.json`
records allocation identity and canonical path/device/inode identities for the cgroup,
workspace, and their roots before launch. Writes use fixed `state.tmp` and
`containment.tmp`, atomic rename, and file/directory fsync. These five filenames
have bounded cardinality; snapshots
and HTTP response bodies are limited to 4 MiB, enough for the maximum legal v1 argv.

The allocation sequence starts at 1. Each report attempt, including retries and
heartbeats, durably reserves the next sequence before sending. A crash between
reservation and transmission can leave a gap; numbers are never reused. Restart
continues the same allocation's sequence and first re-polls with the new incarnation.
The sender does not use timestamps for fencing or reset sequence on incarnation
rotation. Integer exhaustion, corrupt/missing initialized state, persistence errors,
invalid wire replies, and rejected fencing/acknowledgments stop the daemon. HTTP
408/429/5xx and transport failures are retried with bounded concurrency. Unknown
error codes remain non-successful; unknown wire fields are ignored by the v1 codecs.

Launch intent is persisted before starting a command. On restart, a started allocation
without acknowledged cleanup is physically discovered before resolution: the agent
matches its durable allocation identity and directory journal against cgroup v2,
`/proc` (including zombies and detached/orphaned descendants), and workspace state.
Discovery does not authorize another launch. A fresh incarnation reports `RECOVERY`
with the next durable allocation sequence and receives the controller's resolution.
A remembered unacknowledged execution result is resent first; it never bypasses
physical discovery or cleanup.

No idempotent-resume proof is supported. The controller directs termination; an
attempt with no previously accepted result becomes `INTERRUPTED` with a durable
recovery reason, while the logical job stays unfinished. The agent terminates and
cleans only the discovered allocation through the ordinary cleanup lifecycle.
Accepted current cleanup proof atomically releases the old allocation and commits
one new attempt/allocation of the same job on this runner with a higher epoch.
Cancellation suppresses that retry. Previously accepted terminal results remain
unchanged and require cleanup without a retry.

Missing, corrupt, or contradictory physical identity is reported as discovery error
and quarantines the runner; the agent cannot kill or scrub through that handle.
The agent records removal authorization only after verifying empty cgroups and no
remaining `/proc` members, before removing the cgroup/workspace. A restart during
removal can therefore verify partial or complete absence against this checkpoint;
absence without the checkpoint is unsafe. Allocations launched by an older agent
without a containment journal also require quarantine and operator triage. Recovered orphans belong to the host's
init/subreaper, which must reap them within the cleanup deadline. Failure to do so,
terminate, or scrub yields negative cleanup and durable quarantine.

Lost recovery responses repeat discovery reports without relaunch. Another restart
repeats physical discovery and controller resolution under a new incarnation before
submitting proof. Lost cleanup acknowledgments cannot create a second retry: a newer
committed allocation establishes accepted release, and a restarted agent rechecks
old physical cleanup before accepting it. A changed allocation still requires
positive cleanup, an acknowledged disposition, a new allocation ID, and a strictly
higher epoch. If that handoff reinspection fails after the controller has committed
a newer allocation, the agent records a never-launch marker for the current ownership
and sends negative discovery to quarantine it, retaining the old containment journal
for triage. Once cleanup is acknowledged, the agent only polls for the next allocation.

Do not delete, roll back, clone to another machine, or reuse the state directory for
another controller/runner. State recovery after disk loss/rollback is outside this
slice. Interrupted first initialization also fails closed when a lock marker exists
without a snapshot; it cannot establish whether an incarnation was already used.

SIGINT/SIGTERM cancels HTTP requests and timers, performs bounded physical cleanup
of locally owned execution, joins workers, closes idle connections, and releases the
state lock. Shutdown does not invent a job cancellation/result for an uncertain
interrupted allocation. A failed termination may leave its OS process and waiter
until process exit; the runner remains held/quarantined and no further workload starts.
Server fencing remains authoritative if an agent crashes. Heartbeat-loss evaluation, general reconciliation, recovery after local-state loss/rollback,
and recovery generations remain deferred.

## Verification and bounded diagnostics

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

### Reproducible agent SIGKILL recovery drill

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

### Isolated delegated Linux verification target

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
