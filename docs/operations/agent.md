# Agent operation and recovery

`cmd/clearance-agent` is the Linux Go daemon for the existing
[v1 wire contract](../../contracts/agent-v1/README.md) and
[fenced controller API](../api/agent-api.md). It uses only
the Go standard library. Go 1.23+ is required.

Build and run after seeding runner inventory and configuring its separate machine
key in the controller. Follow [credential and workload security](../security/README.md)
before running it. From the repository root:

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
service manager with cgroup delegation, or the [isolated verification target](../development/agent-verification.md#isolated-delegated-linux-verification-target). The
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
proof is sent only after all cleanup succeeds. After completed execution, the controller
transaction makes release and availability visible together. Interrupted attempts instead
follow the [atomic recovery retry path](#durable-state-and-failure-behavior).
Losing the acknowledgment retries proof safely;
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
it across process restarts. The agent's incarnation is an increasing signed
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
another controller/runner. State recovery after disk loss/rollback is not implemented.
Interrupted first initialization also fails closed when a lock marker exists
without a snapshot; it cannot establish whether an incarnation was already used.

SIGINT/SIGTERM cancels HTTP requests and timers, performs bounded physical cleanup
of locally owned execution, joins workers, closes idle connections, and releases the
state lock. Shutdown does not invent a job cancellation/result for an uncertain
interrupted allocation. A failed termination may leave its OS process and waiter
until process exit; the runner remains held/quarantined and no further workload starts.
Server fencing remains authoritative if an agent crashes. Heartbeat-loss evaluation, general reconciliation, recovery after local-state loss/rollback,
and recovery generations remain deferred.

## Triage a stopped or quarantined runner

1. Keep the runner out of new work. Record the daemon's stderr and controller
   errors with the runner/allocation identity. Workload stdout/stderr are not
   captured by this agent, so a missing workload log does not prove it stopped.
2. Inspect the runner's `state`, `epoch`, `agent_incarnation`, and
   `quarantine_reason`, then its allocation's `state`, `report_status`, `max_seq`,
   `cleanup_evidence`, `recovery_evidence`, and `retry_allocation_id` using read-only
   database access. The [migrations](../../controller/src/main/resources/db/migration/)
   define these fields. An accepted terminal result is separate from cleanup;
   `INTERRUPTED` describes the old attempt, while a retry has a different allocation.
3. Preserve `state.json`, `containment.json`, the state directory, and the allocation
   cgroup/workspace paths for inspection. Protect diagnostic copies as described in
   [security configuration](../security/README.md). Compare the journal's path and
   directory identities with the actual host state; do not delete evidence to make
   discovery succeed. Backups of these files are diagnostic evidence, not snapshots
   that can safely be restored to reset an agent.
4. For a transport outage, restore the configured controller/database connection;
   retryable requests are paced by the daemon. If the process has stopped and its
   state is intact, restart only the same runner identity with the same durable
   directory and delegated roots. The recovery protocol above discovers and cleans
   uncertain execution before reuse; a restart cannot clear quarantine.
5. Missing/corrupt state, changed physical identity, or durable quarantine requires
   maintainer investigation. There is no implemented unquarantine API or local-state
   loss/rollback recovery procedure. Do not reset epochs/incarnations, mark the
   runner `AVAILABLE` directly, or bypass cleanup evidence. Retain the unavailable
   runner until a verified recovery procedure is implemented for the failure.

For a controlled reproduction, use the [SIGKILL recovery drill and diagnostics](../development/agent-verification.md).
