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
./target/clearance-agent -controller http://localhost:8080 -state-dir /durable/runner-state
```

Every call presents the machine token. Redirects are rejected. The daemon polls
with `timeout_s: 10`, sends heartbeats every second, and paces repeated assignments
and transport retries at one second. Poll HTTP deadlines allow five additional
seconds for transport; report requests have a five-second deadline. A slow request
coalesces heartbeat ticks rather than queuing reports. Valid `poll_after_ms` hints
increase the poll delay, capped at 30 seconds without integer overflow.

Only a fully decoded successful poll can authorize execution. The daemon directly
executes the supplied `argv`, without adding a shell, and reports `STARTING`,
`RUNNING`, then `SUCCEEDED` or `FAILED`. A command-start failure reports `FAILED`.
Standard input/output/error are disconnected; workload log capture is not provided.
Repeated delivery of the same allocation never starts a second command. Reports
and heartbeats use the allocation's exact epoch and this boot's incarnation.

## Durable state and failure behavior

Dedicate one durable local directory to one controller/runner identity and preserve
it across process restarts. The controller's incarnation is an increasing signed
64-bit integer, so the daemon reserves a new number on each boot; it is not a random
UUID. `state.lock` excludes simultaneous processes using that directory. `state.json`
contains the incarnation and at most one allocation's assignment, sequence, launch
intent, and terminal acknowledgment. Writes use a fixed `state.tmp`, atomic rename,
and file/directory fsync. All three filenames have bounded cardinality; snapshots
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

Launch intent is persisted before starting a command. A restart after that point
with no durable terminal result resumes **only agent heartbeats**, without replaying
or rediscovering the workload and without claiming it finished. This can leave a
command unexecuted if the crash occurred immediately before launch; uncertainty
must not cause duplicate execution. A durable unacknowledged terminal result is
resent under the new incarnation with the next sequence. Terminal reports and later
heartbeats never release the runner. A changed allocation is accepted only after a
known terminal result, with a new allocation ID and strictly greater epoch; other
assignment changes fail closed.

Do not delete, roll back, clone to another machine, or reuse the state directory for
another controller/runner. State recovery after disk loss/rollback is outside this
slice. Interrupted first initialization also fails closed when a lock marker exists
without a snapshot; it cannot establish whether an incarnation was already used.

SIGINT/SIGTERM cancels HTTP requests and timers, kills/waits for the direct child,
joins the poll/process workers, closes idle connections, and releases the state lock.
Normal cancellation exits successfully. Server fencing remains authoritative if a
process crashes without this shutdown. Cgroups, process-tree reaping, workspace
scrubbing, surviving-workload discovery after SIGKILL, cleanup/release, quarantine
interpretation, and recovery generations remain deferred. This daemon is for trusted
internal workloads and supplies no hostile-workload containment guarantee.

## Verification and bounded diagnostics

```sh
cd agent
go vet ./...
go test -race ./...
cd ..
bash contracts/agent-v1/verify.sh
bash agent/verify-integration.sh
```

The last command requires Java 25 and PostgreSQL (default localhost:5544,
database/user/password `clearance`, overridable with standard `PG*` variables).
Its test-only Java launcher uses Flyway in a private schema and the real job and
scheduler services. Go drives dropped replies, daemon restart, stale-incarnation
rejection, heartbeat liveness, reordered delivery, and invalid/cross-runner identity
checks. Evidence is saved under `controller/target/agent-integration/run.*/` and the
private schema is removed. Run this separately from the existing controller suite:
some existing schema checks count indexes across the entire database. Local evidence
directories can be removed after inspection; CI retains artifacts for 30 days.

The defined hygiene soak uses five real daemon runtimes and an instrumented HTTP
fixture for five minutes, with 10-second poll hints and one-second heartbeat/re-poll
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
one serial report sender, one optional command waiter, and single-item channels.

A shorter `AGENT_SOAK_DURATION=5s` run is a smoke check only. For a bounded execution
trace, add `-trace=target/hygiene-soak/trace.out` to that short run (create the directory
first). Inspect profiles with `go tool pprof` and traces with `go tool trace`.
CI uploads the fixed profile set with 30-day retention. These checks establish
resource hygiene at the defined load, **not throughput or fleet capacity**.
