# Exclusive runner claim (v1)

## Purpose

Define the implemented production slice that lets queued work exclusively claim a compatible
schedulable runner with PostgreSQL as the sole authority for ownership (issue #5). This
specification describes implemented technical truth for the Java 25 / Spring Boot 4.1.1
controller and PostgreSQL claim path. It realizes the database-authoritative assignment portion
of `docs/design/specifications/runner-ownership-semantics.md` (only `AVAILABLE` is schedulable,
assignment creates a new ownership context, epochs increase monotonically). Committed delivery
agent reporting, and physical safe-reuse proof are described in [agent-api.md](agent-api.md).

`SchedulerService.claim` currently has no production caller, scheduling loop, or HTTP
claim endpoint. Tests and the agent integration harness invoke the service directly;
submitting a job and starting an agent alone do not create a claim.

## Scope

Implemented:

- Flyway V3 durable inventory: `runners`, `attempts`, `allocations` with distinct identities;
- exact-`runnerClass` compatibility as the complete v1 rule;
- single-transaction exclusive claim (`SchedulerService.claim`) with inspectable SQL at
  `READ_COMMITTED` isolation;
- PostgreSQL-enforced one-active-allocation-per-runner invariant independent of scheduler code;
- rollback atomicity (no partial allocation, orphan attempt, consumed epoch, or false state);
- finite resource bounds (no new pools/threads; existing HikariCP/Tomcat bounds reused);
- PostgreSQL-backed contention, compatibility, schedulability, invariant, rollback, lock, and
  plan verification plus recorded evidence below.

Out of scope (not claimed here):

- Runner registration protocol. Agent polling, allocation re-delivery, incarnation rotation,
  and stale/duplicate/reordered report handling are implemented separately in
  [agent-api.md](agent-api.md).
- Heartbeat timeout interpretation, desired-versus-observed reconciliation, quarantine loops.
- Linux cgroups, process-tree cleanup, workspace scrubbing, and cleanup attestation
  are implemented separately in the [agent runtime](../../../agent/README.md).
- PostgreSQL PITR, recovery-generation recovery.
- Generalized labels, priorities, resource bin-packing, affinity/anti-affinity, autoscaling,
  operator UI, mixed-version rollout, fleet simulation, capacity characterization beyond the
  stated finite bounds.

## Durable identities

- `jobs.job_id` (V2): logical job identity, primary key.
- `attempts.attempt_id`: execution-attempt identity, one row per claim, primary key,
  `job_id` FK to `jobs`.
- `allocations.allocation_id`: ownership identity binding one attempt to one runner, primary
  key; `attempt_id` is `UNIQUE` (one allocation per attempt); `job_id` and `runner_id` FKs;
  `runner_epoch` records the runner ownership generation established by the claim.
- `runners.epoch`: runner ownership generation, advanced by exactly one per committed claim.
  `allocation_id`, `attempt_id`, `job_id`, and `runner_epoch` are distinct columns and must not
  be treated as interchangeable.

## Compatibility rule (v1)

A runner is compatible with a job only when `runners.runner_class` exactly equals
`jobs.runner_class`. The equality is enforced inside the authoritative `UPDATE` predicate, so
selection and ownership transfer are atomic. Runner inventory rows carry a non-empty
`runner_class` (`chk_runners_runner_class`); queued jobs carry the `runnerClass` established by
issue #4.

## Transaction boundary

`SchedulerService.claim(jobId, runnerId)` runs in one database transaction, explicitly
`READ_COMMITTED`. Critical-path SQL is explicit Spring JDBC (`JdbcTemplate`):

```sql
-- 1. lock ownership first, then job state (matching report lock order)
SELECT runner_id FROM runners WHERE runner_id = ? FOR UPDATE;
SELECT runner_class, result, cancel_requested FROM jobs WHERE job_id = ? FOR UPDATE;
SELECT EXISTS (SELECT 1 FROM allocations WHERE job_id = ? AND state = 'ACTIVE');
-- Refuse missing runner, terminal/cancelled job, or existing active job ownership.
-- 2. atomic compare-and-swap on the locked runner row
UPDATE runners
   SET state = 'ASSIGNED', epoch = epoch + 1, updated_at = now()
 WHERE runner_id = ? AND state = 'AVAILABLE' AND runner_class = ?
RETURNING runner_id, epoch;
-- 3. durable execution-attempt identity for this claim (only if step 2 matched)
INSERT INTO attempts (attempt_id, job_id) VALUES (?, ?);
-- 4. authoritative ownership binding (only if step 2 matched)
INSERT INTO allocations
  (allocation_id, attempt_id, job_id, runner_id, runner_epoch, workload_timeout_ms)
VALUES (?, ?, ?, ?, ?, ?)
RETURNING allocation_id, attempt_id, job_id, runner_id, runner_epoch, created_at;
```

Only a runner authoritatively in `AVAILABLE` state can be claimed. Absence of an
active-allocation row is never treated as proof of schedulability: the `state = 'AVAILABLE'`
predicate is mandatory, so an `ASSIGNED`/`QUARANTINED` runner with no allocation row is still
rejected (verified by test). Unknown `jobId` throws `JobNotFoundException`; a missing,
incompatible, or non-`AVAILABLE` runner yields an empty result with nothing written.

The returned `Claim` is constructed inside the transaction but handed to the caller only when
the transaction commits (Spring commits on method return), so a claimed allocation is never
exposed as authoritative to downstream delivery code before PostgreSQL has committed it.
`AgentService.poll` reads this committed allocation; it never calls the scheduler or creates
an allocation, attempt, or epoch. Terminal execution preserves the `ACTIVE` allocation
while moving the runner to `CLEANING`. Only a current positive cleanup proof releases
ownership and makes the runner available to this claim predicate.

## Concurrency strategy and observed behavior

Technique: conditional row update (compare-and-swap) plus a partial-unique backstop. No
process-local mutexes and no Redis/Kafka/etcd are used.

Observed PostgreSQL 17 behavior (READ COMMITTED), proved by
`SchedulerClaimIntegrationTest`:

- The winner's initial `SELECT ... FOR UPDATE` takes the runner row lock. A concurrent
  claimant for the same runner blocks until commit or rollback, then evaluates the
  `WHERE` predicate against the newest committed row version. After a committed win the runner
  is `ASSIGNED`, so the loser matches no row and observes no allocation (empty result, nothing
  written). After a rollback the runner is `AVAILABLE` again and a waiter may proceed.
- Orchestrated contention evidence: with one connection holding an uncommitted claim-shaped
  `UPDATE` on a runner row, `pg_locks` shows a granted lock on the runners relation
  (`relation:RowExclusiveLock:true`), and a second connection attempting the same row update
  with `SET LOCAL lock_timeout = '1s'` aborts with SQLState `55P03` (`lock_not_available`),
  proving the contender blocks on the holder's uncommitted row update rather than proceeding.
  Rolling back the holder leaves the runner `AVAILABLE` with its epoch unconsumed and no
  allocation behind.
- Query-plan shape depends on table size and statistics and is informational, not a
  locking or performance guarantee. The test logs the current `EXPLAIN` output and checks
  that `runners_pkey` and `ix_runners_class_state` exist; it does not require a specific
  scan type. Row-lock contention and the two-instance single-winner test establish the
  tested ownership behavior.

The reproducible evidence source is
[`SchedulerClaimIntegrationTest`](../../../controller/src/test/java/com/clearance/controller/SchedulerClaimIntegrationTest.java),
including `uncommittedClaimBlocksContenderAndRollsBackCleanly` and
`claimUpdatePlanIsLoggedAndSupportingIndexesExist`. Inspect current output in the
[CI `java` job](https://github.com/G1DO/Clearance/actions/workflows/ci.yml) or local Maven/Surefire output rather
than treating a copied plan as a current result.

## Invariant enforcement

`uq_allocations_runner_active`: partial `UNIQUE(runner_id) WHERE state = 'ACTIVE'` on
`allocations`. It is checked by PostgreSQL on every insert independently of scheduler code, so
a programming mistake in scheduler selection surfaces as a unique violation that rolls back the
whole claim (runner update and attempt row included) instead of a silent second authoritative
allocation. Verified by inserting a second `ACTIVE` allocation outside the scheduler path
(rejected, error names `uq_allocations_runner_active`, survivor intact) and by a claim whose
allocation insert hits the invariant (full rollback: runner still `AVAILABLE`, epoch
unconsumed, no orphan attempt, still exactly one allocation).

## Finite bounds

- HikariCP `maximum-pool-size: 10` and Tomcat `threads.max: 50` from `application.yml` are
  reused unchanged; the claim path creates no threads, pools, or unbounded structures.
- Verification contention uses a fixed 2-thread pool with 10s start-gate and 30s join timeouts;
  lock probing uses a 1s `lock_timeout`. Contention may reduce throughput or increase latency
  but cannot weaken the single-allocation invariant.

## Seeding (dev/test)

v1 has no runner-registration endpoint; inventory is seeded directly in PostgreSQL.
Minimal row (see `controller/src/main/resources/db/migration/V3__scheduling.sql` for
constraints and `SchedulerClaimIntegrationTest.insertRunner` for the canonical example):

```sql
INSERT INTO runners (runner_id, runner_class, state, epoch)
VALUES ('<uuid>', 'default', 'AVAILABLE', 0);
```

## Known limitations

- Claims serialize on the job row and refuse a job with existing active ownership.
  The independent partial unique database index protects the per-runner invariant;
  the per-job check is enforced by the scheduler transaction.
- `allocations.runner_epoch` equality with the post-claim `runners.epoch` is established by the
  single-transaction claim path (verified per claim), not by a cross-table database constraint;
  the schema enforces `runner_epoch > 0`.
- Runner inventory is seeded directly in PostgreSQL for allocation and tests; the agent
  registration protocol belongs to the later agent Outcome and was deliberately not built here.
- Full open-loop overload/capacity characterization is deferred; correctness is proved for the
  required contention scenario under finite bounds.

## Evidence

- `controller/src/test/.../SchedulerClaimIntegrationTest`: PostgreSQL-backed verification for
  successful claim (distinct identities, epoch +1, authoritative row), incompatible-class
  rejection, non-`AVAILABLE` rejection without allocation rows, unknown job/runner handling,
  F09 two-context contention (separate application contexts and Hikari pools, start gate,
  exactly one winner, final single-allocation state, runner no longer schedulable), invariant
  rejection outside the scheduler path, invariant-triggered full rollback, `pg_locks` /
  `lock_timeout` blocking evidence, indexed claim-plan check, and V3 migration/schema
  invariants.

Reproduce: start PostgreSQL (`docker compose up -d postgres` exposing `5544`, or any
PostgreSQL 17 reachable at `localhost:5544` as `clearance`/`clearance`), then
`cd controller && ./mvnw test`.

## Terminal execution and reuse

The claim path locks the runner, then the job. It refuses terminal or cancelled jobs,
jobs with an active allocation, and runners outside `AVAILABLE`. A committed claim
snapshots the configured workload timeout into the allocation. Cancellation locks the
job only, so it serializes with claim without inverting ownership lock order.

Terminal reports durably complete the job and attempt, while the runner enters
`CLEANING` and the allocation remains active. Positive current cleanup proof releases
the allocation and changes the runner to `AVAILABLE` in one transaction under the
same runner lock. A racing claim can observe either the held runner or the complete
release, never an available runner with unresolved cleanup. The next claim creates
new attempt/allocation identities and increments the runner epoch. Cleanup failure
persists `QUARANTINED`, its reason, and active ownership; terminal results and
heartbeats cannot clear it. See [agent API](agent-api.md) for proof fencing and retries.
