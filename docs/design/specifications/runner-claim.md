# Exclusive runner claim (v1)

## Purpose

Define the implemented production slice that lets queued work exclusively claim a compatible
schedulable runner with PostgreSQL as the sole authority for ownership (issue #5). This
specification describes implemented technical truth for the Java 25 / Spring Boot 4.1.1
controller and PostgreSQL claim path. It realizes the database-authoritative assignment portion
of `docs/design/specifications/runner-ownership-semantics.md` (only `AVAILABLE` is schedulable,
assignment creates a new ownership context, epochs increase monotonically). Committed delivery
and agent reporting are described in [agent-api.md](agent-api.md); physical safe-reuse proof
remains later work.

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
- Linux cgroups, process-tree cleanup, workspace scrubbing, cleanup attestation.
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
-- 1. load the immutable job row for its runnerClass
SELECT runner_class FROM jobs WHERE job_id = ?;
-- 2. atomic compare-and-swap on the runner row
UPDATE runners
   SET state = 'ASSIGNED', epoch = epoch + 1, updated_at = now()
 WHERE runner_id = ? AND state = 'AVAILABLE' AND runner_class = ?
RETURNING runner_id, epoch;
-- 3. durable execution-attempt identity for this claim (only if step 2 matched)
INSERT INTO attempts (attempt_id, job_id) VALUES (?, ?);
-- 4. authoritative ownership binding (only if step 2 matched)
INSERT INTO allocations (allocation_id, attempt_id, job_id, runner_id, runner_epoch)
VALUES (?, ?, ?, ?, ?)
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
an allocation, attempt, or epoch. Reports preserve the `ACTIVE` allocation and runner lifecycle
state, including after terminal results, so they cannot bypass the claim predicate.

## Concurrency strategy and observed behavior

Technique: conditional row update (compare-and-swap) plus a partial-unique backstop. No
process-local mutexes and no Redis/Kafka/etcd are used.

Observed PostgreSQL 17 behavior (READ COMMITTED), proved by
`SchedulerClaimIntegrationTest`:

- The winner's step-2 `UPDATE` takes the runner row lock. A concurrent claimant for the same
  runner blocks on that row until the winner commits or rolls back, then re-evaluates the
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
- Query plan for the claim UPDATE (observed via `EXPLAIN`, logged by test output, informational
  only): the shape depends on table size and statistics. On tiny verification tables PostgreSQL
  correctly chooses a `Seq Scan` (cost ~2); on larger tables it chooses an `Index Scan` (either
  `runners_pkey` with `Index Cond: runner_id = '<uuid>'` or `ix_runners_class_state`). Example
  small-table plan:

```text
Update on runners  (cost=0.00..2.21 rows=1 width=54)
  ->  Seq Scan on runners  (cost=0.00..2.21 rows=1 width=54)
        Filter: ((state = 'AVAILABLE'::text) AND (runner_class = 'default'::text)
          AND (runner_id = '<uuid>'::uuid))
```

  Row-level locking is proven by `pg_locks` contention (`RowExclusiveLock`, `55P03`) and the F09
  two-instance single-winner test, not by the plan shape. The test asserts the supporting indexes
  (`runners_pkey`, `ix_runners_class_state`) exist rather than asserting a specific plan.

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

- No per-job single-active invariant in v1: two concurrent claims for the *same job* on
  *different* runners would each commit. Only the per-runner property is invariant-protected,
  per the issue.
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
- Existing idempotent-job (`JobsApiIntegrationTest`, `RestartIntegrationTest`,
  `CanonicalizationTest`) and deterministic ownership-model (`python -m unittest discover -s
  tests -v`) verification remains passing.

Reproduce: start PostgreSQL (`docker compose up -d postgres` exposing `5544`, or any
PostgreSQL 17 reachable at `localhost:5544` as `clearance`/`clearance`), then
`cd controller && ./mvnw test`.
