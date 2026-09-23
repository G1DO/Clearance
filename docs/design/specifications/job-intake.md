# Job intake (v1)

## Purpose

Define the implemented production slice that durably accepts project-scoped idempotent jobs
(issue #4). This specification describes implemented technical truth for the Java 25 /
Spring Boot 4.1.1 controller and PostgreSQL persistence path. It does not establish
runner allocation, agent, scheduling, or cleanup behavior.

## Scope

Implemented:

- project-scoped job submission and query over HTTP;
- durable operation/job identities in PostgreSQL;
- canonicalization and payload-hash rule for v1 semantic requests;
- single-transaction idempotent submit boundary with inspectable SQL;
- Flyway-versioned schema and startup against an empty database;
- finite resource bounds for connections and request execution.

Out of scope (not claimed here):

- Runner inventory, scheduling, attempts, allocations, runner-claim contention.
- Environment variables, secrets, artifacts, workflow/DAG, priority, resource requests,
  generalized labels/affinity, autoscaling.
- General heartbeat failure detection and background reconciliation loops.
- Agent transport and physical cleanup are specified separately in [agent-api.md](agent-api.md).
- PITR/recovery-generation behavior, operator UI, rollout, fleet simulation, capacity
  characterization beyond the stated finite bounds.

## API contract

Base: `/api/v1/jobs`. JSON only.

Authentication (v1 implementation choice): static API-key to project mapping
(`clearance.auth.api-keys` in `controller/src/main/resources/application.yml`).
Callers send `Authorization: Bearer <key>` or `X-API-Key: <key>`.
The filter resolves the authenticated `project_id` and exposes it as request attribute
`projectId`. Missing or unknown keys yield `401 {"error":"unauthorized"}`.
`project_id` never comes from the request body.

### POST /api/v1/jobs

Headers:

- `Idempotency-Key` (required): v1 `operation_id`, scoped by the authenticated
  `project_id`. Trimmed, non-empty, at most 128 characters. Missing or blank yields 400.
- `Authorization` / `X-API-Key` (required): see above.

Body (v1 semantic fields only):

```json
{"argv": ["echo", "hi"], "runnerClass": "default"}
```

- `argv`: process argument vector, 1..128 elements, each 1..4096 characters.
  Not shell-parsed; callers needing shell semantics request an explicit shell
  executable in the vector. Element order and scalar values are significant.
- `runnerClass`: only v1 scheduling compatibility input, `[A-Za-z0-9._-]{1,128}`.
- Unknown JSON properties are ignored and do not affect identity.

Responses:

- `201` on first creation with the durable job view.
- `200` with the current durable view of the same job on same-operation/same-payload
  retry. The job identity and submitted payload are unchanged; `result` and
  `cancelRequested` may reflect progress since the original submission.
- `409 {"error":"idempotency_conflict","existingJobId":...,"existingPayloadHash":...}`
  when the same `(project_id, operation_id)` is reused with a different canonical
  payload. No second job is created; the original row is unchanged.
- `400` on validation faults (empty `argv`, missing `runnerClass`, missing/blank key,
  malformed JSON).
- `401` on missing/unknown API key.

### GET /api/v1/jobs/{jobId}

Requires the same authentication. Returns the durable job view when the job exists and
its `project_id` matches the caller. Cross-project and unknown ids both yield
`404 {"error":"not_found"}` (no existence oracle). Malformed UUID yields 400.
Missing auth yields 401.

Job view:

```json
{
  "jobId": "uuid",
  "projectId": "project-alpha",
  "operationId": "op-123",
  "argv": ["echo", "hi"],
  "runnerClass": "default",
  "payloadHash": "sha256 hex",
  "createdAt": "2026-09-19T02:10:07.062145Z",
  "result": null,
  "cancelRequested": false
}
```

### POST /api/v1/jobs/{jobId}/cancel

Uses the same project authentication and existence rules as GET; no request body is
required. Returns `200` with the current durable job view. Repeated cancellation is
idempotent. A queued job with no active allocation becomes `CANCELLED` immediately;
an active allocation receives the durable `cancelRequested` flag through agent polling.
Claim and cancellation serialize on the job row, so a cancellation cannot disappear
between checking for an allocation and committing a claim. A terminal job cannot be claimed.

`result` is null before terminal execution, then one of `SUCCEEDED`, `FAILED`,
`CANCELLED`, or `TIMED_OUT`. The first accepted terminal report wins; requesting
cancellation after completion preserves that result. The associated attempt has its
own durable result. Cleanup failure never rewrites either execution result.
Already observable completion takes precedence over a racing stop. Otherwise an expired
workload deadline takes precedence over cancellation; cancellation observed before that
deadline produces `CANCELLED`. Repeated or conflicting reports cannot regress the
accepted terminal result.

Job result is independent of runner availability: terminal execution enters `CLEANING`;
only current positive cleanup evidence permits reuse. See [agent API](agent-api.md).

## Durable identities

- `operation_id`: client mutation identity = HTTP `Idempotency-Key`, scoped by
  `project_id`. The same key may be used independently by different projects.
- `job_id`: logical job identity, random UUID v4 per logical job, primary key.
- The two are distinct columns and must not be collapsed. A later operator view can
  join operation identity with the stored payload hash to explain conflicts.

## Canonicalization / payload-hash rule

Canonicalization operates on the validated typed request, not raw HTTP bytes:

- canonical form (UTF-8): `{"argv":[...],"runnerClass":"..."}`;
- keys in lexicographic order, no insignificant whitespace, RFC 8259 string escaping
  via Jackson (`Canonicalization.canonicalize`);
- JSON object-key order and serialization whitespace in the inbound payload do not
  change identity; `argv` element order, `argv` scalar values, and `runnerClass` do;
- `payload_hash = hex(SHA-256(canonical_utf8))`, 64 lowercase hex chars, stored
  durably with the operation/job row for identical-vs-conflicting inspection.

## Transaction boundary

`JobService.submit` runs in one database transaction (READ COMMITTED). Critical-path SQL
is explicit Spring JDBC (`JdbcTemplate`), not opaque ORM. The canonical statements are
`INSERT_SQL` and `SELECT_BY_OPERATION_SQL` in
[`JobService`](../../../controller/src/main/java/com/clearance/controller/jobs/JobService.java).
The insert uses `ON CONFLICT (project_id, operation_id) DO NOTHING`; a conflict reads
the existing row and compares its payload hash. Both paths return the stored job view,
including its current result and cancellation state.

PostgreSQL's `UNIQUE(project_id, operation_id)` is the sole convergence mechanism.
Concurrent identical inserts serialize on the speculative unique key; losers observe the
winner via SELECT and return the same `job_id`. Conflicting reuse compares hashes and
returns 409 without mutating the winner. A bounded single retry covers the
winner-rolled-back race. No process-local locks, no unbounded in-memory dedup, no
Redis/Kafka/etcd.

## Schema invariants (Flyway V1+V2+V5)

- `V1__init.sql`: bootstrap baseline (`bootstrap_check`).
- `V2__jobs.sql`: `jobs(job_id UUID PK, project_id TEXT, operation_id TEXT,
  argv JSONB, runner_class TEXT, payload_hash TEXT, created_at TIMESTAMPTZ)`.
- `UNIQUE(project_id, operation_id)`; non-empty and length checks on project/operation/
  runnerClass; `payload_hash ~ '^[0-9a-f]{64}$'`; `argv` is a non-empty JSON array.
- Index `ix_jobs_project(project_id)`.
- [V5](../../../controller/src/main/resources/db/migration/V5__workload_lifecycle.sql)
  adds the job's nullable terminal `result` and durable `cancel_requested` flag.
- Controller starts against PostgreSQL from an empty database via Flyway; restart
  revalidates without drift (`Schema "public" is up to date`).

## Finite bounds

- HikariCP: `maximum-pool-size: 10`, `minimum-idle: 2`, `connection-timeout: 5000ms`,
  plus idle/max-lifetime/keepalive (see `application.yml`).
- Tomcat: `threads.max: 50`, `threads.min-spare: 5`.
- Request payload: `argv` 1..128 x 1..4096 chars, `runnerClass` pattern/length,
  `operation_id` 1..128 chars. Validation rejects oversize payloads with 400.
- Correctness does not depend on unbounded structures; `clearance.auth.api-keys` is
  bounded static configuration, not per-request growth.

## Evidence

- `controller/src/test/.../CanonicalizationTest`: key-order/whitespace invariance,
  argv-order/value and runnerClass sensitivity.
- `controller/src/test/.../JobsApiIntegrationTest`: PostgreSQL-backed HTTP tests for
  create/get, identical retry, canonical-over-HTTP, 409 invariance, 16-way identical
  concurrency (one `job_id`, one row), 8-way conflicting concurrency (original
  unchanged), lost-response retry, cross-project/unauth isolation, validation,
  migration/schema invariants.
- `controller/src/test/.../RestartIntegrationTest`: commits in context 1, closes it,
  recreates context 2 against the same PostgreSQL state, proves retry still
  deduplicates and GET still works.

Reproduce: start PostgreSQL (`docker compose up -d postgres` exposing `5544`),
then `cd controller && ./mvnw test`.
