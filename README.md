# Clearance

Clearance is a recovery-first control plane for trusted Linux CI runners.

Its central safety rule is that a runner must never be reused while previous work may still legitimately be executing. Loss of contact, stale messages, controller failures, and ambiguous execution state are treated as unsafe conditions rather than evidence that a runner is free.

## Status

Clearance is under active development.

Implemented so far: deterministic runner-ownership semantics with mechanical checks, v1 project-scoped idempotent job intake (PostgreSQL-backed POST/GET /api/v1/jobs), exclusive PostgreSQL-authoritative runner claim with contention evidence, v1 Java↔Go agent wire codecs with shared compatibility fixtures, machine-authenticated agent polling/reporting with durable incarnation and sequence fencing, and a standalone Go daemon with durable restart state and bounded runtime verification. Linux containment and cleanup remain future work.

## Core safety model

Clearance is designed around several principles:

- runner ownership is explicit and fenced;
- stale actors cannot mutate current ownership;
- heartbeat loss does not imply safe reuse;
- uncertainty leads to quarantine;
- terminal state cannot regress because of delayed or reordered reports;
- runner release requires positive cleanup evidence;
- persistence, physical cleanup, reconciliation, and disaster-recovery guarantees require their own later evidence.

## Repository

Technical documentation lives under `docs/`.

Current specifications:

- `docs/design/specifications/runner-ownership-semantics.md` — deterministic ownership, fencing, quarantine, and release semantics.
- `docs/design/specifications/job-intake.md` — implemented v1 project-scoped idempotent job intake (API contract, identities, canonicalization/hash, transaction boundary, schema invariants).
- `docs/design/specifications/runner-claim.md` — implemented exclusive runner claim under PostgreSQL authority (identities, compatibility, transaction boundary, contention strategy with observed lock/plan evidence, invariant enforcement, bounds, limitations).
- `docs/design/specifications/agent-api.md` — committed allocation delivery, machine authentication, incarnation rotation, durable report fencing, and test-only transport faults.
- `contracts/agent-v1/README.md` — versioned Java↔Go agent wire contract v1 (poll, report/heartbeat, error shapes; fencing, seq, terminal vocabulary, timestamps, optional/null, unknown-field policy; `recoveryGeneration` reserved) with shared fixtures in `contracts/agent-v1/fixtures/`.

Additional documentation is added when the implemented system creates durable architecture, development, API, operational, security, or reference information worth preserving.

## Project structure

The delivered system is intended to include:

- Java 25 / Spring Boot 4.1.1 control plane;
- PostgreSQL as durable allocation authority;
- thin Go runner agents on Linux;
- React/TypeScript operator interface;
- Python verification and simulation tooling.

Not all of these components exist yet. Repository documentation describes implemented technical truth rather than planned architecture unless explicitly identified as a specification.

Currently implemented:

- `clearance/model.py` — deterministic reference model (`apply`/`fold`, ownership context, epoch/incarnation fencing, quarantine, cleanup proof);
- `clearance/explore.py` — bounded-exhaustive mechanical checks with stated strategy, bounds, and assumptions;
- `tests/` — lifecycle, fencing, quarantine/release, non-regression, duplicate/reorder, determinism, and mechanical interleaving checks;
- `controller/` — Java 25 / Spring Boot 4.1.1 control plane with PostgreSQL-backed `POST/GET /api/v1/jobs` (project-scoped idempotency via `UNIQUE(project_id, operation_id)`, canonical payload hash, Flyway V1+V2) and exclusive runner claim (`SchedulerService.claim`: conditional `AVAILABLE` + exact-`runnerClass` row update, distinct `attempt_id`/`allocation_id`, monotonic epoch, partial-unique one-active-allocation invariant, Flyway V3), plus committed long-poll delivery and fenced report ingestion under `/internal/v1/agents/*` (Flyway V4);
- `controller/src/test/` — PostgreSQL-backed HTTP integration tests (canonicalization, concurrency, retry, restart, auth, migration invariants) plus exclusive-claim tests (two-instance contention, compatibility, schedulability, invariant, rollback, lock/plan evidence) plus agent delivery, fencing, auth, profile, and contention tests and the DB-free `AgentWireContractTest` matrix over `contracts/agent-v1/fixtures/`.
- `contracts/agent-v1/` — versioned wire shapes plus shared fixtures (normal, optional-absent, explicit-null, unknown-fields, error, timestamp round-trip, `recoveryGeneration present-but-ignored`).
- `agent/` — v1 codec plus standalone Go daemon (`cmd/clearance-agent`) with durable incarnation/sequence state, execution replay prevention, heartbeats, and bounded shutdown. See [agent/README.md](agent/README.md) for operation, real-controller integration tests, and the five-agent hygiene soak.

Verify with `python -m unittest discover -s tests -v` and, with PostgreSQL up (`docker compose up -d postgres`), `cd controller && ./mvnw test`. The full DB-free Java↔Go exchange runs with `bash contracts/agent-v1/verify.sh` and preserves logs and exchanged JSON in `controller/target/agent-wire-contract/` (uploaded by CI). Standalone codec checks run as `cd controller && ./mvnw -B -ntp -Dtest=com.clearance.controller.AgentWireContractTest test` and `cd agent && go test ./... -v` (both in CI).

See CONTRIBUTING.md for the development workflow and SECURITY.md for reporting and dev-key handling.
