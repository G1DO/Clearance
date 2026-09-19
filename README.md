# Clearance

Clearance is a recovery-first control plane for trusted Linux CI runners.

Its central safety rule is that a runner must never be reused while previous work may still legitimately be executing. Loss of contact, stale messages, controller failures, and ambiguous execution state are treated as unsafe conditions rather than evidence that a runner is free.

## Status

Clearance is under active development.

Implemented so far: deterministic runner-ownership semantics with mechanical checks, plus v1 project-scoped idempotent job intake (PostgreSQL-backed POST/GET /api/v1/jobs). Runner-claim contention, runner-agent, and Linux integration remain future work.

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
- `controller/` — Java 25 / Spring Boot 4.1.1 control plane with PostgreSQL-backed `POST/GET /api/v1/jobs` (project-scoped idempotency via `UNIQUE(project_id, operation_id)`, canonical payload hash, Flyway V1+V2);
- `controller/src/test/` — PostgreSQL-backed HTTP integration tests (canonicalization, concurrency, retry, restart, auth, migration invariants).

Verify with `python -m unittest discover -s tests -v` and, with PostgreSQL up (`docker compose up -d postgres`), `cd controller && ./mvnw test`.

See CONTRIBUTING.md for the development workflow and SECURITY.md for reporting and dev-key handling.
