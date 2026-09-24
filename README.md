# Clearance

Clearance is a recovery-first control plane for trusted Linux CI runners.

Its central safety rule is that a runner must never be reused while previous work may still legitimately be executing. Loss of contact, stale messages, controller failures, and ambiguous execution state are unsafe conditions, not evidence that a runner is free.

## Current implementation

Clearance is under active development:

| Component | Implemented responsibility |
| --- | --- |
| [Python reference model](clearance/model.py) and [mechanical checker](clearance/explore.py) | Deterministic ownership, fencing, quarantine, and abstract cleanup/release semantics, checked by [tests](tests/). This is verification tooling, not a running service. |
| [Controller](controller/) | Java/Spring Boot job intake, project-scoped idempotency, exclusive runner claims, committed allocation delivery, durable execution results, cancellation, and fenced cleanup/release. PostgreSQL is the durable ownership authority; [Flyway migrations](controller/src/main/resources/db/migration/) define the schema. |
| [Agent](agent/README.md) | Standalone Linux Go daemon with durable incarnation/sequence state, execution replay prevention, cgroup v2 execution, workload deadlines, descendant cleanup, workspace scrubbing, and physical discovery after agent SIGKILL. |
| [Agent wire contract](contracts/agent-v1/README.md) | Versioned HTTP/JSON behavior with shared Java↔Go compatibility fixtures. |

Job submission does **not** start execution automatically: tests and the integration harness create initial claims through `SchedulerService.claim`; there is no general scheduling loop or claim endpoint. Terminal execution retains ownership until current positive cleanup proof permits reuse. Agent-crash recovery can retry interrupted work after verified cleanup. See [system architecture and implemented limits](docs/architecture/README.md) for the runtime flow, model boundaries, and unsupported recovery guarantees.

## Getting started

Start with [local setup and verification](CONTRIBUTING.md). For a standalone agent, follow [agent operation and restart safety](docs/operations/agent.md). Read [security and credential handling](docs/security/README.md) before configuring access.

## Technical documentation

- [System architecture](docs/architecture/README.md) — component boundaries, data flow, durable state, and implemented limits.
- [Runner ownership model](docs/design/specifications/runner-ownership-semantics.md) — abstract fencing, quarantine, release, and verification bounds.
- [Job intake](docs/api/job-intake.md) — API behavior, identities, canonicalization, transactions, and schema invariants.
- [Exclusive runner claim](docs/architecture/runner-claim.md) — compatibility, ownership transactions, contention, and database invariants.
- [Agent API](docs/api/agent-api.md) — machine authentication, committed delivery, incarnation rotation, report fencing, and test faults.
- [Agent wire contract](contracts/agent-v1/README.md) — wire shapes, compatibility fixtures, and the Java↔Go exchange.
- [Agent operation](docs/operations/agent.md) — startup, durable state, restart safety, and failure triage.
- [Agent verification](docs/development/agent-verification.md) — Linux containment, controller integration, SIGKILL recovery, and bounded diagnostics.
- [Security configuration](docs/security/README.md) — credentials, transport, and trusted-workload boundaries; [report a vulnerability](SECURITY.md).
