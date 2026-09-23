# Clearance

Clearance is a recovery-first control plane for trusted Linux CI runners.

Its central safety rule is that a runner must never be reused while previous work may still legitimately be executing. Loss of contact, stale messages, controller failures, and ambiguous execution state are unsafe conditions, not evidence that a runner is free.

Product context, planned outcomes, and project decisions live in the [Clearance project in Notion](https://app.notion.com/p/3dd0a821b3cc81169910d92e5a9cf56e). [GitHub Issues](https://github.com/G1DO/Clearance/issues) and [pull requests](https://github.com/G1DO/Clearance/pulls) track engineering execution. This repository documents the implemented system.

## Current implementation

Clearance is under active development:

| Component | Implemented responsibility |
| --- | --- |
| [Python reference model](clearance/model.py) and [mechanical checker](clearance/explore.py) | Deterministic ownership, fencing, quarantine, and abstract cleanup/release semantics, checked by [tests](tests/). This is verification tooling, not a running service. |
| [Controller](controller/) | Java/Spring Boot job intake, project-scoped idempotency, exclusive runner claims, committed allocation delivery, durable execution results, cancellation, and fenced cleanup/release. PostgreSQL is the durable ownership authority; [Flyway migrations](controller/src/main/resources/db/migration/) define the schema. |
| [Agent](agent/README.md) | Standalone Linux Go daemon with durable incarnation/sequence state, execution replay prevention, cgroup v2 execution, workload deadlines, descendant cleanup, workspace scrubbing, and physical discovery after agent SIGKILL. |
| [Agent wire contract](contracts/agent-v1/README.md) | Versioned HTTP/JSON behavior with shared Java↔Go compatibility fixtures. |

Job submission does **not** start execution automatically: tests and the integration harness create initial claims through `SchedulerService.claim`; there is no general scheduling loop or claim endpoint. Polling only delivers an existing committed claim. Terminal reports move the runner to `CLEANING` and do **not** release it. A current, positive cleanup attestation atomically releases ownership and makes the runner `AVAILABLE`; failed cleanup leaves durable `QUARANTINED` state. Workload deadlines are independent of heartbeat loss. After agent SIGKILL, physical discovery and conservative termination reuse this cleanup path before a new attempt of the same job is committed. Heartbeat-loss evaluation, general reconciliation, and disaster recovery remain unimplemented runtime guarantees.

## Getting started

Start with [local setup and verification](CONTRIBUTING.md). For a standalone agent, follow [agent operation and restart safety](agent/README.md). Read [security and credential handling](SECURITY.md) before configuring access.

## Technical documentation

- [Runner ownership semantics](docs/design/specifications/runner-ownership-semantics.md) — deterministic model, fencing, quarantine, release, and verification bounds.
- [Job intake](docs/design/specifications/job-intake.md) — API behavior, identities, canonicalization, transactions, and schema invariants.
- [Exclusive runner claim](docs/design/specifications/runner-claim.md) — compatibility, ownership transactions, contention, and database invariants.
- [Agent API](docs/design/specifications/agent-api.md) — machine authentication, committed delivery, incarnation rotation, report fencing, and test faults.
- [Agent wire contract](contracts/agent-v1/README.md) — wire shapes, compatibility fixtures, and the Java↔Go exchange.
- [Agent operation](agent/README.md) — startup, durable state, failure behavior, integration verification, and runtime diagnostics.

Documentation placement follows the canonical [Documentation guidance](https://app.notion.com/p/Documentation-3bb0a821b3cc815099acfd2d5e8b0859); engineering flow follows [Workflow](https://app.notion.com/p/Workflow-3bb0a821b3cc817394cdf93a936a3612).
