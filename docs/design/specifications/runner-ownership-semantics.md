# Runner ownership semantics

## Purpose

Define the deterministic domain semantics for runner ownership, fencing, quarantine, and safe release.

This specification describes the reference model independently of persistence and runtime behavior. Implemented mechanisms have their own contracts and evidence: [PostgreSQL runner claims](runner-claim.md), [agent delivery and reporting](agent-api.md), and the [Go daemon](../../../agent/README.md). Physical cleanup, runtime reconciliation, and recovery-generation mechanics remain later work.

## Scope

This specification covers:

- runner lifecycle state;
- allocation ownership;
- monotonic runner epochs;
- agent incarnations;
- ambiguous execution state;
- quarantine;
- duplicate and reordered observations;
- terminal-state monotonicity;
- abstract cleanup/release evidence;
- deterministic event processing;
- safety properties checked by the reference model.

## Non-goals

This specification does not establish:

- database durability or transaction semantics;
- concurrent PostgreSQL allocation correctness;
- HTTP or JSON contracts;
- real agent failure detection;
- wall-clock timeout correctness;
- cgroup or process-tree cleanup correctness;
- workspace cleanup correctness;
- desired-versus-observed reconciliation;
- recovery-generation or PITR correctness;
- resource or performance bounds.

## Terminology

### Runner

A reusable machine capable of executing one authoritative allocation at a time.

### Allocation

The ownership relationship assigning work to a runner.

### Runner epoch

A monotonically increasing ownership generation associated with runner allocations.

An event from an older epoch cannot mutate current ownership.

### Agent incarnation

An identity representing one lifetime of the runner agent.

A superseded incarnation cannot mutate current ownership.

### Cleanup proof

Abstract positive evidence that the current ownership context has completed the cleanup required before release.

The reference model does not define how physical cleanup is performed.

### Quarantine

A non-reusable state representing uncertainty or insufficient proof of safety.

Quarantine must not be interpreted as availability.

## Runner lifecycle

The valid normal lifecycle is:

```text
AVAILABLE
    ↓
ASSIGNED
    ↓
STARTING
    ↓
RUNNING
    ↓
CLEANING
    ↓
AVAILABLE
```

`QUARANTINED` represents an unsafe or unresolved condition and may be entered when current evidence is insufficient to establish safe reuse.

Implemented in `clearance/model.py` (`RunnerState`, `apply`/`fold`).

## Ownership context

Authoritative ownership must distinguish at least:

- allocation identity
- runner epoch
- agent incarnation

Events referring to superseded ownership context must not advance or release current ownership.

Implemented as `OwnershipContext` bound to `Runner.owner`; non-assign events require an exact match.

## State-transition rules

### Assignment

An available runner may enter `ASSIGNED` under a new ownership context.

A new allocation advances the runner epoch monotonically.

### Start

A valid current ownership context may advance:

- `ASSIGNED → STARTING`
- `STARTING → RUNNING`

A stale ownership context may not cause either transition.

### Heartbeat loss

Loss of heartbeat is evidence of uncertainty, not evidence that execution stopped.

It must not cause:

- `* → AVAILABLE`

It must result in or preserve a non-reusable state such as `QUARANTINED`.

### Timeout

A timeout does not prove remote execution has stopped.

A timeout must therefore not release the runner.

### Stale epoch

An event carrying an epoch older than the authoritative runner epoch must not mutate authoritative ownership or release the runner.

### Stale incarnation

An event from a superseded agent incarnation must not mutate authoritative ownership or release the runner.

### Duplicate report

Reprocessing an already-observed report must be harmless.

It must not create a new state transition that would not otherwise be valid.

### Reordered report

An older observation arriving after a newer observation must not regress authoritative state.

In particular, terminal authoritative state must not regress.

### Cleanup proof

A runner may transition from `CLEANING` to `AVAILABLE` only when valid cleanup/release evidence applies to the current ownership context.

Missing, stale, or mismatched cleanup evidence must not release the runner.

`QUARANTINED → AVAILABLE` directly via current proof is the modeled abstract release path; it prescribes no `CLEANING` intermediate or physical mechanism.

## Safety invariants

The reference model must maintain the following properties.

### Unsafe ambiguity never produces availability

If execution state is unresolved, the runner cannot be `AVAILABLE`.

### Stale ownership cannot mutate current ownership

Events associated with superseded epochs or incarnations cannot advance, regress, or release current authoritative ownership.

### Terminal state is monotonic

Once an authoritative terminal state has been reached, delayed, duplicated, or reordered observations cannot regress it.

### Release requires positive proof

Absence of evidence is insufficient for safe reuse.

Transition to `AVAILABLE` requires explicit current cleanup/release evidence.

### Quarantine is not reusable

`QUARANTINED` cannot be treated as equivalent to `AVAILABLE`.

## Determinism

Given the same:

- initial state; and
- ordered event stream,

the reference model must produce the same:

- authoritative state;
- ownership context; and
- transition decisions.

The model must not depend on wall-clock timing, operating-system state, database state, network behavior, or nondeterministic scheduling.

## Mechanical checking

The mechanical checker must exercise event streams containing the defined event alphabet:

- assign;
- start;
- heartbeat loss;
- timeout;
- stale epoch;
- stale incarnation;
- duplicate report;
- reordered report;
- cleanup proof.

Checks must include interleavings capable of exposing unsafe interactions between these events.

A checked execution is unsafe if it can produce a reusable runner despite unresolved execution, stale authority, regressed terminal state, or absent/stale cleanup proof.

Implemented in `clearance/explore.py` (`build_alphabet`, `explore_traces`, `check_trace`/`check_step`) and exercised in `tests/test_exploration.py` across bounded-exhaustive interleavings plus targeted valid and quarantine/release paths.

## Evidence

Verification output must make the checked claim reproducible.

Evidence should identify:

- exploration strategy;
- deterministic seed, when applicable;
- explored bounds;
- assumptions;
- counterexample trace when an invariant fails;
- commands or CI checks required to reproduce the result.

Machine-generated verification output should remain machine-generated evidence rather than being manually copied into this document.

Reproduce with `python3 -m unittest discover -s tests -v`. Strategy, bounds, and assumptions are stated in `clearance/explore.py` and surfaced in the exploration report.

## Limits of the model

Passing the reference-model checks demonstrates the safety properties only within the modeled semantics and stated exploration bounds.

It does not by itself demonstrate:

- PostgreSQL concurrency safety;
- persistence across crashes;
- real Go-agent fencing;
- Linux cleanup;
- timeout/failure-detector correctness;
- reconciliation correctness;
- PITR safety;
- bounded runtime resource use.

Those claims require evidence from the corresponding implemented subsystems.
