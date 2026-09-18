"""Mechanical (bounded-exhaustive) checks for the reference model.

Exploration strategy (stated for evidence):
  Bounded exhaustive enumeration of all ordered event sequences up to
  ``max_depth`` over a fixed alphabet, folded deterministically from
  ``initial_runner()`` via :func:`clearance.model.apply`. Safety invariants
  are checked after every step. Enumeration order is deterministic
  lexicographic order over the sorted alphabet; no randomness, no
  wall-clock, no I/O.

Bounds (needed to interpret the result):
  - single runner, single active ownership context (A1/e1/i1) plus stale /
    mismatched variants and one next-epoch allocation (A2/e2)
  - default ``max_depth=4`` exhaustive (all lengths 0..4); longer valid and
    quarantine/release paths are checked as targeted traces in tests
  - abstract cleanup proof only (no crypto, no Linux attestation)

Assumptions:
  - input streams are totally ordered lists of :class:`Event`
  - initial state is AVAILABLE with epoch 0 and no owner
  - QUARANTINED -> AVAILABLE directly via current proof is the modeled
    release path (abstract evidence; no prescribed CLEANING intermediate)

No claim is made beyond this checked scope: persistence, crash-recovery,
recovery generations, Linux cleanup, timing, concurrency/locking,
multi-runner interleavings, unbounded depths, capacity, or performance.
"""

from __future__ import annotations

from dataclasses import dataclass

from .model import Event, EventKind, Runner, RunnerState, apply, initial_runner

# Fixed symbolic identities for the bounded scope.
ALLOC_CURRENT = "A1"
ALLOC_OTHER = "A2"
EPOCH_CURRENT = 1
EPOCH_STALE = 0
EPOCH_NEXT = 2
INCARNATION_CURRENT = 1
INCARNATION_STALE = 0


def build_alphabet() -> list[Event]:
    """Fixed event alphabet covering the Issue's required categories."""
    alphabet = [
        Event(EventKind.ASSIGN, ALLOC_CURRENT, EPOCH_CURRENT, INCARNATION_CURRENT),
        Event(EventKind.NOTIFY_STARTING, ALLOC_CURRENT, EPOCH_CURRENT, INCARNATION_CURRENT),
        Event(EventKind.NOTIFY_RUNNING, ALLOC_CURRENT, EPOCH_CURRENT, INCARNATION_CURRENT),
        Event(EventKind.BEGIN_CLEANING, ALLOC_CURRENT, EPOCH_CURRENT, INCARNATION_CURRENT),
        Event(EventKind.HEARTBEAT_LOST, ALLOC_CURRENT, EPOCH_CURRENT, INCARNATION_CURRENT),
        Event(EventKind.TIMED_OUT, ALLOC_CURRENT, EPOCH_CURRENT, INCARNATION_CURRENT),
        Event(EventKind.CLEANUP_PROOF, ALLOC_CURRENT, EPOCH_CURRENT, INCARNATION_CURRENT),
        # Stale-epoch representatives.
        Event(EventKind.NOTIFY_STARTING, ALLOC_CURRENT, EPOCH_STALE, INCARNATION_CURRENT),
        Event(EventKind.CLEANUP_PROOF, ALLOC_CURRENT, EPOCH_STALE, INCARNATION_CURRENT),
        Event(EventKind.HEARTBEAT_LOST, ALLOC_CURRENT, EPOCH_STALE, INCARNATION_CURRENT),
        # Stale-incarnation representatives.
        Event(EventKind.NOTIFY_STARTING, ALLOC_CURRENT, EPOCH_CURRENT, INCARNATION_STALE),
        Event(EventKind.CLEANUP_PROOF, ALLOC_CURRENT, EPOCH_CURRENT, INCARNATION_STALE),
        Event(EventKind.NOTIFY_RUNNING, ALLOC_CURRENT, EPOCH_CURRENT, INCARNATION_STALE),
        # Mismatched / interleaved representatives.
        Event(EventKind.CLEANUP_PROOF, ALLOC_OTHER, EPOCH_CURRENT, INCARNATION_CURRENT),
        Event(EventKind.ASSIGN, ALLOC_OTHER, EPOCH_NEXT, INCARNATION_CURRENT),
    ]
    # Deterministic canonical order for reproducible enumeration.
    alphabet.sort(
        key=lambda e: (e.kind.value, e.allocation_id, e.epoch, e.incarnation)
    )
    return alphabet


@dataclass(frozen=True)
class Violation:
    step_index: int
    invariant: str
    detail: str
    before: Runner
    event: Event
    after: Runner
    reason: str


def check_step(before: Runner, event: Event, after: Runner, reason: str) -> Violation | None:
    """Check safety invariants for a single transition."""
    def viol(invariant: str, detail: str) -> Violation:
        return Violation(
            step_index=-1,
            invariant=invariant,
            detail=detail,
            before=before,
            event=event,
            after=after,
            reason=reason,
        )

    # Epoch must never regress.
    if after.epoch < before.epoch:
        return viol("epoch_monotonic", f"epoch regressed {before.epoch}->{after.epoch}")

    # Stale epoch must never mutate current ownership.
    if event.epoch < before.epoch and after != before:
        return viol(
            "stale_epoch_fencing",
            f"stale epoch {event.epoch} < {before.epoch} mutated {before}->{after}",
        )

    # Stale incarnation (same epoch/allocation, older incarnation) must not mutate.
    if (
        before.owner is not None
        and event.epoch == before.epoch
        and event.allocation_id == before.owner.allocation_id
        and event.incarnation < before.owner.incarnation
        and after != before
    ):
        return viol(
            "stale_incarnation_fencing",
            f"stale incarnation {event.incarnation} < {before.owner.incarnation} mutated",
        )

    # Heartbeat loss / timeout must never directly yield AVAILABLE.
    if (
        event.kind in (EventKind.HEARTBEAT_LOST, EventKind.TIMED_OUT)
        and before.state != RunnerState.AVAILABLE
        and after.state == RunnerState.AVAILABLE
    ):
        return viol(
            "quarantine_not_reusable",
            f"{event.kind.value} produced AVAILABLE from {before.state.value}",
        )

    # QUARANTINED is sticky: only CLEANUP_PROOF to AVAILABLE, never backward.
    if before.state == RunnerState.QUARANTINED:
        if after.state in (
            RunnerState.ASSIGNED,
            RunnerState.STARTING,
            RunnerState.RUNNING,
            RunnerState.CLEANING,
        ):
            return viol(
                "terminal_no_regress",
                f"QUARANTINED regressed to {after.state.value} via {event.kind.value}",
            )
        if after.state == RunnerState.AVAILABLE and before != after:
            if event.kind != EventKind.CLEANUP_PROOF:
                return viol(
                    "release_requires_proof",
                    f"QUARANTINED->AVAILABLE without proof via {event.kind.value}",
                )
            if before.owner is None or (
                event.allocation_id != before.owner.allocation_id
                or event.epoch != before.owner.epoch
                or event.incarnation != before.owner.incarnation
            ):
                return viol(
                    "release_requires_current_proof",
                    "QUARANTINED->AVAILABLE with stale/mismatched proof",
                )

    # Any transition into AVAILABLE must be via current-context cleanup proof.
    if before.state != RunnerState.AVAILABLE and after.state == RunnerState.AVAILABLE:
        if event.kind != EventKind.CLEANUP_PROOF:
            return viol(
                "release_requires_proof",
                f"{before.state.value}->AVAILABLE via {event.kind.value}, not proof",
            )
        if before.owner is None or (
            event.allocation_id != before.owner.allocation_id
            or event.epoch != before.owner.epoch
            or event.incarnation != before.owner.incarnation
        ):
            return viol(
                "release_requires_current_proof",
                "AVAILABLE via missing/stale/mismatched proof",
            )

    # ASSIGN from non-AVAILABLE must not mutate (QUARANTINED not schedulable).
    if (
        event.kind == EventKind.ASSIGN
        and before.state != RunnerState.AVAILABLE
        and after != before
    ):
        return viol(
            "quarantine_not_schedulable",
            f"ASSIGN mutated {before.state.value} -> {after.state.value}",
        )

    # Forward-only lifecycle: explicit backward edges are violations.
    backward = {
        (RunnerState.STARTING, RunnerState.ASSIGNED),
        (RunnerState.RUNNING, RunnerState.ASSIGNED),
        (RunnerState.RUNNING, RunnerState.STARTING),
        (RunnerState.CLEANING, RunnerState.ASSIGNED),
        (RunnerState.CLEANING, RunnerState.STARTING),
        (RunnerState.CLEANING, RunnerState.RUNNING),
    }
    if (before.state, after.state) in backward:
        return viol(
            "terminal_no_regress",
            f"lifecycle regressed {before.state.value}->{after.state.value}",
        )

    return None


def check_trace(initial: Runner, events: list[Event]) -> list[Violation]:
    """Fold a trace and return all invariant violations with step detail."""
    violations: list[Violation] = []
    state = initial
    for idx, event in enumerate(events):
        before = state
        state, outcome = apply(before, event)
        viol = check_step(before, event, state, outcome.reason)
        if viol is not None:
            violations.append(
                Violation(
                    step_index=idx,
                    invariant=viol.invariant,
                    detail=viol.detail,
                    before=viol.before,
                    event=viol.event,
                    after=viol.after,
                    reason=viol.reason,
                )
            )
    return violations


@dataclass
class ExplorationReport:
    strategy: str
    bounds: str
    assumptions: str
    alphabet_size: int
    max_depth: int
    traces_checked: int
    steps_checked: int
    violations: list[tuple[list[Event], list[Violation]]]

    @property
    def ok(self) -> bool:
        return len(self.violations) == 0


def explore_traces(
    initial: Runner | None = None,
    alphabet: list[Event] | None = None,
    max_depth: int = 4,
) -> ExplorationReport:
    """Exhaustively check all sequences of length 0..max_depth."""
    if initial is None:
        initial = initial_runner()
    if alphabet is None:
        alphabet = build_alphabet()

    strategy = (
        "bounded exhaustive enumeration of all ordered event sequences "
        f"of length 0..{max_depth} over a fixed {len(alphabet)}-event alphabet "
        "(assign/start/running/begin-cleaning/heartbeat-loss/timeout/cleanup-proof "
        "plus stale-epoch, stale-incarnation, mismatched-proof, and next-epoch "
        "assign representatives), folded deterministically via apply() with "
        "per-step safety-invariant checks; deterministic lexicographic order, "
        "no randomness, reproducible"
    )
    bounds = (
        f"alphabet_size={len(alphabet)} max_depth={max_depth} "
        f"traces={sum(len(alphabet) ** d for d in range(max_depth + 1))} "
        "single-runner scope A1/e1/i1 with stale (e0/i0), mismatched (A2), "
        "and next-epoch (A2/e2) representatives only"
    )
    assumptions = (
        "totally ordered input streams; initial AVAILABLE epoch=0 no owner; "
        "abstract cleanup proof bound to ownership context; "
        "QUARANTINED->AVAILABLE via current proof is the modeled release path; "
        "no persistence/crash-recovery/Linux/timing/concurrency/multi-runner claims"
    )

    traces_checked = 0
    steps_checked = 0
    found: list[tuple[list[Event], list[Violation]]] = []

    # Length-0 trace.
    violations = check_trace(initial, [])
    traces_checked += 1
    if violations:
        found.append(([], violations))

    # Lengths 1..max_depth via base-N counting in canonical order.
    n = len(alphabet)
    for depth in range(1, max_depth + 1):
        total = n**depth
        for code in range(total):
            events: list[Event] = []
            rest = code
            for _ in range(depth):
                events.append(alphabet[rest % n])
                rest //= n
            events.reverse()
            violations = check_trace(initial, events)
            traces_checked += 1
            steps_checked += depth
            if violations:
                found.append((events, violations))

    return ExplorationReport(
        strategy=strategy,
        bounds=bounds,
        assumptions=assumptions,
        alphabet_size=n,
        max_depth=max_depth,
        traces_checked=traces_checked,
        steps_checked=steps_checked,
        violations=found,
    )
