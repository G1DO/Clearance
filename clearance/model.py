"""Clearance deterministic runner-ownership reference model.

Pure-Python, in-memory executable safety oracle for runner ownership and
safe reuse. No wall-clock, database, HTTP, OS, or Linux facilities are used.

Scope (this Issue only):
  - deterministic ownership transitions with runner-epoch and agent-incarnation fencing
  - quarantine stickiness: heartbeat loss / timeout never yields AVAILABLE
  - release to AVAILABLE only via current abstract cleanup/release proof
  - terminal non-regression and duplicate/reorder tolerance

Intentionally deferred (no claim made here):
  persistence / crash-recovery durability, recovery-generation correctness,
  Linux cleanup correctness, bounded-resource behavior, real timing,
  concurrency / locking, schemas, protocols, reconciliation loops.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class RunnerState(str, Enum):
    AVAILABLE = "AVAILABLE"
    ASSIGNED = "ASSIGNED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    CLEANING = "CLEANING"
    QUARANTINED = "QUARANTINED"


class EventKind(str, Enum):
    ASSIGN = "assign"
    NOTIFY_STARTING = "start"
    NOTIFY_RUNNING = "running"
    BEGIN_CLEANING = "begin_cleaning"
    HEARTBEAT_LOST = "heartbeat_lost"
    TIMED_OUT = "timed_out"
    CLEANUP_PROOF = "cleanup_proof"


@dataclass(frozen=True)
class OwnershipContext:
    """Active allocation bound to a runner epoch and agent incarnation."""

    allocation_id: str
    epoch: int
    incarnation: int


@dataclass(frozen=True)
class Runner:
    """Authoritative in-memory runner record."""

    state: RunnerState
    epoch: int
    owner: OwnershipContext | None


@dataclass(frozen=True)
class Event:
    kind: EventKind
    allocation_id: str
    epoch: int
    incarnation: int


@dataclass(frozen=True)
class Outcome:
    accepted: bool
    reason: str


def initial_runner() -> Runner:
    """Initial authoritative state: schedulable, unfenced, unowned."""
    return Runner(state=RunnerState.AVAILABLE, epoch=0, owner=None)


def is_schedulable(runner: Runner) -> bool:
    """Only AVAILABLE is schedulable/reusable. QUARANTINED is never reusable."""
    return runner.state == RunnerState.AVAILABLE


def _context_matches(runner: Runner, event: Event) -> bool:
    owner = runner.owner
    if owner is None:
        return False
    return (
        event.allocation_id == owner.allocation_id
        and event.epoch == owner.epoch
        and event.incarnation == owner.incarnation
    )


def apply(state: Runner, event: Event) -> tuple[Runner, Outcome]:
    """Pure deterministic transition.

    Same (state, event) always yields the same (new state, outcome).
    Stale or mismatched authority never mutates current ownership.
    """
    # Assign is the only event that may create ownership, and only from AVAILABLE.
    if event.kind == EventKind.ASSIGN:
        if state.state != RunnerState.AVAILABLE:
            return state, Outcome(False, "ignored_wrong_state_not_available")
        if not event.allocation_id:
            return state, Outcome(False, "ignored_empty_allocation")
        if event.epoch <= state.epoch:
            return state, Outcome(False, "ignored_stale_epoch")
        owner = OwnershipContext(
            allocation_id=event.allocation_id,
            epoch=event.epoch,
            incarnation=event.incarnation,
        )
        return (
            Runner(state=RunnerState.ASSIGNED, epoch=event.epoch, owner=owner),
            Outcome(True, "ok_assign"),
        )

    # All non-assign events require a current owner; AVAILABLE has none.
    if state.state == RunnerState.AVAILABLE or state.owner is None:
        return state, Outcome(False, "ignored_wrong_state_available_no_owner")

    # Runner-epoch fencing: exact match required for any mutation.
    if event.epoch != state.epoch:
        if event.epoch < state.epoch:
            return state, Outcome(False, "ignored_stale_epoch")
        return state, Outcome(False, "ignored_epoch_mismatch")

    # Agent-incarnation + allocation fencing: exact match required.
    owner = state.owner
    if event.allocation_id != owner.allocation_id:
        return state, Outcome(False, "ignored_allocation_mismatch")
    if event.incarnation != owner.incarnation:
        if event.incarnation < owner.incarnation:
            return state, Outcome(False, "ignored_stale_incarnation")
        return state, Outcome(False, "ignored_incarnation_mismatch")

    # Current ownership context confirmed; enforce forward-only lifecycle.
    if state.state == RunnerState.QUARANTINED:
        if event.kind == EventKind.CLEANUP_PROOF:
            return (
                Runner(state=RunnerState.AVAILABLE, epoch=state.epoch, owner=None),
                Outcome(True, "ok_release_from_quarantine"),
            )
        return state, Outcome(False, "ignored_quarantined_sticky")

    if event.kind in (EventKind.HEARTBEAT_LOST, EventKind.TIMED_OUT):
        if state.state in (
            RunnerState.ASSIGNED,
            RunnerState.STARTING,
            RunnerState.RUNNING,
            RunnerState.CLEANING,
        ):
            return (
                Runner(
                    state=RunnerState.QUARANTINED,
                    epoch=state.epoch,
                    owner=state.owner,
                ),
                Outcome(True, "ok_quarantined"),
            )
        return state, Outcome(False, "ignored_noop")

    if state.state == RunnerState.ASSIGNED:
        if event.kind == EventKind.NOTIFY_STARTING:
            return (
                Runner(state=RunnerState.STARTING, epoch=state.epoch, owner=owner),
                Outcome(True, "ok_starting"),
            )
        return state, Outcome(False, "ignored_noop")

    if state.state == RunnerState.STARTING:
        if event.kind == EventKind.NOTIFY_RUNNING:
            return (
                Runner(state=RunnerState.RUNNING, epoch=state.epoch, owner=owner),
                Outcome(True, "ok_running"),
            )
        return state, Outcome(False, "ignored_noop")

    if state.state == RunnerState.RUNNING:
        if event.kind == EventKind.BEGIN_CLEANING:
            return (
                Runner(state=RunnerState.CLEANING, epoch=state.epoch, owner=owner),
                Outcome(True, "ok_cleaning"),
            )
        return state, Outcome(False, "ignored_noop")

    if state.state == RunnerState.CLEANING:
        if event.kind == EventKind.CLEANUP_PROOF:
            return (
                Runner(state=RunnerState.AVAILABLE, epoch=state.epoch, owner=None),
                Outcome(True, "ok_release_from_cleaning"),
            )
        return state, Outcome(False, "ignored_noop")

    return state, Outcome(False, "ignored_noop")


def fold(initial: Runner, events: list[Event]) -> tuple[Runner, list[Outcome]]:
    """Deterministically fold an ordered event stream."""
    state = initial
    outcomes: list[Outcome] = []
    for event in events:
        state, outcome = apply(state, event)
        outcomes.append(outcome)
    return state, outcomes
