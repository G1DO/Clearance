"""Deterministic reconciliation classifier for quarantined runners (issue #27).

Pure-Python, in-memory safety helper. No wall-clock, database, HTTP, OS, or
Linux facilities are used.

The classifier is the abstract counterpart of the controller's durable
reconciliation resolution. It correlates a single fresh physical observation
(allocation-owned cgroup/workspace presence, descendant PIDs, and
cleanup booleans) with current controller authority (allocation identity and
whether a terminal disposition exists) and returns a classification plus the
intended action.

Scope:
  - distinguish STILL_RUNNING, FINISHED_NEEDS_CLEANUP, ALREADY_CLEAN,
    STALE_EXECUTION, ORPHANED_EXECUTION, CONTRADICTORY, INSUFFICIENT_EVIDENCE
  - attest-only release only for positively verified already-clean execution
    with a terminal disposition and matching identity
  - uncertain identity never authorizes destructive action or release
  - empty/missing resources, launch markers, heartbeats, or database rows
    alone never establish completion or safe reuse

Intentionally deferred (no claim made here):
  persistence / transaction design, recovery-generation correctness,
  Linux inspection/cleanup correctness, bounded-resource behavior, real timing,
  concurrency / locking, schemas, protocols, background loops.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ReconcileClassification(str, Enum):
    STILL_RUNNING = "STILL_RUNNING"
    FINISHED_NEEDS_CLEANUP = "FINISHED_NEEDS_CLEANUP"
    ALREADY_CLEAN = "ALREADY_CLEAN"
    STALE_EXECUTION = "STALE_EXECUTION"
    ORPHANED_EXECUTION = "ORPHANED_EXECUTION"
    CONTRADICTORY = "CONTRADICTORY"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


class ReconcileAction(str, Enum):
    # No destructive action; runner remains unavailable.
    KEEP = "KEEP"
    # Directed termination + workspace scrub via the existing cleanup lifecycle.
    TERMINATE_CLEANUP = "TERMINATE_CLEANUP"
    # Attest-only release: fresh inspection already proves cleanup complete.
    ATTEST = "ATTEST"


@dataclass(frozen=True)
class ReconcileObservation:
    """One fresh physical observation bound to current authority."""

    current_allocation_id: str
    observed_allocation_id: str | None
    cgroup_present: bool
    workspace_present: bool
    pids: tuple[int, ...]
    execution_empty: bool
    descendants_reaped: bool
    workspace_clean: bool
    error: str | None
    has_terminal: bool


def classify(observation: ReconcileObservation) -> tuple[ReconcileClassification, ReconcileAction]:
    """Pure deterministic classification.

    Same observation always yields the same (classification, action).
    Any present error (including an empty string) is failure evidence.
    Identity mismatch never authorizes destructive action or release.
    Attest-only requires a terminal disposition plus fully positive,
    internally consistent evidence under matching identity.
    """
    # Any reported error (even empty string) is negative evidence.
    if observation.error is not None:
        return (ReconcileClassification.INSUFFICIENT_EVIDENCE, ReconcileAction.KEEP)

    pids = tuple(observation.pids)
    has_pids = len(pids) > 0

    # Internal contradiction: a missing resource cannot still hold execution
    # or dirt. Live PIDs with an empty cgroup are orphaned/detached survivors
    # (handled below), not a contradiction: zombies and detached descendants
    # are absent from cgroup.procs and require /proc correlation.
    if (not observation.cgroup_present) and (not observation.execution_empty):
        return (ReconcileClassification.CONTRADICTORY, ReconcileAction.KEEP)
    if (not observation.workspace_present) and (not observation.workspace_clean):
        return (ReconcileClassification.CONTRADICTORY, ReconcileAction.KEEP)

    # Uncertain resource identity prevents destructive action and release.
    observed = observation.observed_allocation_id
    if observed is not None and observed != observation.current_allocation_id:
        if (not observation.cgroup_present) and has_pids:
            return (ReconcileClassification.ORPHANED_EXECUTION, ReconcileAction.KEEP)
        return (ReconcileClassification.STALE_EXECUTION, ReconcileAction.KEEP)

    # Legitimate still-running work: current identity, live execution, no
    # terminal disposition yet. Recognized without relaunch, invented outcome,
    # termination, or release.
    if not observation.has_terminal:
        if observation.cgroup_present and (has_pids or not observation.execution_empty):
            return (ReconcileClassification.STILL_RUNNING, ReconcileAction.KEEP)
        # Empty/missing resources without a terminal disposition do not imply
        # completion or safe reuse.
        return (ReconcileClassification.INSUFFICIENT_EVIDENCE, ReconcileAction.KEEP)

    # Terminal disposition exists; correlate with fresh physical state.
    positive = (
        not has_pids
        and observation.execution_empty
        and observation.descendants_reaped
        and observation.workspace_clean
    )
    if positive:
        # Dirty workspace alone already prevents this path because
        # workspace_clean is required above.
        return (ReconcileClassification.ALREADY_CLEAN, ReconcileAction.ATTEST)
    return (ReconcileClassification.FINISHED_NEEDS_CLEANUP, ReconcileAction.TERMINATE_CLEANUP)
