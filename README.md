# Clearance — deterministic runner ownership reference model

Pure-Python, in-memory executable safety oracle for runner ownership and
safe reuse. Established before any production controller, persistence,
agent, or Linux integration exists.

## Layout

- `clearance/model.py` — deterministic reference model (`apply`/`fold`,
  ownership context, epoch/incarnation fencing, quarantine, cleanup proof)
- `clearance/explore.py` — bounded-exhaustive mechanical checks with stated
  strategy, bounds, and assumptions
- `tests/test_model.py` — lifecycle, fencing, quarantine/release,
  non-regression, duplicate/reorder, determinism
- `tests/test_exploration.py` — representative paths + exhaustive
  interleavings with no unsafe reuse

## Rules (summary)

- `AVAILABLE → ASSIGNED → STARTING → RUNNING → CLEANING → AVAILABLE` only.
- `ASSIGN` only from `AVAILABLE` with strictly greater epoch.
- Non-assign events require exact `(allocation_id, epoch, incarnation)` match.
- `HEARTBEAT_LOST` / `TIMED_OUT` → `QUARANTINED` (never `AVAILABLE`).
- `QUARANTINED` is sticky and not schedulable; only current-proof release.
- `AVAILABLE` only via `CLEANUP_PROOF` bound to the current ownership context.

## Verification record

- `python -m unittest discover -s tests -v`
- `tests/test_exploration.py` runs bounded-exhaustive interleavings
  (default depth 4 over a 15-event alphabet covering assign/start/
  heartbeat-loss/timeout/stale-epoch/stale-incarnation/duplicate/reordered/
  cleanup-proof) and fails on any unsafe reusable state.
- Strategy/bounds/assumptions are stated in `clearance/explore.py` and
  surfaced in the exploration report; no claim is made beyond that scope.

## What this proves vs deferred

Proves (within the bounded scope above): determinism, valid-lifecycle
acceptance, epoch/incarnation fencing, quarantine stickiness and
non-schedulability, release-only-via-current-proof, duplicate/reorder
harmlessness, and terminal non-regression.

Intentionally deferred (no claim): persistence/crash-recovery durability,
recovery-generation correctness, Linux cleanup correctness,
bounded-resource behavior, real timing, concurrency/locking, schemas,
protocols, reconciliation, PITR, capacity, rollout, UI, fleet simulation,
or performance.
