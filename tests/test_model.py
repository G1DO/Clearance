"""Unit checks for the deterministic runner-ownership reference model."""

import unittest

from clearance.model import (
    Event,
    EventKind,
    RunnerState,
    apply,
    fold,
    initial_runner,
    is_schedulable,
)


def ev(kind, alloc="A1", epoch=1, inc=1):
    return Event(kind, alloc, epoch, inc)


class TestValidLifecycle(unittest.TestCase):
    def test_full_lifecycle_accepted(self):
        s = initial_runner()
        seq = [
            ev(EventKind.ASSIGN),
            ev(EventKind.NOTIFY_STARTING),
            ev(EventKind.NOTIFY_RUNNING),
            ev(EventKind.BEGIN_CLEANING),
            ev(EventKind.CLEANUP_PROOF),
        ]
        final, outcomes = fold(s, seq)
        self.assertTrue(all(o.accepted for o in outcomes), [o.reason for o in outcomes])
        self.assertEqual(final.state, RunnerState.AVAILABLE)
        self.assertIsNone(final.owner)
        self.assertEqual(final.epoch, 1)

    def test_lifecycle_states_in_order(self):
        s = initial_runner()
        s, o = apply(s, ev(EventKind.ASSIGN))
        self.assertEqual((s.state, o.accepted), (RunnerState.ASSIGNED, True))
        s, o = apply(s, ev(EventKind.NOTIFY_STARTING))
        self.assertEqual((s.state, o.accepted), (RunnerState.STARTING, True))
        s, o = apply(s, ev(EventKind.NOTIFY_RUNNING))
        self.assertEqual((s.state, o.accepted), (RunnerState.RUNNING, True))
        s, o = apply(s, ev(EventKind.BEGIN_CLEANING))
        self.assertEqual((s.state, o.accepted), (RunnerState.CLEANING, True))
        s, o = apply(s, ev(EventKind.CLEANUP_PROOF))
        self.assertEqual((s.state, o.accepted), (RunnerState.AVAILABLE, True))


class TestOwnershipFencing(unittest.TestCase):
    def test_superseded_context_cannot_mutate(self):
        s = initial_runner()
        s, _ = apply(s, ev(EventKind.ASSIGN, alloc="A1", epoch=1, inc=1))
        before = s
        # Different allocation, same epoch/incarnation: ignored.
        after, outcome = apply(before, ev(EventKind.NOTIFY_STARTING, alloc="A2"))
        self.assertFalse(outcome.accepted)
        self.assertEqual(after, before)
        # Current context still works.
        after, outcome = apply(before, ev(EventKind.NOTIFY_STARTING, alloc="A1"))
        self.assertTrue(outcome.accepted)
        self.assertEqual(after.state, RunnerState.STARTING)

    def test_stale_epoch_cannot_advance_release_or_mutate(self):
        s = initial_runner()
        s, _ = apply(s, ev(EventKind.ASSIGN, epoch=1))
        # Stale epoch start ignored.
        before = s
        after, outcome = apply(before, ev(EventKind.NOTIFY_STARTING, epoch=0))
        self.assertFalse(outcome.accepted)
        self.assertIn("stale_epoch", outcome.reason)
        self.assertEqual(after, before)
        # Stale epoch proof cannot release from CLEANING.
        s, _ = apply(s, ev(EventKind.NOTIFY_STARTING))
        s, _ = apply(s, ev(EventKind.NOTIFY_RUNNING))
        s, _ = apply(s, ev(EventKind.BEGIN_CLEANING))
        before = s
        after, outcome = apply(before, ev(EventKind.CLEANUP_PROOF, epoch=0))
        self.assertFalse(outcome.accepted)
        self.assertEqual(after, before)
        self.assertEqual(after.state, RunnerState.CLEANING)

    def test_stale_epoch_assign_rejected_and_monotonic(self):
        s = initial_runner()
        s, _ = apply(s, ev(EventKind.ASSIGN, epoch=1))
        # Drive to AVAILABLE via valid path to retain epoch=1.
        for kind in (
            EventKind.NOTIFY_STARTING,
            EventKind.NOTIFY_RUNNING,
            EventKind.BEGIN_CLEANING,
            EventKind.CLEANUP_PROOF,
        ):
            s, _ = apply(s, ev(kind, epoch=1))
        self.assertEqual((s.state, s.epoch), (RunnerState.AVAILABLE, 1))
        # Re-assign with stale/duplicate epoch must not mutate.
        before = s
        after, outcome = apply(before, ev(EventKind.ASSIGN, epoch=1))
        self.assertFalse(outcome.accepted)
        self.assertEqual(after, before)
        after, outcome = apply(before, ev(EventKind.ASSIGN, epoch=0))
        self.assertFalse(outcome.accepted)
        self.assertEqual(after, before)
        # Higher epoch succeeds.
        after, outcome = apply(before, ev(EventKind.ASSIGN, alloc="A2", epoch=2))
        self.assertTrue(outcome.accepted)
        self.assertEqual(after.epoch, 2)

    def test_stale_incarnation_cannot_mutate_or_release(self):
        s = initial_runner()
        s, _ = apply(s, ev(EventKind.ASSIGN, inc=1))
        s, _ = apply(s, ev(EventKind.NOTIFY_STARTING, inc=1))
        s, _ = apply(s, ev(EventKind.NOTIFY_RUNNING, inc=1))
        s, _ = apply(s, ev(EventKind.BEGIN_CLEANING, inc=1))
        before = s
        after, outcome = apply(before, ev(EventKind.CLEANUP_PROOF, inc=0))
        self.assertFalse(outcome.accepted)
        self.assertIn("stale_incarnation", outcome.reason)
        self.assertEqual(after, before)
        # Stale incarnation start also ignored earlier in lifecycle.
        s2 = initial_runner()
        s2, _ = apply(s2, ev(EventKind.ASSIGN, inc=1))
        before = s2
        after, outcome = apply(before, ev(EventKind.NOTIFY_STARTING, inc=0))
        self.assertFalse(outcome.accepted)
        self.assertEqual(after, before)


class TestQuarantine(unittest.TestCase):
    def test_heartbeat_loss_quarantines_and_never_available(self):
        s = initial_runner()
        s, _ = apply(s, ev(EventKind.ASSIGN))
        s, _ = apply(s, ev(EventKind.NOTIFY_STARTING))
        s, outcome = apply(s, ev(EventKind.HEARTBEAT_LOST))
        self.assertTrue(outcome.accepted)
        self.assertEqual(s.state, RunnerState.QUARANTINED)
        self.assertFalse(is_schedulable(s))

    def test_timeout_quarantines_and_preserves_owner(self):
        s = initial_runner()
        s, _ = apply(s, ev(EventKind.ASSIGN))
        s, _ = apply(s, ev(EventKind.NOTIFY_STARTING))
        s, _ = apply(s, ev(EventKind.NOTIFY_RUNNING))
        owner_before = s.owner
        s, _ = apply(s, ev(EventKind.TIMED_OUT))
        self.assertEqual(s.state, RunnerState.QUARANTINED)
        self.assertEqual(s.owner, owner_before)
        self.assertFalse(is_schedulable(s))

    def test_quarantined_not_schedulable_and_sticky(self):
        s = initial_runner()
        s, _ = apply(s, ev(EventKind.ASSIGN))
        s, _ = apply(s, ev(EventKind.HEARTBEAT_LOST))
        self.assertEqual(s.state, RunnerState.QUARANTINED)
        before = s
        # Assign from quarantine must not succeed.
        after, outcome = apply(before, ev(EventKind.ASSIGN, alloc="A2", epoch=2))
        self.assertFalse(outcome.accepted)
        self.assertEqual(after, before)
        # Lifecycle events cannot escape quarantine.
        for kind in (
            EventKind.NOTIFY_STARTING,
            EventKind.NOTIFY_RUNNING,
            EventKind.BEGIN_CLEANING,
        ):
            after, outcome = apply(before, ev(kind))
            self.assertFalse(outcome.accepted)
            self.assertEqual(after, before)

    def test_quarantine_release_requires_current_proof(self):
        s = initial_runner()
        s, _ = apply(s, ev(EventKind.ASSIGN))
        s, _ = apply(s, ev(EventKind.HEARTBEAT_LOST))
        # Missing proof (other events) cannot release.
        before = s
        after, _ = apply(before, ev(EventKind.NOTIFY_RUNNING))
        self.assertEqual(after.state, RunnerState.QUARANTINED)
        # Stale proof cannot release.
        after, outcome = apply(before, ev(EventKind.CLEANUP_PROOF, epoch=0))
        self.assertFalse(outcome.accepted)
        self.assertEqual(after.state, RunnerState.QUARANTINED)
        # Mismatched proof cannot release.
        after, outcome = apply(before, ev(EventKind.CLEANUP_PROOF, alloc="A2"))
        self.assertFalse(outcome.accepted)
        self.assertEqual(after.state, RunnerState.QUARANTINED)
        # Current proof releases.
        after, outcome = apply(before, ev(EventKind.CLEANUP_PROOF))
        self.assertTrue(outcome.accepted)
        self.assertEqual(after.state, RunnerState.AVAILABLE)
        self.assertTrue(is_schedulable(after))

    def test_cleaning_requires_current_proof(self):
        s = initial_runner()
        for kind in (
            EventKind.ASSIGN,
            EventKind.NOTIFY_STARTING,
            EventKind.NOTIFY_RUNNING,
            EventKind.BEGIN_CLEANING,
        ):
            s, _ = apply(s, ev(kind))
        self.assertEqual(s.state, RunnerState.CLEANING)
        # Heartbeat loss from cleaning quarantines (never AVAILABLE).
        q, _ = apply(s, ev(EventKind.HEARTBEAT_LOST))
        self.assertEqual(q.state, RunnerState.QUARANTINED)
        # Valid proof from cleaning releases.
        after, outcome = apply(s, ev(EventKind.CLEANUP_PROOF))
        self.assertTrue(outcome.accepted)
        self.assertEqual(after.state, RunnerState.AVAILABLE)


class TestTerminalAndReorder(unittest.TestCase):
    def test_duplicate_reports_harmless(self):
        s = initial_runner()
        s, _ = apply(s, ev(EventKind.ASSIGN))
        s, _ = apply(s, ev(EventKind.NOTIFY_STARTING))
        before = s
        # Duplicate starting report is a noop, stays STARTING.
        after, outcome = apply(before, ev(EventKind.NOTIFY_STARTING))
        self.assertFalse(outcome.accepted)
        self.assertEqual(after, before)
        # Duplicate assign from non-available is noop.
        s2 = initial_runner()
        s2, _ = apply(s2, ev(EventKind.ASSIGN))
        after, _ = apply(s2, ev(EventKind.ASSIGN))
        self.assertEqual(after, s2)

    def test_reordered_reports_harmless(self):
        s = initial_runner()
        s, _ = apply(s, ev(EventKind.ASSIGN))
        before = s
        # RUNNING before STARTING must not advance.
        after, outcome = apply(before, ev(EventKind.NOTIFY_RUNNING))
        self.assertFalse(outcome.accepted)
        self.assertEqual(after, before)
        # CLEANING before RUNNING must not advance.
        after, outcome = apply(before, ev(EventKind.BEGIN_CLEANING))
        self.assertFalse(outcome.accepted)
        self.assertEqual(after, before)
        # Proof before CLEANING must not release.
        after, outcome = apply(before, ev(EventKind.CLEANUP_PROOF))
        self.assertFalse(outcome.accepted)
        self.assertEqual(after, before)

    def test_terminal_cannot_regress(self):
        s = initial_runner()
        s, _ = apply(s, ev(EventKind.ASSIGN))
        s, _ = apply(s, ev(EventKind.NOTIFY_STARTING))
        s, _ = apply(s, ev(EventKind.NOTIFY_RUNNING))
        s, _ = apply(s, ev(EventKind.BEGIN_CLEANING))
        # CLEANING cannot go back to RUNNING/STARTING.
        for kind in (EventKind.NOTIFY_RUNNING, EventKind.NOTIFY_STARTING):
            after, outcome = apply(s, ev(kind))
            self.assertFalse(outcome.accepted)
            self.assertEqual(after.state, RunnerState.CLEANING)


class TestDeterminism(unittest.TestCase):
    def test_same_stream_same_result(self):
        seq = [
            ev(EventKind.ASSIGN),
            ev(EventKind.NOTIFY_STARTING),
            ev(EventKind.HEARTBEAT_LOST),
            ev(EventKind.CLEANUP_PROOF),
            ev(EventKind.ASSIGN, alloc="A2", epoch=2),
        ]
        final1, outcomes1 = fold(initial_runner(), seq)
        final2, outcomes2 = fold(initial_runner(), seq)
        self.assertEqual(final1, final2)
        self.assertEqual(
            [o.reason for o in outcomes1], [o.reason for o in outcomes2]
        )


if __name__ == "__main__":
    unittest.main()
