"""Mechanical exploration checks: interleaved streams never yield unsafe reuse."""

import unittest

from clearance.explore import (
    build_alphabet,
    check_trace,
    explore_traces,
)
from clearance.model import (
    Event,
    EventKind,
    RunnerState,
    fold,
    initial_runner,
)


def ev(kind, alloc="A1", epoch=1, inc=1):
    return Event(kind, alloc, epoch, inc)


class TestExploration(unittest.TestCase):
    def test_valid_and_quarantine_release_representatives(self):
        # Valid lifecycle path.
        valid = [
            ev(EventKind.ASSIGN),
            ev(EventKind.NOTIFY_STARTING),
            ev(EventKind.NOTIFY_RUNNING),
            ev(EventKind.BEGIN_CLEANING),
            ev(EventKind.CLEANUP_PROOF),
        ]
        final, _ = fold(initial_runner(), valid)
        self.assertEqual(final.state, RunnerState.AVAILABLE)
        self.assertEqual(check_trace(initial_runner(), valid), [])

        # Representative quarantine/release path.
        quarantine = [
            ev(EventKind.ASSIGN),
            ev(EventKind.NOTIFY_STARTING),
            ev(EventKind.HEARTBEAT_LOST),
            ev(EventKind.CLEANUP_PROOF),
        ]
        final, _ = fold(initial_runner(), quarantine)
        self.assertEqual(final.state, RunnerState.AVAILABLE)
        self.assertEqual(check_trace(initial_runner(), quarantine), [])

        # Timeout path from RUNNING via quarantine.
        timeout = [
            ev(EventKind.ASSIGN),
            ev(EventKind.NOTIFY_STARTING),
            ev(EventKind.NOTIFY_RUNNING),
            ev(EventKind.TIMED_OUT),
            ev(EventKind.CLEANUP_PROOF),
        ]
        final, _ = fold(initial_runner(), timeout)
        self.assertEqual(final.state, RunnerState.AVAILABLE)
        self.assertEqual(check_trace(initial_runner(), timeout), [])

    def test_exhaustive_interleavings_have_no_unsafe_reuse(self):
        alphabet = build_alphabet()
        # Alphabet must cover every required category at least once.
        kinds = {e.kind for e in alphabet}
        for required in (
            EventKind.ASSIGN,
            EventKind.NOTIFY_STARTING,
            EventKind.HEARTBEAT_LOST,
            EventKind.TIMED_OUT,
            EventKind.CLEANUP_PROOF,
        ):
            self.assertIn(required, kinds)
        # Stale-epoch and stale-incarnation representatives present.
        self.assertTrue(any(e.epoch == 0 for e in alphabet))
        self.assertTrue(any(e.incarnation == 0 for e in alphabet))

        report = explore_traces(
            initial=initial_runner(), alphabet=alphabet, max_depth=4
        )
        self.assertEqual(report.violations, [])
        self.assertTrue(report.ok)
        # Evidence states strategy and bounds/assumptions.
        self.assertTrue(report.strategy)
        self.assertTrue(report.bounds)
        self.assertTrue(report.assumptions)
        # Sanity on exploration size: sum(n**d for d in 0..4).
        expected = sum(len(alphabet) ** d for d in range(5))
        self.assertEqual(report.traces_checked, expected)
        self.assertGreater(report.steps_checked, 0)

    def test_exploration_is_reproducible(self):
        alphabet = build_alphabet()
        first = explore_traces(
            initial=initial_runner(), alphabet=alphabet, max_depth=3
        )
        second = explore_traces(
            initial=initial_runner(), alphabet=alphabet, max_depth=3
        )
        self.assertEqual(first.traces_checked, second.traces_checked)
        self.assertEqual(first.steps_checked, second.steps_checked)
        self.assertEqual(first.violations, second.violations)

    def test_violation_carries_trace_detail(self):
        # Directly exercise the checker API shape: a violation, if any,
        # must carry step, invariant, detail, before/event/after.
        # Use a deliberately unsafe synthetic check by inspecting a stale
        # trace is clean, then assert the detail fields exist on the type.
        from clearance.explore import Violation

        self.assertTrue(hasattr(Violation, "__dataclass_fields__"))
        for field in (
            "step_index",
            "invariant",
            "detail",
            "before",
            "event",
            "after",
            "reason",
        ):
            self.assertIn(field, Violation.__dataclass_fields__)


if __name__ == "__main__":
    unittest.main()
