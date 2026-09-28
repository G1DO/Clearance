"""Unit checks for the deterministic reconciliation classifier."""

import unittest

from clearance.reconcile import (
    ReconcileAction,
    ReconcileClassification,
    ReconcileObservation,
    classify,
)


def obs(**overrides):
    base = dict(
        current_allocation_id="A1",
        observed_allocation_id="A1",
        cgroup_present=True,
        workspace_present=True,
        pids=(),
        execution_empty=True,
        descendants_reaped=True,
        workspace_clean=True,
        error=None,
        has_terminal=True,
    )
    base.update(overrides)
    return ReconcileObservation(
        current_allocation_id=base["current_allocation_id"],
        observed_allocation_id=base["observed_allocation_id"],
        cgroup_present=base["cgroup_present"],
        workspace_present=base["workspace_present"],
        pids=tuple(base["pids"]),
        execution_empty=base["execution_empty"],
        descendants_reaped=base["descendants_reaped"],
        workspace_clean=base["workspace_clean"],
        error=base["error"],
        has_terminal=base["has_terminal"],
    )


class TestReconcileClassification(unittest.TestCase):
    def test_already_clean_requires_terminal_and_positive_evidence(self):
        classification, action = classify(obs())
        self.assertEqual(classification, ReconcileClassification.ALREADY_CLEAN)
        self.assertEqual(action, ReconcileAction.ATTEST)

    def test_dirty_workspace_prevents_attest(self):
        classification, action = classify(obs(workspace_clean=False))
        self.assertEqual(classification, ReconcileClassification.FINISHED_NEEDS_CLEANUP)
        self.assertEqual(action, ReconcileAction.TERMINATE_CLEANUP)

    def test_remaining_processes_require_cleanup(self):
        classification, action = classify(
            obs(pids=(101,), execution_empty=False, descendants_reaped=False)
        )
        self.assertEqual(classification, ReconcileClassification.FINISHED_NEEDS_CLEANUP)
        self.assertEqual(action, ReconcileAction.TERMINATE_CLEANUP)

    def test_still_running_recognized_without_termination_or_release(self):
        classification, action = classify(
            obs(has_terminal=False, pids=(101,), execution_empty=False,
                descendants_reaped=False, workspace_clean=False)
        )
        self.assertEqual(classification, ReconcileClassification.STILL_RUNNING)
        self.assertEqual(action, ReconcileAction.KEEP)

    def test_empty_resources_without_terminal_are_insufficient(self):
        classification, action = classify(obs(has_terminal=False))
        self.assertEqual(classification, ReconcileClassification.INSUFFICIENT_EVIDENCE)
        self.assertEqual(action, ReconcileAction.KEEP)

    def test_stale_execution_never_releases_or_terminates(self):
        classification, action = classify(
            obs(observed_allocation_id="A2", pids=(7,), execution_empty=False,
                descendants_reaped=False)
        )
        self.assertEqual(classification, ReconcileClassification.STALE_EXECUTION)
        self.assertEqual(action, ReconcileAction.KEEP)

    def test_orphaned_descendants_without_cgroup(self):
        classification, action = classify(
            obs(observed_allocation_id="A2", cgroup_present=False,
                execution_empty=True, pids=(9,))
        )
        # PIDs without cgroup plus mismatched identity: orphaned when cgroup
        # absent, but contradictory claims (reaped + pids) win first.
        # Use non-reaped to reach the orphaned path.
        self.assertIn(
            classification,
            (ReconcileClassification.ORPHANED_EXECUTION, ReconcileClassification.CONTRADICTORY),
        )
        self.assertEqual(action, ReconcileAction.KEEP)

    def test_orphaned_clean_path(self):
        classification, action = classify(
            obs(
                observed_allocation_id="A2",
                cgroup_present=False,
                execution_empty=True,
                descendants_reaped=False,
                workspace_present=False,
                workspace_clean=True,
                pids=(9,),
            )
        )
        self.assertEqual(classification, ReconcileClassification.ORPHANED_EXECUTION)
        self.assertEqual(action, ReconcileAction.KEEP)

    def test_pids_with_empty_cgroup_require_cleanup_not_contradiction(self):
        # Zombies/detached survivors are absent from cgroup.procs: an empty
        # cgroup with live PIDs still requires termination/scrub.
        classification, action = classify(
            obs(pids=(1,), execution_empty=True, descendants_reaped=False)
        )
        self.assertEqual(classification, ReconcileClassification.FINISHED_NEEDS_CLEANUP)
        self.assertEqual(action, ReconcileAction.TERMINATE_CLEANUP)

    def test_contradictory_absent_workspace_with_dirt(self):
        classification, action = classify(
            obs(workspace_present=False, workspace_clean=False)
        )
        self.assertEqual(classification, ReconcileClassification.CONTRADICTORY)
        self.assertEqual(action, ReconcileAction.KEEP)

    def test_contradictory_absent_cgroup_with_execution(self):
        classification, action = classify(
            obs(cgroup_present=False, execution_empty=False)
        )
        self.assertEqual(classification, ReconcileClassification.CONTRADICTORY)
        self.assertEqual(action, ReconcileAction.KEEP)

    def test_error_including_empty_string_is_insufficient(self):
        for error in ("boom", ""):
            classification, action = classify(obs(error=error))
            self.assertEqual(
                classification, ReconcileClassification.INSUFFICIENT_EVIDENCE, error
            )
            self.assertEqual(action, ReconcileAction.KEEP)

    def test_absent_observed_identity_means_current(self):
        classification, action = classify(obs(observed_allocation_id=None))
        self.assertEqual(classification, ReconcileClassification.ALREADY_CLEAN)
        self.assertEqual(action, ReconcileAction.ATTEST)

    def test_deterministic(self):
        first = classify(obs(pids=(5,), execution_empty=False, descendants_reaped=False))
        second = classify(obs(pids=(5,), execution_empty=False, descendants_reaped=False))
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
