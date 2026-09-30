// Reconciliation observation helper: correlates fresh physical discovery with
// local cleanup proof into one RECONCILE report. No wall-clock, database, or
// network is used here; freshness comes from the caller's Discover inspection
// against Linux state, not from a saved marker or heartbeat.
//
// Absent observed identity (nil) means the current allocation: no distinct
// foreign identity was observed. Any present error (including empty) is failure
// evidence and prevents positive claims. Conservative defaults never invent
// cleanup proof: without a local positive cleanup, execution is reported as
// non-empty and unreaped/dirty.
package agent

// BuildReconcileEvidence combines fresh discovery (cgroup/workspace presence,
// PIDs, error) with the daemon's local cleanup proof (if any) into a single
// fresh RECONCILE observation. localCleanup may be nil when the workload is
// still running or cleanup has not yet been performed locally.
func BuildReconcileEvidence(discovery DiscoveryEvidence, localCleanup *CleanupEvidence) ReconcileEvidence {
	evidence := ReconcileEvidence{
		CgroupPresent:    discovery.CgroupPresent,
		WorkspacePresent: discovery.WorkspacePresent,
		PIDs:             discovery.PIDs,
	}
	if evidence.PIDs == nil {
		evidence.PIDs = []int64{}
	}
	if discovery.Error != nil {
		evidence.Error = discovery.Error
		evidence.ExecutionEmpty = false
		evidence.DescendantsReaped = false
		evidence.WorkspaceClean = false
		return evidence
	}
	if discovery.CleanupVerified && !discovery.CgroupPresent && len(discovery.PIDs) == 0 && (localCleanup == nil || positiveCleanup(localCleanup)) {
		// A durable removal checkpoint plus fresh physical absence also works
		// after restart. Workspace scrub may still be pending after cgroup removal.
		evidence.ExecutionEmpty, evidence.DescendantsReaped = true, true
		evidence.WorkspaceClean = !discovery.WorkspacePresent
		return evidence
	}
	if localCleanup == nil {
		// Still-running or not-yet-cleaned: make no positive claims.
		evidence.ExecutionEmpty = false
		evidence.DescendantsReaped = false
		evidence.WorkspaceClean = false
		return evidence
	}
	// Current physical presence vetoes cached positive claims. In particular,
	// dirty workspace alone must never authorize attest-only release.
	evidence.ExecutionEmpty = localCleanup.ExecutionEmpty && len(discovery.PIDs) == 0
	evidence.DescendantsReaped = localCleanup.DescendantsReaped && len(discovery.PIDs) == 0
	evidence.WorkspaceClean = localCleanup.WorkspaceClean && !discovery.WorkspacePresent
	if localCleanup.Error != nil {
		evidence.Error = localCleanup.Error
	}
	return evidence
}
