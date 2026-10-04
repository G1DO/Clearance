//go:build integration

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// PITR rewind drill for issue #34: real PostgreSQL backup at T0, real Linux
// execution at T1, rewind to T0, recovery-mode boot with a fresh non-repeating
// generation, fleet quarantine without scheduling, verified reconciliation with
// graceful-then-forceful reaping, and repeated-rewind freshness.
//
// Uses the real controller, real PostgreSQL schema snapshot/restore (backup
// schema via CREATE TABLE AS TABLE WITH DATA, restore via DELETE + INSERT
// SELECT), and real Linux cgroup/workspace execution (manual containment for
// allocated runners plus the real Go daemon for idle-at-backup runners).
// Trusted test workloads only, isolated schema/state/credentials; never
// contacts production or mutates real fleets. Artifacts are retained under
// run.*/pitr-drill plus per-fixture evidence dirs.

func pitrPsql(t *testing.T, schema, query string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	data, err := exec.CommandContext(ctx, "psql", "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-c", query).CombinedOutput()
	if err != nil {
		t.Fatalf("pitr psql: %v: %s (query %s)", err, data, query)
	}
	return string(bytes.TrimSpace(data))
}

func pitrWrite(t *testing.T, dir, name string, v any) {
	t.Helper()
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func pitrBackup(t *testing.T, h lifecycleHarness, backup string) {
	t.Helper()
	pitrPsql(t, h.Schema, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", backup))
	pitrPsql(t, h.Schema, fmt.Sprintf("CREATE SCHEMA %s", backup))
	for _, table := range []string{"runners", "jobs", "attempts", "allocations", "recovery_authority"} {
		pitrPsql(t, h.Schema, fmt.Sprintf("CREATE TABLE %s.%s AS TABLE %s.%s WITH DATA", backup, table, h.Schema, table))
	}
}

func pitrRestore(t *testing.T, h lifecycleHarness, backup string) {
	t.Helper()
	// FK order alone is relied upon (children-first DELETE, parents-first
	// INSERT): each pitrPsql invocation is a separate psql session, so SET
	// session_replication_role here would be a no-op.
	for _, table := range []string{"allocations", "attempts", "jobs", "runners", "recovery_authority"} {
		pitrPsql(t, h.Schema, fmt.Sprintf("DELETE FROM %s.%s", h.Schema, table))
	}
	for _, table := range []string{"runners", "jobs", "recovery_authority", "attempts", "allocations"} {
		pitrPsql(t, h.Schema, fmt.Sprintf("INSERT INTO %s.%s SELECT * FROM %s.%s", h.Schema, table, backup, table))
	}
}

func pitrAuthority(t *testing.T, h lifecycleHarness) string {
	t.Helper()
	return pitrPsql(t, h.Schema, fmt.Sprintf("SELECT COALESCE(current_generation::text,'') FROM %s.recovery_authority WHERE singleton", h.Schema))
}

func pitrCounts(t *testing.T, h lifecycleHarness) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, table := range []string{"runners", "jobs", "attempts", "allocations", "recovery_authority"} {
		out[table] = pitrPsql(t, h.Schema, fmt.Sprintf("SELECT count(*) FROM %s.%s", h.Schema, table))
	}
	return out
}

func TestControllerPitrRewindDrill(t *testing.T) {
	h := loadLifecycleHarness(t)
	fTerm, ok := h.Cases["reconcile-terminate"]
	if !ok {
		t.Fatal("reconcile-terminate fixture required")
	}
	fRun, ok := h.Cases["reconcile-running"]
	if !ok {
		t.Fatal("reconcile-running fixture required")
	}
	idle, ok := h.IdleRunners["idle-clean"]
	if !ok {
		t.Fatal("idle-clean fixture required")
	}
	drillDir := filepath.Join(filepath.Dir(fTerm.EvidenceDir), "pitr-drill")
	if err := os.MkdirAll(drillDir, 0700); err != nil {
		t.Fatal(err)
	}
	backup := strings.ToLower(h.Schema + "_pitr")
	if len(backup) > 60 {
		backup = backup[:60]
	}
	t.Cleanup(func() { pitrPsql(t, h.Schema, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", backup)) })
	ctx := context.Background()

	// T0 setup with real Linux execution: terminate fixture driven to terminal
	// SUCCEEDED with its TERM-ignoring orphan still alive and workspace dirty;
	// running fixture stays RUNNING with live processes. No heartbeat arming:
	// the default 600s timeout keeps T0 healthy (no quarantine before backup).
	clientTerm := integrationClient(t, h.URL, fTerm.Token)
	assignmentTerm, err := clientTerm.Poll(ctx, 1, 1)
	if err != nil || !assignmentTerm.Assigned {
		t.Fatalf("poll terminate fixture: %+v %v", assignmentTerm, err)
	}
	stateTerm := filepath.Join(fTerm.EvidenceDir, "pitr-state")
	if err := os.Mkdir(stateTerm, 0700); err != nil {
		t.Fatal(err)
	}
	ownerTerm, err := newContainment(containmentConfig{CgroupRoot: os.Getenv("CLEARANCE_CGROUP_ROOT"), WorkspaceRoot: stateTerm + "-workspaces", StateDir: stateTerm, GracePeriod: 200 * time.Millisecond, KillTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	workTerm, err := ownerTerm.Prepare(assignmentTerm)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := os.Stat(workTerm.cgroupPath); os.IsNotExist(err) {
			_ = workTerm.scrubWorkspace()
		} else if _, err := workTerm.cleanup(); err != nil {
			t.Errorf("remove pitr terminate fixture: %v", err)
		}
	})
	if err := workTerm.Start(); err != nil {
		t.Fatal(err)
	}
	seqTerm := int64(0)
	sendTerm := func(status ReportStatus, reconcile *ReconcileEvidence, cleanup *CleanupEvidence, reason string) ReportRequest {
		t.Helper()
		seqTerm++
		report := integrationReport(fTerm.controllerFixture, 1, seqTerm, status)
		report.Reconcile, report.Cleanup = reconcile, cleanup
		ack, err := clientTerm.Report(ctx, report)
		if err != nil || ack.Reason != reason || (reason != "quarantined" && !ack.Accepted) {
			t.Fatalf("terminate %s expected %s: %+v %v", status, reason, ack, err)
		}
		writeLifecycleEvidence(t, fTerm, fmt.Sprintf("pitr-%02d-%s", seqTerm, status), report)
		return report
	}
	sendTerm(StatusStarting, nil, nil, "ok")
	sendTerm(StatusRunning, nil, nil, "ok")
	pidsTerm := captureLifecycleProcesses(t, fTerm, workTerm)
	assertBeforePartition(t, h, fTerm, "RUNNING")
	finishReconciliationWork(t, fTerm)
	if err := workTerm.Wait(); err != nil {
		t.Fatalf("terminate workload did not succeed: %v", err)
	}
	sendTerm(StatusSucceeded, nil, nil, "ok")
	termCleaning := h.waitRows(t, fTerm, func(r lifecycleRows) bool {
		return r.Runner.State == "CLEANING" && r.Allocation.Report == "SUCCEEDED"
	})
	writeLifecycleEvidence(t, fTerm, "pitr-t0-terminal", json.RawMessage(termCleaning.raw))

	clientRun := integrationClient(t, h.URL, fRun.Token)
	assignmentRun, err := clientRun.Poll(ctx, 1, 1)
	if err != nil || !assignmentRun.Assigned {
		t.Fatalf("poll running fixture: %+v %v", assignmentRun, err)
	}
	stateRun := filepath.Join(fRun.EvidenceDir, "pitr-state")
	if err := os.Mkdir(stateRun, 0700); err != nil {
		t.Fatal(err)
	}
	ownerRun, err := newContainment(containmentConfig{CgroupRoot: os.Getenv("CLEARANCE_CGROUP_ROOT"), WorkspaceRoot: stateRun + "-workspaces", StateDir: stateRun, GracePeriod: 200 * time.Millisecond, KillTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	workRun, err := ownerRun.Prepare(assignmentRun)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := os.Stat(workRun.cgroupPath); os.IsNotExist(err) {
			_ = workRun.scrubWorkspace()
		} else if _, err := workRun.cleanup(); err != nil {
			t.Errorf("remove pitr running fixture: %v", err)
		}
	})
	if err := workRun.Start(); err != nil {
		t.Fatal(err)
	}
	seqRun := int64(0)
	sendRun := func(status ReportStatus, reason string) {
		t.Helper()
		seqRun++
		ack, err := clientRun.Report(ctx, integrationReport(fRun.controllerFixture, 1, seqRun, status))
		if err != nil || !ack.Accepted || ack.Reason != reason {
			t.Fatalf("running %s expected %s: %+v %v", status, reason, ack, err)
		}
	}
	sendRun(StatusStarting, "ok")
	sendRun(StatusRunning, "ok")
	pidsRun := captureLifecycleProcesses(t, fRun, workRun)
	assertBeforePartition(t, h, fRun, "RUNNING")

	// T0 backup: real PostgreSQL snapshot (backup schema) with committed
	// ownership plus terminal/running progress, no recovery authority yet.
	pitrBackup(t, h, backup)
	pitrWrite(t, drillDir, "t0-marker.json", map[string]any{
		"t0": "logical PostgreSQL backup via backup schema " + backup,
		"counts": pitrCounts(t, h), "authority": pitrAuthority(t, h),
		"terminate_allocation": fTerm.AllocationID, "running_allocation": fRun.AllocationID,
		"terminate_pids": pidsTerm, "running_pids": pidsRun,
	})
	pitrWrite(t, drillDir, "t0-database.json", map[string]any{
		"terminate": json.RawMessage(h.rows(t, fTerm).raw),
		"running":   json.RawMessage(h.rows(t, fRun).raw),
		"note":      "T0 owns terminal SUCCEEDED (terminate) plus live RUNNING plus idle AVAILABLE; physical orphans survive",
	})
	writeLifecycleEvidence(t, fTerm, "pitr-t0-host", map[string]any{"surviving_pids": pidsTerm, "cgroup": workTerm.cgroupPath, "workspace": workTerm.workspacePath})

	// T1: live host survival is the execution the rewind must forget.
	// Database holds history; Linux holds present truth.
	pitrWrite(t, drillDir, "t1-surviving.json", map[string]any{
		"terminate_orphan_survives": pidsTerm, "running_live": pidsRun,
		"note": "TERM-ignoring detached descendant plus dirty workspace survive; PostgreSQL cannot rewind them",
	})

	generationBefore := enterIdleRecovery(t, h)
	pitrWrite(t, drillDir, "pre-rewind-generation.json", map[string]any{"generation_before_rewind": generationBefore})
	pitrWrite(t, drillDir, "pre-restore-database.json", map[string]any{
		"terminate": json.RawMessage(h.rows(t, fTerm).raw),
		"running":   json.RawMessage(h.rows(t, fRun).raw),
		"authority": pitrAuthority(t, h),
	})

	// Rewind to T0: forget T1 progress beyond backup plus the pre-rewind
	// authority row. Physical cgroups/processes/workspaces are untouched.
	pitrRestore(t, h, backup)
	pitrWrite(t, drillDir, "rewind-marker.json", map[string]any{
		"rewound_to": "T0 backup schema " + backup, "forgot_generation": generationBefore,
		"method": "DELETE + INSERT SELECT (real PostgreSQL restore); host execution untouched",
	})
	pitrWrite(t, drillDir, "post-restore-database.json", map[string]any{
		"terminate": json.RawMessage(h.rows(t, fTerm).raw),
		"running":   json.RawMessage(h.rows(t, fRun).raw),
		"authority": pitrAuthority(t, h),
	})
	if got := pitrAuthority(t, h); got != "" {
		t.Fatalf("rewind must forget pre-rewind authority, got %s", got)
	}

	// Recovery boot: fresh random generation that never repeats the forgotten
	// value (UUIDv4, not a rewound DB increment), fleet-wide quarantine.
	generationAfter := enterIdleRecovery(t, h)
	if generationAfter == generationBefore {
		t.Fatal("post-rewind generation repeated pre-rewind value")
	}
	pitrWrite(t, drillDir, "generations.json", map[string]any{
		"generation_before_rewind": generationBefore, "generation_after_rewind": generationAfter,
		"source": "random UUIDv4 on recovery-mode boot; durable evidence is the fenced boot plus authority persistence",
	})
	termQuarantined := h.waitRows(t, fTerm, func(r lifecycleRows) bool { return r.Runner.State == "QUARANTINED" })
	runQuarantined := h.waitRows(t, fRun, func(r lifecycleRows) bool { return r.Runner.State == "QUARANTINED" })
	if termQuarantined.Runner.QuarantineReason == nil || !strings.Contains(*termQuarantined.Runner.QuarantineReason, generationAfter) {
		t.Fatalf("recovery quarantine reason must name generation: %s", termQuarantined.raw)
	}
	pitrWrite(t, drillDir, "recovery-boot-quarantine.json", map[string]any{
		"terminate": json.RawMessage(termQuarantined.raw), "running": json.RawMessage(runQuarantined.raw),
	})

	// Scheduling stays disabled for unreconciled runners: no new work.
	h.assertClaimRefused(t, fTerm)
	h.assertClaimRefused(t, fRun)
	idleBefore := idleRows(t, h, idle.RunnerID)
	if idleBefore.State != "QUARANTINED" {
		t.Fatalf("idle-at-backup runner must be quarantined after recovery boot: %s", idleBefore.raw)
	}
	if claim := claimIdleRunner(t, h, idle.RunnerID, submitIdleJob(t, h, "pitr-blocked")); claim.Assigned {
		t.Fatalf("quarantined idle runner accepted a claim: %+v", claim)
	}
	pitrWrite(t, drillDir, "claim-refusal-proof.json", map[string]any{"quarantined_claims_refused": true, "generation": generationAfter})

	// Stale-generation evidence makes no writes.
	beforeStale := h.rows(t, fTerm)
	staleHeartbeat := integrationReport(fTerm.controllerFixture, 1, seqTerm+100, StatusHeartbeat)
	if ack, err := clientTerm.Report(ctx, staleHeartbeat); err != nil || ack.Accepted || ack.Reason != "fenced_rejected" {
		t.Fatalf("superseded progress must be fenced: %+v %v", ack, err)
	}
	if after := h.rows(t, fTerm); !bytes.Equal(beforeStale.raw, after.raw) {
		t.Fatalf("stale progress mutated rows: before=%s after=%s", beforeStale.raw, after.raw)
	}
	beforeStaleIdle := idleRows(t, h, idle.RunnerID)
	pitrWrite(t, drillDir, "stale-zero-mutation.json", map[string]any{"stale_progress": "fenced_rejected", "idle_before": json.RawMessage(beforeStaleIdle.raw)})

	// Finished work needing cleanup: fresh observation directs termination,
	// then real graceful-then-forceful cleanup (SIGTERM ignored, cgroup.kill
	// SIGKILL) with reaping plus workspace scrub gates release.
	_, discoveryTerm, err := ownerTerm.Discover(assignmentTerm)
	if err != nil {
		t.Fatalf("fresh discovery before reconcile: %v", err)
	}
	evidenceTerm := BuildReconcileEvidence(discoveryTerm, nil)
	seqTerm++
	reconcileTerm := integrationReport(fTerm.controllerFixture, 1, seqTerm, StatusReconcile)
	reconcileTerm.Reconcile = &evidenceTerm
	reconcileTerm.RecoveryGeneration = &generationAfter
	if ack, err := clientTerm.Report(ctx, reconcileTerm); err != nil || !ack.Accepted || ack.Reason != "reconcile_cleanup_required" {
		t.Fatalf("dirty terminal must direct cleanup: %+v %v", ack, err)
	}
	termDirected := waitClassification(t, h, fTerm, "FINISHED_NEEDS_CLEANUP", "TERMINATE_CLEANUP")
	pitrWrite(t, drillDir, "desired-vs-observed-terminate.json", map[string]any{
		"observed_pids": evidenceTerm.PIDs, "cgroup_present": evidenceTerm.CgroupPresent,
		"classification": "FINISHED_NEEDS_CLEANUP", "action": "TERMINATE_CLEANUP",
		"database": json.RawMessage(termDirected.raw),
	})
	h.assertClaimRefused(t, fTerm)
	beganCleanup := time.Now()
	proofTerm, err := workTerm.Cleanup()
	if err != nil || !positiveCleanup(&proofTerm) {
		t.Fatalf("pitr cleanup must be positive: %+v %v", proofTerm, err)
	}
	if time.Since(beganCleanup) < 150*time.Millisecond {
		t.Fatal("SIGKILL escalation occurred before the graceful bound")
	}
	assertLifecycleGone(t, fTerm, workTerm, pidsTerm)
	seqTerm++
	cleanupTerm := integrationReport(fTerm.controllerFixture, 1, seqTerm, StatusCleanup)
	cleanupTerm.Cleanup = &proofTerm
	cleanupTerm.RecoveryGeneration = &generationAfter
	if ack, err := clientTerm.Report(ctx, cleanupTerm); err != nil || !ack.Accepted || ack.Reason != "ok" {
		t.Fatalf("verified cleanup must release: %+v %v", ack, err)
	}
	releasedTerm := h.waitRows(t, fTerm, func(r lifecycleRows) bool { return r.Allocation.State == "RELEASED" })
	if releasedTerm.Runner.State != "AVAILABLE" {
		t.Fatalf("reconciled runner not reusable: %s", releasedTerm.raw)
	}
	pitrWrite(t, drillDir, "cleanup-attestation.json", map[string]any{
		"cleanup": proofTerm, "grace_ms": 200, "forced": true, "reaped_pids": pidsTerm,
		"database": json.RawMessage(releasedTerm.raw),
	})
	writeLifecycleEvidence(t, fTerm, "pitr-cleanup-proof", proofTerm)
	if _, err := h.claim(fTerm); err != nil {
		t.Fatalf("reconciled runner must accept next claim: %v", err)
	}

	// Still-running live execution is recognized without release.
	_, discoveryRun, err := ownerRun.Discover(assignmentRun)
	if err != nil {
		t.Fatalf("fresh running discovery: %v", err)
	}
	evidenceRun := BuildReconcileEvidence(discoveryRun, nil)
	seqRun++
	reconcileRun := integrationReport(fRun.controllerFixture, 1, seqRun, StatusReconcile)
	reconcileRun.Reconcile = &evidenceRun
	reconcileRun.RecoveryGeneration = &generationAfter
	if ack, err := clientRun.Report(ctx, reconcileRun); err != nil || !ack.Accepted || ack.Reason != "reconcile_still_running" {
		t.Fatalf("live execution must stay running: %+v %v", ack, err)
	}
	stillRunning := waitClassification(t, h, fRun, "STILL_RUNNING", "KEEP")
	if stillRunning.Runner.State != "QUARANTINED" {
		t.Fatalf("still-running must stay quarantined: %s", stillRunning.raw)
	}
	h.assertClaimRefused(t, fRun)
	pitrWrite(t, drillDir, "still-running-evidence.json", map[string]any{
		"observed_pids": evidenceRun.PIDs, "classification": "STILL_RUNNING", "database": json.RawMessage(stillRunning.raw),
	})
	// Leave the still-running workload for teardown cleanup without new proof.
	if err := os.WriteFile(filepath.Join(fRun.EvidenceDir, "finish"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := workRun.Wait(); err != nil {
		t.Fatalf("running workload did not finish for teardown: %v", err)
	}
	if _, err := workRun.Cleanup(); err != nil {
		t.Fatalf("teardown running fixture: %v", err)
	}

	// Idle-at-backup runner via the real daemon: fresh idle observation
	// attests only when physically clean, then advances the generation.
	fIdle := idleLifecycleFixture(idle)
	cgroupIdle := filepath.Join(os.Getenv("CLEARANCE_CGROUP_ROOT"), fmt.Sprintf("pitr-idle-%d", os.Getpid()))
	if err := os.MkdirAll(cgroupIdle, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(cgroupIdle) })
	dIdle, _, _ := idleDaemon(t, h, idle, cgroupIdle, "")
	stopIdle := startIntegrationDaemon(t, dIdle)
	attestedIdle := waitIdleRows(t, h, idle.RunnerID, func(rows idleRunnerRows) bool {
		return rows.State == "AVAILABLE" && rows.ReconciledGeneration != nil && *rows.ReconciledGeneration == generationAfter
	})
	pitrWrite(t, drillDir, "idle-attestation.json", map[string]any{"idle": json.RawMessage(attestedIdle.raw)})
	writeLifecycleEvidence(t, fIdle, "pitr-idle-attested", json.RawMessage(attestedIdle.raw))
	stopIdle()
	if claim := claimIdleRunner(t, h, idle.RunnerID, submitIdleJob(t, h, "pitr-idle-next")); !claim.Assigned || claim.RunnerEpoch != 1 {
		t.Fatalf("advanced idle runner not claimable: %+v", claim)
	}

	// Repeated rewind still issues a new generation.
	pitrRestore(t, h, backup)
	generationThird := enterIdleRecovery(t, h)
	if generationThird == generationBefore || generationThird == generationAfter {
		t.Fatal("repeated rewind must issue a still-new generation")
	}
	pitrWrite(t, drillDir, "repeated-rewind-generations.json", map[string]any{
		"first_post_restore": generationAfter, "second_post_restore": generationThird, "pre_rewind": generationBefore,
	})
	pitrWrite(t, drillDir, "final-database.json", map[string]any{
		"terminate": json.RawMessage(h.rows(t, fTerm).raw),
		"authority": pitrAuthority(t, h),
	})
	t.Logf("pitr drill: backup %s, T1 live %v, rewind forgot %s, recovery %s then %s, graceful-then-forceful cleanup attested", backup, pidsTerm, generationBefore, generationAfter, generationThird)
}
