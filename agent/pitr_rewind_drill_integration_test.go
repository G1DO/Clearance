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
	"strconv"
	"strings"
	"testing"
	"time"
)

// Rewind drill for issues #34 and #40: logic-only precursor rehearsal (NOT physical
// PITR; closes #40 together with the physical drill) with a rows-only full-schema backup at T0, real Linux
// execution at T1 including post-T0 CREATE TABLE/SEQUENCE probe writes, controller stop
// with negative claim proof, logic-only rewind to T0 rows, new-OS/JVM-process
// recovery boot with a fresh non-repeating generation, fleet quarantine without scheduling,
// verified reconciliation with graceful-then-forceful reaping, and repeated-rewind freshness.
//
// Uses the real controller, a rows-only full-schema backup (backup schema via CREATE TABLE
// AS TABLE WITH DATA for every table in the harness schema: rows only, no PK/FK/indexes/
// defaults/identity/views/functions/types) plus a WAL restore point
// (pg_create_restore_point) with LSN/timeline/WAL-file markers checked as hygiene only
// (pg_current_wal_lsn ordering, restore-after-start, no-branch guard), not consumed by
// restore and not AC1 timeline-history validation; logic-only restore to T0 rows
// via single-transaction DELETE + INSERT SELECT with session_replication_role=replica plus
// drop of post-backup tables/sequences only (DROP/ALTER of pre-existing objects and
// sequence-value reset are NOT covered), recovery boot in a
// new OS/JVM child process with clearance.recovery-mode=true exercising RecoveryBootRunner
// (POST /recovery-boot-process stops serving, boots the child, asserts from its retained
// boot log, then recreates serving; control plane stays up throughout), and real Linux
// cgroup/workspace execution (manual containment for allocated runners plus the real Go
// daemon for idle-at-backup runners). Cluster-level pg_basebackup + WAL replay with timeline
// branching plus history-file validation is the production runbook and is NOT exercised here;
// this drill is a logic-only precursor rehearsing the backup/rewind/boot sequence, not the
// same safety property as real PITR restore/replay.
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

func pitrTables(t *testing.T, h lifecycleHarness) []string {
	t.Helper()
	raw := pitrPsql(t, h.Schema, fmt.Sprintf(
		"SELECT table_name FROM information_schema.tables WHERE table_schema='%s' AND table_type='BASE TABLE' AND table_name <> 'flyway_schema_history' ORDER BY table_name",
		h.Schema))
	tables := []string{}
	for _, line := range strings.Split(raw, "\n") {
		name := strings.TrimSpace(line)
		if name != "" {
			tables = append(tables, name)
		}
	}
	if len(tables) == 0 {
		t.Fatalf("no tables discovered in schema %s for logical full-schema backup", h.Schema)
	}
	return tables
}

func pitrLsn(t *testing.T, h lifecycleHarness) string {
	t.Helper()
	return pitrPsql(t, h.Schema, "SELECT pg_current_wal_lsn()::text")
}

func pitrTimeline(t *testing.T, h lifecycleHarness) string {
	t.Helper()
	// Current WAL timeline, recorded alongside each LSN marker and checked for no
	// unexpected branch across the drill window (no-branch guard only, see
	// pitr_timeline.go; NOT AC1 timeline-history validation).
	return pitrPsql(t, h.Schema, "SELECT timeline_id::text FROM pg_control_checkpoint()")
}

func pitrWalFile(t *testing.T, h lifecycleHarness) string {
	t.Helper()
	// Recorded only; never validated for restore selection (logic-only precursor).
	return pitrPsql(t, h.Schema, "SELECT pg_walfile_name(pg_current_wal_lsn())")
}

func pitrRestorePoint(t *testing.T, h lifecycleHarness, name string) string {
	t.Helper()
	safe := ""
	for _, r := range name {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			safe += string(r)
		} else {
			safe += "_"
		}
	}
	return pitrPsql(t, h.Schema, fmt.Sprintf("SELECT pg_create_restore_point('%s')::text", safe))
}

func pitrSequences(t *testing.T, h lifecycleHarness) []string {
	t.Helper()
	raw := pitrPsql(t, h.Schema, fmt.Sprintf(
		"SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema='%s' ORDER BY sequence_name",
		h.Schema))
	seqs := []string{}
	for _, line := range strings.Split(raw, "\n") {
		name := strings.TrimSpace(line)
		if name != "" {
			seqs = append(seqs, name)
		}
	}
	return seqs
}

func pitrCreateDdlProbe(t *testing.T, h lifecycleHarness) {
	t.Helper()
	q := pitrQuoteIdent(h.Schema)
	pitrPsql(t, h.Schema, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s CASCADE", q, pitrQuoteIdent("pitr_drill_ddl_probe")))
	pitrPsql(t, h.Schema, fmt.Sprintf("DROP SEQUENCE IF EXISTS %s.%s CASCADE", q, pitrQuoteIdent("pitr_drill_seq_probe")))
	pitrPsql(t, h.Schema, fmt.Sprintf("CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data TEXT NOT NULL)", q, pitrQuoteIdent("pitr_drill_ddl_probe")))
	pitrPsql(t, h.Schema, fmt.Sprintf("CREATE INDEX %s ON %s.%s (data)", pitrQuoteIdent("ix_pitr_drill_ddl_probe_data"), q, pitrQuoteIdent("pitr_drill_ddl_probe")))
	pitrPsql(t, h.Schema, fmt.Sprintf("CREATE SEQUENCE %s.%s START 100", q, pitrQuoteIdent("pitr_drill_seq_probe")))
	pitrPsql(t, h.Schema, fmt.Sprintf("SELECT nextval('%s.%s')", h.Schema, "pitr_drill_seq_probe"))
	pitrPsql(t, h.Schema, fmt.Sprintf("SELECT nextval('%s.%s')", h.Schema, "pitr_drill_seq_probe"))
	pitrPsql(t, h.Schema, fmt.Sprintf("INSERT INTO %s.%s (id, data) VALUES (nextval('%s.%s'), 't1')", q, pitrQuoteIdent("pitr_drill_ddl_probe"), h.Schema, "pitr_drill_seq_probe"))
}

func pitrDdlProbeState(t *testing.T, h lifecycleHarness) map[string]any {
	t.Helper()
	q := pitrQuoteIdent(h.Schema)
	table := pitrPsql(t, h.Schema, fmt.Sprintf("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='%s' AND table_name='pitr_drill_ddl_probe'", h.Schema))
	seq := pitrPsql(t, h.Schema, fmt.Sprintf("SELECT COUNT(*) FROM information_schema.sequences WHERE sequence_schema='%s' AND sequence_name='pitr_drill_seq_probe'", h.Schema))
	state := map[string]any{"table_count": table, "sequence_count": seq}
	if strings.TrimSpace(table) != "0" {
		state["rows"] = pitrPsql(t, h.Schema, fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", q, pitrQuoteIdent("pitr_drill_ddl_probe")))
	}
	return state
}

func pitrAssertDdlRewound(t *testing.T, h lifecycleHarness, drillDir string) {
	t.Helper()
	table := pitrPsql(t, h.Schema, fmt.Sprintf("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='%s' AND table_name='pitr_drill_ddl_probe'", h.Schema))
	seq := pitrPsql(t, h.Schema, fmt.Sprintf("SELECT COUNT(*) FROM information_schema.sequences WHERE sequence_schema='%s' AND sequence_name='pitr_drill_seq_probe'", h.Schema))
	if strings.TrimSpace(table) != "0" || strings.TrimSpace(seq) != "0" {
		t.Fatalf("rewind must drop post-backup DDL probe (table=%s sequence=%s)", table, seq)
	}
	pitrWrite(t, drillDir, "ddl-probe-rewound.json", map[string]any{"table_exists": false, "sequence_exists": false, "note": "only post-T0 CREATE TABLE/SEQUENCE drop is covered; DROP/ALTER of pre-existing objects and pre-existing sequence values are NOT covered"})
}

func pitrAssertOrdered(t *testing.T, lsns []string) {
	t.Helper()
	if err := assertPitrOrdered(lsns); err != nil {
		t.Fatal(err)
	}
}

func pitrAssertStrictlyOrdered(t *testing.T, before, after, context string) {
	t.Helper()
	if err := assertPitrStrictlyOrdered(before, after, context); err != nil {
		t.Fatal(err)
	}
}

func pitrAssertTimeline(t *testing.T, timelines []string) {
	t.Helper()
	if err := assertPitrNoTimelineBranch(timelines); err != nil {
		t.Fatal(err)
	}
}

func pitrAssertRestoreReachable(t *testing.T, backupStart, restorePoint, backupDone string) {
	t.Helper()
	if err := assertPitrRestorePointReachable(backupStart, restorePoint, backupDone); err != nil {
		t.Fatal(err)
	}
}

func pitrQuoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func pitrBackup(t *testing.T, h lifecycleHarness, backup string) []string {
	t.Helper()
	// Rows-only full-schema backup in one transaction on one PostgreSQL session with a
	// repeatable snapshot, so concurrent heartbeat-evaluator writes cannot tear the
	// T0 view across tables. CREATE TABLE AS WITH DATA copies rows only (no
	// PK/FK/indexes/defaults/identity/views/functions/types). Together with the WAL
	// restore point recorded by the caller this is a logic-only precursor rehearsal for
	// issue #40 (closes #40 together with the physical drill): markers are hygiene-only and the later
	// logical restore does not consume the restore-point LSN.
	tables := pitrTables(t, h)
	var sb strings.Builder
	sb.WriteString("BEGIN ISOLATION LEVEL REPEATABLE READ; ")
	fmt.Fprintf(&sb, "DROP SCHEMA IF EXISTS %s CASCADE; ", pitrQuoteIdent(backup))
	fmt.Fprintf(&sb, "CREATE SCHEMA %s; ", pitrQuoteIdent(backup))
	for _, table := range tables {
		fmt.Fprintf(&sb, "CREATE TABLE %s.%s AS TABLE %s.%s WITH DATA; ",
			pitrQuoteIdent(backup), pitrQuoteIdent(table), pitrQuoteIdent(h.Schema), pitrQuoteIdent(table))
	}
	sb.WriteString("COMMIT;")
	pitrPsql(t, h.Schema, sb.String())
	return tables
}

func pitrRestore(t *testing.T, h lifecycleHarness, backup string, backupSequences []string, backupTables []string) {
	t.Helper()
	// Logic-only restore to T0 rows in one transaction on one PostgreSQL session:
	// SET LOCAL session_replication_role applies to this transaction only, so FK order is
	// irrelevant and the rewind is atomic. Tables/sequences created after T0 (including the
	// DDL probe) are dropped; pre-existing tables get DELETE + INSERT SELECT (rows only,
	// definitions/constraints/indexes not restored, pre-existing sequence values not reset,
	// a T0 table dropped at T1 is NOT recreated). This does NOT consume the restore-point
	// LSN and performs no WAL replay / timeline branch (WAL markers are hygiene-only).
	// Host cgroups/processes/workspaces are untouched; only database rows return to T0.
	// Logic-only precursor for issue #40 (closes #40 together with the physical drill).
	tables := pitrTables(t, h)
	backupRaw := pitrPsql(t, h.Schema, fmt.Sprintf(
		"SELECT table_name FROM information_schema.tables WHERE table_schema='%s' AND table_type='BASE TABLE' ORDER BY table_name",
		strings.ToLower(backup)))
	backed := []string{}
	for _, line := range strings.Split(backupRaw, "\n") {
		name := strings.TrimSpace(line)
		if name != "" {
			backed = append(backed, name)
		}
	}
	backedSet := map[string]bool{}
	for _, table := range backed {
		backedSet[table] = true
	}
	// Fail fast if the backup schema drifted from T0 (mirrors the Java drill guard):
	// silently resurrecting/dropping tables from a mutated backup would rewind wrong.
	if len(backed) != len(backupTables) {
		t.Fatalf("backup schema drifted from T0 (expected %v got %v)", backupTables, backed)
	}
	for _, table := range backupTables {
		if !backedSet[table] {
			t.Fatalf("backup schema drifted from T0 (expected %v got %v)", backupTables, backed)
		}
	}
	seqRaw := pitrPsql(t, h.Schema, fmt.Sprintf(
		"SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema='%s' ORDER BY sequence_name",
		h.Schema))
	currentSeqs := []string{}
	for _, line := range strings.Split(seqRaw, "\n") {
		name := strings.TrimSpace(line)
		if name != "" {
			currentSeqs = append(currentSeqs, name)
		}
	}
	backupSeqSet := map[string]bool{}
	for _, s := range backupSequences {
		backupSeqSet[s] = true
	}
	var sb strings.Builder
	sb.WriteString("BEGIN; SET LOCAL session_replication_role = 'replica'; ")
	for _, table := range tables {
		if !backedSet[table] {
			fmt.Fprintf(&sb, "DROP TABLE %s.%s CASCADE; ", pitrQuoteIdent(h.Schema), pitrQuoteIdent(table))
		}
	}
	for _, table := range tables {
		if backedSet[table] {
			fmt.Fprintf(&sb, "DELETE FROM %s.%s; ", pitrQuoteIdent(h.Schema), pitrQuoteIdent(table))
		}
	}
	for _, table := range backed {
		fmt.Fprintf(&sb, "INSERT INTO %s.%s SELECT * FROM %s.%s; ",
			pitrQuoteIdent(h.Schema), pitrQuoteIdent(table), pitrQuoteIdent(backup), pitrQuoteIdent(table))
	}
	for _, seq := range currentSeqs {
		if !backupSeqSet[seq] {
			fmt.Fprintf(&sb, "DROP SEQUENCE %s.%s; ", pitrQuoteIdent(h.Schema), pitrQuoteIdent(seq))
		}
	}
	sb.WriteString("COMMIT;")
	pitrPsql(t, h.Schema, sb.String())
}

func pitrAuthority(t *testing.T, h lifecycleHarness) string {
	t.Helper()
	return pitrPsql(t, h.Schema, fmt.Sprintf("SELECT COALESCE(current_generation::text,'') FROM %s.recovery_authority WHERE singleton", pitrQuoteIdent(h.Schema)))
}

// pitrUUIDv7TimestampMs parses a recovery generation UUID string and returns
// the leading 48-bit UUIDv7 Unix timestamp in milliseconds, asserting the
// value is a time-ordered UUIDv7. The physical drill runs against the real
// controller, so monotonic freshness on this path is proven from the issued
// values themselves (external monotonic source: wall-clock plus the
// rewind-surviving log); file-level log evidence is proven by
// RecoveryMonotonicGenerationIntegrationTest and the controller
// PitrRewindDrillIntegrationTest.
func pitrUUIDv7TimestampMs(t *testing.T, generation string) int64 {
	t.Helper()
	if !isValidUUID(generation) {
		t.Fatalf("recovery generation is not a valid UUID: %s", generation)
	}
	parts := strings.Split(generation, "-")
	if len(parts) != 5 || len(parts[0]) != 8 || len(parts[1]) != 4 || len(parts[2]) != 4 {
		t.Fatalf("recovery generation has bad UUID layout: %s", generation)
	}
	if parts[2][0] != '7' {
		t.Fatalf("recovery generation is not time-ordered UUIDv7 (version %c): %s", parts[2][0], generation)
	}
	timestamp, err := strconv.ParseUint(parts[0]+parts[1], 16, 64)
	if err != nil {
		t.Fatalf("recovery generation has unparsable UUIDv7 timestamp %s: %v", generation, err)
	}
	return int64(timestamp)
}

// pitrAssertStrictlyNewer fails the drill unless after is a strictly newer
// time-ordered UUIDv7 than before, per the external monotonic source.
func pitrAssertStrictlyNewer(t *testing.T, before, after string) {
	t.Helper()
	beforeMs := pitrUUIDv7TimestampMs(t, before)
	afterMs := pitrUUIDv7TimestampMs(t, after)
	if afterMs <= beforeMs {
		t.Fatalf("post-rewind generation %s (uuidv7 timestamp %d) is not strictly newer than %s (timestamp %d)",
			after, afterMs, before, beforeMs)
	}
}

func pitrCounts(t *testing.T, h lifecycleHarness) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, table := range pitrTables(t, h) {
		out[table] = pitrPsql(t, h.Schema, fmt.Sprintf("SELECT count(*) FROM %s.%s", pitrQuoteIdent(h.Schema), pitrQuoteIdent(table)))
	}
	return out
}

// enterRecoveryViaOsProcess boots a new OS/JVM controller process with
// clearance.recovery-mode=true against the restored DB (harness POST
// /recovery-boot-process), then asserts freshness and quarantine from the retained
// child boot log, as the Java drill does for boot. Returns the child generation and
// its boot log path. Serving is recreated afterwards by the harness.
func enterRecoveryViaOsProcess(t *testing.T, h lifecycleHarness) (string, string) {
	t.Helper()
	data, status, err := lifecycleHTTP(h.ControlURL+"/recovery-boot-process", "X-Harness-Token", h.ControlToken,
		"POST", map[string]string{})
	if err != nil || status != 200 {
		t.Fatalf("os-process recovery boot: status=%d body=%s error=%v", status, data, err)
	}
	var response struct {
		Generation string `json:"generation"`
		Restarted  bool   `json:"restarted"`
		OsProcess  bool   `json:"os_process"`
		Boot       string `json:"boot"`
		BootLog    string `json:"boot_log"`
	}
	if err := json.Unmarshal(data, &response); err != nil || !isValidUUID(response.Generation) || !response.Restarted || !response.OsProcess {
		t.Fatalf("invalid os-process recovery boot: %s %v", data, err)
	}
	generation := strings.ToLower(response.Generation)
	log, err := os.ReadFile(response.BootLog)
	if err != nil || !strings.Contains(string(log), "Recovery-mode boot requested") || !strings.Contains(string(log), generation) {
		t.Fatalf("child boot log missing RecoveryBootRunner evidence: %s %v", response.BootLog, err)
	}
	return generation, response.BootLog
}

// stopController stops the serving Spring controller while keeping the harness control
// plane up, so the rewind-to-reboot window serves no claims while the authority is empty.
func stopController(t *testing.T, h lifecycleHarness) {
	t.Helper()
	data, status, err := lifecycleHTTP(h.ControlURL+"/stop-controller", "X-Harness-Token", h.ControlToken,
		"POST", map[string]string{})
	if err != nil || status != 200 {
		t.Fatalf("stop controller: status=%d body=%s error=%v", status, data, err)
	}
}

// assertNoClaimsWhileStopped proves the rewind window grants no work: a claim attempt
// against the stopped controller must not assign, mirroring the Java drill's
// stopped-controller negative check.
func assertNoClaimsWhileStopped(t *testing.T, h lifecycleHarness, drillDir, artifact, runnerID, jobID string) {
	t.Helper()
	data, status, err := lifecycleHTTP(h.ControlURL+"/claim", "X-Harness-Token", h.ControlToken,
		"POST", map[string]string{"runner_id": runnerID, "job_id": jobID})
	var claim struct {
		Assigned bool `json:"assigned"`
	}
	if err == nil && status == 200 && json.Unmarshal(data, &claim) == nil && claim.Assigned {
		t.Fatalf("rewind-window claim granted work while controller stopped: %s", data)
	}
	pitrWrite(t, drillDir, artifact, map[string]any{
		"serving": false, "claim_in_window": "refused",
		"status": status, "body": string(data),
		"note": "harness Spring controller stopped before rewind (control plane stays up); claim attempt fails so no unreconciled runner receives work while authority is empty",
	})
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
	t.Cleanup(func() {
		pitrPsql(t, h.Schema, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", pitrQuoteIdent(backup)))
		pitrPsql(t, h.Schema, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s CASCADE", pitrQuoteIdent(h.Schema), pitrQuoteIdent("pitr_drill_ddl_probe")))
		pitrPsql(t, h.Schema, fmt.Sprintf("DROP SEQUENCE IF EXISTS %s.%s CASCADE", pitrQuoteIdent(h.Schema), pitrQuoteIdent("pitr_drill_seq_probe")))
	})
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

	// T0 backup: rows-only full-schema backup (backup schema with every table in the
	// harness schema, rows only) with committed ownership plus terminal/running progress, no
	// recovery authority yet, plus a WAL restore point with LSN/timeline/WAL-file markers
	// checked as hygiene only (pg_current_wal_lsn ordering, restore-after-start, no-branch
	// guard; WAL file recorded only, restore does not consume the LSN). The restore-point
	// LSN is excluded from pg_current_wal_lsn ordering (post-point LSN can lag the restore
	// LSN). Logic-only precursor for issue #40 (closes #40 together with the physical drill).
	backupLsn := pitrLsn(t, h)
	backupTimeline := pitrTimeline(t, h)
	backupWalFile := pitrWalFile(t, h)
	backupTables := pitrBackup(t, h, backup)
	backupSequences := pitrSequences(t, h)
	restorePointLsn := pitrRestorePoint(t, h, fmt.Sprintf("pitr_T0_%d", time.Now().UnixMilli()))
	backupDoneLsn := pitrLsn(t, h)
	backupDoneTimeline := pitrTimeline(t, h)
	backupDoneWalFile := pitrWalFile(t, h)
	pitrAssertOrdered(t, []string{backupLsn, backupDoneLsn})
	pitrAssertRestoreReachable(t, backupLsn, restorePointLsn, backupDoneLsn)
	pitrAssertTimeline(t, []string{backupTimeline, backupDoneTimeline})
	pitrWrite(t, drillDir, "t0-marker.json", map[string]any{
		"t0":                  "logic-only precursor rows-only full-schema backup via backup schema " + backup + " (CREATE TABLE AS TABLE WITH DATA rows only for every harness-schema user table excluding flyway_schema_history; no WAL replay / timeline branch; closes #40 together with the physical drill) plus WAL restore point with hygiene-only marker checks (WAL file recorded only, restore does not consume LSN)",
		"backup_tables":       backupTables,
		"backup_sequences":    backupSequences,
		"restore_point_lsn":   restorePointLsn,
		"timeline_backup_lsn": backupLsn, "timeline_backup_timeline": backupTimeline, "timeline_backup_walfile": backupWalFile,
		"timeline_backup_done_lsn": backupDoneLsn, "timeline_backup_done_timeline": backupDoneTimeline, "timeline_backup_done_walfile": backupDoneWalFile,
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
	// Database holds history; Linux holds present truth. Post-T0 CREATE TABLE/SEQUENCE
	// probe writes after T0 are dropped by the rewind (only that narrow case is covered;
	// DROP/ALTER of pre-existing objects and pre-existing sequence values are NOT covered).
	pitrCreateDdlProbe(t, h)
	pitrWrite(t, drillDir, "t1-ddl-probe.json", pitrDdlProbeState(t, h))
	pitrWrite(t, drillDir, "t1-surviving.json", map[string]any{
		"terminate_orphan_survives": pidsTerm, "running_live": pidsRun,
		"note": "TERM-ignoring detached descendant plus dirty workspace survive; PostgreSQL cannot rewind them",
	})
	preT1Lsn := pitrLsn(t, h)
	preT1Timeline := pitrTimeline(t, h)
	pitrAssertOrdered(t, []string{backupDoneLsn, preT1Lsn})
	pitrAssertTimeline(t, []string{backupTimeline, backupDoneTimeline, preT1Timeline})

	generationBefore, preRewindBootLog := enterRecoveryViaOsProcess(t, h)
	preRewindLsn := pitrLsn(t, h)
	preRewindTimeline := pitrTimeline(t, h)
	preRewindWalFile := pitrWalFile(t, h)
	pitrAssertOrdered(t, []string{backupDoneLsn, preT1Lsn, preRewindLsn})
	pitrAssertTimeline(t, []string{backupTimeline, backupDoneTimeline, preT1Timeline, preRewindTimeline})
	pitrAssertRestoreReachable(t, backupLsn, restorePointLsn, backupDoneLsn)
	pitrWrite(t, drillDir, "pre-rewind-generation.json", map[string]any{
		"generation_before_rewind": generationBefore,
		"boot":                     "RecoveryBootRunner with clearance.recovery-mode=true in a new OS/JVM child process via POST /recovery-boot-process (boot log retained)",
		"timeline_lsn":             preRewindLsn,
		"timeline_id":              preRewindTimeline,
		"timeline_walfile":         preRewindWalFile,
		"restore_point_lsn":        restorePointLsn,
		"boot_log":                 preRewindBootLog,
	})
	pitrWrite(t, drillDir, "pre-restore-database.json", map[string]any{
		"terminate": json.RawMessage(h.rows(t, fTerm).raw),
		"running":   json.RawMessage(h.rows(t, fRun).raw),
		"authority": pitrAuthority(t, h),
	})

	// Stop the serving controller before rewind (control plane stays up): with the
	// authority about to be empty, a live instance would grant legacy-availability
	// claims on rewound rows. Prove the window serves nothing, then rewind with
	// nobody serving. The claim job is submitted while live, since submit needs it.
	windowJob := submitIdleJob(t, h, "pitr-window")
	stopController(t, h)
	assertNoClaimsWhileStopped(t, h, drillDir, "controller-stopped-proof.json", idle.RunnerID, windowJob)

	// Logic-only rewind to T0 rows with the controller stopped: forget T1 progress
	// beyond backup (including the post-T0 CREATE probe) plus the pre-rewind authority row.
	// Physical cgroups/processes/workspaces are untouched. NOT a PITR restore/replay to a
	// restore-point LSN (no base backup / WAL replay / timeline branch; issue #40 open).
	rewindLsn := pitrLsn(t, h)
	rewindTimeline := pitrTimeline(t, h)
	rewindWalFile := pitrWalFile(t, h)
	pitrAssertOrdered(t, []string{preRewindLsn, rewindLsn})
	pitrAssertTimeline(t, []string{backupTimeline, preRewindTimeline, rewindTimeline})
	pitrRestore(t, h, backup, backupSequences, backupTables)
	postRestoreLsn := pitrLsn(t, h)
	postRestoreTimeline := pitrTimeline(t, h)
	postRestoreWalFile := pitrWalFile(t, h)
	pitrAssertStrictlyOrdered(t, rewindLsn, postRestoreLsn, "restore writes")
	pitrAssertTimeline(t, []string{backupTimeline, backupDoneTimeline, preT1Timeline, preRewindTimeline, rewindTimeline, postRestoreTimeline})
	pitrAssertRestoreReachable(t, backupLsn, restorePointLsn, backupDoneLsn)
	pitrWrite(t, drillDir, "timeline-validation.json", map[string]any{
		"backup_lsn": backupLsn, "restore_point_lsn": restorePointLsn, "backup_done_lsn": backupDoneLsn,
		"pre_rewind_lsn": preRewindLsn, "rewind_lsn": rewindLsn, "post_restore_lsn": postRestoreLsn,
		"timeline":       postRestoreTimeline,
		"backup_walfile": backupWalFile, "backup_done_walfile": backupDoneWalFile,
		"pre_rewind_walfile": preRewindWalFile, "rewind_walfile": rewindWalFile,
		"post_restore_walfile": postRestoreWalFile, "result": "ordered pg_current_wal_lsn, restore-after-start, no-branch (hygiene-only; NOT AC1 timeline-history validation; WAL files recorded only; logical restore did not consume LSN; restore <= backupDone NOT required)",
		"note": "logic-only precursor: no base backup / WAL replay / timeline branch; closes #40 together with the physical drill",
	})
	pitrWrite(t, drillDir, "rewind-marker.json", map[string]any{
		"rewound_to": "T0 rows from backup schema " + backup + " (logic-only precursor; recorded restore point " + restorePointLsn + " NOT consumed by restore; no WAL replay / timeline branch)", "forgot_generation": generationBefore,
		"restore_point_lsn":   restorePointLsn,
		"timeline_rewind_lsn": rewindLsn, "timeline_rewind_timeline": rewindTimeline, "timeline_rewind_walfile": rewindWalFile,
		"timeline_post_restore_lsn": postRestoreLsn, "timeline_post_restore_timeline": postRestoreTimeline, "timeline_post_restore_walfile": postRestoreWalFile,
		"method": "logic-only rows-only restore to T0 rows: DELETE + INSERT SELECT in one transaction with session_replication_role=replica plus drop of post-backup tables/sequences only (DROP/ALTER of pre-existing objects and sequence values NOT covered); host execution untouched; closes #40 together with the physical drill",
	})
	pitrAssertDdlRewound(t, h, drillDir)
	pitrWrite(t, drillDir, "post-restore-database.json", map[string]any{
		"terminate": json.RawMessage(h.rows(t, fTerm).raw),
		"running":   json.RawMessage(h.rows(t, fRun).raw),
		"authority": pitrAuthority(t, h),
	})
	if got := pitrAuthority(t, h); got != "" {
		t.Fatalf("rewind must forget pre-rewind authority, got %s", got)
	}

	// Recovery boot starts a new OS/JVM child process with
	// clearance.recovery-mode=true, exercising RecoveryBootRunner: fresh time-ordered
	// UUIDv7 from the external monotonic source (wall-clock plus rewind-surviving log
	// outside PostgreSQL) that is strictly newer than the forgotten value, fleet-wide
	// quarantine, asserted from the retained child boot log.
	generationAfter, postRestoreBootLog := enterRecoveryViaOsProcess(t, h)
	if generationAfter == generationBefore {
		t.Fatal("post-rewind generation repeated pre-rewind value")
	}
	pitrAssertStrictlyNewer(t, generationBefore, generationAfter)
	tsBefore := pitrUUIDv7TimestampMs(t, generationBefore)
	tsAfter := pitrUUIDv7TimestampMs(t, generationAfter)
	pitrWrite(t, drillDir, "generations.json", map[string]any{
		"generation_before_rewind": generationBefore, "generation_after_rewind": generationAfter,
		"generation_before_timestamp_ms": tsBefore, "generation_after_timestamp_ms": tsAfter,
		"boot":     "RecoveryBootRunner with clearance.recovery-mode=true in a new OS/JVM child process via POST /recovery-boot-process (boot log retained)",
		"boot_log": postRestoreBootLog,
		"source":   "time-ordered UUIDv7 from the external monotonic source plus rewind-surviving log; file-level log evidence is proven by RecoveryMonotonicGenerationIntegrationTest and the controller PitrRewindDrillIntegrationTest",
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

	// Repeated rewind still issues a strictly newer time-ordered UUIDv7 via another
	// new-OS/JVM-process recovery boot.
	// Same protection as the first rewind: stop serving first (the earlier window job
	// was rewound away with the jobs table, so submit a fresh one while live).
	windowJob2 := submitIdleJob(t, h, "pitr-window-2")
	stopController(t, h)
	assertNoClaimsWhileStopped(t, h, drillDir, "repeated-rewind-stopped-proof.json", idle.RunnerID, windowJob2)
	repeatedRewindLsn := pitrLsn(t, h)
	repeatedRewindTimeline := pitrTimeline(t, h)
	pitrAssertTimeline(t, []string{backupTimeline, repeatedRewindTimeline})
	pitrRestore(t, h, backup, backupSequences, backupTables)
	pitrAssertStrictlyOrdered(t, repeatedRewindLsn, pitrLsn(t, h), "repeated restore writes")
	pitrAssertTimeline(t, []string{backupTimeline, pitrTimeline(t, h)})
	pitrAssertDdlRewound(t, h, drillDir)
	generationThird, repeatedBootLog := enterRecoveryViaOsProcess(t, h)
	if generationThird == generationBefore || generationThird == generationAfter {
		t.Fatal("repeated rewind must issue a still-new generation")
	}
	pitrAssertStrictlyNewer(t, generationAfter, generationThird)
	pitrAssertStrictlyNewer(t, generationBefore, generationThird)
	pitrWrite(t, drillDir, "repeated-rewind-generations.json", map[string]any{
		"first_post_restore": generationAfter, "second_post_restore": generationThird, "pre_rewind": generationBefore,
		"first_post_restore_timestamp_ms":  pitrUUIDv7TimestampMs(t, generationAfter),
		"second_post_restore_timestamp_ms": pitrUUIDv7TimestampMs(t, generationThird),
		"pre_rewind_timestamp_ms":          pitrUUIDv7TimestampMs(t, generationBefore),
		"second_post_restore_boot_log":     repeatedBootLog,
	})
	pitrWrite(t, drillDir, "final-database.json", map[string]any{
		"terminate": json.RawMessage(h.rows(t, fTerm).raw),
		"authority": pitrAuthority(t, h),
	})
	t.Logf("rewind drill (logic-only precursor, NOT PITR by itself; closes #40 together with the physical drill): rows-only backup %s with recorded restore point %s (hygiene-only markers; restore did not consume LSN), T1 live %v, rewind forgot %s, recovery %s then %s, graceful-then-forceful cleanup attested", backup, restorePointLsn, pidsTerm, generationBefore, generationAfter, generationThird)
}
