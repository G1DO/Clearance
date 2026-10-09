# Controller operation

Run the Java controller against PostgreSQL as the durable ownership authority.
Agent host prerequisites are in [agent operation](agent.md); credential handling
is in [security configuration](../security/README.md).

## Boot modes

Normal boot preserves `recovery_authority` untouched. Recovery boot uses
`clearance.recovery-mode=true` after a restore: it issues one fresh time-ordered
UUIDv7 from the external monotonic source, persists it as fleet authority,
and quarantines every runner regardless of restored row state. See
[recovery generation authority](../api/agent-api.md#recovery-generation-authority).

## Configuration

Replace the bundled development configuration outside local development:

```sh
SPRING_CONFIG_LOCATION=file:/absolute/path/clearance.yml
```

Supply the intended API-key and runner-key mappings, database connection, and
runtime settings there; retain the finite bounds from the bundled
[application.yml](../../controller/src/main/resources/application.yml).
Never enable the `test` profile in deployment. See
[controller configuration](../security/README.md#controller-configuration).

Provision `clearance.recovery-generation-log` on durable storage outside rewound
PostgreSQL state (default `/var/lib/clearance/recovery-generations.log`),
writable by the controller. For multi-instance deployments use a shared path
or supply one operator-attested `clearance.recovery-generation` UUIDv7.

## Backup and restore

Back up PostgreSQL with a tested procedure before relying on recovery.
For production use physical base-backup plus WAL replay: take a base backup
(`pg_basebackup` with WAL streaming), archive WAL continuously, record the T0 restore point
(`SELECT pg_create_restore_point('pitr_T0_...')`) with `pg_current_wal_lsn()`,
`pg_walfile_name()`, and `pg_control_checkpoint()` timeline IDs, rewind via restore/replay to
the documented point with real timeline-history validation (WAL archive `*.history` files,
expected timeline branch, restore-point reachability through the WAL stream), then follow
the reboot sequence below. The F14 runbook is proven by the isolated
[physical PITR drill](pitr-physical-drill.md) (`docker-compose.pitr.yml` +
`scripts/pitr-physical-drill.sh`, evidence under `controller/target/pitr-physical/`,
validators `PitrHistoryValidator` / `pitr_history.go`): base backup before T0 with T0 idle + terminal seeds plus pre-existing ALTER/DROP/sequence probes, T1
DDL/sequence plus post-T0 runner writes plus real host PIDs/workspaces, stop-before-rewind with vacuously closed claim window, restore/replay to the T0 name with
`recovery_target_timeline='latest'` + `recovery_target_action='promote'`, timeline `1 -> 2`
plus strict `*.history` validation (`history-validation.json`, last-branch for multi-hop), full-database rewind (T0 runners back to `AVAILABLE`/`CLEANING` with allocation intact, pre-existing ALTER/DROP/sequence values rewound, post-T0 objects absent),
durable host log outside the `PGDATA` wipe with UUIDv7 chain, and OS-process recovery-mode boot with fresh generation plus fleet
quarantine plus claim/stale/reconcile/cleanup gates on `:5545` (`PitrPhysicalPostRestoreIntegrationTest`) with repeated physical restore proving G4 distinct/newer. The rows-only `PitrTimelineValidator` / `pitr_timeline.go` checks remain
hygiene-only guards for the fast logic-only precursor rehearsal (`pg_current_wal_lsn`
ordering, restore-after-start, no-branch guard; WAL file recorded only; restore does not
consume the LSN; no WAL replay / timeline branch; strict `restore <= backupDone` NOT required) and must NOT be reused as production
 history validation. The [destructive rewind drill](../development/agent-verification.md#destructive-rewind-drill-logic-only-precursor-with-real-reboot-physical-f14-core-in-pitr-physical-drill)
remains the fast logic-only precursor with real
OS-process recovery boot (post-T0 CREATE drop only; DROP/ALTER and sequence values NOT covered
in that precursor; the physical drill covers them). Stop serving before any
rewind: a live controller would grant legacy-availability claims on rewound
rows. Boot the restored database once in recovery mode, then reconcile every
runner with fresh physical evidence before reuse.

## Triage

Use read-only database access. Check `recovery_authority.current_generation`,
then `runners.state`, `runners.epoch`, `runners.reconciled_generation`,
`runners.idle_reconcile_seq`, and `runners.quarantine_reason`, plus the
allocation fields listed in [agent triage](agent.md#triage-a-stopped-or-quarantined-runner).
Restored `AVAILABLE` rows without current `reconciled_generation` are refused
by claims with nothing written.

Finite bounds: HikariCP `maximum-pool-size: 10`, Tomcat `threads.max: 50`,
heartbeat/reconciliation batch size `50`. See
[configuration reference](../reference/configuration.md).
