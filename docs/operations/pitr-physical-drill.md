# Physical PITR F14 drill (issue #40)

Isolated cluster only. Never touches dev `clearance-postgres:5544` or production. This drill closes issue #40 together with the fast rows-only precursors (which pin controller logic and the live-cgroup matrix).

- Compose: [`docker-compose.pitr.yml`](../../docker-compose.pitr.yml) (`postgres-pitr:5545`,
  `wal_level=replica`, `archive_mode=on`, idempotent archive
  `test -f ... || cp ...`, bind mounts `./controller/target/pitr-physical/wal-archive`
  and `./controller/target/pitr-physical/backup` with `777` so postgres uid 70 can write).
- Harness: [`scripts/pitr-physical-drill.sh`](../../scripts/pitr-physical-drill.sh).
- Run: `bash scripts/pitr-physical-drill.sh` (needs Docker, `psql`, `pg_isready`, Java 25 + `./mvnw`, `python3`).
  It starts `postgres-pitr`, wipes stale WAL from previous system IDs (fresh initdb =>
  new system ID), migrates via Flyway, seeds T0 (idle `AVAILABLE` runner + `CLEANING`
  runner with terminal `SUCCEEDED` allocation + pre-existing `ALTER`/`DROP`/sequence
  probes), takes a plain-format `pg_basebackup -Fp -X stream`
  **before** T0, records T0 via `pg_create_restore_point` + `pg_current_wal_lsn()` +
  `pg_control_checkpoint()` timeline, writes T1 probes (new table + `ALTER` + sequence +
  post-T0 table + post-T0 runner + pre-existing `ALTER`/`DROP`/sequence advances +
  real host PIDs/workspaces for both T0 runners), stops serving (no controller
  serves across the wipe, so the claim window is vacuously closed), wipes `PGDATA`, restores the basebackup, replays WAL to
  the T0 name with `recovery_target_timeline='latest'` + `recovery_target_action='promote'`,
  then verifies: post-T0 objects absent (probe table/sequence, post-T0 table, post-T0 runner),
  pre-existing `ALTER`/`DROP`/sequence values rewound to T0, T0 runners rewound
  (`AVAILABLE` + `CLEANING` with allocation intact), host PIDs/workspaces survived,
  host evidence dir survived, timeline branched (`1 -> 2`),
  `*.history` present in archive and `pg_wal`, strict history validation
  (last branch inside `[backupStart, replayEnd]`, `backupStart <= restorePoint <= replayEnd`),
  then runs `PitrPhysicalPostRestoreIntegrationTest` against `:5545` with the
  rewind-surviving durable log: real OS/JVM child `Application
  --clearance.recovery-mode=true` exercising `RecoveryBootRunner` (boot log
  retained), fresh UUIDv7 strictly newer than log history, fleet quarantine,
  claim refusal (quarantined and forced-`AVAILABLE`), stale-generation
  zero-mutation, `RECONCILE` → `TERMINATE_CLEANUP` → verified `CLEANUP` →
  `AVAILABLE` with `reconciled_generation` advancement using the surviving real
  PIDs/workspaces (bounded `SIGTERM` then `SIGKILL` with `/proc` reaping +
  workspace scrub), then repeats the physical restore → boot to prove G4 is
  distinct and strictly newer than G3 with `GEN_LOG` history retained.
- Evidence: `controller/target/pitr-physical/run.<UTC>/` (`t0-marker.json` with T0 runners/allocation,
  `t0-runner.txt`, `t0-alloc-runner.txt`, `pre-restore-runners-t0.json`, `pre-rewind-authority.txt`,
  `basebackup.log`, `t1-marker.json`, `live-execution.json`, `t1-live-ps.txt`,
  `pre-restore-runners-t1.txt`, `wal-after-t1.txt`, `pre-restore-volumes.txt`,
  `post-restore.json`, `post-restore-runners.txt`, `post-restore-database.json`,
  `live-execution-survival.json`, `history-files.txt`, `wal-archive-copy/`, `history-validation.json`, `rewind-check.txt`,
  `post-restore-recovery-boot.json` + `-child-boot.log`, `recovery-boot-quarantine.json`,
  `claim-refusal-proof.json`, `stale-zero-mutation.json`, `desired-vs-observed-*.json`,
  `termination-*.json`, `cleanup-attestation*.json`, `idle-attestation.json`,
  `generations.json`, `gen-history-1.json`, `post-restore-2.json`, `history-files-2.txt`,
  `history-validation-2.json`, `recovery-boot-2.log`, `gen-history-2.json`,
  `post-boot-runners.json`, `recovery-generations.log` durable UUIDv7 log).
  Bind dirs `wal-archive/` + `backup/` persist across runs by design;
  per-run evidence is under `run.*/`. `target/` is gitignored; retain the CI artifact link,
  not log copies.
- Validators: `PitrHistoryValidator.java` + `pitr_history.go` (real AC1 path: `*.history`
  parse, expected parent→child branch inside `[backupStart, replayEnd]` checked on
  the last record for multi-hop histories, strict
  `backupStart <= restorePoint <= replayEnd`). `PitrTimelineValidator` / `pitr_timeline.go`
  remain hygiene-only guards for the fast rows-only precursor and must not be reused as
  history validation.

What this proves (F14): physical `pg_basebackup` + continuous WAL archiving +
restore/replay to a named point + real timeline branch + strict `*.history` validation (last-branch for repeated rewind) +
full-database rewind (post-T0 `CREATE` vanishes plus pre-existing `ALTER`/`DROP`/sequence values rewound; T0 idle survives as `AVAILABLE`, T0 terminal survives as `CLEANING` with allocation, post-T0 vanishes)
+ stop-before-rewind with vacuously closed claim window + durable host log outside `PGDATA` + real Linux PIDs/workspaces surviving the wipe + OS-process recovery boot with strictly newer UUIDv7 generation + fleet quarantine + claim refusal on restored rows (quarantined and forced-`AVAILABLE`) + stale-generation zero-mutation + reuse-without-cleanup refusal + `RECONCILE` → `TERMINATE_CLEANUP` → verified `CLEANUP` → `AVAILABLE` with `reconciled_generation` advancement on `:5545` + repeated physical restore proving G4 distinct/newer.

Fleet reuse on the physical timeline is proven by `PitrPhysicalPostRestoreIntegrationTest`
(OS-process `RecoveryBootRunner` boot with retained logs; artifacts in the same `run.*/`);
graceful-then-`cgroup.kill` SIGKILL specifics with `cgroup.procs` gating remain proven by the real-cgroup drill (`TestControllerPitrRewindDrill`,
partition matrix, artifacts under `controller/target/agent-integration/run.*/pitr-drill/`), while the physical drill proves PID/`/proc`/workspace survival, bounded `SIGTERM`-then-`SIGKILL` termination with reaping/scrub, and generation advancement on `:5545` through the same `RecoveryBootRunner` + `AgentService` paths.
The `pitr-physical` CI job preserves the physical evidence as one linked run; the rows-only `RecoveryGenerationIntegrationTest` + `PitrRewindDrillIntegrationTest` continue to run on `:5544` in the `java` job and must not be rerun on `:5545` after the physical drill (their logic-only rewinds would clobber physical evidence).
