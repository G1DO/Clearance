# Physical PITR F14 drill (issue #40)

Isolated cluster only. Never touches dev `clearance-postgres:5544` or production. Together with the fast rows-only precursors plus the controller gates below, this drill closes issue #40.

- Compose: [`docker-compose.pitr.yml`](../../docker-compose.pitr.yml) (`postgres-pitr:5545`,
  `wal_level=replica`, `archive_mode=on`, idempotent archive
  `test -f ... || cp ...`, bind mounts `./controller/target/pitr-physical/wal-archive`
  and `./controller/target/pitr-physical/backup` with `777` so postgres uid 70 can write).
- Harness: [`scripts/pitr-physical-drill.sh`](../../scripts/pitr-physical-drill.sh).
- Run: `bash scripts/pitr-physical-drill.sh` (needs Docker, `psql`, `pg_isready`, Java 25 + `./mvnw`, `python3`).
  It starts `postgres-pitr`, wipes stale WAL from previous system IDs (fresh initdb =>
  new system ID), migrates via Flyway, seeds a T0 idle `AVAILABLE` runner plus a host-side
  durable-log marker (outside `PGDATA` so a volume wipe cannot rewind it), takes a plain-format `pg_basebackup -Fp -X stream`
  **before** T0, records T0 via `pg_create_restore_point` + `pg_current_wal_lsn()` +
  `pg_control_checkpoint()` timeline, writes T1 probes (table + `ALTER` + sequence +
  post-T0 table + post-T0 runner), stops serving, wipes `PGDATA`, restores the basebackup, replays WAL to
  the T0 name with `recovery_target_timeline='latest'` + `recovery_target_action='promote'`,
  then verifies: post-T0 objects absent (probe table/sequence, post-T0 table, post-T0 runner),
  T0 runner rewound to `AVAILABLE`, durable log survived, timeline branched (`1 -> 2`),
  `*.history` present in archive and `pg_wal`, strict history validation
  (branch inside `[backupStart, replayEnd]`, `backupStart <= restorePoint <= replayEnd`),
  and recovery-mode boot issuing fresh UUIDv7 generations with fleet quarantine
  (`RecoveryMonotonicGenerationIntegrationTest` against `:5545`, with the rewound T0 runner
  checked `QUARANTINED` afterwards).
- Evidence: `controller/target/pitr-physical/run.<UTC>/` (`t0-marker.json` with T0 runner,
  `t0-runner.txt`, `pre-restore-runners-t0.json`, `basebackup.log`, `t1-marker.json` with T0/T1 runners,
  `pre-restore-runners-t1.txt`, `wal-after-t1.txt`, `pre-restore-volumes.txt`,
  `post-restore.json`, `post-restore-runners.txt`, `post-restore-database.json`,
  `history-files.txt`, `wal-archive-copy/`, `history-validation.json`, `rewind-check.txt`,
  `recovery-boot.log`, `post-boot-runners.json`, `recovery-generations.log` host marker).
  Bind dirs `wal-archive/` + `backup/` persist across runs by design;
  per-run evidence is under `run.*/`. `target/` is gitignored; retain the CI artifact link,
  not log copies.
- Validators: `PitrHistoryValidator.java` + `pitr_history.go` (real AC1 path: `*.history`
  parse, expected parent→child branch inside `[backupStart, replayEnd]`, strict
  `backupStart <= restorePoint <= replayEnd`). `PitrTimelineValidator` / `pitr_timeline.go`
  remain hygiene-only guards for the fast rows-only precursor and must not be reused as
  history validation.

What this proves (F14): physical `pg_basebackup` + continuous WAL archiving +
restore/replay to a named point + real timeline branch + strict `*.history` validation +
full-database rewind (DDL/sequence plus `runners` rows: T0 idle survives as `AVAILABLE`, post-T0 vanishes)
+ stop-before-rewind + durable host log surviving the `PGDATA` wipe + single recovery boot with strictly newer
generation + fleet quarantine (rewound T0 runner checked `QUARANTINED`) + claim refusal on restored rows.

Composition for full fleet reuse: controller classification (`STILL_RUNNING` keep,
`FINISHED_NEEDS_CLEANUP` → `TERMINATE_CLEANUP` → verified `CLEANUP` → `AVAILABLE`,
idle attest, stale `fenced_rejected` zero-mutation, I7 cleanup gate) is proven on the
physical timeline by `RecoveryGenerationIntegrationTest` (16 tests) and
`PitrRewindDrillIntegrationTest` run with `-Dspring.datasource.url=...5545` after the
physical restore (both exercise real OS/JVM-process `RecoveryBootRunner` boots with retained
logs; artifacts under `controller/target/pitr-drill/` plus `surefire-reports/`); graceful-then-`cgroup.kill` SIGKILL with reaping/scrub on live Linux
runners is proven by the existing real-cgroup drill (`TestControllerPitrRewindDrill`,
partition matrix, artifacts under `controller/target/agent-integration/run.*/pitr-drill/`).
Host cgroups/workspaces are untouched by the database rewind by design (`PGDATA` volume vs
`cgroupfs`/workspace dirs), so the live-runner survival and termination path proven there
transfers to the physical timeline using the same `RecoveryBootRunner` + `AgentService` paths.
The `pitr-physical` CI job preserves all three evidence sets as one linked run.
