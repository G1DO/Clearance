#!/usr/bin/env bash
# Physical PITR F14 drill for issue #40 (isolated postgres-pitr:5545 only).
# Never touches dev clearance-postgres:5544 or production.
# Proves: pg_basebackup + WAL replay + timeline branch + *.history validation,
# T1 DDL/sequence rewind incl pre-existing ALTER/DROP/sequence values, real Linux
# PIDs/workspaces surviving the PGDATA wipe, OS-process recovery-mode boot with
# fresh UUIDv7 + fleet quarantine, claim/stale/reuse gates and
# RECONCILE->CLEANUP->AVAILABLE with generation advancement on :5545,
# plus repeated-rewind G3->G4 freshness.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="$ROOT/docker-compose.pitr.yml"
PGPORT="${PGPORT:-5545}"
PGUSER="${PGUSER:-clearance}"
PGPASSWORD="${PGPASSWORD:-clearance}"
PGDATABASE="${PGDATABASE:-clearance}"
export PGPASSWORD

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE="$ROOT/controller/target/pitr-physical/run.$STAMP"
mkdir -p "$EVIDENCE"
# Bind-mounted WAL/backup dirs live on the host so postgres (uid 70) can write:
# pre-create with open perms before container start.
mkdir -p "$ROOT/controller/target/pitr-physical/wal-archive" "$ROOT/controller/target/pitr-physical/backup"
chmod 777 "$ROOT/controller/target/pitr-physical/wal-archive" "$ROOT/controller/target/pitr-physical/backup" || true

log() { echo "[pitr-physical $STAMP] $*"; }
psql_pitr() { psql -X -v ON_ERROR_STOP=1 -h localhost -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "$1"; }
psql_pitr_verbose() { psql -X -v ON_ERROR_STOP=1 -h localhost -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "$1"; }

log "starting isolated postgres-pitr on $PGPORT"
docker compose -f "$COMPOSE" up -d --wait postgres-pitr
for i in $(seq 1 30); do
  if pg_isready -h localhost -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" >/dev/null 2>&1; then break; fi
  sleep 2
done
pg_isready -h localhost -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE"

log "wiping stale WAL archive + backup from previous system IDs (fresh initdb => new system ID)"
docker exec clearance-postgres-pitr sh -lc 'rm -f /var/lib/postgresql/wal-archive/* /var/lib/postgresql/backup/base.tar.gz 2>/dev/null; rm -rf /var/lib/postgresql/backup/base 2>/dev/null; ls /var/lib/postgresql/wal-archive 2>&1; echo CLEANED'

log "recording server settings"
{
  echo "wal_level=$(psql_pitr "SHOW wal_level;")"
  echo "archive_mode=$(psql_pitr "SHOW archive_mode;")"
  echo "archive_command=$(psql_pitr "SHOW archive_command;")"
} | tee "$EVIDENCE/server-settings.txt"

log "ensuring schema via Flyway (DB-backed Spring test boot runs migrations on :$PGPORT)"
(cd "$ROOT/controller" && SPRING_DATASOURCE_URL="jdbc:postgresql://localhost:$PGPORT/clearance" \
  ./mvnw -B -ntp -Dspring.datasource.url="jdbc:postgresql://localhost:$PGPORT/clearance" \
  -Dtest='RecoveryGenerationIntegrationTest#schemaV9IncludesRecoveryAuthority' test >/dev/null)
# Drop probe artifacts from any previous run so T0 starts clean (fresh CI volumes are
# empty, but a local re-run would otherwise mistake leftovers for T0 state).
psql_pitr "DROP TABLE IF EXISTS pitr_f14_probe; DROP TABLE IF EXISTS pitr_f14_post_t0;"
psql_pitr "DROP TABLE IF EXISTS pitr_f14_pre_alter; DROP TABLE IF EXISTS pitr_f14_pre_drop;"
psql_pitr "DROP SEQUENCE IF EXISTS pitr_f14_seq_probe; DROP SEQUENCE IF EXISTS pitr_f14_pre_seq;"

log "T0 fleet seed: idle AVAILABLE runner that must survive rewind then quarantine"
T0_RUNNER_ID="$(cat /proc/sys/kernel/random/uuid)"
psql_pitr "INSERT INTO runners (runner_id, runner_class, state, epoch) VALUES ('$T0_RUNNER_ID', 'default', 'AVAILABLE', 0) ON CONFLICT DO NOTHING;"
psql_pitr "SELECT runner_id::text || ' ' || state FROM runners WHERE runner_id = '$T0_RUNNER_ID';" | tee "$EVIDENCE/t0-runner.txt"
# T0 allocated runner with terminal SUCCEEDED work (survives rewind; host execution
# started at T1 survives the PGDATA wipe so post-restore RECONCILE can direct
# TERMINATE_CLEANUP -> verified CLEANUP -> AVAILABLE on :5545).
T0_ALLOC_RUNNER_ID="$(cat /proc/sys/kernel/random/uuid)"
T0_JOB_ID="$(cat /proc/sys/kernel/random/uuid)"
T0_ATTEMPT_ID="$(cat /proc/sys/kernel/random/uuid)"
T0_ALLOC_ID="$(cat /proc/sys/kernel/random/uuid)"
T0_OP_ID="pitr-physical-t0-$STAMP"
psql_pitr "INSERT INTO runners (runner_id, runner_class, state, epoch, agent_incarnation) VALUES ('$T0_ALLOC_RUNNER_ID', 'default', 'CLEANING', 1, 7) ON CONFLICT DO NOTHING;"
psql_pitr "INSERT INTO jobs (job_id, project_id, operation_id, argv, runner_class, payload_hash) VALUES ('$T0_JOB_ID', 'project-alpha', '$T0_OP_ID', '[\"echo\",\"hi\"]', 'default', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') ON CONFLICT DO NOTHING;"
psql_pitr "INSERT INTO attempts (attempt_id, job_id, result) VALUES ('$T0_ATTEMPT_ID', '$T0_JOB_ID', 'SUCCEEDED') ON CONFLICT DO NOTHING;"
psql_pitr "INSERT INTO allocations (allocation_id, attempt_id, job_id, runner_id, runner_epoch, state, report_status, max_seq, agent_incarnation, workload_timeout_ms, heartbeat_timeout_ms, last_contact_at) VALUES ('$T0_ALLOC_ID', '$T0_ATTEMPT_ID', '$T0_JOB_ID', '$T0_ALLOC_RUNNER_ID', 1, 'ACTIVE', 'SUCCEEDED', 1, 7, 3600000, 15000, now()) ON CONFLICT DO NOTHING;"
echo "$T0_ALLOC_RUNNER_ID $T0_ALLOC_ID" | tee "$EVIDENCE/t0-alloc-runner.txt"
# Pre-existing DDL/sequence probes at T0 (ALTER/DROP/value rewind is verified post-restore).
psql_pitr_verbose "CREATE TABLE pitr_f14_pre_alter (id BIGINT PRIMARY KEY, val TEXT); INSERT INTO pitr_f14_pre_alter VALUES (1,'t0') ON CONFLICT DO NOTHING;"
psql_pitr_verbose "CREATE TABLE pitr_f14_pre_drop (id BIGINT PRIMARY KEY); INSERT INTO pitr_f14_pre_drop VALUES (1) ON CONFLICT DO NOTHING;"
psql_pitr_verbose "CREATE SEQUENCE pitr_f14_pre_seq START 50; SELECT nextval('pitr_f14_pre_seq'); SELECT nextval('pitr_f14_pre_seq');"
psql_pitr "SELECT last_value FROM pitr_f14_pre_seq;" | tee "$EVIDENCE/t0-pre-seq.txt"
psql_pitr "SELECT column_name FROM information_schema.columns WHERE table_name='pitr_f14_pre_alter' ORDER BY ordinal_position;" | tee "$EVIDENCE/t0-pre-alter-cols.txt"
# Durable generation log lives on the host (outside PGDATA) so a PGDATA wipe cannot rewind it.
# It must hold only UUID lines (RecoveryGenerationSource parses every non-blank line
# as a UUID); the human marker lives beside it, never inside it.
GEN_LOG="$EVIDENCE/recovery-generations.log"
rm -f "$GEN_LOG"
echo "pitr-physical drill $STAMP durable log outside PGDATA (host evidence dir, not in PGDATA volume)" > "$EVIDENCE/durable-log-location.txt"
echo "log_file=$GEN_LOG (starts empty; child recovery boots append UUIDv7)" | tee -a "$EVIDENCE/durable-log-location.txt"
log "durable log at $GEN_LOG (host path, survives PGDATA wipe by design; starts empty)"

log "BASE: pg_basebackup first (plain format + WAL stream, before T0 so T0 is reachable)"
BACKUP_START_LSN="$(psql_pitr "SELECT pg_current_wal_lsn();")"
T0_TIMELINE="$(psql_pitr "SELECT timeline_id FROM pg_control_checkpoint();")"
docker exec clearance-postgres-pitr sh -lc 'rm -rf /var/lib/postgresql/backup/base-plain && mkdir -p /var/lib/postgresql/backup && pg_basebackup -D /var/lib/postgresql/backup/base-plain -Fp -P -X stream -U clearance' 2>&1 | tee "$EVIDENCE/basebackup.log"
docker exec clearance-postgres-pitr sh -lc 'du -sh /var/lib/postgresql/backup/base-plain; ls /var/lib/postgresql/backup/base-plain | head -n 20; ls /var/lib/postgresql/wal-archive | head -n 20' | tee -a "$EVIDENCE/basebackup.log"
# Backup files may be root-owned; open perms for host evidence + restore helpers.
docker exec clearance-postgres-pitr sh -lc 'chmod -R a+rX /var/lib/postgresql/backup /var/lib/postgresql/wal-archive; ls -ld /var/lib/postgresql/backup/base-plain' | tee -a "$EVIDENCE/basebackup.log"

log "T0: restore point AFTER basebackup (so replay from backup can reach it)"
T0_NAME="pitr_T0_$STAMP"
RESTORE_LSN="$(psql_pitr "SELECT pg_create_restore_point('$T0_NAME');")"
BACKUP_DONE_LSN="$(psql_pitr "SELECT pg_current_wal_lsn();")"
WALFILE="$(psql_pitr "SELECT pg_walfile_name('$RESTORE_LSN');")"
log "backup_start=$BACKUP_START_LSN restore=$RESTORE_LSN timeline=$T0_TIMELINE walfile=$WALFILE"
cat > "$EVIDENCE/t0-marker.json" <<JSON
{"t0_name":"$T0_NAME","backup_start_lsn":"$BACKUP_START_LSN","restore_point_lsn":"$RESTORE_LSN","backup_done_lsn":"$BACKUP_DONE_LSN","timeline":"$T0_TIMELINE","walfile":"$WALFILE","t0_runner_id":"$T0_RUNNER_ID","t0_alloc_runner_id":"$T0_ALLOC_RUNNER_ID","t0_allocation_id":"$T0_ALLOC_ID","t0_job_id":"$T0_JOB_ID"}
JSON
psql_pitr "SELECT row_to_json(t)::text FROM runners t ORDER BY runner_id;" | tee "$EVIDENCE/pre-restore-runners-t0.json"
psql_pitr "SELECT count(*) FROM runners;" | tee "$EVIDENCE/pre-restore-runners-count.txt"
psql_pitr "SELECT current_generation::text FROM recovery_authority;" | tee "$EVIDENCE/pre-rewind-authority.txt" || echo "(no authority at T0)" | tee -a "$EVIDENCE/pre-rewind-authority.txt"

log "T1: writes incl DDL ALTER/DROP + sequences (must vanish after PITR)"
psql_pitr_verbose "CREATE TABLE IF NOT EXISTS pitr_f14_probe (id BIGINT PRIMARY KEY, note TEXT); INSERT INTO pitr_f14_probe VALUES (1,'t1') ON CONFLICT DO NOTHING;"
psql_pitr_verbose "CREATE SEQUENCE IF NOT EXISTS pitr_f14_seq_probe; SELECT nextval('pitr_f14_seq_probe');"
psql_pitr_verbose "ALTER TABLE pitr_f14_probe ADD COLUMN IF NOT EXISTS t1_extra TEXT; UPDATE pitr_f14_probe SET t1_extra='dirty' WHERE id=1;"
psql_pitr_verbose "CREATE TABLE pitr_f14_post_t0 (id BIGINT PRIMARY KEY); INSERT INTO pitr_f14_post_t0 VALUES (42);"
# Pre-existing objects mutated at T1 (must rewind to T0 state after PITR).
psql_pitr_verbose "ALTER TABLE pitr_f14_pre_alter ADD COLUMN t1_added TEXT; UPDATE pitr_f14_pre_alter SET t1_added='dirty' WHERE id=1;"
psql_pitr_verbose "DROP TABLE pitr_f14_pre_drop;"
psql_pitr_verbose "SELECT nextval('pitr_f14_pre_seq'); SELECT nextval('pitr_f14_pre_seq');"
psql_pitr "SELECT last_value FROM pitr_f14_pre_seq;" | tee "$EVIDENCE/t1-pre-seq.txt"
# Post-T0 runner must vanish after PITR (proves full-database rewind covers runners, not just probes).
T1_RUNNER_ID="$(cat /proc/sys/kernel/random/uuid)"
psql_pitr "INSERT INTO runners (runner_id, runner_class, state, epoch) VALUES ('$T1_RUNNER_ID', 'default', 'AVAILABLE', 0);"
# Real Linux execution at T1 on host workspaces/PIDs (survives the PGDATA wipe by
# design: PGDATA volume vs host evidence dir). One orphan on the idle runner plus
# one live execution on the allocated runner; both are reconciled on :5545 after
# the recovery boot with the same AgentService paths.
mkdir -p "$EVIDENCE/workspaces/$T0_RUNNER_ID" "$EVIDENCE/workspaces/$T0_ALLOC_RUNNER_ID"
echo "dirty-t1" > "$EVIDENCE/workspaces/$T0_RUNNER_ID/dirty.marker"
echo "dirty-t1" > "$EVIDENCE/workspaces/$T0_ALLOC_RUNNER_ID/dirty.marker"
sleep 600 & PID_IDLE=$!
echo "$PID_IDLE" > "$EVIDENCE/workspaces/$T0_RUNNER_ID/pid"
sleep 600 & PID_ALLOC=$!
echo "$PID_ALLOC" > "$EVIDENCE/workspaces/$T0_ALLOC_RUNNER_ID/pid"
log "T1 live execution: idle pid $PID_IDLE workspace $EVIDENCE/workspaces/$T0_RUNNER_ID, alloc pid $PID_ALLOC workspace $EVIDENCE/workspaces/$T0_ALLOC_RUNNER_ID"
kill -0 "$PID_IDLE" && kill -0 "$PID_ALLOC"
ps -p "$PID_IDLE" -p "$PID_ALLOC" | tee "$EVIDENCE/t1-live-ps.txt"
cat > "$EVIDENCE/live-execution.json" <<JSON
{"idle_runner":"$T0_RUNNER_ID","alloc_runner":"$T0_ALLOC_RUNNER_ID","allocation_id":"$T0_ALLOC_ID","runner_epoch":1,"pid_idle":$PID_IDLE,"pid_alloc":$PID_ALLOC,"workspace_idle":"$EVIDENCE/workspaces/$T0_RUNNER_ID","workspace_alloc":"$EVIDENCE/workspaces/$T0_ALLOC_RUNNER_ID","incarnation":7}
JSON
T1_LSN="$(psql_pitr "SELECT pg_current_wal_lsn();")"
psql_pitr "SELECT pg_switch_wal();" >/dev/null || true
sleep 3
docker exec clearance-postgres-pitr sh -lc 'ls /var/lib/postgresql/wal-archive | wc -l; ls /var/lib/postgresql/wal-archive | tail -n 5' | tee "$EVIDENCE/wal-after-t1.txt"
cat > "$EVIDENCE/t1-marker.json" <<JSON
{"t1_lsn":"$T1_LSN","probe":"pitr_f14_probe + pitr_f14_seq_probe + pitr_f14_post_t0 + pre-existing ALTER/DROP + pre_seq values + live PIDs/workspaces","t0_runner_id":"$T0_RUNNER_ID","t0_alloc_runner_id":"$T0_ALLOC_RUNNER_ID","t1_runner_id":"$T1_RUNNER_ID","pid_idle":$PID_IDLE,"pid_alloc":$PID_ALLOC}
JSON
psql_pitr "SELECT runner_id::text || ' ' || state FROM runners ORDER BY runner_id;" | tee "$EVIDENCE/pre-restore-runners-t1.txt"

log "checkpoint + stop before rewind (no serving during restore)"
psql_pitr "CHECKPOINT;" >/dev/null
docker compose -f "$COMPOSE" stop postgres-pitr

log "restore: wipe PGDATA, extract basebackup, configure replay to T0"
docker exec clearance-postgres-pitr true 2>/dev/null || true
# PGDATA volume name is prefixed by the compose project (directory) name; discover it
# robustly instead of hardcoding clearance_ so the drill works from any checkout path.
PGDATA_VOL="$(docker volume ls -q | grep -E 'clearance-pitr-pgdata$' | head -n 1 || true)"
if [ -z "$PGDATA_VOL" ]; then
  echo "FAIL: could not discover PGDATA volume ending in clearance-pitr-pgdata" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
fi
log "using PGDATA volume $PGDATA_VOL"
HOST_BACKUP="$ROOT/controller/target/pitr-physical/backup"
HOST_WAL="$ROOT/controller/target/pitr-physical/wal-archive"
ls "$HOST_BACKUP/base-plain" | head -n 20 | tee "$EVIDENCE/pre-restore-volumes.txt"
ls "$HOST_WAL" | head -n 20 | tee -a "$EVIDENCE/pre-restore-volumes.txt"
# Stop ensures data container is down; now clear PGDATA via a temp postgres container as root.
# Plain-format backup restores via file copy (no tar), preserving system identifier.
docker run --rm -v "$PGDATA_VOL:/var/lib/postgresql/data" -v "$HOST_BACKUP:/backup" postgres:17-alpine sh -c 'rm -rf /var/lib/postgresql/data/pgdata/*; cp -a /backup/base-plain/. /var/lib/postgresql/data/pgdata/; rm -f /var/lib/postgresql/data/pgdata/postmaster.pid /var/lib/postgresql/data/pgdata/postmaster.opts; chown -R 70:70 /var/lib/postgresql/data/pgdata; ls /var/lib/postgresql/data/pgdata | head -n 20'
# Configure recovery: restore_command + target + signal
docker run --rm -v "$PGDATA_VOL:/var/lib/postgresql/data" postgres:17-alpine sh -c "cat >> /var/lib/postgresql/data/pgdata/postgresql.auto.conf <<CONF
restore_command = 'cp /var/lib/postgresql/wal-archive/%f %p'
recovery_target_name = '$T0_NAME'
recovery_target_timeline = 'latest'
recovery_target_action = 'promote'
CONF
touch /var/lib/postgresql/data/pgdata/recovery.signal
chown 70:70 /var/lib/postgresql/data/pgdata/recovery.signal /var/lib/postgresql/data/pgdata/postgresql.auto.conf
cat /var/lib/postgresql/data/pgdata/postgresql.auto.conf | tail -n 10"

log "restart for WAL replay (expect new timeline + history file)"
docker compose -f "$COMPOSE" up -d --wait postgres-pitr
for i in $(seq 1 60); do
  if pg_isready -h localhost -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" >/dev/null 2>&1; then break; fi
  sleep 2
done
pg_isready -h localhost -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE"

log "waiting for promotion (recovery_target_action=promote) before WAL control functions"
for i in $(seq 1 60); do
  IN_RECOVERY="$(psql_pitr "SELECT pg_is_in_recovery();")"
  if [ "$IN_RECOVERY" = "f" ]; then break; fi
  sleep 2
done
IN_RECOVERY="$(psql_pitr "SELECT pg_is_in_recovery();")"
if [ "$IN_RECOVERY" != "f" ]; then
  echo "FAIL: still in recovery after promote (pg_is_in_recovery=$IN_RECOVERY)" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
fi

POST_TIMELINE="$(psql_pitr "SELECT timeline_id FROM pg_control_checkpoint();")"
REPLAY_END_LSN="$(psql_pitr "SELECT pg_current_wal_lsn();")"
log "post_timeline=$POST_TIMELINE replay_end=$REPLAY_END_LSN (pre=$T0_TIMELINE)"
cat > "$EVIDENCE/post-restore.json" <<JSON
{"pre_timeline":"$T0_TIMELINE","post_timeline":"$POST_TIMELINE","replay_end_lsn":"$REPLAY_END_LSN","restore_point_lsn":"$RESTORE_LSN","backup_start_lsn":"$BACKUP_START_LSN"}
JSON
docker exec clearance-postgres-pitr sh -lc 'ls /var/lib/postgresql/wal-archive/*.history 2>&1; echo ---PGDATA-WAL---; ls /var/lib/postgresql/data/pgdata/pg_wal/*.history 2>&1' | tee "$EVIDENCE/history-files.txt"
# WALs archived after the base-backup chmod are postgres-owned 600; open perms now so
# the host copy below inherits world-readable modes and artifact upload can read them.
docker exec clearance-postgres-pitr sh -lc 'chmod -R a+rX /var/lib/postgresql/wal-archive /var/lib/postgresql/backup'
docker cp clearance-postgres-pitr:/var/lib/postgresql/wal-archive "$EVIDENCE/wal-archive-copy" 2>&1 || true
ls "$EVIDENCE/wal-archive-copy" 2>&1 | tee -a "$EVIDENCE/history-files.txt" || true

log "verify T1 writes rewound, T0 retained"
psql_pitr_verbose "SELECT count(*) AS post_t0_count FROM pitr_f14_post_t0;" 2>&1 | tee "$EVIDENCE/rewind-check.txt" || echo "post_t0 table missing (expected: created after restore target)" | tee -a "$EVIDENCE/rewind-check.txt"
# The post-T0 table was created after the restore target, so it must be absent (to_regclass returns NULL).
if [ -n "$(psql_pitr "SELECT to_regclass('pitr_f14_post_t0');")" ]; then
  echo "FAIL: post-T0 table survived PITR" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
else
  echo "OK: post-T0 table absent after PITR" | tee -a "$EVIDENCE/rewind-check.txt"
fi
# Full-database rewind must also drop the T1 probe table/sequence and the post-T0 runner,
# while the T0 runner survives as AVAILABLE (to be quarantined by recovery boot below).
if [ -n "$(psql_pitr "SELECT to_regclass('pitr_f14_probe');")" ]; then
  echo "FAIL: T1 probe table survived PITR" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
else
  echo "OK: T1 probe table absent after PITR" | tee -a "$EVIDENCE/rewind-check.txt"
fi
if [ -n "$(psql_pitr "SELECT to_regclass('pitr_f14_seq_probe');")" ]; then
  echo "FAIL: T1 sequence survived PITR" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
else
  echo "OK: T1 sequence absent after PITR" | tee -a "$EVIDENCE/rewind-check.txt"
fi
# Pre-existing objects must rewind to T0 state (proves full-DDL coverage, not just post-T0 CREATE).
if [ -z "$(psql_pitr "SELECT to_regclass('pitr_f14_pre_alter');")" ]; then
  echo "FAIL: pre-existing table pitr_f14_pre_alter missing after PITR" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
fi
if [ -n "$(psql_pitr "SELECT column_name FROM information_schema.columns WHERE table_name='pitr_f14_pre_alter' AND column_name='t1_added';")" ]; then
  echo "FAIL: pre-existing ALTER survived PITR (t1_added still present)" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
else
  echo "OK: pre-existing ALTER rewound (t1_added absent)" | tee -a "$EVIDENCE/rewind-check.txt"
fi
if [ "$(psql_pitr "SELECT count(*) FROM pitr_f14_pre_alter WHERE id=1 AND val='t0';")" != "1" ]; then
  echo "FAIL: pre-existing row lost after PITR" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
else
  echo "OK: pre-existing row intact after PITR" | tee -a "$EVIDENCE/rewind-check.txt"
fi
if [ -z "$(psql_pitr "SELECT to_regclass('pitr_f14_pre_drop');")" ]; then
  echo "FAIL: pre-existing DROP not rewound (pitr_f14_pre_drop still absent)" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
else
  echo "OK: pre-existing DROP rewound (table present)" | tee -a "$EVIDENCE/rewind-check.txt"
fi
T0_SEQ_VAL="$(cat "$EVIDENCE/t0-pre-seq.txt" | tr -d '[:space:]')"
POST_SEQ_VAL="$(psql_pitr "SELECT last_value FROM pitr_f14_pre_seq;" | tr -d '[:space:]')"
if [ "$POST_SEQ_VAL" != "$T0_SEQ_VAL" ]; then
  echo "FAIL: pre-existing sequence value not rewound (T0=$T0_SEQ_VAL post=$POST_SEQ_VAL)" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
else
  echo "OK: pre-existing sequence rewound to $POST_SEQ_VAL" | tee -a "$EVIDENCE/rewind-check.txt"
fi
T0_STATE="$(psql_pitr "SELECT state FROM runners WHERE runner_id = '$T0_RUNNER_ID';")"
if [ "$T0_STATE" != "AVAILABLE" ]; then
  echo "FAIL: T0 runner not rewound to AVAILABLE (got '$T0_STATE')" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
else
  echo "OK: T0 runner rewound to AVAILABLE" | tee -a "$EVIDENCE/rewind-check.txt"
fi
if [ -n "$(psql_pitr "SELECT runner_id FROM runners WHERE runner_id = '$T1_RUNNER_ID';")" ]; then
  echo "FAIL: post-T0 runner survived PITR" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
else
  echo "OK: post-T0 runner absent after PITR" | tee -a "$EVIDENCE/rewind-check.txt"
fi
T0_ALLOC_STATE="$(psql_pitr "SELECT state FROM runners WHERE runner_id = '$T0_ALLOC_RUNNER_ID';")"
if [ "$T0_ALLOC_STATE" != "CLEANING" ]; then
  echo "FAIL: T0 alloc runner not rewound to CLEANING (got '$T0_ALLOC_STATE')" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
else
  echo "OK: T0 alloc runner rewound to CLEANING (terminal work survives for reconcile)" | tee -a "$EVIDENCE/rewind-check.txt"
fi
if [ -z "$(psql_pitr "SELECT allocation_id FROM allocations WHERE allocation_id = '$T0_ALLOC_ID';")" ]; then
  echo "FAIL: T0 allocation missing after PITR" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
else
  echo "OK: T0 allocation survives PITR" | tee -a "$EVIDENCE/rewind-check.txt"
fi
psql_pitr "SELECT runner_id::text || ' ' || state FROM runners ORDER BY runner_id;" | tee "$EVIDENCE/post-restore-runners.txt"
psql_pitr "SELECT row_to_json(t)::text FROM runners t ORDER BY runner_id;" | tee "$EVIDENCE/post-restore-database.json"
# Host PIDs/workspaces survive the PGDATA wipe by design (host evidence dir vs
# PGDATA volume); verify liveness here so the post-restore gates reconcile real
# surviving execution, not SQL-only rows.
if kill -0 "$PID_IDLE" 2>/dev/null && kill -0 "$PID_ALLOC" 2>/dev/null; then
  echo "OK: T1 PIDs $PID_IDLE/$PID_ALLOC survive PGDATA wipe (host, not PGDATA)" | tee -a "$EVIDENCE/rewind-check.txt"
else
  echo "FAIL: T1 PIDs did not survive wipe (idle=$PID_IDLE alloc=$PID_ALLOC)" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
fi
if [ -f "$EVIDENCE/workspaces/$T0_RUNNER_ID/dirty.marker" ] && [ -f "$EVIDENCE/workspaces/$T0_ALLOC_RUNNER_ID/dirty.marker" ]; then
  echo "OK: T1 workspaces survive PGDATA wipe (dirty markers present)" | tee -a "$EVIDENCE/rewind-check.txt"
else
  echo "FAIL: T1 workspaces missing after wipe" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
fi
cat > "$EVIDENCE/live-execution-survival.json" <<JSON
{"pid_idle":$PID_IDLE,"pid_alloc":$PID_ALLOC,"survives_pgdata_wipe":true,"workspaces_dirty":true}
JSON
# Durable log lives outside PGDATA by design (host evidence dir). It starts
# empty and each recovery child appends UUIDv7; survival means the directory +
# prior history survive the wipe (verified after boots via GEN_LOG history).
if [ ! -d "$EVIDENCE" ]; then
  echo "FAIL: evidence dir $EVIDENCE missing after PGDATA wipe" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
else
  echo "OK: host evidence dir survives PGDATA wipe (durable log path outside volume)" | tee -a "$EVIDENCE/rewind-check.txt"
fi

if [ "$POST_TIMELINE" = "$T0_TIMELINE" ]; then
  echo "FAIL: timeline did not branch (pre=$T0_TIMELINE post=$POST_TIMELINE)" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
else
  echo "OK: timeline branched $T0_TIMELINE -> $POST_TIMELINE" | tee -a "$EVIDENCE/rewind-check.txt"
fi

if ! grep -q ".history" "$EVIDENCE/history-files.txt"; then
  echo "FAIL: no *.history file after replay" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
else
  echo "OK: history file present" | tee -a "$EVIDENCE/rewind-check.txt"
fi

log "strict AC1 timeline-history validation (mirrors PitrHistoryValidator / pitr_history.go)"
# Real path: *.history parse, expected parent->child branch inside [backupStart, replayEnd],
# strict backupStart <= restorePoint <= replayEnd (physical restore consumes the target).
# Reference implementations are unit-tested (PitrHistoryValidatorTest, pitr_history_test.go);
# this step applies the same rule to the live history file with per-run evidence.
# Multi-hop histories (repeated rewind: 00000003.history with 1 ... then 2 ...)
# carry the full chain; only the last record is the new branch.
EXPECTED_CHILD_HEX="$(printf '%08X' "$POST_TIMELINE")"
HISTORY_FILE="$(ls "$EVIDENCE/wal-archive-copy"/"$EXPECTED_CHILD_HEX".history 2>/dev/null | head -n 1 || true)"
if [ -z "$HISTORY_FILE" ]; then
  # Fall back to any history file in the copy (single-branch drill has exactly one).
  HISTORY_FILE="$(ls "$EVIDENCE/wal-archive-copy"/*.history 2>/dev/null | head -n 1 || true)"
fi
if [ -z "$HISTORY_FILE" ]; then
  echo "FAIL: expected history file $EXPECTED_CHILD_HEX.history not found" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
fi
log "validating $HISTORY_FILE parent=$T0_TIMELINE child=$POST_TIMELINE backupStart=$BACKUP_START_LSN restore=$RESTORE_LSN replayEnd=$REPLAY_END_LSN"
export PITR_HISTORY_FILE="$HISTORY_FILE" PITR_PARENT="$T0_TIMELINE" PITR_CHILD="$POST_TIMELINE" PITR_BACKUP_START="$BACKUP_START_LSN" PITR_RESTORE="$RESTORE_LSN" PITR_REPLAY_END="$REPLAY_END_LSN" PITR_EVIDENCE="$EVIDENCE/history-validation.json"
python3 - <<'PY'
import json, os, re, sys
def parse_lsn(lsn):
    m = re.fullmatch(r'\s*([0-9A-Fa-f]{1,8})/([0-9A-Fa-f]{1,8})\s*', lsn)
    if not m:
        raise ValueError(f"not a PostgreSQL LSN: {lsn!r}")
    return (int(m.group(1), 16) << 32) | int(m.group(2), 16)
def cmp(a, b):
    av, bv = parse_lsn(a), parse_lsn(b)
    return (av > bv) - (av < bv)
hist = os.environ["PITR_HISTORY_FILE"]
parent = int(os.environ["PITR_PARENT"])
child = int(os.environ["PITR_CHILD"])
backup_start = os.environ["PITR_BACKUP_START"]
restore = os.environ["PITR_RESTORE"]
replay_end = os.environ["PITR_REPLAY_END"]
base = os.path.basename(hist)
expected = f"{child:08X}.history"
if base.upper() != expected.upper():
    print(f"FAIL: history file {base} != expected {expected}")
    sys.exit(1)
branches = []
with open(hist) as f:
    for raw in f:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            print(f"FAIL: not a history record: {raw!r}")
            sys.exit(1)
        try:
            p = int(parts[0])
            parse_lsn(parts[1])
        except Exception as e:
            print(f"FAIL: bad history record {raw!r}: {e}")
            sys.exit(1)
        branches.append((p, parts[1]))
if not branches:
    print("FAIL: history file has no branch records")
    sys.exit(1)
branch_parent, branch_lsn = branches[-1]
if branch_parent != parent:
    print(f"FAIL: last branch parent {branch_parent} != expected {parent} (full chain {[p for p, _ in branches]})")
    sys.exit(1)
if cmp(backup_start, branch_lsn) > 0:
    print(f"FAIL: branch LSN {branch_lsn} precedes backup start {backup_start}")
    sys.exit(1)
if cmp(branch_lsn, replay_end) > 0:
    print(f"FAIL: branch LSN {branch_lsn} follows replay end {replay_end}")
    sys.exit(1)
if cmp(backup_start, restore) > 0:
    print(f"FAIL: restore {restore} precedes backup start {backup_start}")
    sys.exit(1)
if cmp(restore, replay_end) > 0:
    print(f"FAIL: replay end {replay_end} precedes restore {restore}: target not reached")
    sys.exit(1)
with open(os.environ["PITR_EVIDENCE"], "w") as out:
    json.dump({"history_file": base, "expected_child": expected, "branch_parent": branch_parent,
               "branch_lsn": branch_lsn, "backup_start_lsn": backup_start,
               "restore_point_lsn": restore, "replay_end_lsn": replay_end,
               "result": "branch inside [backupStart, replayEnd], restore reachable (strict); mirrors PitrHistoryValidator/pitr_history.go"}, out, indent=2)
    out.write("\n")
print(f"OK: history {base} branches {branch_parent}->{child} at {branch_lsn} inside [{backup_start}, {replay_end}], restore {restore} reached")
PY
echo "OK: strict history validation passed (see history-validation.json)" | tee -a "$EVIDENCE/rewind-check.txt"

log "recovery-mode boot + quarantine via real OS/JVM child on :$PGPORT (RecoveryBootRunner)"
# No controller serves across wipe->quarantine: postgres was stopped for the
# wipe/restore above and no controller process runs during it, so the window is
# vacuously closed (a live controller would grant legacy-availability claims on
# rewound rows). The gates below prove quarantine + refusal on physical rows.
(cd "$ROOT/controller" && ./mvnw -B -ntp -Dspring.datasource.url="jdbc:postgresql://localhost:$PGPORT/clearance" -Dclearance.recovery-generation-log="$GEN_LOG" -Dpitr.physical.evidence="$EVIDENCE" -Dtest='PitrPhysicalPostRestoreIntegrationTest' test 2>&1 | tail -n 30 | tee "$EVIDENCE/recovery-boot.log")
# The child boot quarantines the whole fleet (QUARANTINE_FLEET_SQL) including rewound T0 rows;
# the test already asserted QUARANTINED + reason + claim/stale/reconcile/cleanup on :5545
# with retained claim-refusal-proof.json / stale-zero-mutation.json /
# desired-vs-observed-*.json / cleanup-attestation.json / idle-attestation.json.
T0_POST_BOOT_STATE="$(psql_pitr "SELECT state FROM runners WHERE runner_id = '$T0_RUNNER_ID';")"
echo "t0_runner_post_boot_state=$T0_POST_BOOT_STATE" | tee -a "$EVIDENCE/rewind-check.txt"
if [ "$T0_POST_BOOT_STATE" != "QUARANTINED" ] && [ "$T0_POST_BOOT_STATE" != "AVAILABLE" ] && [ "$T0_POST_BOOT_STATE" != "ASSIGNED" ]; then
  echo "FAIL: T0 runner in unexpected state after post-restore gates (got '$T0_POST_BOOT_STATE')" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
else
  echo "OK: post-restore gates ran on physical rows (state=$T0_POST_BOOT_STATE; QUARANTINED before reconcile, AVAILABLE after attest, ASSIGNED once reclaimed)" | tee -a "$EVIDENCE/rewind-check.txt"
fi
psql_pitr "SELECT row_to_json(t)::text FROM runners t ORDER BY runner_id;" | tee "$EVIDENCE/post-boot-runners.json"
for f in claim-refusal-proof.json stale-zero-mutation.json generations.json post-restore-recovery-boot.json; do
  if [ ! -f "$EVIDENCE/$f" ]; then
    echo "FAIL: missing physical gate evidence $f" | tee -a "$EVIDENCE/rewind-check.txt"
    exit 1
  fi
done
echo "OK: claim/stale/generation proofs retained on physical timeline" | tee -a "$EVIDENCE/rewind-check.txt"

log "GEN_LOG history check: UUIDv7 + strictly newer timestamps + chain survives wipe"
export PITR_GEN_LOG="$GEN_LOG" PITR_GEN_EVIDENCE="$EVIDENCE/gen-history-1.json"
python3 - <<'PY'
import json, os, sys
path = os.environ["PITR_GEN_LOG"]
gens = []
with open(path) as f:
  for raw in f:
    line = raw.strip()
    if not line:
      continue
    gens.append(line.split()[0])
if len(gens) < 1:
  print("FAIL: durable log has no generations"); sys.exit(1)
def ts_ms(u):
  hexs = u.replace("-", "")
  return int(hexs[0:12], 16)
for g in gens:
  v = int(g.split("-")[2][0], 16)
  if v != 7:
    print(f"FAIL: non-UUIDv7 in durable log: {g}"); sys.exit(1)
timestamps = [ts_ms(g) for g in gens]
for i in range(1, len(timestamps)):
  if timestamps[i] <= timestamps[i-1]:
    print(f"FAIL: log timestamps not strictly newer: {gens}"); sys.exit(1)
if len(set(gens)) != len(gens):
  print(f"FAIL: duplicate generation in durable log: {gens}"); sys.exit(1)
with open(os.environ["PITR_GEN_EVIDENCE"], "w") as out:
  json.dump({"generations": gens, "timestamps_ms": timestamps, "result": "UUIDv7 strictly newer chain survives PGDATA wipe"}, out, indent=2)
  out.write("\n")
print(f"OK: durable log holds strictly newer UUIDv7 chain {gens}")
PY

log "second physical restore -> boot proves repeated-rewind freshness (G4 newer than G3)"
# The gates test cleans recovery_authority on exit (shared-DB isolation for the
# :5544 suite), so G3/G4 come from its retained evidence, not the DB.
G3="$(PITR_GEN_JSON="$EVIDENCE/generations.json" python3 -c 'import json,os; print(json.load(open(os.environ["PITR_GEN_JSON"]))["generation_after"])')"
echo "generation_after_first_boot=$G3" | tee -a "$EVIDENCE/rewind-check.txt"
psql_pitr "CHECKPOINT;" >/dev/null
docker compose -f "$COMPOSE" stop postgres-pitr
docker run --rm -v "$PGDATA_VOL:/var/lib/postgresql/data" -v "$HOST_BACKUP:/backup" postgres:17-alpine sh -c 'rm -rf /var/lib/postgresql/data/pgdata/*; cp -a /backup/base-plain/. /var/lib/postgresql/data/pgdata/; rm -f /var/lib/postgresql/data/pgdata/postmaster.pid /var/lib/postgresql/data/pgdata/postmaster.opts; chown -R 70:70 /var/lib/postgresql/data/pgdata'
docker run --rm -v "$PGDATA_VOL:/var/lib/postgresql/data" postgres:17-alpine sh -c "cat >> /var/lib/postgresql/data/pgdata/postgresql.auto.conf <<CONF
restore_command = 'cp /var/lib/postgresql/wal-archive/%f %p'
recovery_target_name = '$T0_NAME'
recovery_target_timeline = 'latest'
recovery_target_action = 'promote'
CONF
touch /var/lib/postgresql/data/pgdata/recovery.signal
chown 70:70 /var/lib/postgresql/data/pgdata/recovery.signal /var/lib/postgresql/data/pgdata/postgresql.auto.conf"
docker compose -f "$COMPOSE" up -d --wait postgres-pitr
for i in $(seq 1 60); do
  if pg_isready -h localhost -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" >/dev/null 2>&1; then break; fi
  sleep 2
done
pg_isready -h localhost -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE"
for i in $(seq 1 60); do
  IN_RECOVERY="$(psql_pitr "SELECT pg_is_in_recovery();")"
  if [ "$IN_RECOVERY" = "f" ]; then break; fi
  sleep 2
done
POST_TIMELINE2="$(psql_pitr "SELECT timeline_id FROM pg_control_checkpoint();")"
REPLAY_END_LSN2="$(psql_pitr "SELECT pg_current_wal_lsn();")"
log "second post_timeline=$POST_TIMELINE2 replay_end=$REPLAY_END_LSN2 (first post=$POST_TIMELINE)"
cat > "$EVIDENCE/post-restore-2.json" <<JSON
{"first_post_timeline":"$POST_TIMELINE","second_post_timeline":"$POST_TIMELINE2","replay_end_lsn":"$REPLAY_END_LSN2","restore_point_lsn":"$RESTORE_LSN"}
JSON
if [ "$POST_TIMELINE2" = "$POST_TIMELINE" ]; then
  echo "FAIL: second timeline did not branch ($POST_TIMELINE -> $POST_TIMELINE2)" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
else
  echo "OK: second timeline branched $POST_TIMELINE -> $POST_TIMELINE2" | tee -a "$EVIDENCE/rewind-check.txt"
fi
docker exec clearance-postgres-pitr sh -lc 'ls /var/lib/postgresql/wal-archive/*.history 2>&1' | tee "$EVIDENCE/history-files-2.txt"
docker exec clearance-postgres-pitr sh -lc 'chmod -R a+rX /var/lib/postgresql/wal-archive'
rm -rf "$EVIDENCE/wal-archive-copy-2" && docker cp clearance-postgres-pitr:/var/lib/postgresql/wal-archive "$EVIDENCE/wal-archive-copy-2" 2>&1 || true
EXPECTED_CHILD_HEX2="$(printf '%08X' "$POST_TIMELINE2")"
HISTORY_FILE2="$(ls "$EVIDENCE/wal-archive-copy-2"/"$EXPECTED_CHILD_HEX2".history 2>/dev/null | head -n 1 || true)"
if [ -z "$HISTORY_FILE2" ]; then
  HISTORY_FILE2="$(ls "$EVIDENCE/wal-archive-copy-2"/*.history 2>/dev/null | head -n 1 || true)"
fi
if [ -z "$HISTORY_FILE2" ]; then
  echo "FAIL: expected second history file $EXPECTED_CHILD_HEX2.history not found" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
fi
export PITR_HISTORY_FILE="$HISTORY_FILE2" PITR_PARENT="$POST_TIMELINE" PITR_CHILD="$POST_TIMELINE2" PITR_BACKUP_START="$BACKUP_START_LSN" PITR_RESTORE="$RESTORE_LSN" PITR_REPLAY_END="$REPLAY_END_LSN2" PITR_EVIDENCE="$EVIDENCE/history-validation-2.json"
python3 - <<'PY'
import json, os, re, sys
def parse_lsn(lsn):
    m = re.fullmatch(r'\s*([0-9A-Fa-f]{1,8})/([0-9A-Fa-f]{1,8})\s*', lsn)
    if not m:
        raise ValueError(f"not a PostgreSQL LSN: {lsn!r}")
    return (int(m.group(1), 16) << 32) | int(m.group(2), 16)
def cmp(a, b):
    av, bv = parse_lsn(a), parse_lsn(b)
    return (av > bv) - (av < bv)
hist = os.environ["PITR_HISTORY_FILE"]
parent = int(os.environ["PITR_PARENT"])
child = int(os.environ["PITR_CHILD"])
backup_start = os.environ["PITR_BACKUP_START"]
restore = os.environ["PITR_RESTORE"]
replay_end = os.environ["PITR_REPLAY_END"]
base = os.path.basename(hist)
expected = f"{child:08X}.history"
if base.upper() != expected.upper():
    print(f"FAIL: history file {base} != expected {expected}")
    sys.exit(1)
branches = []
with open(hist) as f:
    for raw in f:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        p = int(parts[0]); parse_lsn(parts[1])
        branches.append((p, parts[1]))
if not branches:
    print("FAIL: history file has no branch records"); sys.exit(1)
branch_parent, branch_lsn = branches[-1]
if branch_parent != parent:
    print(f"FAIL: last branch parent {branch_parent} != expected {parent}"); sys.exit(1)
if cmp(backup_start, branch_lsn) > 0 or cmp(branch_lsn, replay_end) > 0 or cmp(backup_start, restore) > 0 or cmp(restore, replay_end) > 0:
    print("FAIL: LSN window violated on second restore"); sys.exit(1)
with open(os.environ["PITR_EVIDENCE"], "w") as out:
    json.dump({"history_file": base, "branch_parent": branch_parent, "branch_lsn": branch_lsn, "result": "second branch inside window; last-branch check for multi-hop"}, out, indent=2)
    out.write("\n")
print(f"OK: second history {base} branches {branch_parent}->{child}")
PY
echo "OK: second strict history validation passed" | tee -a "$EVIDENCE/rewind-check.txt"
# Second boot on the rewound DB must issue a still-newer G4 (GEN_LOG history proves chain).
(cd "$ROOT/controller" && ./mvnw -B -ntp -Dspring.datasource.url="jdbc:postgresql://localhost:$PGPORT/clearance" -Dclearance.recovery-generation-log="$GEN_LOG" -Dpitr.physical.evidence="$EVIDENCE" -Dtest='PitrPhysicalPostRestoreIntegrationTest' test 2>&1 | tail -n 30 | tee "$EVIDENCE/recovery-boot-2.log")
G4="$(PITR_GEN_JSON="$EVIDENCE/generations.json" python3 -c 'import json,os; print(json.load(open(os.environ["PITR_GEN_JSON"]))["generation_after"])')"
echo "generation_after_second_boot=$G4 (first $G3)" | tee -a "$EVIDENCE/rewind-check.txt"
if [ "$G4" = "$G3" ]; then
  echo "FAIL: repeated rewind repeated generation ($G4)" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
else
  echo "OK: repeated rewind issued distinct generation" | tee -a "$EVIDENCE/rewind-check.txt"
fi
export PITR_GEN_LOG="$GEN_LOG" PITR_GEN_EVIDENCE="$EVIDENCE/gen-history-2.json" PITR_G3="$G3" PITR_G4="$G4"
python3 - <<'PY'
import json, os, sys
path = os.environ["PITR_GEN_LOG"]
gens = [l.strip().split()[0] for l in open(path) if l.strip()]
def ts_ms(u):
  return int(u.replace("-", "")[0:12], 16)
assert os.environ["PITR_G3"] in gens and os.environ["PITR_G4"] in gens, f"G3/G4 missing from log: {gens}"
assert ts_ms(os.environ["PITR_G4"]) > ts_ms(os.environ["PITR_G3"]), "G4 not strictly newer than G3"
with open(os.environ["PITR_GEN_EVIDENCE"], "w") as out:
  json.dump({"g3": os.environ["PITR_G3"], "g4": os.environ["PITR_G4"], "log": gens, "result": "successive post-restore generations distinct and monotonic"}, out, indent=2)
  out.write("\n")
print(f"OK: G3 {os.environ['PITR_G3']} -> G4 {os.environ['PITR_G4']} strictly newer")
PY
# Clean up surviving sleepers from T1 if the gates left any (normally reaped by CLEANUP).
kill "$PID_IDLE" "$PID_ALLOC" 2>/dev/null || true

log "evidence at $EVIDENCE"
ls -lh "$EVIDENCE"
log "to clean volumes: docker compose -f $COMPOSE down -v"
