#!/usr/bin/env bash
# Physical PITR F14 drill for issue #40 (isolated postgres-pitr:5545 only).
# Never touches dev clearance-postgres:5544 or production.
# Proves: pg_basebackup + WAL replay + timeline branch + *.history validation,
# T1 DDL/sequence rewind, recovery-mode fresh generation, fleet quarantine.
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

log "ensuring schema via Flyway (controller test migration run)"
(cd "$ROOT/controller" && ./mvnw -B -ntp -Dspring.datasource.url="jdbc:postgresql://localhost:$PGPORT/clearance" -Dtest=PitrHistoryValidatorTest test >/dev/null)

log "T0 fleet seed: idle AVAILABLE runner that must survive rewind then quarantine"
T0_RUNNER_ID="$(cat /proc/sys/kernel/random/uuid)"
psql_pitr "INSERT INTO runners (runner_id, runner_class, state, epoch) VALUES ('$T0_RUNNER_ID', 'default', 'AVAILABLE', 0) ON CONFLICT DO NOTHING;"
psql_pitr "SELECT runner_id::text || ' ' || state FROM runners WHERE runner_id = '$T0_RUNNER_ID';" | tee "$EVIDENCE/t0-runner.txt"
# Durable generation log lives on the host (outside PGDATA) so a PGDATA wipe cannot rewind it.
GEN_LOG="$EVIDENCE/recovery-generations.log"
echo "pitr-physical drill $STAMP durable log outside PGDATA (host evidence dir, not in PGDATA volume)" > "$GEN_LOG"
log "durable log marker at $GEN_LOG (host path, survives PGDATA wipe by design)"

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
{"t0_name":"$T0_NAME","backup_start_lsn":"$BACKUP_START_LSN","restore_point_lsn":"$RESTORE_LSN","backup_done_lsn":"$BACKUP_DONE_LSN","timeline":"$T0_TIMELINE","walfile":"$WALFILE","t0_runner_id":"$T0_RUNNER_ID"}
JSON
psql_pitr "SELECT row_to_json(t)::text FROM runners t ORDER BY runner_id;" | tee "$EVIDENCE/pre-restore-runners-t0.json"
psql_pitr "SELECT count(*) FROM runners;" | tee "$EVIDENCE/pre-restore-runners-count.txt"

log "T1: writes incl DDL ALTER/DROP + sequences (must vanish after PITR)"
psql_pitr_verbose "CREATE TABLE IF NOT EXISTS pitr_f14_probe (id BIGINT PRIMARY KEY, note TEXT); INSERT INTO pitr_f14_probe VALUES (1,'t1') ON CONFLICT DO NOTHING;"
psql_pitr_verbose "CREATE SEQUENCE IF NOT EXISTS pitr_f14_seq_probe; SELECT nextval('pitr_f14_seq_probe');"
psql_pitr_verbose "ALTER TABLE pitr_f14_probe ADD COLUMN IF NOT EXISTS t1_extra TEXT; UPDATE pitr_f14_probe SET t1_extra='dirty' WHERE id=1;"
psql_pitr_verbose "CREATE TABLE pitr_f14_post_t0 (id BIGINT PRIMARY KEY); INSERT INTO pitr_f14_post_t0 VALUES (42);"
# Post-T0 runner must vanish after PITR (proves full-database rewind covers runners, not just probes).
T1_RUNNER_ID="$(cat /proc/sys/kernel/random/uuid)"
psql_pitr "INSERT INTO runners (runner_id, runner_class, state, epoch) VALUES ('$T1_RUNNER_ID', 'default', 'AVAILABLE', 0);"
T1_LSN="$(psql_pitr "SELECT pg_current_wal_lsn();")"
psql_pitr "SELECT pg_switch_wal();" >/dev/null || true
sleep 3
docker exec clearance-postgres-pitr sh -lc 'ls /var/lib/postgresql/wal-archive | wc -l; ls /var/lib/postgresql/wal-archive | tail -n 5' | tee "$EVIDENCE/wal-after-t1.txt"
cat > "$EVIDENCE/t1-marker.json" <<JSON
{"t1_lsn":"$T1_LSN","probe":"pitr_f14_probe + pitr_f14_seq_probe + pitr_f14_post_t0","t0_runner_id":"$T0_RUNNER_ID","t1_runner_id":"$T1_RUNNER_ID"}
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
psql_pitr "SELECT runner_id::text || ' ' || state FROM runners ORDER BY runner_id;" | tee "$EVIDENCE/post-restore-runners.txt"
psql_pitr "SELECT row_to_json(t)::text FROM runners t ORDER BY runner_id;" | tee "$EVIDENCE/post-restore-database.json"
if [ ! -f "$GEN_LOG" ]; then
  echo "FAIL: durable generation log $GEN_LOG did not survive PGDATA wipe (must live outside PGDATA)" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
else
  echo "OK: durable generation log survives PGDATA wipe (host path outside volume)" | tee -a "$EVIDENCE/rewind-check.txt"
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
branch_parent, branch_lsn = branches[0]
if branch_parent != parent:
    print(f"FAIL: branch parent {branch_parent} != expected {parent}")
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

log "recovery-mode boot + quarantine (uses durable log outside PGDATA)"
export CLEARANCE_LOG="$GEN_LOG"
(cd "$ROOT/controller" && SPRING_DATASOURCE_URL="jdbc:postgresql://localhost:$PGPORT/clearance" ./mvnw -B -ntp -Dspring.datasource.url="jdbc:postgresql://localhost:$PGPORT/clearance" -Dclearance.recovery-generation-log="$GEN_LOG" -Dtest=RecoveryMonotonicGenerationIntegrationTest test 2>&1 | tail -n 20 | tee "$EVIDENCE/recovery-boot.log")
# The boot quarantines the whole fleet (QUARANTINE_FLEET_SQL) including the rewound T0 runner.
T0_POST_BOOT_STATE="$(psql_pitr "SELECT state FROM runners WHERE runner_id = '$T0_RUNNER_ID';")"
T0_POST_BOOT_REASON="$(psql_pitr "SELECT quarantine_reason FROM runners WHERE runner_id = '$T0_RUNNER_ID';")"
echo "t0_runner_post_boot_state=$T0_POST_BOOT_STATE reason=$T0_POST_BOOT_REASON" | tee -a "$EVIDENCE/rewind-check.txt"
if [ "$T0_POST_BOOT_STATE" != "QUARANTINED" ]; then
  echo "FAIL: T0 runner not quarantined after recovery boot (got '$T0_POST_BOOT_STATE')" | tee -a "$EVIDENCE/rewind-check.txt"
  exit 1
else
  echo "OK: T0 restored runner quarantined after recovery boot regardless of AVAILABLE" | tee -a "$EVIDENCE/rewind-check.txt"
fi
psql_pitr "SELECT row_to_json(t)::text FROM runners t ORDER BY runner_id;" | tee "$EVIDENCE/post-boot-runners.json"
# Claim-refusal, stale zero-mutation, reconciliation and repeated-rewind freshness on this
# physical timeline are proven by the controller gates that CI runs next against :5545
# (RecoveryGenerationIntegrationTest + PitrRewindDrillIntegrationTest with OS-process
# RecoveryBootRunner boots); live-cgroup termination is proven by the real-cgroup drill.

log "evidence at $EVIDENCE"
ls -lh "$EVIDENCE"
log "to clean volumes: docker compose -f $COMPOSE down -v"
