#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
evidence_dir="$repo_root/controller/target/agent-integration"
mkdir -p "$evidence_dir"
run_dir=$(mktemp -d "$evidence_dir/run.XXXXXX")
schema="agent_integration_$(date +%s)_$$"
export PGHOST="${PGHOST:-localhost}" PGPORT="${PGPORT:-5544}"
export PGDATABASE="${PGDATABASE:-clearance}" PGUSER="${PGUSER:-clearance}"
export PGPASSWORD="${PGPASSWORD:-clearance}"
export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-5}"
controller_pid=
cleanup() {
  if [[ -n "$controller_pid" ]]; then
    kill "$controller_pid" 2>/dev/null || true
    for ((attempt = 0; attempt < 100; attempt++)); do
      if ! kill -0 "$controller_pid" 2>/dev/null; then
        break
      fi
      sleep 0.1
    done
    if kill -0 "$controller_pid" 2>/dev/null; then
      kill -KILL "$controller_pid" 2>/dev/null || true
    fi
    wait "$controller_pid" 2>/dev/null || true
  fi
  # The identifier is generated above; never remove the application's default schema.
  PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=5000" psql -X -q -v ON_ERROR_STOP=1 \
    -c "DROP SCHEMA IF EXISTS $schema CASCADE" >"$run_dir/cleanup.log" 2>&1
}
trap cleanup EXIT

for executable in java javac go psql; do
  command -v "$executable" >/dev/null
done
psql -X -q -v ON_ERROR_STOP=1 -c 'SELECT 1' >"$run_dir/database.log"
(
  cd "$repo_root/controller"
  bash ./mvnw -B -ntp -DskipTests compile dependency:build-classpath \
    -Dmdep.outputFile=target/agent-integration-classpath.txt
) >"$run_dir/build.log" 2>&1 || { cat "$run_dir/build.log"; exit 1; }
classpath="$repo_root/controller/target/classes:$(cat "$repo_root/controller/target/agent-integration-classpath.txt")"
javac -cp "$classpath" -d "$run_dir" "$repo_root/agent/testdata/ControllerHarness.java"
java -cp "$run_dir:$classpath" ControllerHarness "$run_dir/fixtures.json" "$schema" \
  >"$run_dir/controller.log" 2>&1 &
controller_pid=$!
for ((attempt = 0; attempt < 120; attempt++)); do
  if [[ -s "$run_dir/fixtures.json" ]]; then
    break
  fi
  if ! kill -0 "$controller_pid" 2>/dev/null; then
    cat "$run_dir/controller.log"
    exit 1
  fi
  sleep 0.5
done
if [[ ! -s "$run_dir/fixtures.json" ]]; then
  cat "$run_dir/controller.log"
  echo "Controller did not become ready within 60 seconds" >&2
  exit 1
fi
export CLEARANCE_INTEGRATION_FIXTURES="$run_dir/fixtures.json"
export CLEARANCE_INTEGRATION_BINARY="$run_dir/clearance-agent"
cd "$repo_root/agent"
go build -race -o "$CLEARANCE_INTEGRATION_BINARY" ./cmd/clearance-agent
go test -race -tags integration -count=1 -timeout=90s -run '^TestController' -v ./... \
  2>&1 | tee "$run_dir/go-test.log"
