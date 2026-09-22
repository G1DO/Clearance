#!/usr/bin/env bash
# Exchange actual serializer and codec output; no database is required.
set -euo pipefail
repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
mkdir -p "$repo_dir/controller/target/agent-wire-contract"
evidence_dir=$(mktemp -d "$repo_dir/controller/target/agent-wire-contract/run.XXXXXX")
printf 'Agent v1 evidence: %s\n' "$evidence_dir"

(
  cd "$repo_dir/controller"
  env -u WIRE_PEER_DIR WIRE_EXPORT_DIR="$evidence_dir/java" \
    bash ./mvnw -B -ntp -Dtest=AgentWireContractTest test
) 2>&1 | tee "$evidence_dir/java-export.log"

(
  cd "$repo_dir/agent"
  WIRE_PEER_DIR="$evidence_dir/java" WIRE_EXPORT_DIR="$evidence_dir/go" \
    go test ./... -count=1 -v
) 2>&1 | tee "$evidence_dir/java-to-go.log"

(
  cd "$repo_dir/controller"
  env -u WIRE_EXPORT_DIR WIRE_PEER_DIR="$evidence_dir/go" \
    bash ./mvnw -B -ntp -Dtest=AgentWireContractTest test
) 2>&1 | tee "$evidence_dir/go-to-java.log"
