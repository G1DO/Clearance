#!/usr/bin/env bash
set -euo pipefail

# Run from a process inside the same delegated cgroup subtree as the target.
# The target must permit creating allocation cgroups and CLONE_INTO_CGROUP.
: "${AGENT_CGROUP_TEST_ROOT:?set AGENT_CGROUP_TEST_ROOT to a writable delegated cgroup v2 directory}"
agent_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd "$agent_dir"
if [[ $(stat -f --format=%T "$AGENT_CGROUP_TEST_ROOT") != cgroup2fs ]]; then
  echo "AGENT_CGROUP_TEST_ROOT must be a cgroup v2 filesystem" >&2
  exit 1
fi
mkdir -p target/linux-containment
run_dir=$(mktemp -d target/linux-containment/run.XXXXXX)
{
  date -u +'%Y-%m-%dT%H:%M:%SZ'
  uname -a
  go version
  id
  cat /proc/self/cgroup
  printf 'delegated_target=%s\n' "$AGENT_CGROUP_TEST_ROOT"
} > "$run_dir/host.txt"
go test -race -run '^Test(Containment|PhysicalContainment)' -count=1 -v -timeout=1m . 2>&1 | tee "$run_dir/tests.log"
printf 'Physical containment evidence: %s/%s\n' "$agent_dir" "$run_dir"
