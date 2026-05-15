#!/usr/bin/env bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

test_file="workers_raft_tcp_consensus_test.go"
cp .workers/workloads/workers_raft_tcp_consensus_test.go "${test_file}"
trap 'rm -f "${test_file}"' EXIT

export GOTRACEBACK=all
export WORKERS_RAFT_OPS="${WORKERS_RAFT_OPS:-40}"
export WORKERS_RAFT_APPLY_TIMEOUT="${WORKERS_RAFT_APPLY_TIMEOUT:-6s}"
export WORKERS_RAFT_STABILIZE_TIMEOUT="${WORKERS_RAFT_STABILIZE_TIMEOUT:-20s}"

go test -run '^TestWorkersRaftTCPConsensus$' -count=1 -timeout "${WORKERS_RAFT_GO_TEST_TIMEOUT:-180s}" .
