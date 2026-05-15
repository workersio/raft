# Workers IO Raft Workload

Run the baseline workload:

```bash
wio simulate create <project-id> \
  --workload-path .workers/workload.sh \
  --command "bash .workers/workload.sh" \
  --depth 1 \
  --timeout 300 \
  --mem 1024
```

Run the same workload with network fault models:

```bash
wio simulate create <project-id> \
  --workload-path .workers/workload.sh \
  --command "bash .workers/workload.sh" \
  --faults latency-jitter,packet-loss,reorder,rate-limit \
  --depth 25 \
  --timeout 300 \
  --mem 1024
```

Fault names resolve to `.workers/fault/net/<name>.json`.

The workload copies `.workers/workloads/workers_raft_tcp_consensus_test.go`
into the package root, runs one temporary Go test, and removes the temporary
file afterward. The test builds a three-node Raft cluster over real TCP
transports, applies a sequence of log entries, and fails unless all nodes
converge on the same FSM log.

Useful knobs:

- `WORKERS_RAFT_OPS`: number of log entries to apply. Default: `40`.
- `WORKERS_RAFT_APPLY_TIMEOUT`: timeout passed to each `Raft.Apply`. Default: `6s`.
- `WORKERS_RAFT_STABILIZE_TIMEOUT`: leader/convergence wait budget. Default: `20s`.
- `WORKERS_RAFT_GO_TEST_TIMEOUT`: outer `go test` timeout. Default: `180s`.
