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
file afterward. The test builds a five-node Raft cluster over real TCP
transports, derives its operation stream from `FORMAL_SEED` / `WENV_SEED`, and
fails unless live nodes converge on the exact FSM log and expected voter
configuration.

Scenarios:

- `baseline`: add five voters and apply seeded log entries.
- `restart`: force a follower offline, apply entries, snapshot, restart it,
  and require snapshot/log catch-up.
- `membership`: remove and re-add a voter, transfer leadership, then apply
  more entries.
- `churn`: run restart catch-up followed by membership and leadership churn.

Useful knobs:

- `WORKERS_RAFT_SCENARIO`: `baseline`, `restart`, `membership`, or `churn`.
  Default: `churn`.
- `WORKERS_RAFT_OPS`: number of log entries to apply. Default: `72`.
- `WORKERS_RAFT_APPLY_TIMEOUT`: timeout passed to each `Raft.Apply`. Default:
  `8s`.
- `WORKERS_RAFT_STABILIZE_TIMEOUT`: leader/convergence wait budget. Default:
  `25s`.
- `WORKERS_RAFT_GO_TEST_TIMEOUT`: outer `go test` timeout. Default: `180s`.
- `WORKERS_RAFT_VERBOSE=1`: pass `-v` to `go test` so the
  `WORKERS_RAFT_SUMMARY` line is printed on success.

Recommended seed/fault sweep:

```bash
wio simulate create <project-id> \
  --workload-path .workers/workload.sh \
  --command "WORKERS_RAFT_SCENARIO=churn WORKERS_RAFT_OPS=96 bash .workers/workload.sh" \
  --faults latency-jitter,packet-loss,reorder,rate-limit \
  --depth 100 \
  --timeout 300 \
  --mem 1024
```
