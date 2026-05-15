// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

type workersNodeState struct {
	env  *RaftEnv
	live bool
}

func TestWorkersRaftTCPConsensus(t *testing.T) {
	scenario := os.Getenv("WORKERS_RAFT_SCENARIO")
	if scenario == "" {
		scenario = "churn"
	}

	rootSeed := workersRootSeed()
	rng := rand.New(rand.NewSource(int64(rootSeed)))
	ops := workersEnvInt(t, "WORKERS_RAFT_OPS", 72)
	applyTimeout := workersEnvDuration(t, "WORKERS_RAFT_APPLY_TIMEOUT", 8*time.Second)
	stabilizeTimeout := workersEnvDuration(t, "WORKERS_RAFT_STABILIZE_TIMEOUT", 25*time.Second)

	nodes := workersStartCluster(t, 5, stabilizeTimeout, applyTimeout)
	defer workersReleaseAll(nodes)

	applied := make([][]byte, 0, ops)
	switch scenario {
	case "baseline":
		workersApplyRange(t, nodes, &applied, rng, 0, ops, applyTimeout, stabilizeTimeout)
	case "restart":
		workersApplyRange(t, nodes, &applied, rng, 0, ops/3, applyTimeout, stabilizeTimeout)
		workersRestartCatchup(t, nodes, &applied, rng, ops/3, ops, applyTimeout, stabilizeTimeout)
	case "membership":
		workersApplyRange(t, nodes, &applied, rng, 0, ops/3, applyTimeout, stabilizeTimeout)
		workersMembershipChurn(t, nodes, &applied, rng, ops/3, ops, applyTimeout, stabilizeTimeout)
	case "churn":
		workersApplyRange(t, nodes, &applied, rng, 0, ops/4, applyTimeout, stabilizeTimeout)
		workersRestartCatchup(t, nodes, &applied, rng, ops/4, ops/2, applyTimeout, stabilizeTimeout)
		workersMembershipChurn(t, nodes, &applied, rng, ops/2, ops, applyTimeout, stabilizeTimeout)
	default:
		t.Fatalf("unknown WORKERS_RAFT_SCENARIO %q", scenario)
	}

	workersWaitConsistent(t, workersLive(nodes), applied, stabilizeTimeout)
	leader := workersWaitForLeader(t, workersLive(nodes), stabilizeTimeout)
	digest := workersDigest(applied)
	fmt.Printf(
		"WORKERS_RAFT_SUMMARY scenario=%s seed=%016x nodes=%d leader=%s applied=%d digest=%s apply_timeout=%s stabilize_timeout=%s\n",
		scenario,
		rootSeed,
		len(workersLive(nodes)),
		leader.raft.localID,
		len(applied),
		digest,
		applyTimeout,
		stabilizeTimeout,
	)
}

func workersStartCluster(t *testing.T, count int, stabilizeTimeout, applyTimeout time.Duration) []*workersNodeState {
	t.Helper()
	nodes := make([]*workersNodeState, 0, count)
	for i := 0; i < count; i++ {
		conf := workersRaftConfig(ServerID(fmt.Sprintf("workers-%d", i)))
		env := MakeRaft(t, conf, i == 0)
		nodes = append(nodes, &workersNodeState{env: env, live: true})
	}

	leader := workersWaitForLeader(t, workersLive(nodes), stabilizeTimeout)
	for i, node := range nodes[1:] {
		future := leader.raft.AddVoter(node.env.raft.localID, node.env.trans.LocalAddr(), 0, applyTimeout)
		if err := future.Error(); err != nil {
			t.Fatalf("add voter %s: %v", node.env.raft.localID, err)
		}
		workersWaitConsistentConfiguration(t, workersEnvPrefix(nodes, i+2), i+2, stabilizeTimeout)
		leader = workersWaitForLeader(t, workersLive(nodes), stabilizeTimeout)
	}
	return nodes
}

func workersRaftConfig(id ServerID) *Config {
	conf := DefaultConfig()
	conf.LocalID = id
	conf.HeartbeatTimeout = 350 * time.Millisecond
	conf.ElectionTimeout = 350 * time.Millisecond
	conf.LeaderLeaseTimeout = 350 * time.Millisecond
	conf.CommitTimeout = 35 * time.Millisecond
	conf.SnapshotThreshold = 16
	conf.SnapshotInterval = 250 * time.Millisecond
	conf.TrailingLogs = 8
	return conf
}

func workersRestartCatchup(
	t *testing.T,
	nodes []*workersNodeState,
	applied *[][]byte,
	rng *rand.Rand,
	start int,
	end int,
	applyTimeout time.Duration,
	stabilizeTimeout time.Duration,
) {
	t.Helper()
	live := workersLive(nodes)
	workersWaitConsistent(t, live, *applied, stabilizeTimeout)
	leader := workersWaitForLeader(t, live, stabilizeTimeout)
	target := workersPickFollower(t, nodes, leader, rng)
	target.env.Shutdown()
	target.live = false

	mid := start + (end-start)/2
	workersApplyRange(t, nodes, applied, rng, start, mid, applyTimeout, stabilizeTimeout)
	snap := workersWaitForLeader(t, workersLive(nodes), stabilizeTimeout).raft.Snapshot()
	if err := snap.Error(); err != nil {
		t.Fatalf("snapshot during restart catch-up: %v", err)
	}
	workersApplyRange(t, nodes, applied, rng, mid, end, applyTimeout, stabilizeTimeout)

	target.env.Restart(t)
	target.live = true
	workersWaitConsistent(t, workersLive(nodes), *applied, stabilizeTimeout)
}

func workersMembershipChurn(
	t *testing.T,
	nodes []*workersNodeState,
	applied *[][]byte,
	rng *rand.Rand,
	start int,
	end int,
	applyTimeout time.Duration,
	stabilizeTimeout time.Duration,
) {
	t.Helper()
	live := workersLive(nodes)
	leader := workersWaitForLeader(t, live, stabilizeTimeout)
	target := workersPickFollower(t, nodes, leader, rng)

	remove := leader.raft.RemoveServer(target.env.raft.localID, 0, applyTimeout)
	if err := remove.Error(); err != nil {
		t.Fatalf("remove voter %s: %v", target.env.raft.localID, err)
	}
	workersWaitConsistentConfiguration(t, workersLive(nodes), len(nodes)-1, stabilizeTimeout)

	mid := start + (end-start)/2
	workersApplyRange(t, nodes, applied, rng, start, mid, applyTimeout, stabilizeTimeout)

	leader = workersWaitForLeader(t, workersLive(nodes), stabilizeTimeout)
	add := leader.raft.AddVoter(target.env.raft.localID, target.env.trans.LocalAddr(), 0, applyTimeout)
	if err := add.Error(); err != nil {
		t.Fatalf("re-add voter %s: %v", target.env.raft.localID, err)
	}
	workersWaitConsistentConfiguration(t, workersLive(nodes), len(nodes), stabilizeTimeout)
	workersWaitConsistent(t, workersLive(nodes), *applied, stabilizeTimeout)

	leader = workersWaitForLeader(t, workersLive(nodes), stabilizeTimeout)
	transfer := leader.raft.LeadershipTransfer()
	if err := transfer.Error(); err != nil {
		t.Fatalf("leadership transfer from %s: %v", leader.raft.localID, err)
	}
	workersWaitForLeaderChange(t, workersLive(nodes), leader.raft.localID, stabilizeTimeout)
	workersApplyRange(t, nodes, applied, rng, mid, end, applyTimeout, stabilizeTimeout)
}

func workersApplyRange(
	t *testing.T,
	nodes []*workersNodeState,
	applied *[][]byte,
	rng *rand.Rand,
	start int,
	end int,
	applyTimeout time.Duration,
	stabilizeTimeout time.Duration,
) {
	t.Helper()
	for i := start; i < end; i++ {
		payload := workersPayload(rng, i)
		workersApply(t, workersLive(nodes), payload, applyTimeout, stabilizeTimeout)
		*applied = append(*applied, payload)
	}
}

func workersPayload(rng *rand.Rand, index int) []byte {
	size := 32 + rng.Intn(96)
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte('a' + rng.Intn(26))
	}
	return []byte(fmt.Sprintf("workers-raft-entry-%03d-%x-%s", index, rng.Uint64(), string(buf)))
}

func workersApply(t *testing.T, envs []*RaftEnv, payload []byte, applyTimeout, stabilizeTimeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(stabilizeTimeout)
	var lastErr error

	for time.Now().Before(deadline) {
		leader := workersWaitForLeader(t, envs, stabilizeTimeout)
		future := leader.raft.Apply(payload, applyTimeout)
		if err := future.Error(); err == nil {
			return
		} else {
			lastErr = err
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("apply %q did not commit before %s: %v", payload, stabilizeTimeout, lastErr)
}

func workersWaitForLeader(t *testing.T, envs []*RaftEnv, timeout time.Duration) *RaftEnv {
	t.Helper()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		for _, env := range envs {
			if env.raft.State() == Leader {
				return env
			}
		}
		time.Sleep(25 * time.Millisecond)
	}

	var states bytes.Buffer
	for _, env := range envs {
		fmt.Fprintf(&states, "%s=%s ", env.raft.localID, env.raft.State())
	}
	t.Fatalf("no leader before %s: %s", timeout, states.String())
	return nil
}

func workersWaitForLeaderChange(t *testing.T, envs []*RaftEnv, old ServerID, timeout time.Duration) *RaftEnv {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, env := range envs {
			if env.raft.State() == Leader && env.raft.localID != old {
				return env
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("leader did not change from %s before %s", old, timeout)
	return nil
}

func workersWaitConsistent(t *testing.T, envs []*RaftEnv, expected [][]byte, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		if err := workersCheckConsistent(envs, expected); err == nil {
			return
		} else {
			lastErr = err
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("cluster did not converge to %d entries before %s: %v", len(expected), timeout, lastErr)
}

func workersCheckConsistent(envs []*RaftEnv, expected [][]byte) error {
	for _, env := range envs {
		env.fsm.Lock()
		logs := append([][]byte(nil), env.fsm.logs...)
		env.fsm.Unlock()

		if len(logs) < len(expected) {
			return fmt.Errorf("%s applied %d entries, want at least %d", env.raft.localID, len(logs), len(expected))
		}
		logs = logs[:len(expected)]
		for i := range expected {
			if !bytes.Equal(expected[i], logs[i]) {
				return fmt.Errorf("%s entry %d mismatch", env.raft.localID, i)
			}
		}
	}
	return nil
}

func workersWaitConsistentConfiguration(t *testing.T, envs []*RaftEnv, voters int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string

	for time.Now().Before(deadline) {
		ok := true
		parts := make([]string, 0, len(envs))
		for _, env := range envs {
			future := env.raft.GetConfiguration()
			if err := future.Error(); err != nil {
				ok = false
				parts = append(parts, fmt.Sprintf("%s:error=%v", env.raft.localID, err))
				continue
			}
			config := future.Configuration()
			count := 0
			for _, server := range config.Servers {
				if server.Suffrage == Voter {
					count++
				}
			}
			parts = append(parts, fmt.Sprintf("%s:voters=%d", env.raft.localID, count))
			if count != voters {
				ok = false
			}
		}
		if ok {
			return
		}
		last = strings.Join(parts, " ")
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("configuration did not converge to %d voters before %s: %s", voters, timeout, last)
}

func workersPickFollower(t *testing.T, nodes []*workersNodeState, leader *RaftEnv, rng *rand.Rand) *workersNodeState {
	t.Helper()
	var followers []*workersNodeState
	for _, node := range nodes {
		if node.live && node.env.raft.localID != leader.raft.localID {
			followers = append(followers, node)
		}
	}
	if len(followers) == 0 {
		t.Fatalf("no live follower available")
	}
	return followers[rng.Intn(len(followers))]
}

func workersLive(nodes []*workersNodeState) []*RaftEnv {
	envs := make([]*RaftEnv, 0, len(nodes))
	for _, node := range nodes {
		if node.live {
			envs = append(envs, node.env)
		}
	}
	return envs
}

func workersEnvPrefix(nodes []*workersNodeState, count int) []*RaftEnv {
	envs := make([]*RaftEnv, 0, count)
	for _, node := range nodes[:count] {
		envs = append(envs, node.env)
	}
	return envs
}

func workersReleaseAll(nodes []*workersNodeState) {
	for _, node := range nodes {
		if node.live {
			node.env.Release()
		} else {
			_ = os.RemoveAll(node.env.dir)
		}
	}
}

func workersDigest(entries [][]byte) string {
	h := sha256.New()
	var lenBuf [8]byte
	for _, entry := range entries {
		binary.BigEndian.PutUint64(lenBuf[:], uint64(len(entry)))
		_, _ = h.Write(lenBuf[:])
		_, _ = h.Write(entry)
	}
	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}

func workersRootSeed() uint64 {
	raw := os.Getenv("FORMAL_SEED")
	if raw == "" {
		raw = os.Getenv("WENV_SEED")
	}
	if raw == "" {
		raw = "workers-raft-default-seed"
	}
	digest := sha256.Sum256([]byte("workers-raft:" + raw))
	return binary.BigEndian.Uint64(digest[:8])
}

func workersEnvInt(t *testing.T, key string, fallback int) int {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		t.Fatalf("%s must be a positive integer, got %q", key, value)
	}
	return parsed
}

func workersEnvDuration(t *testing.T, key string, fallback time.Duration) time.Duration {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		t.Fatalf("%s must be a positive duration, got %q", key, value)
	}
	return parsed
}
