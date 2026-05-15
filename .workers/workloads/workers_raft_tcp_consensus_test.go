// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestWorkersRaftTCPConsensus(t *testing.T) {
	ops := workersEnvInt(t, "WORKERS_RAFT_OPS", 40)
	applyTimeout := workersEnvDuration(t, "WORKERS_RAFT_APPLY_TIMEOUT", 6*time.Second)
	stabilizeTimeout := workersEnvDuration(t, "WORKERS_RAFT_STABILIZE_TIMEOUT", 20*time.Second)

	envs := make([]*RaftEnv, 0, 3)
	for i := 0; i < 3; i++ {
		conf := workersRaftConfig(ServerID(fmt.Sprintf("workers-%d", i)))
		env := MakeRaft(t, conf, i == 0)
		envs = append(envs, env)
		defer env.Release()
	}

	leader := workersWaitForLeader(t, envs, stabilizeTimeout)
	for _, env := range envs[1:] {
		future := leader.raft.AddVoter(env.raft.localID, env.trans.LocalAddr(), 0, applyTimeout)
		if err := future.Error(); err != nil {
			t.Fatalf("add voter %s: %v", env.raft.localID, err)
		}
		leader = workersWaitForLeader(t, envs, stabilizeTimeout)
	}

	for i := 0; i < ops; i++ {
		payload := []byte(fmt.Sprintf("workers-raft-entry-%03d", i))
		workersApply(t, envs, payload, applyTimeout, stabilizeTimeout)
	}

	workersWaitConsistent(t, envs, ops, stabilizeTimeout)
	leader = workersWaitForLeader(t, envs, stabilizeTimeout)
	fmt.Printf(
		"WORKERS_RAFT_SUMMARY nodes=%d leader=%s applied=%d apply_timeout=%s stabilize_timeout=%s\n",
		len(envs),
		leader.raft.localID,
		ops,
		applyTimeout,
		stabilizeTimeout,
	)
}

func workersRaftConfig(id ServerID) *Config {
	conf := DefaultConfig()
	conf.LocalID = id
	conf.HeartbeatTimeout = 400 * time.Millisecond
	conf.ElectionTimeout = 400 * time.Millisecond
	conf.LeaderLeaseTimeout = 400 * time.Millisecond
	conf.CommitTimeout = 50 * time.Millisecond
	conf.SnapshotThreshold = 128
	conf.TrailingLogs = 32
	return conf
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

func workersWaitConsistent(t *testing.T, envs []*RaftEnv, expected int, timeout time.Duration) {
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

	t.Fatalf("cluster did not converge to %d entries before %s: %v", expected, timeout, lastErr)
}

func workersCheckConsistent(envs []*RaftEnv, expected int) error {
	first := envs[0]
	first.fsm.Lock()
	firstLogs := append([][]byte(nil), first.fsm.logs...)
	first.fsm.Unlock()

	if len(firstLogs) < expected {
		return fmt.Errorf("%s applied %d entries, want at least %d", first.raft.localID, len(firstLogs), expected)
	}

	for _, env := range envs[1:] {
		env.fsm.Lock()
		logs := append([][]byte(nil), env.fsm.logs...)
		env.fsm.Unlock()

		if len(logs) != len(firstLogs) {
			return fmt.Errorf("%s applied %d entries, want %d", env.raft.localID, len(logs), len(firstLogs))
		}
		for i := range firstLogs {
			if !bytes.Equal(firstLogs[i], logs[i]) {
				return fmt.Errorf("%s entry %d mismatch", env.raft.localID, i)
			}
		}
	}
	return nil
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
