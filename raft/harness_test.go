package raft

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// testLogger 打印任期/节点/提交位点等判定依据（-v 时可见）。
type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) {
	l.t.Logf("%s %s", time.Now().Format("15:04:05.000"), fmt.Sprintf(format, args...))
}

type testCluster struct {
	t        *testing.T
	tdir     string
	net      *Network
	nodes    map[string]*Node
	fsms     map[string]*KVStateMachine
	storages map[string]*FileStorage
	peers    []string
	timeout  time.Duration
	cfgMut   sync.Mutex
}

func newTestCluster(t *testing.T, n int, election time.Duration) *testCluster {
	t.Helper()
	tdir := t.TempDir()
	net := NewNetwork(512)
	net.Seed(time.Now().UnixNano())
	peers := make([]string, n)
	for i := range peers {
		peers[i] = fmt.Sprintf("n%d", i+1)
	}
	c := &testCluster{
		t:        t,
		tdir:     tdir,
		net:      net,
		nodes:    map[string]*Node{},
		fsms:     map[string]*KVStateMachine{},
		storages: map[string]*FileStorage{},
		peers:    peers,
		timeout:  20 * time.Second,
	}
	for _, id := range peers {
		c.startNode(id)
	}
	return c
}

func (c *testCluster) startNode(id string) {
	dir := filepath.Join(c.tdir, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		c.t.Fatalf("mkdir: %v", err)
	}
	st, err := NewFileStorage(dir)
	if err != nil {
		c.t.Fatalf("storage %s: %v", id, err)
	}
	fsm := NewKVStateMachine()
	tr := c.net.AddNode(id)
	cfg := Config{
		NodeID:            id,
		Peers:             append([]string(nil), c.peers...),
		DataDir:           dir,
		ElectionTimeout:   300 * time.Millisecond,
		HeartbeatInterval: 60 * time.Millisecond,
		RandomElection:    true,
	}
	if c.nodes != nil {
		if _, exists := c.nodes[id]; exists {
			c.t.Fatalf("node %s already running", id)
		}
	}
	node, err := NewNode(cfg, st, fsm, tr, testLogger{c.t})
	if err != nil {
		c.t.Fatalf("new node %s: %v", id, err)
	}
	node.Start(context.Background())
	c.nodes[id] = node
	c.fsms[id] = fsm
	c.storages[id] = st
}

func (c *testCluster) stopNode(id string) {
	if n, ok := c.nodes[id]; ok {
		n.Stop()
		delete(c.nodes, id)
	}
}

func (c *testCluster) startNodeWithPeers(id string, peers []string) {
	dir := filepath.Join(c.tdir, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		c.t.Fatalf("mkdir: %v", err)
	}
	st, err := NewFileStorage(dir)
	if err != nil {
		c.t.Fatalf("storage %s: %v", id, err)
	}
	fsm := NewKVStateMachine()
	cfg := Config{
		NodeID:            id,
		Peers:             append([]string(nil), peers...),
		DataDir:           dir,
		ElectionTimeout:   300 * time.Millisecond,
		HeartbeatInterval: 60 * time.Millisecond,
		RandomElection:    true,
	}
	node, err := NewNode(cfg, st, fsm, c.net.AddNode(id), testLogger{c.t})
	if err != nil {
		c.t.Fatalf("new node %s: %v", id, err)
	}
	node.Start(context.Background())
	c.nodes[id] = node
	c.fsms[id] = fsm
	c.storages[id] = st
}

func (c *testCluster) restartNode(id string) {
	c.stopNode(id)
	c.startNode(id)
}

func (c *testCluster) stopAll() {
	for id := range c.nodes {
		c.stopNode(id)
	}
}

// awaitLeader 阻塞直到出现一个“在其当前任期内获得多数派”的领导者。
func (c *testCluster) awaitLeader() string {
	deadline := time.Now().Add(c.timeout)
	for time.Now().Before(deadline) {
		terms := map[uint64]map[string]bool{}
		var candidate string
		var candTerm uint64
		for id, n := range c.nodes {
			s := n.State()
			if s.Role == RoleLeader {
				if terms[s.Term] == nil {
					terms[s.Term] = map[string]bool{}
				}
				terms[s.Term][id] = true
				candidate, candTerm = id, s.Term
			}
		}
		// 领导者必须被多数派认可：其余存活节点在同一任期的 leaderHint 指向它。
		if candidate != "" {
			ack := 1
			for id, n := range c.nodes {
				if id == candidate {
					continue
				}
				s := n.State()
				if s.Term == candTerm && s.LeaderHint == candidate && s.Role == RoleFollower {
					ack++
				}
			}
			if ack >= len(c.peers)/2+1 {
				return candidate
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no leader with quorum ack within %v", c.timeout)
	return ""
}

func (c *testCluster) leader() string {
	for id, n := range c.nodes {
		if n.State().Role == RoleLeader {
			return id
		}
	}
	return ""
}

// proposeOnLeader 在领导者上写入并等待应用。
func (c *testCluster) proposeOnLeader(key, value string) error {
	deadline := time.Now().Add(c.timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ldr := c.leader()
		if ldr == "" {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := c.nodes[ldr].Propose(ctx, KVSetCommand(key, value))
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(30 * time.Millisecond)
	}
	return fmt.Errorf("propose %s=%s failed: %w", key, value, lastErr)
}

// waitForValue 轮询所有存活节点，直到 FSM 中出现期望值。
func (c *testCluster) kvGet(id, key string) (string, bool) {
	n, ok := c.nodes[id]
	if !ok {
		// 节点离线时读取其最近一次已知 FSM 引用（仅用于错误诊断）。
		v, exists := c.fsms[id].Get(key)
		return v, exists
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res, err := n.FSMView(ctx, func(sm StateMachine) any {
		v, ok := sm.(*KVStateMachine).Get(key)
		return kvPair{v, ok}
	})
	if err != nil {
		return "", false
	}
	p := res.(kvPair)
	return p.v, p.ok
}

type kvPair struct {
	v  string
	ok bool
}

func (c *testCluster) kvClone(id string) map[string]string {
	n, ok := c.nodes[id]
	if !ok {
		return c.fsms[id].Clone()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res, err := n.FSMView(ctx, func(sm StateMachine) any {
		return sm.(*KVStateMachine).Clone()
	})
	if err != nil {
		return map[string]string{}
	}
	return res.(map[string]string)
}

func (c *testCluster) waitForValue(key, want string) {
	deadline := time.Now().Add(c.timeout)
	for time.Now().Before(deadline) {
		all := true
		for id := range c.nodes {
			got, ok := c.kvGet(id, key)
			if !ok || got != want {
				all = false
			}
		}
		if all && len(c.nodes) > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	snap := map[string]string{}
	for id := range c.fsms {
		if v, ok := c.kvGet(id, key); ok {
			snap[id] = v
		} else {
			snap[id] = "<missing>"
		}
	}
	c.t.Fatalf("value %q=%q not converged; state=%v", key, want, snap)
}

// waitValueOnIds 仅在指定节点集合上等待键值收敛（用于分区多数派侧）。
func (c *testCluster) waitValueOnIds(ids []string, key, want string) {
	deadline := time.Now().Add(c.timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, id := range ids {
			got, exists := c.kvGet(id, key)
			if !exists || got != want {
				all = false
			}
		}
		if all {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	snap := map[string]string{}
	for _, id := range ids {
		if v, ok := c.kvGet(id, key); ok {
			snap[id] = v
		} else {
			snap[id] = "<missing>"
		}
	}
	c.t.Fatalf("value %q=%q not converged on %v; state=%v", key, want, ids, snap)
}

func (c *testCluster) waitAppliedAtLeast(id string, index uint64) {
	deadline := time.Now().Add(c.timeout)
	for time.Now().Before(deadline) {
		if c.nodes[id].State().AppliedIndex >= index {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	c.t.Fatalf("node %s applied index below %d (state=%+v)", id, index, c.nodes[id].State())
}
