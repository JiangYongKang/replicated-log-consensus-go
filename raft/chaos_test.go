package raft

import (
	"context"
	"testing"
	"time"
)

// TestDuplicateAndReorderMessages: 在全部双向链路上持续注入重复投递与乱序，
// 集群仍必须选出唯一领导者并无丢失地复制全部写入。
func TestDuplicateAndReorderMessages(t *testing.T) {
	c := newTestCluster(t, 3, 300*time.Millisecond)
	defer c.stopAll()

	for _, from := range c.peers {
		for _, to := range c.peers {
			if from == to {
				continue
			}
			c.net.SetPolicy(from, to, EdgePolicy{DuplicateRate: 0.5, Reorder: true})
		}
	}
	ldr := c.awaitLeader()
	t.Logf("leader under chaos=%s term=%d", ldr, c.nodes[ldr].State().Term)

	for i := 1; i <= 20; i++ {
		if err := c.proposeOnLeader("c"+itoa(i), "d"+itoa(i)); err != nil {
			t.Fatalf("write %d under duplicate/reorder: %v", i, err)
		}
	}
	for i := 1; i <= 20; i++ {
		c.waitForValue("c"+itoa(i), "d"+itoa(i))
	}
	ref := c.kvClone(ldr)
	if len(ref) != 20 {
		t.Fatalf("leader applied %d, want 20", len(ref))
	}
	for id, fsm := range c.fsms {
		if len(fsm.Clone()) != 20 {
			t.Fatalf("node %s applied %d keys under chaos, want 20", id, len(fsm.Clone()))
		}
	}
	t.Logf("all 20 entries converged under duplicate+reorder injection")
}

// TestMessageLossThenRecovery: 高丢包率（但仍有部分消息通过）期间集群可能
// 反复换任，但不允许同一任期出现两个领导者；网络恢复后状态必须收敛。
func TestMessageLossThenRecovery(t *testing.T) {
	c := newTestCluster(t, 3, 200*time.Millisecond)
	defer c.stopAll()
	c.awaitLeader()

	for _, from := range c.peers {
		for _, to := range c.peers {
			if from == to {
				continue
			}
			c.net.SetPolicy(from, to, EdgePolicy{DropRate: 0.6})
		}
	}
	t.Logf("60%% drop window for 2.5s; checking single-leader-per-term invariant")
	deadline := time.Now().Add(2500 * time.Millisecond)
	seen := map[uint64]map[string]bool{}
	for time.Now().Before(deadline) {
		for id, n := range c.nodes {
			s := n.State()
			if s.Role == RoleLeader {
				if seen[s.Term] == nil {
					seen[s.Term] = map[string]bool{}
				}
				seen[s.Term][id] = true
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	for term, ids := range seen {
		if len(ids) > 1 {
			t.Fatalf("term %d had multiple leaders: %v", term, ids)
		}
	}
	c.net.ResetPolicy()
	t.Logf("network healed after lossy window")

	ldr := c.awaitLeader()
	for i := 1; i <= 10; i++ {
		if err := c.proposeOnLeader("loss"+itoa(i), "x"+itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i <= 10; i++ {
		c.waitForValue("loss"+itoa(i), "x"+itoa(i))
	}
	_ = ldr
}

// TestLogConflictResolution: 隔离一个节点（少于多数派），让其在旧任期保留
// 分叉日志；恢复后它必须明确截断冲突段并追上多数派日志，冲突条目不得被
// 重复或错误应用。
func TestLogConflictResolution(t *testing.T) {
	c := newTestCluster(t, 3, 300*time.Millisecond)
	defer c.stopAll()

	ldr := c.awaitLeader()
	term0 := c.nodes[ldr].State().Term

	// 提交基线条目 a1,a2。
	if err := c.proposeOnLeader("a1", "1"); err != nil {
		t.Fatal(err)
	}
	if err := c.proposeOnLeader("a2", "2"); err != nil {
		t.Fatal(err)
	}
	c.waitForValue("a2", "2")
	baseCommit := c.nodes[ldr].State().CommitIndex
	t.Logf("base committed: leader=%s term=%d commit=%d", ldr, term0, baseCommit)

	// 隔离一个跟随者节点，多数派继续服务。
	victim := pickFollower(c, ldr)
	c.net.Partition(victim, true)
	t.Logf("partition victim=%s; majority continues", victim)

	// 多数派继续写入，形成更长、更高任期的日志（仅等待多数派侧收敛）。
	for i := 1; i <= 6; i++ {
		if err := c.proposeOnLeader("maj"+itoa(i), "m"+itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
	majLeader := c.leader()
	c.waitValueOnIds([]string{majLeader, otherOnline(c, victim, majLeader)}, "maj6", "m6")
	majCommit := c.nodes[majLeader].State().CommitIndex
	majLast := c.nodes[majLeader].State().LastLogIndex
	t.Logf("majority commit=%d last=%d under leader=%s", majCommit, majLast, majLeader)

	// 在隔离侧无法获得多数派：尝试制造分叉写入（必然失败）。
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	err := c.nodes[victim].Propose(ctx, KVSetCommand("fork", "evil"))
	cancel()
	if err == nil {
		t.Fatalf("partitioned node unexpectedly committed fork")
	}
	t.Logf("victim fork write rejected as expected: %v", err)

	// 恢复网络，受害者必须回退冲突并追平。
	c.net.Partition(victim, false)
	c.waitAppliedAtLeast(victim, majCommit)
	if v, ok := c.kvGet(victim, "maj6"); !ok || v != "m6" {
		t.Fatalf("victim missing majority entries after heal: maj6=%q ok=%v", v, ok)
	}
	if v, ok := c.kvGet(victim, "fork"); ok {
		t.Fatalf("uncommitted fork entry became visible: %q", v)
	}
	vs := c.nodes[victim].State()
	if vs.LastLogIndex != majLast {
		t.Fatalf("victim last=%d != majority last=%d after conflict resolution",
			vs.LastLogIndex, majLast)
	}
	t.Logf("victim resolved conflict: last=%d applied=%d commit=%d term=%d",
		vs.LastLogIndex, vs.AppliedIndex, vs.CommitIndex, vs.Term)
}

func otherOnline(c *testCluster, exclude ...string) string {
	banned := map[string]bool{}
	for _, e := range exclude {
		banned[e] = true
	}
	for id := range c.nodes {
		if !banned[id] {
			return id
		}
	}
	return ""
}
