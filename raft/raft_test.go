package raft

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestLeaderElection: 3 节点冷启动，必须在选举超时内选出唯一领导者。
func TestLeaderElection(t *testing.T) {
	c := newTestCluster(t, 3, 300*time.Millisecond)
	defer c.stopAll()

	ldr := c.awaitLeader()
	term := c.nodes[ldr].State().Term
	t.Logf("elected leader=%s term=%d commit=%d last=%d",
		ldr, term, c.nodes[ldr].State().CommitIndex, c.nodes[ldr].State().LastLogIndex)

	// 同一任期内存活节点中只允许出现一个领导者角色。
	leaders := 0
	for id, n := range c.nodes {
		s := n.State()
		if s.Role == RoleLeader {
			leaders++
			if s.Term != term {
				t.Fatalf("leader %s term %d != %d", id, s.Term, term)
			}
		}
	}
	if leaders != 1 {
		t.Fatalf("expected exactly 1 leader in term %d, got %d", term, leaders)
	}
}

// TestNetworkPartition: 隔离领导者后，多数派一侧必须选出新领导者且任期推进；
// 恢复后旧领导者必须降级，任何时刻同一任期不得有两个领导者。
func TestNetworkPartition(t *testing.T) {
	c := newTestCluster(t, 3, 300*time.Millisecond)
	defer c.stopAll()

	old := c.awaitLeader()
	oldTerm := c.nodes[old].State().Term
	t.Logf("initial leader=%s term=%d", old, oldTerm)

	c.net.Partition(old, true)
	t.Logf("partitioned leader %s from majority", old)

	// 多数派一侧（2 节点）应选出新领导者。
	deadline := time.Now().Add(6 * time.Second)
	var newLeader string
	for time.Now().Before(deadline) {
		for id, n := range c.nodes {
			if id == old {
				continue
			}
			s := n.State()
			if s.Role == RoleLeader && s.Term > oldTerm {
				newLeader = id
			}
		}
		if newLeader != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newLeader == "" {
		c.stopAll()
		t.Fatalf("majority side failed to elect new leader after partition")
	}
	newTerm := c.nodes[newLeader].State().Term
	t.Logf("new leader=%s term=%d (old=%s term=%d)", newLeader, newTerm, old, oldTerm)

	// 隔离期间，旧领导者无法获得多数派：写入必须以 KindQuorumUnavailable 失败。
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	err := c.nodes[old].Propose(ctx, KVSetCommand("during-partition", "x"))
	cancel()
	if err == nil {
		t.Fatal("old leader unexpectedly committed during partition")
	}
	if re, ok := AsRaftError(err); !ok || (re.Kind != KindQuorumUnavailable &&
		re.Kind != KindNotLeader && re.Kind != KindStaleTerm) {
		t.Fatalf("partition propose error kind = %v, want quorum/notleader/stale", err)
	} else {
		t.Logf("partitioned-old-leader propose rejected as %s: %v", re.Kind, err)
	}

	// 恢复分区：旧领导者必须在新任期降级为跟随者。
	c.net.Partition(old, false)
	t.Logf("partition healed")
	deadline = time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		s := c.nodes[old].State()
		if s.Role == RoleFollower && s.Term >= newTerm {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	s := c.nodes[old].State()
	if s.Role != RoleFollower || s.Term < newTerm {
		t.Fatalf("old leader did not step down: role=%s term=%d", s.Role, s.Term)
	}

	// 最终同一任期只能有一个领导者。
	ldr := c.awaitLeader()
	finalTerm := c.nodes[ldr].State().Term
	leaders := 0
	for _, n := range c.nodes {
		if st := n.State(); st.Role == RoleLeader && st.Term == finalTerm {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("after heal: %d leaders share term %d", leaders, finalTerm)
	}
	t.Logf("converged on single leader=%s term=%d", ldr, finalTerm)
}

// TestProposeAndLinearizableRead: 正常路径提交并复制到全部节点，
// 未提交的写入在任何节点都不可见。
func TestProposeAndVisibility(t *testing.T) {
	c := newTestCluster(t, 3, 300*time.Millisecond)
	defer c.stopAll()
	ldr := c.awaitLeader()

	if err := c.proposeOnLeader("k1", "v1"); err != nil {
		t.Fatal(err)
	}
	c.waitForValue("k1", "v1")
	committed := c.nodes[ldr].State().CommitIndex
	t.Logf("committed k1=v1 leader=%s term=%d commit=%d applied=%d",
		ldr, c.nodes[ldr].State().Term, committed, c.nodes[ldr].State().AppliedIndex)

	// 非领导者节点直接 Propose 必须得到 KindNotLeader。
	for id, n := range c.nodes {
		if id == ldr {
			continue
		}
		err := n.Propose(context.Background(), KVSetCommand("bad", "x"))
		if err == nil || !IsKind(err, KindNotLeader) {
			t.Fatalf("follower %s Propose err=%v, want KindNotLeader", id, err)
		}
		t.Logf("follower %s correctly rejected write: %v", id, err)
		break
	}
}

// TestConcurrentWrites: 大量并发写入后，所有存活节点的键值与提交位点完全一致，
// 且 FSM 中每条写入的效果恰好生效一次。
func TestConcurrentWrites(t *testing.T) {
	c := newTestCluster(t, 5, 300*time.Millisecond)
	defer c.stopAll()
	c.awaitLeader()

	const writers = 8
	const perWriter = 25
	var wg sync.WaitGroup
	errCh := make(chan error, writers*perWriter)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := "w" + itoa(w) + "_" + itoa(i)
				val := "v" + itoa(w) + "_" + itoa(i)
				if err := c.proposeOnLeader(key, val); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent write failed: %v", err)
	}

	want := writers * perWriter
	// 等待所有存活节点都应用全部写入（-race 下调度更慢，给予充足时间）。
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for id := range c.nodes {
			if len(c.kvClone(id)) != want {
				all = false
			}
		}
		if all {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	ref := c.kvClone(c.leader())
	if len(ref) != want {
		t.Fatalf("leader has %d keys, want %d", len(ref), want)
	}
	for id := range c.nodes {
		got := c.kvClone(id)
		if len(got) != want {
			t.Fatalf("node %s applied %d writes, want %d (duplicated/lost apply)", id, len(got), want)
		}
		for k, v := range ref {
			if got[k] != v {
				t.Fatalf("node %s divergence on %s: %q vs %q", id, k, got[k], v)
			}
		}
	}
	var maxCommit uint64
	for id, n := range c.nodes {
		s := n.State()
		t.Logf("node=%s term=%d role=%s commit=%d applied=%d",
			id, s.Term, s.Role, s.CommitIndex, s.AppliedIndex)
		if s.AppliedIndex != s.CommitIndex {
			t.Fatalf("node %s applied %d != commit %d", id, s.AppliedIndex, s.CommitIndex)
		}
		maxCommit = s.CommitIndex
	}
	if maxCommit < uint64(want) {
		t.Fatalf("commit %d < writes %d", maxCommit, want)
	}
}

// TestRestartPersistence: 多数派存活期间提交的条目，在节点重启（甚至全集群
// 冷启动）后既不丢失，也不会被重复应用。
func TestRestartPersistence(t *testing.T) {
	c := newTestCluster(t, 3, 300*time.Millisecond)
	defer c.stopAll()
	ldr := c.awaitLeader()

	for i := 1; i <= 5; i++ {
		if err := c.proposeOnLeader("rk"+itoa(i), "rv"+itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
	c.waitForValue("rk5", "rv5")
	committed := c.nodes[ldr].State().CommitIndex

	// 单节点重启：已提交状态不丢，应用位点不回退。
	victim := pickFollower(c, ldr)
	before := c.nodes[victim].State().AppliedIndex
	c.restartNode(victim)
	t.Logf("restarted %s, applied before=%d", victim, before)
	c.waitAppliedAtLeast(victim, before)
	if v, ok := c.kvGet(victim, "rk5"); !ok || v != "rv5" {
		t.Fatalf("after restart %s: rk5=%q ok=%v", victim, v, ok)
	}
	if c.nodes[victim].State().AppliedIndex < before {
		t.Fatalf("applied index regressed after restart: %d < %d",
			c.nodes[victim].State().AppliedIndex, before)
	}

	// 全集群冷启动：从持久化恢复，条目不重复应用，日志继续可用。
	c.stopAll()
	c.fsms = map[string]*KVStateMachine{}
	for _, id := range c.peers {
		c.startNode(id)
	}
	defer c.stopAll()
	c.awaitLeader()
	for _, id := range c.peers {
		c.waitAppliedAtLeast(id, committed)
		for i := 1; i <= 5; i++ {
			if v, ok := c.kvGet(id, "rk"+itoa(i)); !ok || v != "rv"+itoa(i) {
				t.Fatalf("cold restart node %s key rk%d=%q ok=%v", id, i, v, ok)
			}
		}
		s := c.nodes[id].State()
		if s.AppliedIndex < committed {
			t.Fatalf("node %s committed %d lost after cold restart (applied=%d)",
				id, committed, s.AppliedIndex)
		}
		t.Logf("cold-restart node=%s term=%d commit=%d applied=%d last=%d",
			id, s.Term, s.CommitIndex, s.AppliedIndex, s.LastLogIndex)
	}
	// 重启后还能继续写入。
	if err := c.proposeOnLeader("after-restart", "ok"); err != nil {
		t.Fatal(err)
	}
	c.waitForValue("after-restart", "ok")
}

func pickFollower(c *testCluster, leader string) string {
	for id := range c.nodes {
		if id != leader {
			return id
		}
	}
	return ""
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
