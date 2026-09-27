package raft

import (
	"errors"
	"testing"
)

// TestElectSingleLeader 多数节点可用时应选出唯一领导者并推进任期。
func TestElectSingleLeader(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	if l == "" {
		t.Fatal("expected a leader")
	}
	c.checkSameTermSingleLeader()
	_, term := c.nodes[l].State()
	if term < 1 {
		t.Fatalf("term should advance, got %d", term)
	}
	c.steps(5)
	c.checkSameTermSingleLeader()
	if got := c.leader(); got != l {
		t.Fatalf("leader changed unexpectedly: %s -> %s", l, got)
	}
	t.Logf("stable leader=%s term=%d", l, term)
}

// TestNoLeaderWithoutMajority 多数派不足时集群不得有领导者：
// 完全分区（任意两节点互不可达）后，旧领导者退位且无人能当选。
func TestNoLeaderWithoutMajority(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	// 切断所有链路：每个节点都是少数派。
	for _, id := range c.ids {
		c.net.Partition(id)
	}
	c.steps(30)
	for _, id := range c.ids {
		r, term := c.nodes[id].State()
		if r == Leader {
			t.Fatalf("node %s is leader without majority (term=%d)", id, term)
		}
		t.Logf("node=%s role=%s term=%d commit=%d", id, r, term, c.nodes[id].Committed())
	}
	if !errors.Is(c.nodes[l].LastError(), ErrNoMajority) {
		t.Fatalf("expected ErrNoMajority on stepped-down leader, got %v", c.nodes[l].LastError())
	}
	// 少数派节点上的写请求必须失败并可分类。
	if _, _, err := c.nodes[l].Propose([]byte("x")); !errors.Is(err, ErrNotLeader) && !errors.Is(err, ErrNoMajority) {
		t.Fatalf("expected ErrNotLeader/ErrNoMajority, got %v", err)
	}
}

// TestIsolatedLeaderStepsDown 被隔离的领导者失去多数派后必须退位，
// 多数派一侧可继续服务；恢复后同任期不得出现两个领导者。
func TestIsolatedLeaderStepsDown(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	_, term1 := c.nodes[l].State()
	c.net.Partition(l) // l 成为 1 节点少数派
	c.steps(25)
	if r, _ := c.nodes[l].State(); r == Leader {
		t.Fatalf("isolated leader %s should step down", l)
	}
	if !errors.Is(c.nodes[l].LastError(), ErrNoMajority) {
		t.Fatalf("expected ErrNoMajority, got %v", c.nodes[l].LastError())
	}
	// 多数派一侧选出新领导者并推进任期。
	var l2 NodeID
	for _, id := range c.ids {
		if id != l {
			if r, _ := c.nodes[id].State(); r == Leader {
				l2 = id
			}
		}
	}
	if l2 == "" {
		t.Fatal("majority side should have a leader")
	}
	_, term2 := c.nodes[l2].State()
	if term2 <= term1 {
		t.Fatalf("term should advance: %d -> %d", term1, term2)
	}
	c.net.Heal()
	c.steps(20)
	c.checkSameTermSingleLeader()
	t.Logf("isolated leader %s(term=%d) stepped down; new leader=%s term=%d", l, term1, l2, term2)
}

// TestPartitionHealsAndReelects 分区恢复后集群重新收敛到唯一领导者。
func TestPartitionHealsAndReelects(t *testing.T) {
	c := newCluster(t, 3)
	l1 := c.waitLeader(30)
	_, term1 := c.nodes[l1].State()
	// 隔离旧领导者，其余两个节点应选出新领导者。
	c.net.Partition(l1)
	var l2 NodeID
	for i := 0; i < 40; i++ {
		c.step()
		for _, id := range c.ids {
			if id == l1 {
				continue
			}
			if r, _ := c.nodes[id].State(); r == Leader {
				l2 = id
			}
		}
		if l2 != "" {
			break
		}
	}
	if l2 == "" {
		t.Fatal("majority side should elect a new leader")
	}
	_, term2 := c.nodes[l2].State()
	if term2 <= term1 {
		t.Fatalf("term should advance: term1=%d term2=%d", term1, term2)
	}
	// 恢复分区：旧领导者发现更高任期后应退位，集群收敛。
	c.net.Heal()
	c.steps(20)
	c.checkSameTermSingleLeader()
	if got := c.leader(); got != l2 {
		t.Fatalf("expected leader %s after heal, got %s", l2, got)
	}
	t.Logf("re-election: old=%s(term=%d) new=%s(term=%d)", l1, term1, l2, term2)
}

// TestStaleTermRejected 过期任期的消息必须被拒绝并分类为 ErrStaleTerm。
func TestStaleTermRejected(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	_, term := c.nodes[l].State()
	// 构造一条来自过期任期领导者的 AppendEntries。
	stale := Message{
		Type: MsgAppendEntries, From: "ghost", To: c.ids[0],
		Term: term - 1, PrevLogIndex: 0, PrevLogTerm: 0,
	}
	before := c.nodes[c.ids[0]].Committed()
	c.nodes[c.ids[0]].Handle(stale)
	if !errors.Is(c.nodes[c.ids[0]].LastError(), ErrStaleTerm) {
		t.Fatalf("expected ErrStaleTerm, got %v", c.nodes[c.ids[0]].LastError())
	}
	if got := c.nodes[c.ids[0]].Committed(); got != before {
		t.Fatalf("stale message changed commit index: %d -> %d", before, got)
	}
	t.Logf("stale term %d rejected at node=%s (current term>=%d)", stale.Term, c.ids[0], term)
}

// TestProposeOnFollower 非领导者写请求返回 ErrNotLeader。
func TestProposeOnFollower(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	for _, id := range c.ids {
		if id == l {
			continue
		}
		if _, _, err := c.nodes[id].Propose([]byte("x")); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("node %s: expected ErrNotLeader, got %v", id, err)
		}
	}
}
