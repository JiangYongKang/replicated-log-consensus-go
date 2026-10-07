package raft

import (
	"errors"
	"fmt"
	"testing"
)

// membershipOf 读取节点当前成员视图（测试辅助）。
func (c *cluster) membershipOf(id NodeID) (voters, learners []NodeID) {
	return c.nodes[id].Membership()
}

// checkMembershipConverged 断言所有存活节点的成员视图一致。
func (c *cluster) checkMembershipConverged() {
	var baseV, baseL []NodeID
	var baseID NodeID
	first := true
	for _, id := range c.ids {
		n, ok := c.nodes[id]
		if !ok || n.Removed() {
			continue
		}
		v, l := n.Membership()
		if first {
			baseV, baseL, baseID, first = v, l, id, false
			continue
		}
		if fmt.Sprint(v) != fmt.Sprint(baseV) || fmt.Sprint(l) != fmt.Sprint(baseL) {
			c.t.Fatalf("membership diverged: %s=(v%v,l%v) %s=(v%v,l%v)",
				baseID, baseV, baseL, id, v, l)
		}
	}
	c.t.Logf("membership converged: voters=%v learners=%v (ref=%s)", baseV, baseL, baseID)
}

// TestAddNodeLearnerThenPromote 新节点先以学习者身份追赶，
// 追平后自动提升为投票成员，且能补齐加入前的历史数据。
func TestAddNodeLearnerThenPromote(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	for i := 0; i < 3; i++ {
		if _, _, err := c.nodes[l].Propose([]byte(fmt.Sprintf("before-%d", i))); err != nil {
			t.Fatal(err)
		}
		c.steps(3)
	}
	// 新节点以空配置启动：不参与选举，等待被加入。
	c.join("n4")
	c.steps(3)
	if r, _ := c.nodes["n4"].State(); r != Follower {
		t.Fatalf("fresh node should stay passive follower, got %s", r)
	}
	// 追加即生效：AddNode 返回后领导者视图中 n4 已是学习者。
	if _, err := c.nodes[l].AddNode("n4"); err != nil {
		t.Fatalf("add node: %v", err)
	}
	voters, learners := c.membershipOf(l)
	if fmt.Sprint(learners) != "[n4]" || len(voters) != 3 {
		t.Fatalf("after AddNode: voters=%v learners=%v, want learners=[n4]", voters, learners)
	}
	t.Logf("n4 joined as learner: voters=%v learners=%v", voters, learners)
	// 学习者追平后应被自动提升为投票成员。
	c.steps(20)
	voters, learners = c.membershipOf(l)
	if len(learners) != 0 || fmt.Sprint(voters) != "[n1 n2 n3 n4]" {
		t.Fatalf("after auto-promote: voters=%v learners=%v", voters, learners)
	}
	t.Logf("n4 promoted to voter: voters=%v", voters)
	// 新节点应补齐加入前的历史条目。
	c.steps(5)
	if got := len(c.applied["n4"]); got != 3 {
		t.Fatalf("n4 applied %d entries, want 3 (historical catch-up)", got)
	}
	// 扩容后新写入照常提交并应用到全部 4 个节点。
	if _, _, err := c.nodes[l].Propose([]byte("after-join")); err != nil {
		t.Fatal(err)
	}
	c.steps(5)
	if got := len(c.applied["n4"]); got != 4 {
		t.Fatalf("n4 applied %d entries, want 4", got)
	}
	c.checkAppliedConsistent()
	c.checkSameTermSingleLeader()
	c.checkMembershipConverged()
}

// TestRemoveNode 节点被移除后转为完全被动，集群按新成员集合继续提交。
func TestRemoveNode(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	if _, _, err := c.nodes[l].Propose([]byte("committed-before")); err != nil {
		t.Fatal(err)
	}
	c.steps(4)
	// 选一个没有担任领导者的节点移除。
	victim := NodeID("")
	for _, id := range c.ids {
		if id != l {
			victim = id
			break
		}
	}
	if _, err := c.nodes[l].RemoveNode(victim); err != nil {
		t.Fatalf("remove node: %v", err)
	}
	c.steps(15)
	// 被移除节点应已转为被动；集群成员收敛为剩余两人。
	if !c.nodes[victim].Removed() {
		t.Fatalf("removed node %s should be passive", victim)
	}
	voters, _ := c.membershipOf(l)
	if len(voters) != 2 {
		t.Fatalf("voters after removal: %v, want 2", voters)
	}
	t.Logf("node %s removed: voters=%v", victim, voters)
	// 已提交的写入不丢，新写入照常提交。
	if _, _, err := c.nodes[l].Propose([]byte("after-remove")); err != nil {
		t.Fatal(err)
	}
	c.steps(5)
	c.checkAppliedConsistent()
	c.checkSameTermSingleLeader()
	// 被移除节点不得再应用新条目（它已脱离集群）。
	for _, a := range c.applied[victim] {
		if a == "after-remove" {
			t.Fatalf("removed node %s applied post-removal entry", victim)
		}
	}
	c.checkMembershipConverged()
}

// TestRemoveLeaderItself 领导者移除自己：提交后退位，
// 剩余成员选出新领导者并继续服务。
func TestRemoveLeaderItself(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	if _, _, err := c.nodes[l].Propose([]byte("durable-1")); err != nil {
		t.Fatal(err)
	}
	c.steps(4)
	if _, err := c.nodes[l].RemoveNode(l); err != nil {
		t.Fatalf("leader self-removal: %v", err)
	}
	c.steps(25)
	if !c.nodes[l].Removed() {
		t.Fatalf("old leader %s should be passive after self-removal", l)
	}
	if r, _ := c.nodes[l].State(); r == Leader {
		t.Fatalf("removed leader %s still leading", l)
	}
	// 剩余两节点应选出新领导者。
	var l2 NodeID
	for _, id := range c.ids {
		if id == l {
			continue
		}
		if r, _ := c.nodes[id].State(); r == Leader {
			l2 = id
		}
	}
	if l2 == "" {
		t.Fatal("remaining members should elect a new leader")
	}
	if _, _, err := c.nodes[l2].Propose([]byte("durable-2")); err != nil {
		t.Fatalf("new leader cannot propose: %v", err)
	}
	c.steps(5)
	c.checkSameTermSingleLeader()
	c.checkMembershipConverged()
	t.Logf("leader %s removed itself; new leader=%s", l, l2)
}

// TestConfChangeRejectedWhilePending 上一个变更未提交时拒绝并发变更。
func TestConfChangeRejectedWhilePending(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	c.join("n4")
	c.join("n5")
	// 不投递消息，第一个变更悬置未提交。
	if _, err := c.nodes[l].AddNode("n4"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.nodes[l].AddNode("n5"); !errors.Is(err, ErrConfChangeInProgress) {
		t.Fatalf("expected ErrConfChangeInProgress, got %v", err)
	}
	if _, err := c.nodes[l].RemoveNode("n3"); !errors.Is(err, ErrConfChangeInProgress) {
		t.Fatalf("expected ErrConfChangeInProgress for remove, got %v", err)
	}
	// 非法变更：重复添加、移除不存在的节点。
	c.steps(5) // 让第一个变更提交
	if _, err := c.nodes[l].AddNode("n4"); !errors.Is(err, ErrInvalidConfChange) {
		t.Fatalf("expected ErrInvalidConfChange for duplicate add, got %v", err)
	}
	if _, err := c.nodes[l].RemoveNode("n9"); !errors.Is(err, ErrInvalidConfChange) {
		t.Fatalf("expected ErrInvalidConfChange for unknown remove, got %v", err)
	}
	t.Logf("pending/invalid conf changes rejected with classified errors")
}
