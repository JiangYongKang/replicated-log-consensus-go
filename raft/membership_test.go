package raft

import (
	"errors"
	"fmt"
	"testing"
)

// waitConfigSettled 推进直到领导者配置变为非联合且已提交（变更完成）。
func (c *cluster) waitConfigSettled(leader NodeID, maxSteps int) Configuration {
	for i := 0; i < maxSteps; i++ {
		conf := c.nodes[leader].Configuration()
		jointIdx := c.nodes[leader].latestConfigIndex()
		if !conf.IsJoint() && jointIdx <= c.nodes[leader].Committed() && jointIdx > 0 {
			return conf
		}
		c.step()
		// 领导可能在变更中退位：跟随当前唯一领导。
		if l := c.leader(); l != "" {
			leader = l
		}
	}
	c.t.Fatalf("membership change did not settle in %d steps (conf=%+v)",
		maxSteps, c.nodes[leader].Configuration())
	return Configuration{}
}

// TestAddNodeOnline 不停机加入新节点：学习者补齐后提升为投票成员，
// 期间与之后的已确认写入不丢、不重复，且新配置生效。
func TestAddNodeOnline(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	t.Logf("场景: 初始 3 节点，领导=%s", l)

	for i := 0; i < 3; i++ {
		if _, _, err := c.nodes[l].Propose([]byte(fmt.Sprintf("before-%d", i))); err != nil {
			t.Fatal(err)
		}
		c.steps(3)
	}

	// 1) 新节点以当前成员为引导配置启动，先作为学习者加入。
	const newID NodeID = "n4"
	c.startNewNode(newID)
	if err := c.nodes[l].AddLearner(newID); err != nil {
		t.Fatalf("AddLearner: %v", err)
	}
	t.Logf("阶段1: %s 作为学习者加入，开始追赶", newID)
	c.steps(8)
	if got := len(c.applied[newID]); got != 3 {
		t.Fatalf("learner applied %d, want 3", got)
	}

	// 2) 追平后提升为投票成员（两阶段联合共识）。
	if err := c.nodes[l].PromoteMember(newID); err != nil {
		t.Fatalf("PromoteMember: %v", err)
	}
	t.Logf("阶段2: 提升 %s 为投票成员（C -> C_old,new -> C_new）", newID)
	conf := c.waitConfigSettled(l, 30)
	if conf.IsJoint() || len(conf.Voters) != 4 {
		t.Fatalf("expected 4 voters non-joint, got %+v", conf)
	}

	// 3) 四节点下继续写入并在全部 4 节点提交应用。
	for i := 0; i < 3; i++ {
		if _, _, err := c.nodes[l].Propose([]byte(fmt.Sprintf("after-%d", i))); err != nil {
			t.Fatal(err)
		}
		c.steps(3)
	}
	c.steps(5)
	want := []string{"before-0", "before-1", "before-2", "after-0", "after-1", "after-2"}
	for _, id := range c.ids {
		if got := c.applied[id]; len(got) != len(want) {
			t.Fatalf("node %s applied %v, want %v", id, got, want)
		}
	}
	c.checkAppliedConsistent()
	t.Logf("加节点完成: voters=%v，全部应用=%v", conf.Voters, want)
}

// TestRemoveVoterOnline 在线踢掉一个投票成员，集群按新 2 节点集合继续工作；
// 被踢节点不再影响提交与选主。
func TestRemoveVoterOnline(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	victim := c.ids[1]
	if victim == l {
		victim = c.ids[2]
	}
	t.Logf("场景: 踢掉投票成员 %s，领导=%s", victim, l)
	if _, _, err := c.nodes[l].Propose([]byte("keep-0")); err != nil {
		t.Fatal(err)
	}
	c.steps(4)

	if err := c.nodes[l].RemoveMember(victim); err != nil {
		t.Fatalf("RemoveMember: %v", err)
	}
	conf := c.waitConfigSettled(l, 30)
	if len(conf.Voters) != 2 || containsID(conf.Voters, victim) {
		t.Fatalf("expected victim removed, got %+v", conf)
	}
	t.Logf("缩容完成: voters=%v", conf.Voters)

	// 剩余两节点（仍连通）应持续提交。
	if _, _, err := c.nodes[l].Propose([]byte("keep-1")); err != nil {
		t.Fatalf("propose after shrink: %v", err)
	}
	c.steps(5)
	survivors := []NodeID{l}
	for _, id := range c.ids {
		if id != victim && id != l {
			survivors = append(survivors, id)
		}
	}
	c.checkAppliedConsistentAmong(survivors)
	for _, id := range survivors {
		if len(c.applied[id]) != 2 {
			t.Fatalf("survivor %s applied %v, want 2", id, c.applied[id])
		}
	}
	c.checkSameTermSingleLeader()

	// 被踢节点即便收到残留写请求也不能影响提交：它不应在当前任期成为领导。
	if r, _ := c.nodes[victim].State(); r == Leader {
		t.Fatalf("removed node %s must not be leader", victim)
	}
	_ = errors.Is // 预留错误断言
}
