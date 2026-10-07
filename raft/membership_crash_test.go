package raft

import (
	"errors"
	"fmt"
	"testing"
)

// TestMembershipLeaderCrashMidChange 联合变更进行到一半领导崩溃：
// 恢复后集群必须选出唯一领导并把变更收敛到一个确定配置，已确认写入不丢。
func TestMembershipLeaderCrashMidChange(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	for i := 0; i < 2; i++ {
		if _, _, err := c.nodes[l].Propose([]byte(fmt.Sprintf("pre-%d", i))); err != nil {
			t.Fatal(err)
		}
		c.steps(3)
	}
	const newID NodeID = "n4"
	c.startNewNode(newID)
	if err := c.nodes[l].AddLearner(newID); err != nil {
		t.Fatal(err)
	}
	c.steps(6)

	if err := c.nodes[l].PromoteMember(newID); err != nil {
		t.Fatal(err)
	}
	// 制造真实的中间态：只让一个旧跟随者收到联合配置（C_old,new），
	// 不投递任何响应，因此联合配置远未提交。此时领导与新节点同时崩溃。
	delivered := 0
	for c.net.Pending() > 0 && delivered < 1 {
		m, ok := c.net.PeekFirst()
		if !ok {
			break
		}
		if m.Type == MsgAppendEntries && m.To != l {
			c.net.DeliverOne()
			delivered++
		} else {
			c.net.DropFirst()
		}
	}
	// 丢弃其余在途消息，确保没有 AppendEntriesResp 回到领导处推进提交。
	for c.net.Pending() > 0 {
		c.net.DropFirst()
	}
	t.Logf("联合配置仅扩散到 %d 个旧成员（未提交），立即杀掉领导 %s", delivered, l)
	jointBefore := c.nodes[l].Configuration()
	if !jointBefore.IsJoint() {
		t.Fatal("expected joint config on leader right after promote")
	}
	c.crash(l)
	// 同时把新加入节点也杀掉。
	c.crash(newID)

	// 关键安全断言：联合配置（C_old,new）尚未提交而 C_new 成员 n4 已死时，
	// 任何人都不能当选/提交——否则可能与"另一拨按 C_new 提交"的集群分叉。
	c.steps(25)
	for _, id := range c.ids {
		if n, ok := c.nodes[id]; ok {
			if r, term := n.State(); r == Leader {
				t.Fatalf("联合期间 C_new 多数派不可达，节点 %s 不应成为领导 term=%d", id, term)
			}
		}
	}
	t.Logf("联合变更悬而未决且 C_new 不足时，集群正确地保持无领导（杜绝双配置分叉）")

	// 新成员恢复：联合配置现在可以在 C_old 与 C_new 双多数派下完成，
	// 集群选出唯一领导并把变更收敛为确定的 C_new。
	c.restart(newID)
	var l2 NodeID
	for i := 0; i < 40 && l2 == ""; i++ {
		c.step()
		for _, id := range c.ids {
			if id == l {
				continue
			}
			if r, _ := c.nodes[id].State(); r == Leader {
				l2 = id
			}
		}
	}
	if l2 == "" {
		t.Fatal("新成员恢复后集群未能选出领导")
	}
	c.checkSameTermSingleLeader()
	t.Logf("新成员恢复后选出唯一领导=%s", l2)

	c.steps(10)
	conf := c.waitConfigSettled(c.leader(), 30)
	if conf.IsJoint() || len(conf.Voters) != 4 {
		t.Fatalf("变更应收敛到 4 选民的确定配置, got %+v", conf)
	}

	// 原领导稍后拉起，必须收敛到同一配置、同一条已提交前缀。
	c.restart(l)
	c.steps(12)
	c.checkSameTermSingleLeader()
	finalLeader := c.leader()
	if finalLeader == "" {
		t.Fatal("最终无领导")
	}
	for _, id := range c.ids {
		got := c.nodes[id].Configuration()
		if len(got.Voters) != 4 || got.IsJoint() {
			t.Fatalf("节点 %s 配置未收敛: %+v", id, got)
		}
	}
	c.checkAppliedConsistent()
	for _, id := range c.ids {
		for _, want := range []string{"pre-0", "pre-1"} {
			found := false
			for _, a := range c.applied[id] {
				if a == want {
					found = true
				}
			}
			if !found {
				t.Fatalf("节点 %s 丢失已确认写入 %q: %v", id, want, c.applied[id])
			}
		}
	}
	t.Logf("中途崩溃后收敛: voters=%v 唯一领导=%s", conf.Voters, finalLeader)
}

// TestRemovedNodeCannotDisrupt 被踢节点带着旧状态与残留消息继续运行，
// 不能再左右提交或选主；它的 PreVote/RequestVote 必须被新配置成员拒绝。
func TestRemovedNodeCannotDisrupt(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	victim := c.ids[1]
	if victim == l {
		victim = c.ids[2]
	}
	if _, _, err := c.nodes[l].Propose([]byte("a")); err != nil {
		t.Fatal(err)
	}
	c.steps(4)
	if err := c.nodes[l].RemoveMember(victim); err != nil {
		t.Fatal(err)
	}
	conf := c.waitConfigSettled(l, 30)
	t.Logf("%s 已被移除, voters=%v；它仍存活并持有旧状态", victim, conf.Voters)

	// 被踢节点超时后会尝试 PreVote；新配置成员必须拒绝。
	c.steps(20)
	c.checkSameTermSingleLeader()
	if r, term := c.nodes[victim].State(); r == Leader {
		t.Fatalf("被踢节点 %s 成了领导 term=%d", victim, term)
	}
	// 它的任期不应因预投票被拒绝而超过集群当前任期。
	leaderTerm := uint64(0)
	if r, term := c.nodes[l].State(); r == Leader {
		leaderTerm = term
	}
	if _, vt := c.nodes[victim].State(); vt > leaderTerm {
		t.Fatalf("被踢节点任期 %d 超过在任领导任期 %d", vt, leaderTerm)
	}
	// 手动注入一条被踢节点伪造的正式 RequestVote（更高任期）也必须被拒。
	survivor := conf.Voters[0]
	if survivor == victim {
		survivor = conf.Voters[1]
	}
	beforeTerm := uint64(0)
	if st, ok, _ := c.storages[survivor].LoadState(); ok {
		beforeTerm = st.CurrentTerm
	}
	c.nodes[survivor].Handle(Message{
		Type: MsgRequestVote, From: victim, To: survivor,
		Term: beforeTerm + 5, LastLogIndex: 100, LastLogTerm: 100,
	})
	if !errors.Is(c.nodes[survivor].LastError(), ErrRejectVote) {
		t.Fatalf("期望 ErrRejectVote, got %v", c.nodes[survivor].LastError())
	}
	st, _, _ := c.storages[survivor].LoadState()
	if st.CurrentTerm != beforeTerm {
		t.Fatalf("拒绝被踢节点投票不得抬高任期: %d -> %d", beforeTerm, st.CurrentTerm)
	}

	// 新配置两节点仍能稳定提交。
	if _, _, err := c.nodes[l].Propose([]byte("b")); err != nil {
		t.Fatalf("移除后写入失败: %v", err)
	}
	c.steps(6)
	survivors := conf.Voters
	c.checkAppliedConsistentAmong(survivors)
	c.checkSameTermSingleLeader()
	t.Logf("被踢节点无法干扰，剩余集群稳定提交")
}
