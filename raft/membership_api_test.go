package raft

import (
	"errors"
	"testing"
)

// TestMembershipAPIValidation 成员变更 API 的参数与状态校验必须返回分类错误。
func TestMembershipAPIValidation(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)

	// 非领导者调用一律 ErrNotLeader。
	f := c.ids[1]
	if f == l {
		f = c.ids[2]
	}
	if err := c.nodes[f].AddLearner("zz"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower AddLearner: want ErrNotLeader, got %v", err)
	}
	if err := c.nodes[f].RemoveMember(l); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower RemoveMember: want ErrNotLeader, got %v", err)
	}

	// 重复加入已存在成员。
	if err := c.nodes[l].AddLearner(f); !errors.Is(err, ErrMemberExists) {
		t.Fatalf("add existing: want ErrMemberExists, got %v", err)
	}

	// 提升一个不是学习者的节点。
	if err := c.nodes[l].PromoteMember(f); !errors.Is(err, ErrNotLearner) {
		t.Fatalf("promote non-learner: want ErrNotLearner, got %v", err)
	}

	// 移除不存在的成员。
	if err := c.nodes[l].RemoveMember("ghost"); !errors.Is(err, ErrUnknownMember) {
		t.Fatalf("remove unknown: want ErrUnknownMember, got %v", err)
	}

	// 学习者可以直接（单条配置）移除。
	const ln NodeID = "n9"
	c.startNewNode(ln)
	if err := c.nodes[l].AddLearner(ln); err != nil {
		t.Fatal(err)
	}
	c.steps(4)
	if err := c.nodes[l].RemoveMember(ln); err != nil {
		t.Fatalf("remove learner should succeed: %v", err)
	}
	c.steps(4)
	if c.nodes[l].Configuration().HasMember(ln) {
		t.Fatalf("学习者应已被移除: %+v", c.nodes[l].Configuration())
	}
	t.Logf("学习者 %s 直接移除成功，配置=%+v", ln, c.nodes[l].Configuration().Voters)
}

// TestConcurrentChangesRejected 上一次投票成员变更未完成时，
// 新的变更必须被拒绝（ErrMembershipChange），保证配置切换串行化。
func TestConcurrentChangesRejected(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	const a, b NodeID = "na", "nb"
	c.startNewNode(a)
	c.startNewNode(b)
	if err := c.nodes[l].AddLearner(a); err != nil {
		t.Fatal(err)
	}
	c.steps(5)
	if err := c.nodes[l].AddLearner(b); err != nil {
		t.Fatal(err)
	}
	c.steps(5)
	// 发起 a 的提升（联合配置进入未决状态）。
	if err := c.nodes[l].PromoteMember(a); err != nil {
		t.Fatal(err)
	}
	// 联合配置尚未提交：任何新变更都应被拒。
	if err := c.nodes[l].PromoteMember(b); !errors.Is(err, ErrMembershipChange) {
		t.Fatalf("重叠提升: want ErrMembershipChange, got %v", err)
	}
	if err := c.nodes[l].RemoveMember(c.ids[1]); !errors.Is(err, ErrMembershipChange) {
		t.Fatalf("重叠移除: want ErrMembershipChange, got %v", err)
	}
	// 待 a 的变更完成后，新变更才被接受。
	c.waitConfigSettled(l, 30)
	if err := c.nodes[l].PromoteMember(b); err != nil {
		t.Fatalf("变更完成后应能继续: %v", err)
	}
	c.waitConfigSettled(l, 30)
	conf := c.nodes[l].Configuration()
	if len(conf.Voters) != 5 {
		t.Fatalf("期望 5 选民, got %+v", conf)
	}
	t.Logf("串行化成员变更通过: 最终 voters=%v", conf.Voters)
}

// TestPromoteBeforeCaughtUp 学习者未追平时提升必须被明确拒绝。
func TestPromoteBeforeCaughtUp(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	const slow NodeID = "ns"
	c.startNewNode(slow)
	if err := c.nodes[l].AddLearner(slow); err != nil {
		t.Fatal(err)
	}
	// 不投递任何消息：学习者完全没追赶。
	// 制造一些领导者写入。
	for i := 0; i < 3; i++ {
		if _, _, err := c.nodes[l].Propose([]byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.nodes[l].PromoteMember(slow); !errors.Is(err, ErrMembershipChange) {
		t.Fatalf("未追平提升应失败: want ErrMembershipChange, got %v", err)
	} else {
		t.Logf("未追平学习者的提升被明确拒绝: %v", err)
	}
}
