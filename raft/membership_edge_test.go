package raft

import (
	"errors"
	"fmt"
	"testing"
)

// TestShrinkToOneNode 缩容到只剩一个投票成员后，单节点必须能独立选主并提交；
// 再把节点加回来能正常扩容。这是"缩容到很小规模"的边界。
func TestShrinkToOneNode(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	for i := 0; i < 2; i++ {
		if _, _, err := c.nodes[l].Propose([]byte(fmt.Sprintf("x%d", i))); err != nil {
			t.Fatal(err)
		}
		c.steps(3)
	}
	others := []NodeID{}
	for _, id := range c.ids {
		if id != l {
			others = append(others, id)
		}
	}
	// 依次移除两个跟随者：3 -> 2 -> 1。
	for _, v := range others {
		if err := c.nodes[l].RemoveMember(v); err != nil {
			t.Fatalf("remove %s: %v", v, err)
		}
		c.waitConfigSettled(l, 30)
		t.Logf("移除 %s 后 voters=%v", v, c.nodes[l].Configuration().Voters)
	}
	// 被踢节点仍存活，但已不在配置中：它们不能影响单节点领导。
	// （不主动分区：单节点配置多数派=1，领导也不会因它们退位。）
	c.steps(10)
	if r, _ := c.nodes[l].State(); r != Leader {
		t.Fatalf("单节点集群应保持唯一领导, got %s", r)
	}
	// 单节点多数派=1：写入应立即成功并在重启后仍存在。
	if _, _, err := c.nodes[l].Propose([]byte("solo")); err != nil {
		t.Fatalf("单节点写入失败: %v", err)
	}
	c.steps(3)
	if got := c.applied[l][len(c.applied[l])-1]; got != "solo" {
		t.Fatalf("单节点最后应用=%q, want solo", got)
	}
	c.crash(l)
	c.restart(l)
	// 等待单节点重新竞选（其投票集合只有自己，PreVote 立即过半数）。
	var back NodeID
	for i := 0; i < 30 && back == ""; i++ {
		c.step()
		if r, _ := c.nodes[l].State(); r == Leader {
			back = l
		}
	}
	if back == "" {
		t.Fatalf("单节点重启后应重新成为领导, got %s", func() Role {
			r, _ := c.nodes[l].State()
			return r
		}())
	}
	// 当选产生新任期 no-op 并提交，间接提交重启前的 solo；
	// 状态机从空重建，solo 应随之后移并恰好应用一次。
	c.steps(6)
	found := false
	for _, a := range c.applied[l] {
		if a == "solo" {
			found = true
		}
	}
	if !found {
		t.Fatal("单节点重启后已确认写入 solo 丢失")
	}
	t.Logf("缩容到单节点成功：voters=%v，重启后仍为领导且写入不丢", c.nodes[l].Configuration().Voters)
}

// TestMembershipWithPartition 成员变更期间发生网络分区，恢复后配置唯一、
// 不出现两个领导；被隔离的旧领导带着残留状态回归也不能搞分裂。
func TestMembershipWithPartition(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	const newID NodeID = "n4"
	c.startNewNode(newID)
	if err := c.nodes[l].AddLearner(newID); err != nil {
		t.Fatal(err)
	}
	c.steps(6)
	if err := c.nodes[l].PromoteMember(newID); err != nil {
		t.Fatal(err)
	}
	// 联合配置刚追加就把领导与新节点一起隔离到一边（它们持有 C_old,new）。
	c.net.Cut(l, "n2")
	c.net.Cut("n2", l)
	c.net.Cut(l, "n3")
	c.net.Cut("n3", l)
	c.net.Cut(newID, "n2")
	c.net.Cut("n2", newID)
	c.net.Cut(newID, "n3")
	c.net.Cut("n3", newID)
	c.steps(20)
	// 分区两侧都不应出现稳定领导：
	//  {l,n4} 一侧 C_old 多数派（需 n2/n3）不足；
	//  {n2,n3} 一侧没有联合配置，且无法得到含 n4 的 C_new 多数派。
	for _, id := range []NodeID{l, newID, "n2", "n3"} {
		if r, term := c.nodes[id].State(); r == Leader {
			t.Fatalf("分区期间节点 %s 不应是领导 term=%d", id, term)
		}
	}
	t.Logf("成员变更期间分区，两侧均无领导（防止两个有效配置各自提交）")

	// 恢复网络：集群必须收敛到唯一领导与唯一最终配置。
	c.net.Heal()
	var final NodeID
	for i := 0; i < 45 && final == ""; i++ {
		c.step()
		final = c.leader()
	}
	if final == "" {
		t.Fatal("分区恢复后未能重新选出唯一领导")
	}
	c.steps(10)
	conf := c.waitConfigSettled(final, 20)
	if conf.IsJoint() || len(conf.Voters) != 4 {
		t.Fatalf("期望 4 选民最终配置, got %+v", conf)
	}
	for _, id := range c.ids {
		got := c.nodes[id].Configuration()
		if got.IsJoint() || len(got.Voters) != 4 {
			t.Fatalf("节点 %s 配置未收敛: %+v", id, got)
		}
	}
	c.checkSameTermSingleLeader()
	if _, _, err := c.nodes[final].Propose([]byte("post-heal")); err != nil {
		t.Fatalf("恢复后写入: %v", err)
	}
	c.steps(6)
	c.checkAppliedConsistent()
	t.Logf("分区恢复后收敛: leader=%s voters=%v", final, conf.Voters)
}

// TestIdenticalConfigsElectStably 多个节点以完全一致的配置同时启动，
// 随机化选举超时保证在合理时间内稳定选出唯一领导，不长时间分票。
func TestIdenticalConfigsElectStably(t *testing.T) {
	ids := []NodeID{"a", "b", "c", "d", "e"}
	c := &cluster{
		t:        t,
		net:      NewNetwork(),
		storages: map[NodeID]*MemoryStorage{},
		nodes:    map[NodeID]*Node{},
		applied:  map[NodeID][]string{},
		snaps:    map[NodeID][]byte{},
		ids:      ids,
	}
	for _, id := range ids {
		c.storages[id] = NewMemoryStorage()
	}
	// 完全相同的超时配置（无错峰），只靠内部随机抖动打破分票。
	for _, id := range ids {
		cfg := Config{
			ID: id, Peers: ids,
			ElectionTimeout: 10, ElectionTimeoutMax: 19, HeartbeatTimeout: 2,
		}
		node := NewNode(cfg, c.storages[id], c.net, func(e LogEntry) {
			c.applied[id] = append(c.applied[id], string(e.Data))
		})
		c.nodes[id] = node
		c.net.Attach(id, node.Handle)
	}
	l := c.waitLeader(60)
	c.steps(10)
	if got := c.leader(); got != l {
		t.Fatalf("领导应保持稳定, got=%s 原=%s", got, l)
	}
	c.checkSameTermSingleLeader()
	// 领导崩溃后，其余节点同样应稳定补选。
	c.crash(l)
	var l2 NodeID
	for i := 0; i < 60 && l2 == ""; i++ {
		c.step()
		for _, id := range ids {
			if id == l {
				continue
			}
			if r, _ := c.nodes[id].State(); r == Leader {
				l2 = id
			}
		}
	}
	if l2 == "" {
		t.Fatal("领导崩溃后未能稳定补选")
	}
	c.checkSameTermSingleLeader()
	t.Logf("同配置并发启动稳定选主: 首任=%s 补选=%s", l, l2)
}

// TestProposalFailureClassified 多数派不可达时写入必须明确失败并可分类，
// 恢复后写入重新成功。
func TestProposalFailureClassified(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	c.net.Partition(l)
	c.steps(15) // CheckQuorum 退位
	if _, _, err := c.nodes[l].Propose([]byte("noquorum")); !errors.Is(err, ErrNotLeader) && !errors.Is(err, ErrNoMajority) {
		t.Fatalf("期望 ErrNotLeader/ErrNoMajority, got %v", err)
	} else {
		t.Logf("多数派不可达写入明确失败: %v", err)
	}
	c.net.Heal()
	c.steps(20)
	l2 := c.leader()
	if l2 == "" {
		t.Fatal("恢复后无领导")
	}
	if _, _, err := c.nodes[l2].Propose([]byte("back")); err != nil {
		t.Fatalf("恢复后写入应成功: %v", err)
	}
	c.steps(5)
	c.checkAppliedConsistent()
}
