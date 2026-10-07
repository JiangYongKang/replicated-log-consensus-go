package raft

import "testing"

// TestRemovedQuorumCannotCommit 被移除的节点彼此连通也不能提交任何写入：
// 它们凑不出当前配置的多数派，也选不出领导。
func TestRemovedQuorumCannotCommit(t *testing.T) {
	c := newCluster(t, 5)
	l := c.waitLeader(40)
	// 提交基线。
	if _, _, err := c.nodes[l].Propose([]byte("base")); err != nil {
		t.Fatal(err)
	}
	c.steps(4)

	// 把 5 节点缩容到 2 节点（保留 l 与一个存活者），其余 3 个被踢但仍运行。
	victims := []NodeID{}
	for _, id := range c.ids {
		if id != l && len(victims) < 3 {
			victims = append(victims, id)
		}
	}
	for _, v := range victims {
		if err := c.nodes[l].RemoveMember(v); err != nil {
			t.Fatal(err)
		}
		c.waitConfigSettled(l, 30)
	}
	conf := c.nodes[l].Configuration()
	t.Logf("缩容到 2 节点: voters=%v；被踢但仍运行=%v", conf.Voters, victims)

	// 隔离健康的 2 节点集群，只让 3 个被踢节点彼此连通。
	healthy := conf.Voters
	for _, h := range healthy {
		c.net.Partition(h)
	}
	// 解除被踢节点之间的隔离（Partition 是双向全切断，这里恢复它们互相链路）。
	for i := 0; i < len(victims); i++ {
		for j := i + 1; j < len(victims); j++ {
			c.net.HealPair(victims[i], victims[j])
		}
	}
	c.steps(30)

	// 三个被踢节点凑在一起也选不出领导、提交不了任何东西。
	for _, v := range victims {
		if r, term := c.nodes[v].State(); r == Leader {
			t.Fatalf("被踢节点 %v 成为领导 term=%d（旧配置多数派不得复活）", v, term)
		}
		before := len(c.applied[v])
		if _, _, err := c.nodes[v].Propose([]byte("ghost-write")); err == nil {
			t.Fatalf("被踢节点 %v 居然接受了写入", v)
		}
		if len(c.applied[v]) != before {
			t.Fatalf("被踢节点 %v 应用了非法写入", v)
		}
	}

	// 恢复健康集群：仍然是唯一配置、唯一领导，继续提交。
	c.net.Heal()
	c.steps(25)
	l2 := c.leader()
	if l2 == "" {
		t.Fatal("健康集群恢复后无领导")
	}
	if _, _, err := c.nodes[l2].Propose([]byte("final")); err != nil {
		t.Fatalf("健康集群写入失败: %v", err)
	}
	c.steps(6)
	c.checkSameTermSingleLeader()
	c.checkAppliedConsistentAmong(healthy)
	t.Logf("被踢多数派无法复活；健康集群 leader=%s voters=%v 继续提交", l2, conf.Voters)
}
