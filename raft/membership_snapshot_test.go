package raft

import (
	"fmt"
	"testing"
)

// TestConfigInSnapshot 成员配置必须随快照持久化与恢复：
// 加入新节点并提交后做快照压缩，重启的落后节点通过 InstallSnapshot
// 直接拿到包含新成员的配置，不会回退到旧成员集合。
func TestConfigInSnapshot(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	const newID NodeID = "n4"
	c.startNewNode(newID)
	if err := c.nodes[l].AddLearner(newID); err != nil {
		t.Fatal(err)
	}
	c.steps(6)
	// 学习者刚补齐早期日志即崩溃；之后的提升与大量写入它都错过。
	c.crash(newID)

	if err := c.nodes[l].PromoteMember(newID); err != nil {
		t.Fatal(err)
	}
	c.waitConfigSettled(l, 30)
	for i := 0; i < 5; i++ {
		if _, _, err := c.nodes[l].Propose([]byte(fmt.Sprintf("c%d", i))); err != nil {
			t.Fatal(err)
		}
		c.steps(3)
	}
	// 领导者压缩已提交前缀（含全部配置条目）。快照必须内嵌 4 选民配置。
	upto := c.nodes[l].Committed()
	snap, err := c.nodes[l].Compact(upto, []byte("sm-state"))
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if snap.Config == nil || len(snap.Config.Voters) != 4 {
		t.Fatalf("快照未携带 4 选民配置: %+v", snap.Config)
	}
	t.Logf("快照内嵌配置 voters=%v (index=%d)", snap.Config.Voters, upto)

	// 编解码往返后配置仍在。
	enc, err := snap.Encode()
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodeSnapshot(enc)
	if err != nil || back.Config == nil || len(back.Config.Voters) != 4 {
		t.Fatalf("快照编解码丢失配置: %v %+v", err, back)
	}

	// 再写几条，使落后节点必然需要快照（nextIdx 已在快照点之前）。
	for i := 0; i < 3; i++ {
		if _, _, err := c.nodes[l].Propose([]byte(fmt.Sprintf("late-%d", i))); err != nil {
			t.Fatal(err)
		}
		c.steps(3)
	}
	// 崩溃节点带着旧状态重启：本地配置仍把 n4 当学习者/旧成员，
	// 必须通过 InstallSnapshot 一次性拿到含 4 选民的最新配置。
	c.restart(newID)
	c.steps(14)
	got := c.nodes[newID].Configuration()
	if len(got.Voters) != 4 || got.IsJoint() {
		t.Fatalf("追赶节点通过快照恢复后配置错误: %+v", got)
	}
	if string(c.snaps[newID]) != "sm-state" {
		t.Fatalf("追赶节点应安装快照数据, got %q", c.snaps[newID])
	}
	c.checkSameTermSingleLeader()
	t.Logf("配置随快照恢复成功: node=%s voters=%v snap=%q", newID, got.Voters, c.snaps[newID])
}

// TestDuplicatedConfigMessages 配置变更期间的 AppendEntries 重复/乱序投递
// 不得产生重复应用或配置抖动，所有节点收敛到同一最终配置。
func TestDuplicatedConfigMessages(t *testing.T) {
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
	// 变更消息全程重复 + 乱序投递。
	for i := 0; i < 12; i++ {
		pending := c.net.Pending()
		if pending == 0 {
			c.step()
			continue
		}
		for j := 0; j < pending; j++ {
			c.net.DuplicateFirst()
			c.net.DeliverOne()
		}
		for j := 0; j < pending*2; j++ {
			c.net.ReorderFirstToEnd()
		}
		c.net.DeliverAll()
	}
	c.steps(10)
	conf := c.waitConfigSettled(l, 20)
	if len(conf.Voters) != 4 || conf.IsJoint() {
		t.Fatalf("期望收敛到 4 选民, got %+v", conf)
	}
	for _, id := range c.ids {
		got := c.nodes[id].Configuration()
		if len(got.Voters) != 4 || got.IsJoint() {
			t.Fatalf("节点 %s 配置异常: %+v", id, got)
		}
	}
	c.checkAppliedConsistent()
	c.checkSameTermSingleLeader()
	t.Logf("重复/乱序配置消息幂等，最终 voters=%v", conf.Voters)
}
