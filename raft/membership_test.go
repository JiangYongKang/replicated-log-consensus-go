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

// TestConfChangeCrashMidway 变更未提交时领导者崩溃：
// 其余节点选出新领导者并提交新条目后，旧领导者重启，
// 其悬置变更条目必须被覆盖回滚，集群收敛到唯一确定的配置。
func TestConfChangeCrashMidway(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	if _, _, err := c.nodes[l].Propose([]byte("stable-1")); err != nil {
		t.Fatal(err)
	}
	c.steps(4)
	// 领导者本地追加 remove 条目，但不投递任何消息（变更悬置）。
	if _, err := c.nodes[l].RemoveNode("n3"); err != nil {
		t.Fatal(err)
	}
	for c.net.DropFirst() {
	}
	// 领导者崩溃且暂不重启：其余节点选出新领导者并提交冲突条目，
	// 使旧领导者的悬置变更永远不可能再提交。
	c.crash(l)
	c.steps(30)
	l2 := c.leader()
	if l2 == "" || l2 == l {
		t.Fatalf("expected new leader among remaining nodes, got %q", l2)
	}
	if _, _, err := c.nodes[l2].Propose([]byte("stable-2")); err != nil {
		t.Fatal(err)
	}
	c.steps(5)
	// 旧领导者重启：悬置条目被新领导者的日志覆盖，配置回滚。
	c.restart(l)
	c.steps(15)
	c.checkSameTermSingleLeader()
	c.checkMembershipConverged()
	voters, _ := c.membershipOf(l2)
	if len(voters) != 3 {
		t.Fatalf("uncommitted removal should be rolled back, voters=%v", voters)
	}
	c.checkAppliedConsistent()
	t.Logf("mid-change crash recovered via rollback: leader=%s voters=%v", l2, voters)
}

// TestConfChangeCrashLeaderReelected 变更未提交时领导者崩溃并立即重启：
// 旧领导者凭更长的日志重新当选，悬置变更随新任期条目间接提交，
// 集群同样收敛到唯一确定的配置（变更生效）。
func TestConfChangeCrashLeaderReelected(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	if _, _, err := c.nodes[l].Propose([]byte("stable-1")); err != nil {
		t.Fatal(err)
	}
	c.steps(4)
	if _, err := c.nodes[l].RemoveNode("n3"); err != nil {
		t.Fatal(err)
	}
	for c.net.DropFirst() {
	}
	// 立即崩溃重启：旧领导者日志最长，重新当选后变更会被提交。
	c.crash(l)
	c.restart(l)
	c.steps(40)
	c.checkSameTermSingleLeader()
	c.checkMembershipConverged()
	// 无论变更最终生效还是回滚，配置必须唯一确定；本场景中
	// 旧领导者重新当选，变更生效，n3 应已被动退出。
	voters, _ := c.membershipOf(c.leader())
	if len(voters) != 2 {
		t.Fatalf("reelected leader should complete the removal, voters=%v", voters)
	}
	if !c.nodes["n3"].Removed() {
		t.Fatalf("n3 should be passive after committed removal")
	}
	l2 := c.leader()
	if _, _, err := c.nodes[l2].Propose([]byte("stable-2")); err != nil {
		t.Fatal(err)
	}
	c.steps(5)
	c.checkAppliedConsistent()
	t.Logf("reelected leader completed the change: leader=%s voters=%v", l2, voters)
}

// TestConfChangeCommittedThenLeaderCrash 变更已提交后领导者崩溃：
// 新配置必须存活于重启，被移除节点不得回到配置中。
func TestConfChangeCommittedThenLeaderCrash(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	victim := NodeID("")
	for _, id := range c.ids {
		if id != l {
			victim = id
			break
		}
	}
	if _, err := c.nodes[l].RemoveNode(victim); err != nil {
		t.Fatal(err)
	}
	c.steps(8) // 变更提交
	voters, _ := c.membershipOf(l)
	if len(voters) != 2 {
		t.Fatalf("removal not committed: voters=%v", voters)
	}
	// 领导者崩溃重启：已提交的配置变更不得丢失。
	c.crash(l)
	c.restart(l)
	c.steps(30)
	c.checkSameTermSingleLeader()
	c.checkMembershipConverged()
	voters, _ = c.membershipOf(c.leader())
	if len(voters) != 2 {
		t.Fatalf("committed removal lost after restart: voters=%v", voters)
	}
	for _, v := range voters {
		if v == victim {
			t.Fatalf("removed node %s back in config after restart", victim)
		}
	}
	l2 := c.leader()
	if _, _, err := c.nodes[l2].Propose([]byte("post-crash")); err != nil {
		t.Fatal(err)
	}
	c.steps(5)
	c.checkAppliedConsistent()
	t.Logf("committed removal survived leader crash: leader=%s voters=%v", l2, voters)
}

// TestRemovedNodeCannotDisruptAfterHeal 被移除节点在分区中错过变更、
// 长期分区（超过通知宽限期）后恢复：它残留的拉票行为不得影响
// 集群的提交与选主，自己也不得成为领导者。
func TestRemovedNodeCannotDisruptAfterHeal(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	victim := NodeID("")
	for _, id := range c.ids {
		if id != l {
			victim = id
			break
		}
	}
	// 先分区被移除节点，再在多数派一侧完成移除。
	c.net.Partition(victim)
	c.steps(2)
	if _, err := c.nodes[l].RemoveNode(victim); err != nil {
		t.Fatalf("remove under partition: %v", err)
	}
	// 超过通知宽限期（2*ElectionTimeout），领导者停止向 victim 复制。
	c.steps(40)
	voters, _ := c.membershipOf(l)
	if len(voters) != 2 {
		t.Fatalf("removal not committed on majority side: voters=%v", voters)
	}
	if c.nodes[victim].Removed() {
		t.Fatalf("partitioned victim should NOT have learned removal yet")
	}
	_, termBefore := c.nodes[l].State()
	// 分区恢复：victim 持旧配置、旧状态重新连入。
	c.net.Heal()
	c.steps(30)
	// 集群侧：领导者稳定、任期不被抬高、提交继续推进。
	if got := c.leader(); got != l {
		t.Fatalf("leader changed after removed node rejoined: %s -> %s", l, got)
	}
	_, termAfter := c.nodes[l].State()
	if termAfter != termBefore {
		t.Fatalf("term inflated by removed node: %d -> %d", termBefore, termAfter)
	}
	if r, _ := c.nodes[victim].State(); r == Leader {
		t.Fatalf("removed node %s became leader", victim)
	}
	// 直接注入 victim 的高任期拉票消息：活跃领导者不得被打断。
	c.nodes[l].Handle(Message{
		Type: MsgRequestVote, From: victim, To: l,
		Term: termAfter + 10, LastLogIndex: 99, LastLogTerm: 99,
	})
	if r, _ := c.nodes[l].State(); r != Leader {
		t.Fatalf("active leader disrupted by stale high-term vote request")
	}
	if _, _, err := c.nodes[l].Propose([]byte("still-working")); err != nil {
		t.Fatalf("cluster not serving after removed node rejoined: %v", err)
	}
	c.steps(5)
	c.checkSameTermSingleLeader()
	// 当前成员（n1、n3）之间配置必须一致；victim 错过通知且无人再
	// 与其通信，保留旧配置属预期（它已无法造成任何影响）。
	for _, id := range []NodeID{"n1", "n3"} {
		v, _ := c.membershipOf(id)
		if len(v) != 2 {
			t.Fatalf("member %s should have 2 voters, got %v", id, v)
		}
	}
	t.Logf("removed node %s rejoined harmlessly: leader=%s term=%d", victim, l, termAfter)
}

// TestPartitionDuringAddNode 扩容途中新节点短暂失联：
// 恢复后追赶完成并自动提升，集群配置唯一确定。
func TestPartitionDuringAddNode(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	for i := 0; i < 3; i++ {
		if _, _, err := c.nodes[l].Propose([]byte(fmt.Sprintf("pre-%d", i))); err != nil {
			t.Fatal(err)
		}
		c.steps(3)
	}
	c.join("n4")
	if _, err := c.nodes[l].AddNode("n4"); err != nil {
		t.Fatal(err)
	}
	c.steps(2)
	// 追赶途中 n4 失联一阵子。
	c.net.Partition("n4")
	c.steps(10)
	if _, _, err := c.nodes[l].Propose([]byte("during-partition")); err != nil {
		t.Fatalf("cluster should keep serving with learner partitioned: %v", err)
	}
	c.steps(5)
	c.net.Heal()
	c.steps(25)
	// n4 恢复后补齐全部历史并被提升。
	voters, learners := c.membershipOf(l)
	if len(learners) != 0 || len(voters) != 4 {
		t.Fatalf("n4 not promoted after heal: voters=%v learners=%v", voters, learners)
	}
	c.steps(3)
	if got := len(c.applied["n4"]); got != 4 {
		t.Fatalf("n4 applied %d entries, want 4", got)
	}
	c.checkAppliedConsistent()
	c.checkSameTermSingleLeader()
	c.checkMembershipConverged()
	t.Logf("learner recovered from partition and promoted: voters=%v", voters)
}

// TestShrinkToSingleNodeAndGrowBack 缩容到单节点：单节点独立提交；
// 之后再扩容回多节点，全程已确认写入不丢。
func TestShrinkToSingleNodeAndGrowBack(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	if _, _, err := c.nodes[l].Propose([]byte("epoch-1")); err != nil {
		t.Fatal(err)
	}
	c.steps(4)
	// 逐个移除，直到只剩领导者自己。
	removed := []NodeID{}
	for _, id := range c.ids {
		if id != l {
			removed = append(removed, id)
		}
	}
	for _, victim := range removed {
		if _, err := c.nodes[l].RemoveNode(victim); err != nil {
			t.Fatalf("remove %s: %v", victim, err)
		}
		c.steps(15)
	}
	voters, _ := c.membershipOf(l)
	if fmt.Sprint(voters) != fmt.Sprint([]NodeID{l}) {
		t.Fatalf("expected single-node cluster {%s}, got %v", l, voters)
	}
	// 单节点独立提交写入。
	if _, _, err := c.nodes[l].Propose([]byte("solo-write")); err != nil {
		t.Fatalf("single-node cluster cannot commit: %v", err)
	}
	c.steps(4)
	if got := len(c.applied[l]); got != 2 {
		t.Fatalf("single node applied %d entries, want 2", got)
	}
	t.Logf("shrunk to single node %s and committing solo", l)
	// 再扩容：全新节点加入并提升。
	c.join("n4")
	if _, err := c.nodes[l].AddNode("n4"); err != nil {
		t.Fatal(err)
	}
	c.steps(25)
	voters, _ = c.membershipOf(l)
	if len(voters) != 2 {
		t.Fatalf("grow back failed: voters=%v", voters)
	}
	if _, _, err := c.nodes[l].Propose([]byte("epoch-2")); err != nil {
		t.Fatal(err)
	}
	c.steps(5)
	if got := len(c.applied["n4"]); got != 3 {
		t.Fatalf("n4 applied %d entries, want 3 (full history)", got)
	}
	c.checkAppliedConsistent()
	c.checkSameTermSingleLeader()
	c.checkMembershipConverged()
	t.Logf("grew back to voters=%v", voters)
}

// TestRemovedNodeRestartStaysPassive 被移除节点崩溃重启后
// 必须保持被动：不发起选举、不响应消息。
func TestRemovedNodeRestartStaysPassive(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	victim := NodeID("")
	for _, id := range c.ids {
		if id != l {
			victim = id
			break
		}
	}
	if _, err := c.nodes[l].RemoveNode(victim); err != nil {
		t.Fatal(err)
	}
	c.steps(15)
	if !c.nodes[victim].Removed() {
		t.Fatalf("victim should have learned removal")
	}
	// 崩溃重启：持久化配置中不含自己，必须保持被动。
	c.crash(victim)
	c.restart(victim)
	if !c.nodes[victim].Removed() {
		t.Fatalf("restarted removed node should stay passive")
	}
	c.steps(20)
	if r, _ := c.nodes[victim].State(); r != Follower {
		t.Fatalf("removed node became %s after restart", r)
	}
	if got := c.leader(); got != l {
		t.Fatalf("leader changed: %s -> %s", l, got)
	}
	t.Logf("removed node %s stays passive across restart", victim)
}

// TestSnapshotCarriesMembership 快照必须携带成员配置：
// 新学习者通过 InstallSnapshot 加入时，能还原完整成员集并继续追赶。
func TestSnapshotCarriesMembership(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	for i := 0; i < 3; i++ {
		if _, _, err := c.nodes[l].Propose([]byte(fmt.Sprintf("s%d", i))); err != nil {
			t.Fatal(err)
		}
		c.steps(3)
	}
	// 新节点启动即被分区，领导者先把它加为学习者。
	c.join("n4")
	c.net.Partition("n4")
	if _, err := c.nodes[l].AddNode("n4"); err != nil {
		t.Fatal(err)
	}
	c.steps(8) // add-learner 在多数派提交（学习者不计入多数派）
	// 领导者压缩掉含变更条目的日志：n4 只能通过快照追赶。
	idx := c.nodes[l].Committed()
	if _, err := c.nodes[l].Compact(idx, []byte("state-with-conf")); err != nil {
		t.Fatalf("compact: %v", err)
	}
	c.net.Heal()
	c.steps(25)
	// n4 应通过快照还原成员集（自己是学习者），追平后被提升。
	if c.snaps["n4"] == nil {
		t.Fatalf("n4 should have installed snapshot")
	}
	voters, learners := c.membershipOf(l)
	if len(learners) != 0 || len(voters) != 4 {
		t.Fatalf("n4 not promoted after snapshot catch-up: voters=%v learners=%v", voters, learners)
	}
	v4, _ := c.membershipOf("n4")
	if len(v4) != 4 {
		t.Fatalf("n4 membership derived from snapshot+log: voters=%v", v4)
	}
	// 快照覆盖了全部历史条目：n4 的状态机由快照整体重建。
	if string(c.snaps["n4"]) != "state-with-conf" {
		t.Fatalf("n4 snapshot data = %q, want state-with-conf", c.snaps["n4"])
	}
	// 快照之后的新写入照常复制到 n4。
	if _, _, err := c.nodes[l].Propose([]byte("post-snapshot")); err != nil {
		t.Fatal(err)
	}
	c.steps(5)
	if got := len(c.applied["n4"]); got != 1 {
		t.Fatalf("n4 applied %d post-snapshot entries, want 1", got)
	}
	c.checkSameTermSingleLeader()
	c.checkMembershipConverged()
	t.Logf("n4 joined via snapshot: voters=%v", voters)
}

// TestSimultaneousStartElectsQuickly 多节点同时启动、配置一致时，
// 应在合理时间内选出唯一领导者；短暂心跳抖动不得引发换主。
func TestSimultaneousStartElectsQuickly(t *testing.T) {
	c := newCluster(t, 5)
	l := c.waitLeader(40) // 5 节点最大选举超时 30，预投票一轮内须完成
	if l == "" {
		t.Fatal("no leader")
	}
	_, term := c.nodes[l].State()
	t.Logf("5-node simultaneous start: leader=%s term=%d", l, term)
	// 注入心跳抖动：丢掉两轮心跳（远小于选举超时），领导者必须保持稳定。
	for i := 0; i < 4; i++ {
		for c.net.DropFirst() {
		}
		c.step()
	}
	c.steps(3)
	if got := c.leader(); got != l {
		t.Fatalf("leader changed after brief heartbeat loss: %s -> %s", l, got)
	}
	if _, term2 := c.nodes[l].State(); term2 != term {
		t.Fatalf("term changed after heartbeat jitter: %d -> %d", term, term2)
	}
	c.steps(10)
	c.checkSameTermSingleLeader()
	t.Logf("leader stable through heartbeat jitter: %s term=%d", l, term)
}

// TestCommittedWriteSurvivesFullRestart 被多数派确认的写入，
// 在全部节点崩溃重启后仍然存在且按序重放。
func TestCommittedWriteSurvivesFullRestart(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	for i := 0; i < 4; i++ {
		if _, _, err := c.nodes[l].Propose([]byte(fmt.Sprintf("durable-%d", i))); err != nil {
			t.Fatal(err)
		}
		c.steps(3)
	}
	// 做一次成员变更，确认变更后的配置同样耐重启。
	c.join("n4")
	if _, err := c.nodes[l].AddNode("n4"); err != nil {
		t.Fatal(err)
	}
	c.steps(20)
	if _, _, err := c.nodes[l].Propose([]byte("durable-4")); err != nil {
		t.Fatal(err)
	}
	c.steps(5)
	// 全部节点崩溃并重启（存储保留）。
	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		c.restart(id)
	}
	c.steps(30)
	c.checkSameTermSingleLeader()
	c.checkMembershipConverged()
	// 已确认写入在重启后重新提交并应用，不丢不重。
	c.steps(5)
	c.checkAppliedConsistent()
	l2 := c.leader()
	if l2 == "" {
		t.Fatal("no leader after full restart")
	}
	if got := len(c.applied[l2]); got != 5 {
		t.Fatalf("leader applied %d entries after full restart, want 5", got)
	}
	for i, want := range []string{"durable-0", "durable-1", "durable-2", "durable-3", "durable-4"} {
		if c.applied[l2][i] != want {
			t.Fatalf("applied[%d]=%q, want %q", i, c.applied[l2][i], want)
		}
	}
	t.Logf("all committed writes survived full cluster restart: leader=%s applied=%v",
		l2, c.applied[l2])
}
