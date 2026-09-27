package raft

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestSnapshotCompactionAndCatchUp: 领导者压缩旧日志后，一个长期离线的
// 落后节点必须通过 InstallSnapshot 完成追赶，且不丢失任何已提交状态。
func TestSnapshotCompactionAndCatchUp(t *testing.T) {
	c := newTestCluster(t, 3, 300*time.Millisecond)
	defer c.stopAll()
	ldr := c.awaitLeader()

	// 停掉一个节点模拟新加入/长期落后者。
	laggard := pickFollower(c, ldr)
	c.stopNode(laggard)
	t.Logf("laggard=%s stopped before writes", laggard)

	// 多数派写入并提交 10 条，然后在领导者上做快照、压缩旧日志。
	for i := 1; i <= 10; i++ {
		if err := c.proposeOnLeader("s"+itoa(i), "val"+itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
	c.waitValueOnIds(c.onlineIds(), "s10", "val10")
	leaderNode := c.nodes[ldr]
	commitBefore := leaderNode.State().CommitIndex
	ctx := context.Background()
	if err := leaderNode.ForceSnapshot(ctx); err != nil {
		t.Fatalf("force snapshot: %v", err)
	}
	s := leaderNode.State()
	t.Logf("leader compacted: term=%d commit=%d applied=%d last=%d snapshot=%d",
		s.Term, s.CommitIndex, s.AppliedIndex, s.LastLogIndex, leaderNode.log.snapshotIndex)
	if leaderNode.log.snapshotIndex != commitBefore {
		t.Fatalf("snapshot index=%d want %d", leaderNode.log.snapshotIndex, commitBefore)
	}
	// 快照之后继续写入，保证既有快照数据、又有快照后日志需要追赶。
	for i := 11; i <= 14; i++ {
		if err := c.proposeOnLeader("s"+itoa(i), "val"+itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
	c.waitValueOnIds(c.onlineIds(), "s14", "val14")

	// 落后节点重新加入（沿用同一数据目录，相当于带旧状态恢复）。
	c.startNode(laggard)
	c.awaitLeader()
	c.waitAppliedAtLeast(laggard, c.nodes[ldr].State().CommitIndex)
	for i := 1; i <= 14; i++ {
		if v, ok := c.kvGet(laggard, "s"+itoa(i)); !ok || v != "val"+itoa(i) {
			t.Fatalf("laggard after snapshot catch-up missing s%d=%q ok=%v", i, v, ok)
		}
	}
	ls := c.nodes[laggard].State()
	t.Logf("laggard caught up via snapshot: term=%d commit=%d applied=%d last=%d",
		ls.Term, ls.CommitIndex, ls.AppliedIndex, ls.LastLogIndex)
}

// TestSnapshotCatchUpFreshNode: 全新数据目录的节点加入已有集群，
// 必须通过快照获得全部已提交状态。
func TestSnapshotCatchUpFreshNode(t *testing.T) {
	c := newTestCluster(t, 2, 300*time.Millisecond)
	defer c.stopAll()
	ldr := c.awaitLeader()
	for i := 1; i <= 6; i++ {
		if err := c.proposeOnLeader("f"+itoa(i), "g"+itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.nodes[ldr].ForceSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 7; i <= 9; i++ {
		if err := c.proposeOnLeader("f"+itoa(i), "g"+itoa(i)); err != nil {
			t.Fatal(err)
		}
	}

	freshID := "fresh"
	dir := c.tdir + "/" + freshID
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// 新节点以包含既有成员的完整配置启动，但尚无任何日志/快照。
	c.peers = append(c.peers, freshID)
	c.startNodeWithPeers(freshID, c.peers)
	// 通知两个既有节点扩展集群成员视图（领导者据此开始快照追赶）。
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	for _, node := range c.nodes {
		if err := node.AddPeer(ctx2, freshID); err != nil {
			cancel2()
			t.Fatalf("add peer: %v", err)
		}
	}
	cancel2()
	c.waitAppliedAtLeast(freshID, c.nodes[ldr].State().CommitIndex)
	for i := 1; i <= 9; i++ {
		if v, ok := c.kvGet(freshID, "f"+itoa(i)); !ok || v != "g"+itoa(i) {
			t.Fatalf("fresh node missing f%d=%q ok=%v", i, v, ok)
		}
	}
	t.Logf("fresh node fully caught up: %+v", c.nodes[freshID].State())
}

// TestCorruptSnapshotRejected: 损坏的快照载荷必须被明确拒绝
// （KindSnapshotMismatch / CRC 失败），且不得污染本地状态；移除故障后
// 重传必须成功。
func TestCorruptSnapshotRejected(t *testing.T) {
	c := newTestCluster(t, 3, 300*time.Millisecond)
	defer c.stopAll()
	ldr := c.awaitLeader()
	laggard := pickFollower(c, ldr)

	// 提交 4 条基线条目（落后者也有），随后隔离落后者。
	for i := 1; i <= 4; i++ {
		if err := c.proposeOnLeader("x"+itoa(i), "y"+itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
	c.waitForValue("x4", "y4")
	laggardApplied := c.nodes[laggard].State().AppliedIndex

	c.net.Partition(laggard, true)
	t.Logf("laggard=%s isolated at applied=%d; majority writes + compaction follow",
		laggard, laggardApplied)
	majoritySide := []string{}
	for _, id := range c.peers {
		if id != laggard {
			majoritySide = append(majoritySide, id)
		}
	}

	// 多数派继续写入到 12 条，并压缩，使落后者必然需要快照。
	for i := 5; i <= 12; i++ {
		if err := c.proposeOnLeader("x"+itoa(i), "y"+itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
	c.waitValueOnIds(majoritySide, "x12", "y12")
	majorityLeader := c.leader()
	if err := c.nodes[majorityLeader].ForceSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Logf("leader=%s snapshot index=%d (laggard stuck at %d)",
		majorityLeader, c.nodes[majorityLeader].log.snapshotIndex, laggardApplied)

	// 先恢复链路，再在当前领导者 -> 落后者方向注入快照损坏。
	c.net.Partition(laggard, false)
	// 恢复后集群可能已换任，等待稳定的领导者。
	newLeader := c.awaitLeader()
	c.net.SetPolicy(newLeader, laggard, EdgePolicy{CorruptSnapshot: true})
	t.Logf("corrupting snapshots on edge %s -> %s", newLeader, laggard)

	// 损坏窗口：落后者不得被污染，也不得追上。
	time.Sleep(1200 * time.Millisecond)
	if v, ok := c.kvGet(laggard, "x12"); ok && v == "y12" {
		t.Fatalf("laggard converged despite corrupt snapshot injection")
	}
	if _, ok := c.kvGet(laggard, "x12"); ok {
		t.Fatalf("corrupt snapshot data leaked into FSM")
	}
	ls := c.nodes[laggard].State()
	t.Logf("during corruption window laggard stable: commit=%d applied=%d",
		ls.CommitIndex, ls.AppliedIndex)

	// 移除故障：心跳重传快照，必须自动成功并安全收敛。
	c.net.SetPolicy(newLeader, laggard, EdgePolicy{})
	c.waitAppliedAtLeast(laggard, c.nodes[newLeader].State().CommitIndex)
	if v, ok := c.kvGet(laggard, "x1"); !ok || v != "y1" {
		t.Fatalf("after corruption recovery x1=%q ok=%v", v, ok)
	}
	if v, ok := c.kvGet(laggard, "x12"); !ok || v != "y12" {
		t.Fatalf("after corruption recovery x12=%q ok=%v", v, ok)
	}
	t.Logf("corrupt snapshots rejected, clean retry converged: applied=%d",
		c.nodes[laggard].State().AppliedIndex)
}

// TestSnapshotFileVersionAndCRCCheck: 直接构造损坏/错误版本的快照文件，
// LoadSnapshot 必须返回 KindSnapshotMismatch，绝不静默接受。
func TestSnapshotFileVersionAndCRCCheck(t *testing.T) {
	dir := t.TempDir()
	st, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	meta := SnapshotMeta{LastIndex: 5, LastTerm: 2, Peers: []string{"a", "b"}}
	fsmData := []byte("SET k v\n")
	if err := st.SaveSnapshot(meta, fsmData); err != nil {
		t.Fatal(err)
	}
	if gotMeta, _, ok, err := st.LoadSnapshot(); err != nil || !ok ||
		gotMeta.LastIndex != 5 || gotMeta.LastTerm != 2 {
		t.Fatalf("round-trip snapshot failed: ok=%v err=%v meta=%+v", ok, err, gotMeta)
	}

	// 损坏最后一个字节（CRC 区）。
	raw, err := os.ReadFile(st.snapshotPath())
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), raw...)
	corrupt[len(corrupt)-1] ^= 0xFF
	if err := os.WriteFile(st.snapshotPath(), corrupt, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := st.LoadSnapshot(); !IsKind(err, KindSnapshotMismatch) {
		t.Fatalf("corrupt snapshot err=%v, want KindSnapshotMismatch", err)
	}
	t.Logf("corrupted snapshot file rejected: %v", err)

	// 错误魔数。
	bogus := append([]byte(nil), raw...)
	copy(bogus[:4], "XXXX")
	if err := os.WriteFile(st.snapshotPath(), bogus, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := st.LoadSnapshot(); !IsKind(err, KindSnapshotMismatch) {
		t.Fatalf("bogus-magic snapshot err=%v, want KindSnapshotMismatch", err)
	}
}

func (c *testCluster) onlineIds() []string {
	var out []string
	for id := range c.nodes {
		out = append(out, id)
	}
	return out
}
