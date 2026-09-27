package raft

import (
	"errors"
	"fmt"
	"testing"
)

// TestReplicateAndCommit 写入应被复制到多数派、提交并按序应用到状态机。
func TestReplicateAndCommit(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	for i := 0; i < 5; i++ {
		idx, term, err := c.nodes[l].Propose([]byte(fmt.Sprintf("cmd-%d", i)))
		if err != nil {
			t.Fatalf("propose failed: %v", err)
		}
		t.Logf("proposed cmd-%d at index=%d term=%d", i, idx, term)
		c.steps(3)
	}
	c.steps(5)
	for _, id := range c.ids {
		if got := len(c.applied[id]); got != 5 {
			t.Fatalf("node %s applied %d entries, want 5 (commit=%d)", id, got, c.nodes[id].Committed())
		}
	}
	c.checkAppliedConsistent()
}

// TestDuplicateAndReorderedMessages 重复与乱序消息不得使状态机偏移。
func TestDuplicateAndReorderedMessages(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	// 提交 3 条，但每条 AppendEntries 都先复制再乱序投递。
	for i := 0; i < 3; i++ {
		if _, _, err := c.nodes[l].Propose([]byte(fmt.Sprintf("m%d", i))); err != nil {
			t.Fatal(err)
		}
		// 对队列中的每条消息注入一次重复，再整体打乱顺序后投递。
		n := c.net.Pending()
		for j := 0; j < n; j++ {
			c.net.DuplicateFirst()
			c.net.DeliverOne()
		}
		for j := 0; j < n; j++ {
			c.net.ReorderFirstToEnd()
		}
		c.net.DeliverAll()
	}
	c.steps(6)
	c.checkAppliedConsistent()
	for _, id := range c.ids {
		want := []string{"m0", "m1", "m2"}
		got := c.applied[id]
		if len(got) != len(want) {
			t.Fatalf("node %s applied %v, want %v", id, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("node %s applied[%d]=%q want %q", id, i, got[i], want[i])
			}
		}
		t.Logf("node=%s commit=%d applied=%v", id, c.nodes[id].Committed(), got)
	}
}

// TestDroppedMessagesRecover 丢包后心跳重试应能完成复制。
func TestDroppedMessagesRecover(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	if _, _, err := c.nodes[l].Propose([]byte("durable")); err != nil {
		t.Fatal(err)
	}
	// 丢掉第一批 AppendEntries。
	for c.net.DropFirst() {
	}
	c.steps(6) // 心跳重试补上
	c.checkAppliedConsistent()
	for _, id := range c.ids {
		if len(c.applied[id]) != 1 {
			t.Fatalf("node %s applied %d entries, want 1", id, len(c.applied[id]))
		}
	}
}

// TestRestartRecovery 节点崩溃重启后应依据持久化状态恢复，
// 已提交条目不丢失、不重复应用。
func TestRestartRecovery(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	for i := 0; i < 3; i++ {
		if _, _, err := c.nodes[l].Propose([]byte(fmt.Sprintf("r%d", i))); err != nil {
			t.Fatal(err)
		}
		c.steps(3)
	}
	// 让一个跟随者崩溃，期间领导者继续提交。
	follower := NodeID("")
	for _, id := range c.ids {
		if id != l {
			follower = id
			break
		}
	}
	c.crash(follower)
	if _, _, err := c.nodes[l].Propose([]byte("while-down")); err != nil {
		t.Fatal(err)
	}
	c.steps(4)
	// 重启：状态机从空开始，已提交条目应被重新补齐且仅应用一次。
	c.restart(follower)
	c.steps(8)
	want := []string{"r0", "r1", "r2", "while-down"}
	got := c.applied[follower]
	if len(got) != len(want) {
		t.Fatalf("restarted node applied %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("restarted node applied[%d]=%q want %q", i, got[i], want[i])
		}
	}
	c.checkAppliedConsistent()
	t.Logf("restarted node=%s commit=%d applied=%v", follower, c.nodes[follower].Committed(), got)
}

// TestRestartPreservesVote 重启后不得在同一任期重复投票（防止双领导者）。
func TestRestartPreservesVote(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	_, term := c.nodes[l].State()
	// 跟随者重启：votedFor 必须从存储恢复。
	f := c.ids[1]
	if f == l {
		f = c.ids[2]
	}
	c.crash(f)
	c.restart(f)
	st, ok, err := c.storages[f].LoadState()
	if err != nil || !ok {
		t.Fatalf("load state: %v ok=%v", err, ok)
	}
	if st.CurrentTerm != term || st.VotedFor == "" {
		t.Fatalf("persisted vote lost: term=%d votedFor=%q", st.CurrentTerm, st.VotedFor)
	}
	t.Logf("node=%s restarted with term=%d votedFor=%s", f, st.CurrentTerm, st.VotedFor)
}

// TestConflictingLogsResolved 分区期间产生的冲突日志必须被新领导者覆盖。
func TestConflictingLogsResolved(t *testing.T) {
	c := newCluster(t, 3)
	l1 := c.waitLeader(30)
	if _, _, err := c.nodes[l1].Propose([]byte("committed-1")); err != nil {
		t.Fatal(err)
	}
	c.steps(4)
	// 隔离旧领导者；它在少数派中接受一条无法提交的写入。
	c.net.Partition(l1)
	if _, _, err := c.nodes[l1].Propose([]byte("uncommitted-x")); err != nil {
		t.Fatal(err)
	}
	// 多数派一侧选出新领导者并提交不同条目。
	var l2 NodeID
	for i := 0; i < 40 && l2 == ""; i++ {
		c.step()
		for _, id := range c.ids {
			if id == l1 {
				continue
			}
			if r, _ := c.nodes[id].State(); r == Leader {
				l2 = id
			}
		}
	}
	if l2 == "" {
		t.Fatal("no new leader on majority side")
	}
	if _, _, err := c.nodes[l2].Propose([]byte("committed-2")); err != nil {
		t.Fatal(err)
	}
	c.steps(4)
	// 恢复分区：旧领导者退位，其未提交条目必须被覆盖。
	c.net.Heal()
	c.steps(15)
	c.checkSameTermSingleLeader()
	for _, id := range c.ids {
		for _, a := range c.applied[id] {
			if a == "uncommitted-x" {
				t.Fatalf("node %s applied uncommitted conflicting entry", id)
			}
		}
	}
	c.checkAppliedConsistent()
	// 旧领导者应已收敛到与新领导者相同的提交位点。
	if c.nodes[l1].Committed() != c.nodes[l2].Committed() {
		t.Fatalf("commit diverged: %s=%d %s=%d", l1, c.nodes[l1].Committed(), l2, c.nodes[l2].Committed())
	}
	t.Logf("conflict resolved: old leader=%s commit=%d, new leader=%s commit=%d",
		l1, c.nodes[l1].Committed(), l2, c.nodes[l2].Committed())
}

// TestLogInconsistencyClassified prevLog 冲突必须分类为 ErrLogInconsistent。
func TestLogInconsistencyClassified(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	f := c.ids[1]
	if f == l {
		f = c.ids[2]
	}
	_, term := c.nodes[l].State()
	// 伪造一条 prevLog 不存在的 AppendEntries。
	c.nodes[f].Handle(Message{
		Type: MsgAppendEntries, From: l, To: f, Term: term,
		PrevLogIndex: 99, PrevLogTerm: 1,
	})
	if !errors.Is(c.nodes[f].LastError(), ErrLogInconsistent) {
		t.Fatalf("expected ErrLogInconsistent, got %v", c.nodes[f].LastError())
	}
	t.Logf("node=%s rejected inconsistent append at prevLogIndex=99", f)
}
