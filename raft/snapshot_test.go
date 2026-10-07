package raft

import (
	"errors"
	"fmt"
	"testing"
)

// TestSnapshotCatchUp 落后节点（崩溃期间错过大量条目且领导者已压缩）
// 应通过 InstallSnapshot 完成追赶，且不丢失已提交状态。
func TestSnapshotCatchUp(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	for i := 0; i < 4; i++ {
		if _, _, err := c.nodes[l].Propose([]byte(fmt.Sprintf("s%d", i))); err != nil {
			t.Fatal(err)
		}
		c.steps(3)
	}
	// 跟随者 f 崩溃；领导者继续写入并做快照压缩。
	f := c.ids[1]
	if f == l {
		f = c.ids[2]
	}
	c.crash(f)
	for i := 4; i < 8; i++ {
		if _, _, err := c.nodes[l].Propose([]byte(fmt.Sprintf("s%d", i))); err != nil {
			t.Fatal(err)
		}
		c.steps(3)
	}
	snap, err := c.nodes[l].Compact(8, []byte("state-through-8"))
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	t.Logf("leader compacted: lastIncludedIndex=%d term=%d", snap.LastIncludedIndex, snap.LastIncludedTerm)
	// f 重启：本地日志远落后于领导者快照点，必须通过快照追赶。
	c.restart(f)
	c.steps(10)
	if c.snaps[f] == nil {
		t.Fatalf("node %s should have installed a snapshot", f)
	}
	if string(c.snaps[f]) != "state-through-8" {
		t.Fatalf("snapshot data mismatch: %q", c.snaps[f])
	}
	// 快照之后的条目（s7）仍按日志复制并应用。
	if got := c.applied[f]; len(got) != 1 || got[0] != "s7" {
		t.Fatalf("post-snapshot entries applied=%v, want [s7]", got)
	}
	if c.nodes[f].Committed() != c.nodes[l].Committed() {
		t.Fatalf("commit diverged after catch-up: f=%d l=%d",
			c.nodes[f].Committed(), c.nodes[l].Committed())
	}
	t.Logf("caught up: node=%s commit=%d snapData=%q applied=%v",
		f, c.nodes[f].Committed(), c.snaps[f], c.applied[f])
}

// TestCorruptSnapshotRejected 损坏的快照必须被明确拒绝且可安全重试。
func TestCorruptSnapshotRejected(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	f := c.ids[1]
	if f == l {
		f = c.ids[2]
	}
	_, term := c.nodes[l].State()
	bad := NewSnapshot(5, 1, []byte("tampered"))
	bad.Data = []byte("corrupted") // 篡改数据但不重算校验和
	commitBefore := c.nodes[f].Committed()
	c.nodes[f].Handle(Message{
		Type: MsgInstallSnapshot, From: l, To: f, Term: term, Snapshot: bad,
	})
	if !errors.Is(c.nodes[f].LastError(), ErrSnapshotCorrupt) {
		t.Fatalf("expected ErrSnapshotCorrupt, got %v", c.nodes[f].LastError())
	}
	if got := c.nodes[f].Committed(); got != commitBefore {
		t.Fatalf("corrupt snapshot changed commit: %d -> %d", commitBefore, got)
	}
	// 重试一条完好的快照应被接受。
	good := NewSnapshot(5, 1, []byte("valid-state"))
	c.nodes[f].Handle(Message{
		Type: MsgInstallSnapshot, From: l, To: f, Term: term, Snapshot: good,
	})
	if c.nodes[f].Committed() != 5 {
		t.Fatalf("valid snapshot retry should advance commit to 5, got %d", c.nodes[f].Committed())
	}
	t.Logf("corrupt snapshot rejected at node=%s; valid retry committed=5", f)
}

// TestSnapshotVersionMismatch 版本不符的快照必须被拒绝。
func TestSnapshotVersionMismatch(t *testing.T) {
	s := NewSnapshot(3, 1, []byte("v"))
	s.Version = 999
	if err := s.Valid(); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("expected ErrVersionMismatch, got %v", err)
	}
	// 编码层同样拒绝。
	if _, err := s.Encode(); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("encode: expected ErrVersionMismatch, got %v", err)
	}
	// 解码损坏字节流。
	if _, err := DecodeSnapshot([]byte{1, 2, 3}); !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("decode: expected ErrSnapshotCorrupt, got %v", err)
	}
	// 正常往返。
	ok := NewSnapshot(3, 1, []byte("round-trip"))
	b, err := ok.Encode()
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodeSnapshot(b)
	if err != nil {
		t.Fatal(err)
	}
	if back.LastIncludedIndex != 3 || string(back.Data) != "round-trip" {
		t.Fatalf("round-trip mismatch: %+v", back)
	}
	// 篡改编码后的数据字节。
	b[len(b)-6] ^= 0xff
	if _, err := DecodeSnapshot(b); !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("tampered decode: expected ErrSnapshotCorrupt, got %v", err)
	}
}

// TestCompactUncommittedRejected 未提交的条目不得被压缩。
func TestCompactUncommittedRejected(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	// 不投递任何消息：条目未提交。
	idx, _, err := c.nodes[l].Propose([]byte("pending"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.nodes[l].Compact(idx, []byte("x")); !errors.Is(err, ErrLogInconsistent) {
		t.Fatalf("expected error for compacting uncommitted entry, got %v", err)
	}
	c.steps(4) // 现在提交了
	if _, err := c.nodes[l].Compact(idx, []byte("x")); err != nil {
		t.Fatalf("compact committed entry should succeed: %v", err)
	}
	// 重复压缩已回收区间。
	if _, err := c.nodes[l].Compact(idx, []byte("y")); !errors.Is(err, ErrCompacted) {
		t.Fatalf("expected ErrCompacted, got %v", err)
	}
}

// TestSnapshotSurvivesRestart 快照持久化后，重启节点不得重复应用已压缩条目。
func TestSnapshotSurvivesRestart(t *testing.T) {
	c := newCluster(t, 3)
	l := c.waitLeader(30)
	for i := 0; i < 3; i++ {
		if _, _, err := c.nodes[l].Propose([]byte(fmt.Sprintf("k%d", i))); err != nil {
			t.Fatal(err)
		}
		c.steps(3)
	}
	// 领导者压缩已提交前缀（含 no-op 共 4 个位点），随后继续提交 k3。
	if _, err := c.nodes[l].Compact(4, []byte("snap-4")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.nodes[l].Propose([]byte("k3")); err != nil {
		t.Fatal(err)
	}
	c.steps(4)
	// 领导者自身重启：快照从存储恢复，k0..k2 不得再次应用。
	before := len(c.applied[l])
	c.crash(l)
	c.restart(l)
	c.steps(20)                             // 集群重新选举（可能是新领导者），提交继续推进
	if got := len(c.applied[l]); got != 1 { // 重启后状态机只应应用快照之后的 k3
		t.Fatalf("restarted leader applied %d entries (%v), want 1 ([k3]); before=%d",
			got, c.applied[l], before)
	}
	if c.applied[l][0] != "k3" {
		t.Fatalf("applied %q, want k3", c.applied[l][0])
	}
	t.Logf("restart with snapshot: node=%s applied(post-restart)=%v snaps=%q",
		l, c.applied[l], c.snaps[l])
}
