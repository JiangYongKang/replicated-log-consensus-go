package raft

import "fmt"

// raftLog 维护快照位点之后的日志条目。entries 连续且无空洞：
// entries[0] 对应 snapshotIndex+1。所有方法假设调用方已串行化访问。
type raftLog struct {
	snapshotIndex uint64
	snapshotTerm  uint64
	entries       []Entry // 有序、连续，index 单调递增
}

func newRaftLog(snapIndex, snapTerm uint64) *raftLog {
	return &raftLog{snapshotIndex: snapIndex, snapshotTerm: snapTerm}
}

func (l *raftLog) firstIndex() uint64 { return l.snapshotIndex + 1 }

func (l *raftLog) lastIndex() uint64 {
	return l.snapshotIndex + uint64(len(l.entries))
}

// termAt 返回 index 处的任期。index == snapshotIndex 时返回快照任期；
// index 早于快照或晚于末尾时 ok 为 false。
func (l *raftLog) termAt(index uint64) (uint64, bool) {
	if index == 0 {
		return 0, true
	}
	if index == l.snapshotIndex {
		return l.snapshotTerm, true
	}
	if index < l.snapshotIndex || index > l.lastIndex() {
		return 0, false
	}
	return l.entries[index-l.firstIndex()].Term, true
}

// entryAt 返回指定索引的条目。
func (l *raftLog) entryAt(index uint64) (Entry, bool) {
	if index < l.firstIndex() || index > l.lastIndex() {
		return Entry{}, false
	}
	return l.entries[index-l.firstIndex()], true
}

// appendEntries 按 Raft 论文的冲突解决规则写入：
//   - prevIndex 必须是快照位点（允许零空洞追平）或已存在的日志索引；
//   - 逐条写入时，若同 index 已存在但 term 不同，截断该 index 及其后的
//     全部本地条目后再写入（明确解决冲突）；
//   - 已存在且 term 相同的前缀条目直接保留，天然支持重复/乱序 RPC 重试
//     产生的幂等覆盖。
func (l *raftLog) appendEntries(prevIndex uint64, entries ...Entry) error {
	if prevIndex < l.snapshotIndex {
		// 对端在发送我们已经压缩掉的历史段，属于调用方协议错误。
		return raftErrorf(KindLogInconsistent,
			"prevLogIndex %d older than snapshot index %d", prevIndex, l.snapshotIndex)
	}
	if prevIndex > l.lastIndex() {
		return raftErrorf(KindLogInconsistent,
			"prevLogIndex %d ahead of last log index %d (gap)", prevIndex, l.lastIndex())
	}
	for i, e := range entries {
		wantIndex := prevIndex + 1 + uint64(i)
		if e.Index != 0 && e.Index != wantIndex {
			return raftErrorf(KindLogInconsistent,
				"entry %d declares index %d, expected %d", i, e.Index, wantIndex)
		}
		e.Index = wantIndex
		if wantIndex <= l.lastIndex() {
			existing := l.entries[wantIndex-l.firstIndex()]
			if existing.Term == e.Term {
				continue // 幂等：重复投递完全相同的条目
			}
			// 冲突：截断该位置及之后的全部本地未提交/已提交条目。
			l.entries = l.entries[:wantIndex-l.firstIndex()]
		}
		l.entries = append(l.entries, e)
	}
	return nil
}

// entriesAfter 返回 index 之后的全部条目（不含 index）。
func (l *raftLog) entriesAfter(index uint64) []Entry {
	if index < l.snapshotIndex {
		index = l.snapshotIndex
	}
	if index >= l.lastIndex() {
		return nil
	}
	out := make([]Entry, l.lastIndex()-index)
	copy(out, l.entries[index-l.firstIndex()+1:])
	return out
}

// slice 返回 [from, to] 闭区间的条目拷贝。
func (l *raftLog) slice(from, to uint64) []Entry {
	if from < l.firstIndex() {
		from = l.firstIndex()
	}
	if to > l.lastIndex() || from > to {
		return nil
	}
	out := make([]Entry, 0, to-from+1)
	out = append(out, l.entries[from-l.firstIndex():to-l.firstIndex()+1]...)
	return out
}

// upToDate 判断 (lastLogIndex, lastLogTerm) 是否至少与本地日志一样新
// （选举投票限制：先比 term，再比 index）。
func (l *raftLog) upToDate(lastIndex, lastTerm uint64) bool {
	myTerm, ok := l.termAt(l.lastIndex())
	if !ok {
		myTerm = 0
	}
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= l.lastIndex()
}

// compact 丢弃 index 及之前的条目并前移快照位点；term 必须与该索引处的
// 条目任期一致，防止错误压缩。
func (l *raftLog) compact(index, term uint64) error {
	if index <= l.snapshotIndex {
		return nil // 已压缩过，幂等
	}
	if index > l.lastIndex() {
		return raftErrorf(KindLogInconsistent,
			"compact index %d beyond last index %d", index, l.lastIndex())
	}
	gotTerm, ok := l.termAt(index)
	if !ok || gotTerm != term {
		return raftErrorf(KindLogInconsistent,
			"compact term mismatch at %d: want %d got %d", index, term, gotTerm)
	}
	l.entries = append([]Entry(nil), l.entries[index-l.firstIndex()+1:]...)
	l.snapshotIndex = index
	l.snapshotTerm = term
	return nil
}

func (l *raftLog) debugString() string {
	return fmt.Sprintf("snap=(%d,%d) len=%d last=%d",
		l.snapshotIndex, l.snapshotTerm, len(l.entries), l.lastIndex())
}
