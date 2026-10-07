package raft

import "sync"

// PersistentState 需要持久化的节点状态（Raft 硬状态）。
type PersistentState struct {
	CurrentTerm uint64
	VotedFor    NodeID
	Log         []LogEntry // 仅含快照之后的条目
}

// Storage 持久化接口：崩溃恢复的唯一依据。
// SaveState 必须在继续处理前完成（同步写盘语义）。
type Storage interface {
	SaveState(s PersistentState) error
	LoadState() (PersistentState, bool, error)
	SaveSnapshot(s *Snapshot) error
	LoadSnapshot() (*Snapshot, bool, error)
}

// MemoryStorage 进程内存储；测试中用同一实例模拟"重启后磁盘仍在"。
// 所有读写均做深拷贝，避免调用方修改污染已持久化状态。
type MemoryStorage struct {
	mu       sync.Mutex
	state    PersistentState
	hasState bool
	snap     *Snapshot
	hasSnap  bool
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func cloneLog(l []LogEntry) []LogEntry {
	out := make([]LogEntry, len(l))
	for i, e := range l {
		out[i] = e
		out[i].Data = append([]byte(nil), e.Data...)
		if e.Config != nil {
			c := cloneConfig(*e.Config)
			out[i].Config = &c
		}
	}
	return out
}

func (m *MemoryStorage) SaveState(s PersistentState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = PersistentState{
		CurrentTerm: s.CurrentTerm,
		VotedFor:    s.VotedFor,
		Log:         cloneLog(s.Log),
	}
	m.hasState = true
	return nil
}

func (m *MemoryStorage) LoadState() (PersistentState, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.hasState {
		return PersistentState{}, false, nil
	}
	s := m.state
	s.Log = cloneLog(s.Log)
	return s, true, nil
}

func (m *MemoryStorage) SaveSnapshot(s *Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *s
	cp.Data = append([]byte(nil), s.Data...)
	m.snap = &cp
	m.hasSnap = true
	return nil
}

func (m *MemoryStorage) LoadSnapshot() (*Snapshot, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.hasSnap {
		return nil, false, nil
	}
	cp := *m.snap
	cp.Data = append([]byte(nil), m.snap.Data...)
	return &cp, true, nil
}
