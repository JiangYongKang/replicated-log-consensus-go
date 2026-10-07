package raft

// NodeID 集群节点标识。
type NodeID string

// Role 节点角色。
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return "Unknown"
}

// EntryType 日志条目类型。
type EntryType int

const (
	// EntryNormal 普通客户端命令。
	EntryNormal EntryType = iota
	// EntryConfChange 成员变更条目（Data 为编码后的 ConfChange）。
	EntryConfChange
)

// LogEntry 单条日志条目。
type LogEntry struct {
	Term  uint64    // 产生该条目的领导者任期
	Index uint64    // 日志位点（从 1 开始）
	Type  EntryType // 条目类型（零值为普通命令）
	Data  []byte    // 客户端命令或编码后的 ConfChange
}

// MessageType 节点间消息类型。
type MessageType int

const (
	MsgRequestVote MessageType = iota
	MsgRequestVoteResp
	MsgAppendEntries
	MsgAppendEntriesResp
	MsgInstallSnapshot
	MsgInstallSnapshotResp
	// MsgPreVote 预投票请求：不抬升任期，先探测自己是否可能当选，
	// 避免分区/被移除节点重连后以高任期扰乱集群。
	MsgPreVote
	MsgPreVoteResp
)

// Message 节点间传递的消息（值类型，便于复制与重排注入）。
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID
	Term uint64 // 发送方任期

	// RequestVote / PreVote
	LastLogIndex uint64
	LastLogTerm  uint64

	// PreVote 响应：回显请求中的目标任期，便于发起方忽略过期响应。
	VoteTerm uint64

	// AppendEntries
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64

	// AppendEntries 响应
	MatchIndex uint64 // 接收方已匹配的最大位点
	Success    bool
	RejectHint uint64 // 冲突时提示的回退位点

	// InstallSnapshot
	Snapshot *Snapshot
}
