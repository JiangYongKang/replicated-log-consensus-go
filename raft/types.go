package raft

import "time"

// Role 表示节点在当前任期中的角色。
type Role int

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

func (r Role) String() string {
	switch r {
	case RoleFollower:
		return "follower"
	case RoleCandidate:
		return "candidate"
	case RoleLeader:
		return "leader"
	default:
		return "unknown"
	}
}

// Entry 是一条日志条目。Index 从 1 开始单调递增。
type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// MessageType 标识 RPC 消息类型。
type MessageType int

const (
	MsgVoteRequest MessageType = iota
	MsgVoteResponse
	MsgAppendRequest
	MsgAppendResponse
	MsgSnapshotRequest
	MsgSnapshotResponse
	MsgPreVoteRequest
	MsgPreVoteResponse
)

// Message 是节点之间交换的唯一信封结构。
type Message struct {
	Type   MessageType
	From   string
	To     string
	Term   uint64
	Reject bool
	// 选举
	LastLogIndex uint64
	LastLogTerm  uint64
	// 复制
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
	Success      bool
	MatchIndex   uint64
	HintIndex    uint64 // 冲突提示：对端建议的回退位点
	HintTerm     uint64
	// 快照
	SnapshotMeta SnapshotMeta
	SnapshotData []byte
	// RejectKind/RejectReason 携带被拒原因（如 KindLogInconsistent、
	// KindSnapshotMismatch、KindStaleTerm），便于按原因处理与测试断言。
	RejectKind   ErrorKind
	RejectReason string
}

// SnapshotMeta 描述快照覆盖的位点与集群配置。
type SnapshotMeta struct {
	LastIndex uint64
	LastTerm  uint64
	Peers     []string
}

// StateInfo 是对外暴露的可观测状态。
type StateInfo struct {
	NodeID       string
	Role         Role
	Term         uint64
	VotedFor     string
	CommitIndex  uint64
	AppliedIndex uint64
	LastLogIndex uint64
	LeaderHint   string
	Peers        []string
}

// Config 是节点的静态配置。
type Config struct {
	NodeID            string
	Peers             []string // 包含本节点
	DataDir           string
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	RandomElection    bool // 测试可关闭随机抖动以获得确定性
}

func (c *Config) validate() error {
	if c.NodeID == "" {
		return raftErrorf(KindInternal, "NodeID required")
	}
	if len(c.Peers) == 0 {
		return raftErrorf(KindInternal, "at least one peer required")
	}
	if c.ElectionTimeout <= 0 {
		c.ElectionTimeout = 300 * time.Millisecond
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = c.ElectionTimeout / 3
	}
	if c.HeartbeatInterval >= c.ElectionTimeout {
		return raftErrorf(KindInternal, "heartbeat interval must be shorter than election timeout")
	}
	return nil
}
