package raft

import "errors"

// 错误分类：调用方与测试可据此区分失败原因。
var (
	// ErrNotLeader 当前节点不是领导者，无法处理写请求。
	ErrNotLeader = errors.New("raft: not leader")
	// ErrNoMajority 多数派不足，无法提交或推进。
	ErrNoMajority = errors.New("raft: no majority")
	// ErrStaleTerm 消息或请求的任期已过期。
	ErrStaleTerm = errors.New("raft: stale term")
	// ErrLogInconsistent 日志与前序条目不匹配（prevLog 冲突）。
	ErrLogInconsistent = errors.New("raft: log inconsistent")
	// ErrSnapshotCorrupt 快照校验和验证失败。
	ErrSnapshotCorrupt = errors.New("raft: snapshot corrupt")
	// ErrVersionMismatch 快照格式版本不兼容。
	ErrVersionMismatch = errors.New("raft: snapshot version mismatch")
	// ErrCompacted 请求的条目已被快照回收。
	ErrCompacted = errors.New("raft: log compacted")
	// ErrMembershipChange 已有一次成员变更未完成（配置条目尚未提交/生效）。
	ErrMembershipChange = errors.New("raft: membership change in progress")
	// ErrUnknownMember 目标节点不在当前成员集合中。
	ErrUnknownMember = errors.New("raft: unknown member")
	// ErrMemberExists 目标节点已在成员集合中。
	ErrMemberExists = errors.New("raft: member already exists")
	// ErrNotLearner 目标节点不是学习者，无法直接提升为投票成员。
	ErrNotLearner = errors.New("raft: member is not a learner")
	// ErrRejectVote 非投票成员或旧配置成员的投票请求被拒绝。
	ErrRejectVote = errors.New("raft: vote not permitted by configuration")
)
