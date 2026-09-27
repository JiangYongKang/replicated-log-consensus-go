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
)
