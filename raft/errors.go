package raft

import (
	"errors"
	"fmt"
)

// ErrorKind 将失败原因分类，调用方可据此区分多数派不足、任期过期、
// 日志不一致、快照损坏等不同情况，而无需解析错误文本。
type ErrorKind string

const (
	KindQuorumUnavailable ErrorKind = "quorum_unavailable" // 多数派不足，无法提交/选出领导者
	KindStaleTerm         ErrorKind = "stale_term"         // 请求来自过期任期，被明确拒绝
	KindLogInconsistent   ErrorKind = "log_inconsistent"   // 追加位置的任期/索引不匹配
	KindSnapshotMismatch  ErrorKind = "snapshot_mismatch"  // 快照版本不匹配或校验失败
	KindNotLeader         ErrorKind = "not_leader"         // 当前节点不是领导者（可能正在选举）
	KindShutdown          ErrorKind = "shutdown"           // 节点已关闭
	KindRejected          ErrorKind = "rejected"           // 被对端明确拒绝
	KindInternal          ErrorKind = "internal"           // 不应发生的内部不变量破坏
)

// RaftError 携带可机读的错误类别与上下文信息。
type RaftError struct {
	Kind    ErrorKind
	Message string
}

func (e *RaftError) Error() string {
	if e == nil {
		return ""
	}
	return "raft: " + string(e.Kind) + ": " + e.Message
}

func raftErrorf(kind ErrorKind, format string, args ...any) *RaftError {
	return &RaftError{Kind: kind, Message: fmt.Sprintf(format, args...)}
}

// AsRaftError 从任意 error 中提取 *RaftError，便于按 Kind 判定。
func AsRaftError(err error) (*RaftError, bool) {
	var re *RaftError
	if errors.As(err, &re) {
		return re, true
	}
	return nil, false
}

// IsKind 判断 err 是否属于指定分类。
func IsKind(err error, kind ErrorKind) bool {
	if re, ok := AsRaftError(err); ok {
		return re.Kind == kind
	}
	return false
}
