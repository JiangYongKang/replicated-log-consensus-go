package raft

import (
	"bytes"
	"sort"
)

// StateMachine 接收已提交条目。条目是否被重复应用由节点层通过持久化
// appliedIndex 保证；Restore 用快照数据整体重置状态机。
type StateMachine interface {
	Apply(entry Entry) error
	Restore(snapshotData []byte) error
	Snapshot() ([]byte, error)
}

// KVStateMachine 是最小的确定性键值状态机。
// 命令为文本行："SET <key> <value>" 或 "DELETE <key>"。
// 快照编码为按 key 排序的 SET 行集合，保证字节级确定性。
type KVStateMachine struct {
	data map[string]string
}

func NewKVStateMachine() *KVStateMachine {
	return &KVStateMachine{data: make(map[string]string)}
}

// ParseKVCommand 暴露给外部（如 demo/测试）以构造命令。
func ParseKVCommand(line string) (op, key, value string, err error) {
	fields := splitFields(line)
	if len(fields) < 2 {
		return "", "", "", raftErrorf(KindRejected, "malformed command: %q", line)
	}
	switch fields[0] {
	case "SET":
		if len(fields) != 3 {
			return "", "", "", raftErrorf(KindRejected, "SET needs key and value: %q", line)
		}
		return "SET", fields[1], fields[2], nil
	case "DELETE":
		if len(fields) != 2 {
			return "", "", "", raftErrorf(KindRejected, "DELETE needs key: %q", line)
		}
		return "DELETE", fields[1], "", nil
	default:
		return "", "", "", raftErrorf(KindRejected, "unknown op: %q", fields[0])
	}
}

// KVSetCommand / KVDeleteCommand 构造命令字节。
func KVSetCommand(key, value string) []byte {
	return []byte("SET " + key + " " + value)
}

func KVDeleteCommand(key string) []byte {
	return []byte("DELETE " + key)
}

func splitFields(line string) []string {
	var fields []string
	var cur bytes.Buffer
	inToken := false
	for i := 0; i < len(line); i++ {
		c := line[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			if inToken {
				fields = append(fields, cur.String())
				cur.Reset()
				inToken = false
			}
			continue
		}
		inToken = true
		cur.WriteByte(c)
	}
	if inToken {
		fields = append(fields, cur.String())
	}
	return fields
}

func (m *KVStateMachine) Apply(entry Entry) error {
	// 领导者当选时写入的空 no-op 条目用于确立本任期提交权，对状态机无影响。
	if len(entry.Data) == 0 {
		return nil
	}
	op, key, value, err := ParseKVCommand(string(entry.Data))
	if err != nil {
		return err
	}
	switch op {
	case "SET":
		m.data[key] = value
	case "DELETE":
		delete(m.data, key)
	}
	return nil
}

func (m *KVStateMachine) Restore(data []byte) error {
	next := make(map[string]string)
	lines := bytes.Split(data, []byte{'\n'})
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		op, key, value, err := ParseKVCommand(string(line))
		if err != nil {
			return err
		}
		if op != "SET" {
			return raftErrorf(KindSnapshotMismatch, "snapshot contains non-SET line: %q", line)
		}
		next[key] = value
	}
	m.data = next
	return nil
}

func (m *KVStateMachine) Snapshot() ([]byte, error) {
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b bytes.Buffer
	for _, k := range keys {
		b.WriteString("SET ")
		b.WriteString(k)
		b.WriteByte(' ')
		b.WriteString(m.data[k])
		b.WriteByte('\n')
	}
	return b.Bytes(), nil
}

func (m *KVStateMachine) Get(key string) (string, bool) {
	v, ok := m.data[key]
	return v, ok
}

func (m *KVStateMachine) Clone() map[string]string {
	out := make(map[string]string, len(m.data))
	for k, v := range m.data {
		out[k] = v
	}
	return out
}
