package raft

import "sort"

// Configuration 集群成员配置（C 配置，随日志复制）。
//
// 采用 Raft 联合共识（joint consensus）表示成员变更：
//   - VotersNew 为空：常规配置，多数派只计 Voters。
//   - VotersNew 非空：联合配置（C_old,new），任何提交与当选都必须
//     同时在 Voters（C_old）与 VotersNew（C_new）两个多数派中通过。
//
// Learners 为非投票成员：接收日志与快照追赶进度，但不参与投票、
// 选举计票与提交多数派计数。
type Configuration struct {
	Voters    []NodeID // 当前投票成员（联合期间为 C_old）
	VotersNew []NodeID // 联合期间的新投票成员 C_new；空表示非联合配置
	Learners  []NodeID // 非投票成员（追赶中的新节点）
}

func containsID(list []NodeID, id NodeID) bool {
	for _, x := range list {
		if x == id {
			return true
		}
	}
	return false
}

// IsJoint 是否处于联合共识配置。
func (c Configuration) IsJoint() bool {
	return len(c.VotersNew) > 0
}

// HasVoter 报告 id 是否为有效投票成员（联合期间在任一投票集合中即算）。
func (c Configuration) HasVoter(id NodeID) bool {
	if containsID(c.Voters, id) {
		return true
	}
	return c.IsJoint() && containsID(c.VotersNew, id)
}

// HasLearner 报告 id 是否为学习者。
func (c Configuration) HasLearner(id NodeID) bool {
	return containsID(c.Learners, id)
}

// HasMember 报告 id 是否属于集群（投票成员或学习者）。
func (c Configuration) HasMember(id NodeID) bool {
	return c.HasVoter(id) || c.HasLearner(id)
}

// AllPeers 返回需要领导者主动联系（发 AppendEntries/Snapshot）的全部成员。
func (c Configuration) AllPeers() []NodeID {
	seen := map[NodeID]bool{}
	var out []NodeID
	for _, id := range c.Voters {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	for _, id := range c.VotersNew {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	for _, id := range c.Learners {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func quorumOf(n int) int { return n/2 + 1 }

func majorityAck(list []NodeID, acks map[NodeID]bool, self NodeID) bool {
	count := 0
	for _, id := range list {
		if acks[id] || id == self {
			count++
		}
	}
	return count >= quorumOf(len(list))
}

// Quorum 报告给定确认集合是否满足配置的多数派要求。
// self 始终视为已确认（领导者自己）；联合配置下两个投票集合都需过半。
func (c Configuration) Quorum(acks map[NodeID]bool, self NodeID) bool {
	if len(c.Voters) == 0 {
		return false
	}
	if !majorityAck(c.Voters, acks, self) {
		return false
	}
	if c.IsJoint() && !majorityAck(c.VotersNew, acks, self) {
		return false
	}
	return true
}

// cloneConfig 深拷贝配置，避免切片别名。
func cloneConfig(c Configuration) Configuration {
	cp := func(s []NodeID) []NodeID {
		if s == nil {
			return nil
		}
		return append([]NodeID(nil), s...)
	}
	return Configuration{Voters: cp(c.Voters), VotersNew: cp(c.VotersNew), Learners: cp(c.Learners)}
}

// removeID 返回删除指定 id 后的新列表。
func removeID(list []NodeID, id NodeID) []NodeID {
	out := make([]NodeID, 0, len(list))
	for _, x := range list {
		if x != id {
			out = append(out, x)
		}
	}
	return out
}
