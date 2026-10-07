package raft

import (
	"fmt"
	"sort"
)

// ConfChangeType 成员变更类型。每次变更只增删一个节点（单节点变更），
// 任意时刻至多一个变更未提交，这是安全性的关键前提。
type ConfChangeType int

const (
	// ConfAddLearner 以学习者身份加入：参与复制但不计入多数派、不参与选举。
	ConfAddLearner ConfChangeType = iota
	// ConfPromote 学习者提升为投票成员（追平后由领导者自动发起）。
	ConfPromote
	// ConfRemove 移除节点（投票成员或学习者）。
	ConfRemove
)

func (t ConfChangeType) String() string {
	switch t {
	case ConfAddLearner:
		return "AddLearner"
	case ConfPromote:
		return "Promote"
	case ConfRemove:
		return "Remove"
	}
	return "Unknown"
}

// ConfChange 一次成员变更（API 层语义）。
// 写入日志时会被展开为变更后的完整成员集（见 encodeMembership）：
// 这样任何节点仅凭最后一条变更条目即可还原完整配置，
// 不依赖"初始配置也在日志里"的假设。
type ConfChange struct {
	Type   ConfChangeType
	NodeID NodeID
}

// encodeMembership 编码完整成员集（与快照中的编码一致）。
func encodeMembership(m Membership) []byte {
	out := encodeIDList(m.sortedVoters())
	out = append(out, encodeIDList(m.sortedLearners())...)
	return out
}

// decodeMembership 解码完整成员集；格式非法返回 ErrInvalidConfChange。
func decodeMembership(b []byte) (Membership, error) {
	voters, rest, err := decodeIDList(b)
	if err != nil {
		return Membership{}, fmt.Errorf("%w: %v", ErrInvalidConfChange, err)
	}
	learners, rest, err := decodeIDList(rest)
	if err != nil {
		return Membership{}, fmt.Errorf("%w: %v", ErrInvalidConfChange, err)
	}
	if len(rest) != 0 {
		return Membership{}, fmt.Errorf("%w: trailing %d bytes", ErrInvalidConfChange, len(rest))
	}
	return membershipFromLists(voters, learners), nil
}

// Membership 集群成员集合：投票成员 + 学习者。
// 成员变更条目追加到日志即生效（单节点变更语义），提交即不可回滚；
// 若条目被覆盖截断，节点从快照成员集 + 剩余日志重放恢复出一致视图。
type Membership struct {
	Voters   map[NodeID]bool
	Learners map[NodeID]bool
}

func newMembership() Membership {
	return Membership{Voters: map[NodeID]bool{}, Learners: map[NodeID]bool{}}
}

// membershipFromVoters 以给定投票成员构造（用于集群初始引导）。
func membershipFromVoters(ids []NodeID) Membership {
	m := newMembership()
	for _, id := range ids {
		m.Voters[id] = true
	}
	return m
}

// membershipFromLists 由投票成员与学习者列表构造（用于快照恢复）。
func membershipFromLists(voters, learners []NodeID) Membership {
	m := newMembership()
	for _, id := range voters {
		m.Voters[id] = true
	}
	for _, id := range learners {
		delete(m.Voters, id)
		m.Learners[id] = true
	}
	return m
}

func (m Membership) clone() Membership {
	out := newMembership()
	for id := range m.Voters {
		out.Voters[id] = true
	}
	for id := range m.Learners {
		out.Learners[id] = true
	}
	return out
}

// Contains 是否为成员（投票成员或学习者）。
func (m Membership) Contains(id NodeID) bool { return m.Voters[id] || m.Learners[id] }

// IsVoter 是否为投票成员（只有投票成员计入多数派、参与选举）。
func (m Membership) IsVoter(id NodeID) bool { return m.Voters[id] }

// Majority 多数派人数（仅按投票成员计）。
func (m Membership) Majority() int { return len(m.Voters)/2 + 1 }

// apply 应用一次成员变更。
func (m Membership) apply(cc ConfChange) {
	switch cc.Type {
	case ConfAddLearner:
		delete(m.Voters, cc.NodeID)
		m.Learners[cc.NodeID] = true
	case ConfPromote:
		delete(m.Learners, cc.NodeID)
		m.Voters[cc.NodeID] = true
	case ConfRemove:
		delete(m.Voters, cc.NodeID)
		delete(m.Learners, cc.NodeID)
	}
}

// validate 校验变更对当前集合是否合法。
func (m Membership) validate(cc ConfChange) error {
	switch cc.Type {
	case ConfAddLearner:
		if cc.NodeID == "" || m.Contains(cc.NodeID) {
			return fmt.Errorf("%w: cannot add learner %q (empty or already member)", ErrInvalidConfChange, cc.NodeID)
		}
	case ConfPromote:
		if !m.Learners[cc.NodeID] {
			return fmt.Errorf("%w: cannot promote %q (not a learner)", ErrInvalidConfChange, cc.NodeID)
		}
	case ConfRemove:
		if !m.Contains(cc.NodeID) {
			return fmt.Errorf("%w: cannot remove %q (not a member)", ErrInvalidConfChange, cc.NodeID)
		}
	}
	return nil
}

// sortedVoters 排序后的投票成员列表（确定性编码/遍历用）。
func (m Membership) sortedVoters() []NodeID {
	out := make([]NodeID, 0, len(m.Voters))
	for id := range m.Voters {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// sortedLearners 排序后的学习者列表。
func (m Membership) sortedLearners() []NodeID {
	out := make([]NodeID, 0, len(m.Learners))
	for id := range m.Learners {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// ---------- Node 成员变更 API ----------

// Membership 返回当前成员视图（投票成员与学习者，均为排序拷贝）。
// 注意：未提交的变更也会反映在此视图中（追加即生效）。
func (n *Node) Membership() (voters, learners []NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.membership.sortedVoters(), n.membership.sortedLearners()
}

// Removed 报告本节点是否已被提交的配置移除（转为完全被动）。
func (n *Node) Removed() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.removed
}

// AddNode 以学习者身份加入新节点：先复制追赶，追平已提交位点后
// 由领导者自动发起提升为投票成员。返回变更条目的日志位点。
func (n *Node) AddNode(id NodeID) (uint64, error) {
	return n.ProposeConfChange(ConfChange{Type: ConfAddLearner, NodeID: id})
}

// RemoveNode 移除一个节点（投票成员或学习者）。变更提交后，
// 被移除节点转为完全被动，集群按新成员集合继续。
func (n *Node) RemoveNode(id NodeID) (uint64, error) {
	return n.ProposeConfChange(ConfChange{Type: ConfRemove, NodeID: id})
}

// ProposeConfChange 向日志追加一条成员变更。约束：
//   - 只能由领导者发起（ErrNotLeader）；
//   - 上一个变更必须已提交（ErrConfChangeInProgress），即任意时刻
//     至多一个未提交变更——这是单节点变更安全性的前提；
//   - 变更对当前成员集必须合法（ErrInvalidConfChange）；
//   - 多数派可达（ErrNoMajority），避免变更在分区中悬置。
func (n *Node) ProposeConfChange(cc ConfChange) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.proposeConfChangeLocked(cc)
}

func (n *Node) proposeConfChangeLocked(cc ConfChange) (uint64, error) {
	if n.role != Leader {
		return 0, fmt.Errorf("%w: node %s is %s", ErrNotLeader, n.id, n.role)
	}
	if n.pendingConfIdx > n.commitIdx {
		n.lastErr = ErrConfChangeInProgress
		return 0, fmt.Errorf("%w: pending conf change at index %d (commit=%d)",
			ErrConfChangeInProgress, n.pendingConfIdx, n.commitIdx)
	}
	if err := n.membership.validate(cc); err != nil {
		n.lastErr = err
		return 0, err
	}
	if active := n.activeVoters(); active < n.majority() {
		n.lastErr = ErrNoMajority
		return 0, fmt.Errorf("%w: only %d/%d voters reachable", ErrNoMajority, active, len(n.membership.Voters))
	}
	newM := n.membership.clone()
	newM.apply(cc)
	e := LogEntry{
		Term:  n.term,
		Index: n.lastLogIndex() + 1,
		Type:  EntryConfChange,
		Data:  encodeMembership(newM), // 条目携带变更后的完整成员集
	}
	n.log = append(n.log, e)
	n.matchIdx[n.id] = e.Index
	n.applyMembershipLocked(newM, cc, e.Index)
	n.persist()
	n.broadcastAppend()
	return e.Index, nil
}

// applyMembershipLocked 变更条目追加即生效（单节点变更语义）。
// newM 为条目携带的完整成员集；cc 用于领导者维护复制状态。
func (n *Node) applyMembershipLocked(newM Membership, cc ConfChange, index uint64) {
	n.membership = newM
	n.pendingConfIdx = index
	if n.role == Leader {
		switch cc.Type {
		case ConfAddLearner:
			// 立即开始向新学习者复制（从最新位点起，冲突回退会将其快速拉回）。
			n.nextIdx[cc.NodeID] = n.lastLogIndex() + 1
			n.matchIdx[cc.NodeID] = 0
			n.lastAck[cc.NodeID] = n.tick
		case ConfRemove:
			if cc.NodeID != n.id {
				// 继续向被移除节点复制一段时间，让它得知结果并转入被动，
				// 而不是无声断连后一直预投票骚扰集群。
				n.removedPeers[cc.NodeID] = 0 // 0 = 移除尚未提交
			}
		}
	}
}

// maybePromoteLocked 学习者追平已提交位点后，自动发起提升为投票成员。
func (n *Node) maybePromoteLocked() {
	if n.role != Leader || n.removed {
		return
	}
	if n.pendingConfIdx > n.commitIdx || n.commitIdx == 0 {
		return // 有未提交变更：一次只进行一个
	}
	for _, id := range n.membership.sortedLearners() {
		if n.matchIdx[id] >= n.commitIdx {
			if _, err := n.proposeConfChangeLocked(ConfChange{Type: ConfPromote, NodeID: id}); err == nil {
				return // 一次只提升一个
			}
		}
	}
}
