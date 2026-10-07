package raft

import "fmt"

// ---- 对外成员变更 API -------------------------------------------------------
//
// 变更流程（联合共识，单成员变更每次只改一个投票成员）：
//
//	AddLearner(id)   学习者阶段：id 只接收日志/快照，不参与投票与提交。
//	PromoteMember(id) 待学习者追平后，走 C -> C_old,new -> C_new 两阶段加入。
//	RemoveMember(id)  投票成员走同样的两阶段移除；学习者直接单条配置移除。
//
// 同一时刻只允许一次未完成的投票成员变更（ErrMembershipChange），
// 从"配置条目已追加"到"最终配置已提交"期间拒绝新的变更，保证
// 任意时刻配置唯一、确定，崩溃/分区后不会分叉为两拨合法配置。

// Configuration 返回节点当前生效的成员配置（拷贝，可安全读取）。
func (n *Node) Configuration() Configuration {
	n.mu.Lock()
	defer n.mu.Unlock()
	return cloneConfig(n.conf)
}

// changeInProgress 是否存在尚未提交完成的（联合）配置变更。
func (n *Node) changeInProgress() bool {
	if !n.conf.IsJoint() {
		return false
	}
	return n.latestConfigIndex() > n.commitIdx
}

// AddLearner 把新节点以学习者身份加入集群。学习者不影响多数派，
// 因此用单条（非联合）配置条目完成；加入前新节点应已用包含本集群的
// 引导配置启动，以便能收到 AppendEntries/Snapshot 完成追赶。
func (n *Node) AddLearner(id NodeID) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return fmt.Errorf("%w: node %s is %s", ErrNotLeader, n.id, n.role)
	}
	if n.changeInProgress() {
		return fmt.Errorf("%w: joint config uncommitted", ErrMembershipChange)
	}
	if n.conf.HasMember(id) {
		return fmt.Errorf("%w: %s already in configuration", ErrMemberExists, id)
	}
	c := cloneConfig(n.conf)
	c.Learners = append(c.Learners, id)
	n.appendConfigEntry(c)
	return nil
}

// PromoteMember 将已追平的学习者提升为投票成员，走两阶段联合共识。
func (n *Node) PromoteMember(id NodeID) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return fmt.Errorf("%w: node %s is %s", ErrNotLeader, n.id, n.role)
	}
	if n.changeInProgress() {
		return fmt.Errorf("%w: joint config uncommitted", ErrMembershipChange)
	}
	if !n.conf.HasLearner(id) {
		return fmt.Errorf("%w: %s is not a learner", ErrNotLearner, id)
	}
	if n.matchIdx[id] < n.lastLogIndex() {
		return fmt.Errorf("%w: learner %s not caught up (match=%d last=%d)",
			ErrMembershipChange, id, n.matchIdx[id], n.lastLogIndex())
	}
	// C_old,new：旧投票集合 + 新投票集合（学习者转正式，移出学习者列表）。
	newVoters := append(append([]NodeID(nil), n.conf.Voters...), id)
	joint := Configuration{
		Voters:    append([]NodeID(nil), n.conf.Voters...),
		VotersNew: newVoters,
		Learners:  removeID(append([]NodeID(nil), n.conf.Learners...), id),
	}
	n.appendConfigEntry(joint)
	return nil
}

// RemoveMember 移除一个成员。投票成员走两阶段联合共识；学习者直接单条移除。
func (n *Node) RemoveMember(id NodeID) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return fmt.Errorf("%w: node %s is %s", ErrNotLeader, n.id, n.role)
	}
	if n.changeInProgress() {
		return fmt.Errorf("%w: joint config uncommitted", ErrMembershipChange)
	}
	if !n.conf.HasMember(id) {
		return fmt.Errorf("%w: %s not in configuration", ErrUnknownMember, id)
	}
	// 学习者不影响多数派：直接从学习者列表删除即可。
	if n.conf.HasLearner(id) {
		c := cloneConfig(n.conf)
		c.Learners = removeID(c.Learners, id)
		n.appendConfigEntry(c)
		return nil
	}
	newVoters := removeID(n.conf.Voters, id)
	if n.conf.IsJoint() {
		// 理论上 changeInProgress 已拦截；防御性处理。
		return fmt.Errorf("%w: cannot remove during joint config", ErrMembershipChange)
	}
	// C_old,new：C_new 为移除后的投票集合。
	joint := Configuration{
		Voters:    append([]NodeID(nil), n.conf.Voters...),
		VotersNew: newVoters,
		Learners:  append([]NodeID(nil), n.conf.Learners...),
	}
	n.appendConfigEntry(joint)
	return nil
}
