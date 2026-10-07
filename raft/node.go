package raft

import (
	"fmt"
	"sync"
)

// Config 节点配置。
type Config struct {
	ID               NodeID
	Peers            []NodeID // 初始成员（含自己）；仅在无任何持久化状态时用于引导
	ElectionTimeout  int      // 选举超时（tick 数）
	HeartbeatTimeout int      // 心跳间隔（tick 数）
}

// ApplyFunc 已提交条目按序回调；同一 Index 在同一节点生命周期内不会被重复调用。
type ApplyFunc func(e LogEntry)

// ApplySnapshotFunc 安装快照时回调，状态机应整体替换为快照内容。
type ApplySnapshotFunc func(s *Snapshot)

// Node 单个共识节点。所有外部方法（Tick/Handle/Propose）均加锁，
// 测试以单线程方式驱动即可，但并发调用也安全。
type Node struct {
	mu      sync.Mutex
	id      NodeID
	cfg     Config
	storage Storage
	net     Transport

	role     Role
	term     uint64
	votedFor NodeID

	log       []LogEntry // 仅含快照之后的条目；log[i].Index == snapIndex+1+i
	snapIndex uint64
	snapTerm  uint64

	commitIdx uint64
	lastApply uint64

	applyFn     ApplyFunc
	applySnapFn ApplySnapshotFunc

	votesRecv   map[NodeID]bool
	electionEl  int // 距上次收到有效领导者消息的 tick 数
	heartbeatEl int
	tick        uint64
	leaderID    NodeID // 当前已知领导者（投票租约用）

	// 预投票状态：先在当前任期上探测可否当选，成功后才真正抬升任期，
	// 避免分区/被移除节点重连时以膨胀的任期打断集群。
	preVoting   bool
	preVoteTerm uint64 // 预投票探测的目标任期（term+1）
	preVotes    map[NodeID]bool

	// 成员管理：membership 由快照成员集 + 日志中的变更条目重放得出，
	// 追加即生效；pendingConfIdx 为最后一条未确定提交的变更条目位点。
	membership    Membership
	snapMember    Membership // 当前快照点对应的成员集
	pendingConfIdx uint64
	removed       bool // 本节点已被提交的配置移除：完全转为被动

	// 领导者视角
	nextIdx  map[NodeID]uint64
	matchIdx map[NodeID]uint64
	lastAck  map[NodeID]uint64 // 各 peer 最近一次回应时的 tick

	lastErr error // 最近一次分类错误（供测试与诊断）
}

func NewNode(cfg Config, storage Storage, net Transport, apply ApplyFunc) *Node {
	n := &Node{
		id:        cfg.ID,
		cfg:       cfg,
		storage:   storage,
		net:       net,
		role:      Follower,
		applyFn:   apply,
		votesRecv: map[NodeID]bool{},
		preVotes:  map[NodeID]bool{},
		nextIdx:   map[NodeID]uint64{},
		matchIdx:  map[NodeID]uint64{},
		lastAck:   map[NodeID]uint64{},
	}
	// 崩溃恢复：以持久化状态为唯一依据。
	// 成员集推导：无快照时以 Config.Peers 为引导基线，有快照时以快照为准，
	// 再重放快照之后日志中的成员变更条目。
	n.snapMember = membershipFromVoters(cfg.Peers)
	if snap, ok, err := storage.LoadSnapshot(); err == nil && ok {
		n.snapIndex = snap.LastIncludedIndex
		n.snapTerm = snap.LastIncludedTerm
		n.snapMember = membershipFromLists(snap.Voters, snap.Learners)
	}
	if st, ok, err := storage.LoadState(); err == nil && ok {
		n.term = st.CurrentTerm
		n.votedFor = st.VotedFor
		n.log = cloneLog(st.Log)
	}
	n.recomputeMembershipLocked()
	// 已被移除的节点（持久化配置中不含自己）转为完全被动：
	// 不参与选举、不响应消息，避免旧节点干扰集群。
	if len(n.membership.Voters)+len(n.membership.Learners) > 0 && !n.membership.Contains(n.id) {
		n.removed = true
	}
	// 提交位点不持久化：重启后由领导者重新推进；
	// 快照覆盖的条目视为已应用，绝不重复应用。
	n.commitIdx = n.snapIndex
	n.lastApply = n.snapIndex
	return n
}

// SetApplySnapshotFunc 注册快照安装回调（需在启动前设置）。
func (n *Node) SetApplySnapshotFunc(f ApplySnapshotFunc) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.applySnapFn = f
}

func (n *Node) ID() NodeID { return n.id }

// State 返回角色与任期（用于测试断言与日志）。
func (n *Node) State() (Role, uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role, n.term
}

// Committed 返回当前提交位点。
func (n *Node) Committed() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIdx
}

// LastError 返回最近一次分类错误（ErrStaleTerm / ErrLogInconsistent 等）。
func (n *Node) LastError() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastErr
}

func (n *Node) majority() int { return n.membership.Majority() }

// recomputeMembershipLocked 由快照成员集 + 日志中的变更条目重放当前成员集。
// 日志追加、截断、快照安装后都必须调用，保证成员视图与日志严格一致。
func (n *Node) recomputeMembershipLocked() {
	m := n.snapMember.clone()
	pending := uint64(0)
	for _, e := range n.log {
		if e.Type != EntryConfChange {
			continue
		}
		if cc, err := decodeConfChange(e.Data); err == nil {
			m.apply(cc)
			pending = e.Index
		}
	}
	n.membership = m
	n.pendingConfIdx = pending
}

func (n *Node) lastLogIndex() uint64 {
	if len(n.log) > 0 {
		return n.log[len(n.log)-1].Index
	}
	return n.snapIndex
}

func (n *Node) lastLogTerm() uint64 {
	if len(n.log) > 0 {
		return n.log[len(n.log)-1].Term
	}
	return n.snapTerm
}

// termAt 返回指定位点的任期；ok=false 表示该位点已被快照回收。
func (n *Node) termAt(index uint64) (uint64, bool) {
	if index == n.snapIndex {
		return n.snapTerm, true
	}
	if index < n.snapIndex || index > n.lastLogIndex() {
		return 0, false
	}
	return n.log[index-n.snapIndex-1].Term, true
}

func (n *Node) persist() {
	_ = n.storage.SaveState(PersistentState{
		CurrentTerm: n.term,
		VotedFor:    n.votedFor,
		Log:         n.log,
	})
}

func (n *Node) send(msg Message) {
	msg.From = n.id
	msg.Term = n.term
	n.net.Send(msg)
}

// sendWithTerm 以指定任期发送（仅预投票使用：探测目标任期而不抬升本地任期）。
func (n *Node) sendWithTerm(msg Message, term uint64) {
	msg.From = n.id
	msg.Term = term
	n.net.Send(msg)
}

// Tick 驱动选举与心跳计时；测试按固定步进调用。
func (n *Node) Tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.removed {
		return // 已被移除：完全被动
	}
	n.tick++
	if n.role == Leader {
		n.heartbeatEl++
		if n.heartbeatEl >= n.cfg.HeartbeatTimeout {
			n.heartbeatEl = 0
			n.broadcastAppend()
		}
		// CheckQuorum：多数派在选举超时内无回应则退位，
		// 避免分区中的旧领导者继续接受写入。
		if n.activeVoters() < n.majority() {
			n.becomeFollower(n.term)
			n.lastErr = ErrNoMajority
		}
		return
	}
	n.electionEl++
	if n.electionEl >= n.cfg.ElectionTimeout {
		if n.membership.IsVoter(n.id) {
			n.startPreVote()
		} else {
			n.electionEl = 0 // 学习者/非成员不发起选举
		}
	}
}

// leaseTicks 投票租约窗口：刚收到过领导者消息的期间内拒绝投票类请求。
// 心跳间隔的 2 倍加 1：领导者存活时跟随者的 electionEl 不会超过该窗口。
func (n *Node) leaseTicks() int { return n.cfg.HeartbeatTimeout*2 + 1 }

// startPreVote 发起预投票：不抬升任期、不持久化投票，
// 得到多数派预投票同意后才进入真正的选举。
func (n *Node) startPreVote() {
	if !n.membership.IsVoter(n.id) {
		return
	}
	n.preVoting = true
	n.preVoteTerm = n.term + 1
	n.preVotes = map[NodeID]bool{n.id: true}
	n.electionEl = 0
	for id := range n.membership.Voters {
		if id == n.id {
			continue
		}
		n.sendWithTerm(Message{
			Type:         MsgPreVote,
			To:           id,
			LastLogIndex: n.lastLogIndex(),
			LastLogTerm:  n.lastLogTerm(),
		}, n.preVoteTerm)
	}
	n.maybeStartElection()
}

// maybeStartElection 预投票获得多数派后进入真正的选举。
func (n *Node) maybeStartElection() {
	if !n.preVoting || len(n.preVotes) < n.majority() {
		return
	}
	n.preVoting = false
	n.startElection()
}

// activeVoters 统计选举超时内有回应的投票成员数（含自己）。
func (n *Node) activeVoters() int {
	active := 0
	for id := range n.membership.Voters {
		if id == n.id {
			active++
			continue
		}
		if n.tick-n.lastAck[id] <= uint64(n.cfg.ElectionTimeout) {
			active++
		}
	}
	return active
}

func (n *Node) becomeFollower(term uint64) {
	n.role = Follower
	if term > n.term {
		n.term = term
		n.votedFor = ""
	}
	n.electionEl = 0
	n.votesRecv = map[NodeID]bool{}
	n.preVoting = false
	n.persist()
}

// logUpToDate 候选人日志是否至少与本地一样新。
func (n *Node) logUpToDate(lastTerm, lastIndex uint64) bool {
	myT, myI := n.lastLogTerm(), n.lastLogIndex()
	return lastTerm > myT || (lastTerm == myT && lastIndex >= myI)
}

// handlePreVote 处理预投票请求：不修改任期与投票记录，纯探测。
func (n *Node) handlePreVote(m Message) {
	grant := false
	// 只接受成员集合内投票成员的探测，且其目标任期更高、日志足够新。
	if m.Term > n.term && n.membership.IsVoter(m.From) && n.logUpToDate(m.LastLogTerm, m.LastLogIndex) {
		grant = true
	}
	n.send(Message{Type: MsgPreVoteResp, To: m.From, Success: grant, VoteTerm: m.Term})
}

func (n *Node) handlePreVoteResp(m Message) {
	// 只统计本轮预投票的响应（按目标任期区分，过期响应忽略）。
	if !n.preVoting || m.VoteTerm != n.preVoteTerm {
		return
	}
	if m.Success {
		n.preVotes[m.From] = true
		n.maybeStartElection()
	}
}

func (n *Node) startElection() {
	if !n.membership.IsVoter(n.id) {
		return
	}
	n.role = Candidate
	n.term++
	n.votedFor = n.id
	n.leaderID = ""
	n.electionEl = 0
	n.votesRecv = map[NodeID]bool{n.id: true}
	n.persist()
	for id := range n.membership.Voters {
		if id == n.id {
			continue
		}
		n.send(Message{
			Type:         MsgRequestVote,
			To:           id,
			LastLogIndex: n.lastLogIndex(),
			LastLogTerm:  n.lastLogTerm(),
		})
	}
	n.maybeBecomeLeader()
}

func (n *Node) maybeBecomeLeader() {
	if n.role != Candidate || len(n.votesRecv) < n.majority() {
		return
	}
	n.role = Leader
	n.heartbeatEl = 0
	last := n.lastLogIndex()
	for id := range n.membership.Voters {
		n.nextIdx[id] = last + 1
		n.matchIdx[id] = 0
		n.lastAck[id] = n.tick
	}
	for id := range n.membership.Learners {
		n.nextIdx[id] = last + 1
		n.matchIdx[id] = 0
		n.lastAck[id] = n.tick
	}
	n.matchIdx[n.id] = last
	// 追加一条 no-op 条目：只有当前任期的条目才能按计数规则提交，
	// no-op 使之前任期的条目随之间接提交（Raft 论文 5.4.2）。
	noop := LogEntry{Term: n.term, Index: n.lastLogIndex() + 1}
	n.log = append(n.log, noop)
	n.matchIdx[n.id] = noop.Index
	n.persist()
	n.broadcastAppend()
}

// Propose 向领导者提交一条命令；返回其日志位点与任期。
// 非领导者返回 ErrNotLeader；多数派失联返回 ErrNoMajority。
func (n *Node) Propose(data []byte) (index uint64, term uint64, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return 0, 0, fmt.Errorf("%w: node %s is %s", ErrNotLeader, n.id, n.role)
	}
	if active := n.activeVoters(); active < n.majority() {
		n.lastErr = ErrNoMajority
		return 0, 0, fmt.Errorf("%w: only %d/%d voters reachable", ErrNoMajority, active, len(n.membership.Voters))
	}
	e := LogEntry{Term: n.term, Index: n.lastLogIndex() + 1, Data: append([]byte(nil), data...)}
	n.log = append(n.log, e)
	n.matchIdx[n.id] = e.Index
	n.persist()
	n.broadcastAppend()
	return e.Index, e.Term, nil
}

// Handle 处理一条入站消息。
func (n *Node) Handle(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.removed {
		return // 已被移除：不再响应任何消息
	}
	// 投票租约：刚收到过有效领导者消息时，拒绝投票类请求且不抬升任期。
	// 这样被移除/分区节点即使用高任期四处拉票，也无法打断活跃领导者。
	if (msg.Type == MsgRequestVote || msg.Type == MsgPreVote) &&
		n.leaderID != "" && n.electionEl < n.leaseTicks() {
		respType := MsgRequestVoteResp
		if msg.Type == MsgPreVote {
			respType = MsgPreVoteResp
		}
		n.send(Message{Type: respType, To: msg.From, Success: false, VoteTerm: msg.Term})
		return
	}
	// 预投票请求只是探测：绝不抬升本地任期（其响应携带响应方任期，可正常抬升）。
	if msg.Type != MsgPreVote && msg.Term > n.term {
		n.becomeFollower(msg.Term)
	}
	switch msg.Type {
	case MsgPreVote:
		n.handlePreVote(msg)
	case MsgPreVoteResp:
		n.handlePreVoteResp(msg)
	case MsgRequestVote:
		n.handleRequestVote(msg)
	case MsgRequestVoteResp:
		n.handleRequestVoteResp(msg)
	case MsgAppendEntries:
		n.handleAppendEntries(msg)
	case MsgAppendEntriesResp:
		n.handleAppendEntriesResp(msg)
	case MsgInstallSnapshot:
		n.handleInstallSnapshot(msg)
	case MsgInstallSnapshotResp:
		n.handleInstallSnapshotResp(msg)
	}
}

func (n *Node) handleRequestVote(m Message) {
	grant := false
	// 只给当前成员集合内的投票成员投票：被移除的节点无法获得选票。
	if m.Term >= n.term && n.membership.IsVoter(m.From) && (n.votedFor == "" || n.votedFor == m.From) {
		// 候选人日志至少与本地一样新才投票。
		if n.logUpToDate(m.LastLogTerm, m.LastLogIndex) {
			grant = true
			n.votedFor = m.From
			n.electionEl = 0
			n.persist()
		}
	}
	if m.Term < n.term {
		n.lastErr = ErrStaleTerm
	}
	n.send(Message{Type: MsgRequestVoteResp, To: m.From, Success: grant})
}

func (n *Node) handleRequestVoteResp(m Message) {
	if n.role != Candidate || m.Term != n.term {
		return
	}
	n.lastAck[m.From] = n.tick
	if m.Success {
		n.votesRecv[m.From] = true
		n.maybeBecomeLeader()
	}
}

func (n *Node) handleAppendEntries(m Message) {
	resp := Message{Type: MsgAppendEntriesResp, To: m.From}
	if m.Term < n.term {
		n.lastErr = ErrStaleTerm
		resp.Success = false
		n.send(resp)
		return
	}
	// 任期有效：回到跟随者、记录领导者并重置选举计时。
	n.role = Follower
	n.leaderID = m.From
	n.preVoting = false
	n.electionEl = 0

	if m.PrevLogIndex > n.snapIndex {
		t, ok := n.termAt(m.PrevLogIndex)
		if !ok {
			// 前序位点超出本地日志：日志不一致。
			n.lastErr = ErrLogInconsistent
			resp.Success = false
			resp.RejectHint = n.lastLogIndex() + 1
			n.send(resp)
			return
		}
		if t != m.PrevLogTerm {
			// 前序任期冲突：回退到冲突任期的第一个位点。
			n.lastErr = ErrLogInconsistent
			hint := m.PrevLogIndex
			for hint > n.snapIndex+1 {
				if pt, _ := n.termAt(hint - 1); pt != t {
					break
				}
				hint--
			}
			resp.Success = false
			resp.RejectHint = hint
			n.send(resp)
			return
		}
	} else if m.PrevLogIndex == n.snapIndex && m.PrevLogTerm != n.snapTerm {
		n.lastErr = ErrLogInconsistent
		resp.Success = false
		resp.RejectHint = n.snapIndex + 1
		n.send(resp)
		return
	}

	// 过滤掉已被快照覆盖的条目，再逐条比对冲突。
	var entries []LogEntry
	for _, e := range m.Entries {
		if e.Index > n.snapIndex {
			entries = append(entries, e)
		}
	}
	changed := false
	for i, e := range entries {
		pos := e.Index - n.snapIndex - 1
		if pos < uint64(len(n.log)) {
			if n.log[pos].Term != e.Term {
				// 冲突条目及其后续全部删除，以领导者为准。
				n.log = n.log[:pos]
				n.log = append(n.log, entries[i:]...)
				changed = true
				break
			}
		} else {
			n.log = append(n.log, entries[i:]...)
			changed = true
			break
		}
	}
	if changed {
		// 日志变更（含冲突截断）后重放成员配置，保证视图与日志一致。
		n.recomputeMembershipLocked()
		n.persist()
	}
	if m.LeaderCommit > n.commitIdx {
		n.commitIdx = min(m.LeaderCommit, n.lastLogIndex())
		n.applyCommitted()
	}
	resp.Success = true
	resp.MatchIndex = n.lastLogIndex()
	n.send(resp)
}

func (n *Node) handleAppendEntriesResp(m Message) {
	if n.role != Leader || m.Term != n.term {
		return
	}
	n.lastAck[m.From] = n.tick
	if !m.Success {
		// 日志不一致：按提示回退并重试。
		next := m.RejectHint
		if next == 0 {
			next = n.nextIdx[m.From] - 1
		}
		if next < 1 {
			next = 1
		}
		n.nextIdx[m.From] = next
		n.sendAppendTo(m.From)
		return
	}
	if m.MatchIndex > n.matchIdx[m.From] {
		n.matchIdx[m.From] = m.MatchIndex
		n.nextIdx[m.From] = m.MatchIndex + 1
	}
	n.advanceCommit()
}

func (n *Node) advanceCommit() {
	for idx := n.commitIdx + 1; idx <= n.lastLogIndex(); idx++ {
		t, ok := n.termAt(idx)
		if !ok || t != n.term {
			continue // 仅提交当前任期的条目（Raft 安全性要求）
		}
		count := 0
		for id := range n.membership.Voters {
			if n.matchIdx[id] >= idx {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIdx = idx
		}
	}
	n.applyCommitted()
	n.maybePromoteLocked()
}

// applyCommitted 按序应用 [lastApply+1, commitIdx]；每位点恰好一次。
// 成员变更条目在此处理提交副作用（如本节点被移除后转为被动）。
func (n *Node) applyCommitted() {
	for n.lastApply < n.commitIdx {
		idx := n.lastApply + 1
		if idx <= n.snapIndex {
			n.lastApply = idx
			continue
		}
		e := n.log[idx-n.snapIndex-1]
		n.lastApply = idx
		switch e.Type {
		case EntryConfChange:
			// 本节点被移除的配置已提交：立即退出领导/选举，转为完全被动。
			if cc, err := decodeConfChange(e.Data); err == nil &&
				cc.Type == ConfRemove && cc.NodeID == n.id {
				n.removed = true
				n.role = Follower
			}
		default:
			// Data 为 nil 的是 no-op 条目，不下发给状态机。
			if n.applyFn != nil && e.Data != nil {
				n.applyFn(e)
			}
		}
	}
}

func (n *Node) broadcastAppend() {
	for id := range n.membership.Voters {
		if id != n.id {
			n.sendAppendTo(id)
		}
	}
	for id := range n.membership.Learners {
		n.sendAppendTo(id)
	}
}

// sendAppendTo 按 nextIdx 向单个 peer 发送 AppendEntries 或 InstallSnapshot。
func (n *Node) sendAppendTo(p NodeID) {
	next := n.nextIdx[p]
	if next <= n.snapIndex {
		// 所需条目已压缩：发送快照。
		if snap, ok, err := n.storage.LoadSnapshot(); err == nil && ok {
			n.send(Message{Type: MsgInstallSnapshot, To: p, Snapshot: snap})
		}
		return
	}
	prevIdx := next - 1
	prevTerm, _ := n.termAt(prevIdx)
	var entries []LogEntry
	if prevIdx >= n.snapIndex+1 || len(n.log) > 0 {
		start := uint64(0)
		if next > n.snapIndex+1 {
			start = next - n.snapIndex - 1
		}
		if start < uint64(len(n.log)) {
			entries = cloneLog(n.log[start:])
		}
	}
	n.send(Message{
		Type:         MsgAppendEntries,
		To:           p,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIdx,
	})
}

func (n *Node) handleInstallSnapshot(m Message) {
	resp := Message{Type: MsgInstallSnapshotResp, To: m.From}
	if m.Term < n.term {
		n.lastErr = ErrStaleTerm
		resp.Success = false
		n.send(resp)
		return
	}
	n.role = Follower
	n.leaderID = m.From
	n.preVoting = false
	n.electionEl = 0
	snap := m.Snapshot
	if snap == nil {
		n.lastErr = ErrSnapshotCorrupt
		resp.Success = false
		n.send(resp)
		return
	}
	if err := snap.Valid(); err != nil {
		// 快照损坏或版本不符：明确拒绝，可安全重试。
		n.lastErr = err
		resp.Success = false
		n.send(resp)
		return
	}
	if snap.LastIncludedIndex <= n.snapIndex {
		// 已覆盖：幂等忽略。
		resp.Success = true
		resp.MatchIndex = n.snapIndex
		n.send(resp)
		return
	}
	_ = n.storage.SaveSnapshot(snap)
	// 截断日志：若快照末位点在本地日志中且任期一致，保留其后缀。
	kept := []LogEntry{}
	if t, ok := n.termAt(snap.LastIncludedIndex); ok && t == snap.LastIncludedTerm {
		for _, e := range n.log {
			if e.Index > snap.LastIncludedIndex {
				kept = append(kept, e)
			}
		}
	}
	n.log = kept
	n.snapIndex = snap.LastIncludedIndex
	n.snapTerm = snap.LastIncludedTerm
	// 快照携带快照点的成员配置；保留后缀中的变更条目在其上重放。
	n.snapMember = membershipFromLists(snap.Voters, snap.Learners)
	n.recomputeMembershipLocked()
	n.persist()
	if n.commitIdx < n.snapIndex {
		n.commitIdx = n.snapIndex
	}
	if n.lastApply < n.snapIndex {
		n.lastApply = n.snapIndex
	}
	if n.applySnapFn != nil {
		n.applySnapFn(snap)
	}
	resp.Success = true
	resp.MatchIndex = n.snapIndex
	n.send(resp)
}

func (n *Node) handleInstallSnapshotResp(m Message) {
	if n.role != Leader || m.Term != n.term {
		return
	}
	n.lastAck[m.From] = n.tick
	if m.Success && m.MatchIndex > n.matchIdx[m.From] {
		n.matchIdx[m.From] = m.MatchIndex
		n.nextIdx[m.From] = m.MatchIndex + 1
		n.sendAppendTo(m.From)
	}
}

// Compact 对 [.., upto] 的已提交日志做快照并回收旧条目。
// upto 必须已提交且未被压缩，否则返回分类错误。
func (n *Node) Compact(upto uint64, data []byte) (*Snapshot, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if upto > n.commitIdx {
		return nil, fmt.Errorf("%w: cannot compact uncommitted index %d (commit=%d)",
			ErrLogInconsistent, upto, n.commitIdx)
	}
	if upto <= n.snapIndex {
		return nil, fmt.Errorf("%w: index %d already covered by snapshot at %d",
			ErrCompacted, upto, n.snapIndex)
	}
	t, ok := n.termAt(upto)
	if !ok {
		return nil, fmt.Errorf("%w: no entry at %d", ErrLogInconsistent, upto)
	}
	// 快照记录 upto 位点对应的成员配置（重放 [snapIndex, upto] 内的变更条目）。
	m := n.snapMember.clone()
	for _, e := range n.log {
		if e.Index > upto {
			break
		}
		if e.Type == EntryConfChange {
			if cc, err := decodeConfChange(e.Data); err == nil {
				m.apply(cc)
			}
		}
	}
	snap := NewSnapshot(upto, t, data, m.sortedVoters(), m.sortedLearners())
	if err := n.storage.SaveSnapshot(snap); err != nil {
		return nil, err
	}
	kept := []LogEntry{}
	for _, e := range n.log {
		if e.Index > upto {
			kept = append(kept, e)
		}
	}
	n.log = kept
	n.snapIndex = upto
	n.snapTerm = t
	n.snapMember = m
	n.persist()
	return snap, nil
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
