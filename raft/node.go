package raft

import (
	"fmt"
	"sync"
)

// Config 节点配置。
type Config struct {
	ID                 NodeID
	Peers              []NodeID // 全部初始投票成员（含自己），作为引导配置
	ElectionTimeout    int      // 选举超时下界（tick 数）
	ElectionTimeoutMax int      // 选举超时上界（含）；>ElectionTimeout 时启用随机抖动
	HeartbeatTimeout   int      // 心跳间隔（tick 数）
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
	snapConf  *Configuration // 快照位点处的配置（重算配置的起点）

	commitIdx uint64
	lastApply uint64

	applyFn     ApplyFunc
	applySnapFn ApplySnapshotFunc

	votesRecv   map[NodeID]bool
	preVotes    map[NodeID]bool // 当前预投票轮收到的同意
	preVoting   bool            // 本节点正在进行 PreVote
	preVoteTerm uint64          // PreVote 使用的候选任期（term+1），不落地、不生效
	electionEl  int             // 距上次收到有效领导者消息的 tick 数
	heartbeatEl int
	tick        uint64

	// 领导者视角
	nextIdx  map[NodeID]uint64
	matchIdx map[NodeID]uint64
	lastAck  map[NodeID]uint64 // 各 peer 最近一次回应时的 tick

	// 成员配置
	bootstrap Configuration // 引导配置（日志/快照中无配置条目时使用）
	conf      Configuration // 当前生效的最新配置（已追加到日志即生效）

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
	// 引导配置来自启动参数；之后以日志中的配置条目为准。
	n.bootstrap = Configuration{Voters: append([]NodeID(nil), cfg.Peers...)}
	n.conf = cloneConfig(n.bootstrap)
	// 崩溃恢复：以持久化状态为唯一依据。
	if snap, ok, err := storage.LoadSnapshot(); err == nil && ok {
		n.snapIndex = snap.LastIncludedIndex
		n.snapTerm = snap.LastIncludedTerm
		if snap.Config != nil {
			c := cloneConfig(*snap.Config)
			n.snapConf = &c
			n.conf = c
		}
	}
	if st, ok, err := storage.LoadState(); err == nil && ok {
		n.term = st.CurrentTerm
		n.votedFor = st.VotedFor
		n.log = cloneLog(st.Log)
	}
	// 以快照配置为底，快照之后的配置条目按序覆盖，得到最新生效配置。
	n.recomputeConf()
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

// confMajority 报告 acks（外加本节点自己）是否满足当前配置的多数派。
func (n *Node) confMajority(acks map[NodeID]bool) bool {
	return n.conf.Quorum(acks, n.id)
}

// activeAcks 返回在一个选举超时内有回应的投票成员集合（不含自己）。
func (n *Node) activeAcks() map[NodeID]bool {
	acks := map[NodeID]bool{}
	for _, p := range n.conf.AllPeers() {
		if p == n.id {
			continue
		}
		if n.tick-n.lastAck[p] <= uint64(n.cfg.ElectionTimeout) {
			acks[p] = true
		}
	}
	return acks
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

// recomputeConf 以快照配置为底，重放日志中的配置条目，得到最新生效配置。
// 日志可能因冲突回退或被快照截断，因此任何这类操作后都应重算。
func (n *Node) recomputeConf() {
	if n.snapConf != nil {
		n.conf = cloneConfig(*n.snapConf)
	} else {
		n.conf = cloneConfig(n.bootstrap)
	}
	for _, e := range n.log {
		if e.Config != nil {
			n.conf = cloneConfig(*e.Config)
		}
	}
}

// latestConfigIndex 返回日志（快照之后）中最近一个配置条目的位点；无则 0。
func (n *Node) latestConfigIndex() uint64 {
	for i := len(n.log) - 1; i >= 0; i-- {
		if n.log[i].Config != nil {
			return n.log[i].Index
		}
	}
	return 0
}

// appendConfigEntry 由领导者追加一条配置条目并立即生效（Raft：配置在追加时生效）。
func (n *Node) appendConfigEntry(c Configuration) uint64 {
	cfg := cloneConfig(c)
	e := LogEntry{Term: n.term, Index: n.lastLogIndex() + 1, Config: &cfg}
	n.log = append(n.log, e)
	n.matchIdx[n.id] = e.Index
	n.conf = cfg
	n.persist()
	// 为新加入的成员初始化复制进度（已存在的不重置）。
	for _, p := range n.conf.AllPeers() {
		if _, ok := n.nextIdx[p]; !ok {
			n.nextIdx[p] = n.snapIndex + 1 // 从快照点之后开始；需要时自动转 InstallSnapshot
			n.matchIdx[p] = 0
			n.lastAck[p] = n.tick
		}
	}
	n.broadcastAppend()
	// 追加配置条目（尤其缩容后领导者是唯一投票成员）时，
	// 立即按新配置重算一次提交，避免该条目要等到下一条响应才提交。
	if n.role == Leader {
		n.advanceCommit()
	}
	// 联合配置（C_old,new）下，若领导者不在 C_new，它无法获得 C_new
	// 多数派来推进提交，应立即退位让 C_new 成员选出新领导。
	if n.role == Leader && cfg.IsJoint() && !cfg.HasVoter(n.id) {
		n.becomeFollower(n.term)
	}
	return e.Index
}

func (n *Node) send(msg Message) {
	msg.From = n.id
	if !msg.PreVote && msg.Type != MsgPreVote && msg.Type != MsgPreVoteResp {
		msg.Term = n.term
	} // PreVote 系列保留调用方填入的候选任期，不暴露真实任期
	n.net.Send(msg)
}

// Tick 驱动选举与心跳计时；测试按固定步进调用。
func (n *Node) Tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.tick++
	if n.role == Leader {
		n.heartbeatEl++
		if n.heartbeatEl >= n.cfg.HeartbeatTimeout {
			n.heartbeatEl = 0
			n.broadcastAppend()
		}
		// CheckQuorum：当前配置多数派在选举超时内无回应则退位，
		// 避免分区中的旧领导者继续接受写入。联合配置下两套多数派都要满足。
		if !n.confMajority(n.activeAcks()) {
			n.becomeFollower(n.term)
			n.lastErr = ErrNoMajority
		}
		return
	}
	n.electionEl++
	// 预投票未获多数派时，按随机化超时重新发起新一轮 PreVote
	// （不抬高任期）；获多数派后由 maybeWinPreVote 转入正式选举。
	if n.electionEl >= n.electionTimeout() {
		n.startPreVote()
	}
}

// electionTimeout 返回本轮随机化的选举超时，避免同配置节点同时启动时
// 持续撞票（同 term 反复分票）。范围 [ElectionTimeout, 2*ElectionTimeout)。
func (n *Node) electionTimeout() int {
	base := n.cfg.ElectionTimeout
	if n.cfg.ElectionTimeoutMax > base {
		span := n.cfg.ElectionTimeoutMax - base + 1
		// 以 (节点ID, 任期) 为种子做确定性抖动：同节点同任期超时稳定，
		// 任期推进后抖动变化，天然打破分票死锁。
		h := uint64(1469598103934665603)
		for _, b := range []byte(string(n.id)) {
			h ^= uint64(b)
			h *= 1099511628211
		}
		h ^= n.term
		h *= 1099511628211
		return base + int(h%uint64(span))
	}
	return base
}

// startPreVote 发起不增加任期的预投票；获得多数派认可后才真正竞选。
// 被移除成员/分区节点的预投票得不到多数派，无法抬高任期，不会干扰在任领导。
func (n *Node) startPreVote() {
	if !n.conf.HasVoter(n.id) {
		n.electionEl = 0
		return
	}
	n.preVoting = true
	n.preVoteTerm = n.term + 1
	n.preVotes = map[NodeID]bool{n.id: true}
	n.electionEl = 0
	req := Message{
		Type:         MsgPreVote,
		PreVote:      true,
		Term:         n.preVoteTerm,
		LastLogIndex: n.lastLogIndex(),
		LastLogTerm:  n.lastLogTerm(),
	}
	for _, p := range n.conf.Voters {
		if p != n.id {
			n.sendTo(p, req)
		}
	}
	if n.conf.IsJoint() {
		for _, p := range n.conf.VotersNew {
			if p != n.id && !containsID(n.conf.Voters, p) {
				n.sendTo(p, req)
			}
		}
	}
	n.maybeWinPreVote()
}

// sendTo 复制一条模板消息发往 p（保留发送方任期字段由 send 统一填写）。
func (n *Node) sendTo(p NodeID, tmpl Message) {
	m := tmpl
	m.To = p
	n.send(m)
}

func (n *Node) maybeWinPreVote() {
	if !n.preVoting || !n.conf.Quorum(n.preVotes, n.id) {
		return
	}
	n.preVoting = false
	// 预投票通过：真正增加任期并发出正式 RequestVote。
	n.startElection()
}

func (n *Node) becomeFollower(term uint64) {
	n.role = Follower
	n.preVoting = false
	if term > n.term {
		n.term = term
		n.votedFor = ""
	}
	n.electionEl = 0
	n.votesRecv = map[NodeID]bool{}
	n.persist()
}

func (n *Node) startElection() {
	// 被移出配置（或仅为学习者）的节点不得再发起选举。
	if !n.conf.HasVoter(n.id) {
		n.electionEl = 0
		return
	}
	n.preVoting = false
	n.role = Candidate
	n.term++
	n.votedFor = n.id
	n.electionEl = 0
	n.votesRecv = map[NodeID]bool{n.id: true}
	n.persist()
	// 只向当前配置中的投票成员拉票（联合配置下两套集合都要争取）。
	for _, p := range n.conf.Voters {
		if p != n.id {
			n.sendVote(p)
		}
	}
	if n.conf.IsJoint() {
		for _, p := range n.conf.VotersNew {
			if p != n.id && !containsID(n.conf.Voters, p) {
				n.sendVote(p)
			}
		}
	}
	n.maybeBecomeLeader()
}

func (n *Node) sendVote(p NodeID) {
	n.send(Message{
		Type:         MsgRequestVote,
		To:           p,
		LastLogIndex: n.lastLogIndex(),
		LastLogTerm:  n.lastLogTerm(),
	})
}

func (n *Node) maybeBecomeLeader() {
	if n.role != Candidate {
		return
	}
	if !n.confMajority(n.votesRecv) {
		return
	}
	n.role = Leader
	n.heartbeatEl = 0
	last := n.lastLogIndex()
	for _, p := range n.conf.AllPeers() {
		n.nextIdx[p] = last + 1
		n.matchIdx[p] = 0
		n.lastAck[p] = n.tick
	}
	n.matchIdx[n.id] = last
	// 追加一条 no-op 条目：只有当前任期的条目才能按计数规则提交，
	// no-op 使之前任期的条目随之间接提交（Raft 论文 5.4.2）。
	noop := LogEntry{Term: n.term, Index: n.lastLogIndex() + 1}
	n.log = append(n.log, noop)
	n.matchIdx[n.id] = noop.Index
	n.persist()
	n.broadcastAppend()
	// 单节点配置（或已凑齐当前配置多数派）时，no-op 当选即可提交，
	// 从而间接提交之前任期遗留、重启后待提交的条目。
	n.advanceCommit()
}

// Propose 向领导者提交一条命令；返回其日志位点与任期。
// 非领导者返回 ErrNotLeader；多数派失联返回 ErrNoMajority。
func (n *Node) Propose(data []byte) (index uint64, term uint64, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return 0, 0, fmt.Errorf("%w: node %s is %s", ErrNotLeader, n.id, n.role)
	}
	// 已被移出配置的领导者不再接受写入。
	if !n.conf.HasVoter(n.id) {
		n.role = Follower
		n.lastErr = ErrNotLeader
		return 0, 0, fmt.Errorf("%w: node %s removed from configuration", ErrNotLeader, n.id)
	}
	if !n.confMajority(n.activeAcks()) {
		n.lastErr = ErrNoMajority
		return 0, 0, fmt.Errorf("%w: only quorum of current config unreachable", ErrNoMajority)
	}
	e := LogEntry{Term: n.term, Index: n.lastLogIndex() + 1, Data: append([]byte(nil), data...)}
	n.log = append(n.log, e)
	n.matchIdx[n.id] = e.Index
	n.persist()
	n.broadcastAppend()
	// 单节点（或本条目恰好已在当前配置多数派）时立即提交，
	// 不必等待下一轮 AppendEntriesResp。
	n.advanceCommit()
	return e.Index, e.Term, nil
}

// Handle 处理一条入站消息。
func (n *Node) Handle(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()
	// PreVote 的 Term 是候选人提议的未来任期，绝不能据此步进本地任期。
	if msg.Type == MsgPreVote || msg.Type == MsgPreVoteResp {
		switch msg.Type {
		case MsgPreVote:
			n.handlePreVote(msg)
		case MsgPreVoteResp:
			n.handlePreVoteResp(msg)
		}
		return
	}
	// RequestVote 必须在任期步进之前做成员守卫：否则一个被移除节点
	// 用更高任期发消息，会先把在任成员的任期抬高、迫使领导退位，
	// 即便最终拒绝投票也已造成干扰。
	if msg.Type == MsgRequestVote && !n.conf.HasVoter(msg.From) {
		n.lastErr = ErrRejectVote
		n.send(Message{Type: MsgRequestVoteResp, To: msg.From, Success: false})
		return
	}
	if msg.Term > n.term {
		n.becomeFollower(msg.Term)
	}
	switch msg.Type {
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

// handlePreVote 回应预投票：不改变本地任期、不记票、不重置选举计时器，
// 仅在"若收到同任期正式投票我会同意"的条件下返回 Success。
func (n *Node) handlePreVote(m Message) {
	grant := false
	// 候选人须在本地配置的投票集合中；被移除成员无法获得预投票认可。
	if n.conf.HasVoter(m.From) {
		logOK := m.LastLogTerm > n.lastLogTerm() ||
			(m.LastLogTerm == n.lastLogTerm() && m.LastLogIndex >= n.lastLogIndex())
		// 候选人提议的未来任期必须高于本地当前任期。
		// 若本地已是领导者（持有同任期/更高任期），则拒绝改选。
		termOK := m.Term > n.term && n.role != Leader
		if logOK && termOK {
			grant = true
		}
	}
	if !grant {
		n.lastErr = ErrRejectVote
	}
	n.send(Message{Type: MsgPreVoteResp, To: m.From, Term: m.Term, PreVote: true, Success: grant})
}

func (n *Node) handlePreVoteResp(m Message) {
	if !n.preVoting || m.Term != n.preVoteTerm {
		return
	}
	if m.Success {
		n.preVotes[m.From] = true
		n.maybeWinPreVote()
	}
}

func (n *Node) handleRequestVote(m Message) {
	grant := false
	// 候选人必须在本节点当前配置的投票集合中；
	// 被移除的旧成员/学习者即使持有更高任期也拿不到票。
	permitted := n.conf.HasVoter(m.From)
	if permitted && m.Term >= n.term && (n.votedFor == "" || n.votedFor == m.From) {
		// 候选人日志至少与本地一样新才投票。
		myT, myI := n.lastLogTerm(), n.lastLogIndex()
		if m.LastLogTerm > myT || (m.LastLogTerm == myT && m.LastLogIndex >= myI) {
			grant = true
			n.votedFor = m.From
			n.electionEl = 0
			n.persist()
		}
	}
	if m.Term < n.term {
		n.lastErr = ErrStaleTerm
	} else if !permitted {
		n.lastErr = ErrRejectVote
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
	// 任期有效：回到跟随者并重置选举计时。
	n.role = Follower
	n.preVoting = false
	n.electionEl = 0

	// 注意：不在此按成员资格拒绝 AppendEntries。跨多次成员变更期间
	// 长期分区、恢复后落后的合法成员，可能不认识在任的新领导；
	// 拒绝它将永远无法补齐新配置。被移除节点的防护由 PreVote 与
	// 任期检查完成（见 handlePreVote/handleRequestVote）。
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
		n.persist()
		// 配置条目一旦追加即生效（即使尚未提交）；日志可能被截断，
		// 统一从快照配置重放，确保回退后配置也正确回退。
		n.recomputeConf()
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
	// 被移除成员的残留响应不参与进度推进（其 matchIdx 不纳入多数派）。
	if !n.conf.HasMember(m.From) {
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
	acks := map[NodeID]bool{}
	for idx := n.commitIdx + 1; idx <= n.lastLogIndex(); idx++ {
		t, ok := n.termAt(idx)
		if !ok || t != n.term {
			continue // 仅提交当前任期的条目（Raft 安全性要求）
		}
		// 用截至当前位点的匹配集合做多数派判定；
		// 联合配置下 C_old 与 C_new 两个多数派都必须满足，学习者不计入。
		for p, mi := range n.matchIdx {
			acks[p] = mi >= idx
		}
		if n.conf.Quorum(acks, n.id) {
			n.commitIdx = idx
		}
	}
	n.applyCommitted()
	n.maybeFinishMembershipChange()
}

// maybeFinishMembershipChange 联合配置（C_old,new）一旦在双方多数派下提交，
// 领导者立即追加一条仅含 C_new 的配置条目，完成成员变更。
// 若领导者自身已不在 C_new，则之后退位为跟随者。
func (n *Node) maybeFinishMembershipChange() {
	if n.role != Leader || !n.conf.IsJoint() {
		return
	}
	jointIdx := n.latestConfigIndex()
	if jointIdx <= n.commitIdx {
		final := Configuration{Voters: cloneConfig(n.conf).VotersNew, Learners: cloneConfig(n.conf).Learners}
		n.appendConfigEntry(final)
	}
}

// applyCommitted 按序应用 [lastApply+1, commitIdx]；每位点恰好一次。
func (n *Node) applyCommitted() {
	for n.lastApply < n.commitIdx {
		idx := n.lastApply + 1
		if idx <= n.snapIndex {
			n.lastApply = idx
			continue
		}
		e := n.log[idx-n.snapIndex-1]
		n.lastApply = idx
		// Data 为 nil 的是 no-op 条目，不下发给状态机。
		if n.applyFn != nil && e.Data != nil {
			n.applyFn(e)
		}
	}
}

func (n *Node) broadcastAppend() {
	for _, p := range n.conf.AllPeers() {
		if p != n.id {
			n.sendAppendTo(p)
		}
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
	// 快照携带的配置成为重算基线，再叠加其后日志中的配置条目。
	if snap.Config != nil {
		c := cloneConfig(*snap.Config)
		n.snapConf = &c
	}
	n.recomputeConf()
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
	// 计算快照位点处生效的配置并随快照落盘，保证追赶者与重启节点
	// 在配置条目被回收后仍能恢复正确的成员集合。
	cfgAt := n.bootstrap
	if n.snapConf != nil {
		cfgAt = *n.snapConf
	}
	for _, e := range n.log {
		if e.Index > upto {
			break
		}
		if e.Config != nil {
			cfgAt = *e.Config
		}
	}
	snap := NewSnapshotWithConfig(upto, t, data, &cfgAt)
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
	snapBase := cloneConfig(cfgAt)
	n.snapConf = &snapBase
	n.recomputeConf()
	n.persist()
	return snap, nil
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
