package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// proposalWaiter 等待一个索引被提交并应用。
type proposalWaiter struct {
	index uint64
	term  uint64
	ch    chan error
}

// Node 是一个 Raft 节点。所有协议状态仅在 runLoop 单 goroutine 中访问，
// 外部交互全部通过 channel，避免加锁竞态。
type Node struct {
	cfg       Config
	peers     []string // 运行时集群成员视图（含本节点），可动态扩展
	storage   Storage
	fsmStore  FSMStore
	fsm       StateMachine
	transport Transport
	logger    Logger

	currentTerm uint64 // 持久：当前任期
	votedFor    string // 持久：本任期投票对象

	role       Role
	commitIdx  uint64
	appliedIdx uint64
	leaderID   string
	log        *raftLog

	// 领导者状态
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// 候选者状态
	votesGranted map[string]bool
	preVoting    bool // PreVote 阶段：尚未自增真实任期
	preVotes     map[string]bool

	proposals      chan clientProposal
	snapReq        chan snapRequest
	queryReq       chan queryRequest
	peerReq        chan peerRequest
	fsmQuery       chan fsmQuery
	timer          *time.Timer
	timerKind      timerKind
	lastTimerReset time.Time
	stopCh         chan struct{}
	doneCh         chan struct{}
	stopped        bool
	wg             sync.WaitGroup

	waitersMu sync.Mutex
	waiters   map[uint64][]*proposalWaiter
}

type timerKind int

const (
	timerElection timerKind = iota
	timerHeartbeat
)

type clientProposal struct {
	data []byte
	ch   chan error
}

type snapRequest struct {
	ch chan error
}

type queryRequest struct {
	ch chan StateInfo
}

type peerRequest struct {
	id string
	ch chan error
}

// fsmQuery 在事件循环内读取状态机的一致性快照，外部不得直接并发访问 FSM。
type fsmQuery struct {
	fn func(StateMachine) any
	ch chan any
}

// Logger 允许注入测试日志器。
type Logger interface {
	Printf(format string, args ...any)
}

// discardLogger 在未注入日志器时使用。
type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// NewNode 创建节点并从持久层恢复，但不启动协议循环。
// 恢复顺序：
//  1. currentTerm/votedFor
//  2. 快照与日志（raftLog 视图）
//  3. FSM 检查点（appliedIndex/term/数据），保证重启后不重复应用
//  4. 已提交位点从 FSM 已应用位点恢复（提交信息不单独持久化，
//     因为“已应用”必然“曾被多数派提交”，且重启后会很快追上）
func NewNode(cfg Config, storage Storage, fsm StateMachine, transport Transport, logger Logger) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = discardLogger{}
	}
	n := &Node{
		cfg:          cfg,
		peers:        append([]string(nil), cfg.Peers...),
		storage:      storage,
		fsm:          fsm,
		transport:    transport,
		logger:       logger,
		nextIndex:    make(map[string]uint64),
		matchIndex:   make(map[string]uint64),
		votesGranted: make(map[string]bool),
		proposals:    make(chan clientProposal, 64),
		snapReq:      make(chan snapRequest, 8),
		queryReq:     make(chan queryRequest, 8),
		peerReq:      make(chan peerRequest, 8),
		fsmQuery:     make(chan fsmQuery, 16),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
		waiters:      make(map[uint64][]*proposalWaiter),
		timer:        time.NewTimer(time.Hour),
	}
	if !n.timer.Stop() {
		<-n.timer.C
	}

	st, err := storage.LoadState()
	if err != nil {
		return nil, err
	}
	n.currentTerm, n.votedFor = st.CurrentTerm, st.VotedFor

	// 载入快照（日志压缩回收后的基线）。
	snapIndex, snapTerm := uint64(0), uint64(0)
	if meta, _, ok, err := storage.LoadSnapshot(); err != nil {
		return nil, err
	} else if ok {
		snapIndex, snapTerm = meta.LastIndex, meta.LastTerm
	}
	entries, err := storage.LoadEntries()
	if err != nil {
		return nil, err
	}
	n.log = newRaftLog(snapIndex, snapTerm)
	if len(entries) > 0 {
		// 磁盘上残留的早于快照的条目（理论上 compact 后不应存在）忽略。
		first := uint64(0)
		for _, e := range entries {
			if e.Index > snapIndex {
				first = e.Index
				break
			}
		}
		if first > 0 {
			filtered := make([]Entry, 0, len(entries))
			for _, e := range entries {
				if e.Index > snapIndex {
					filtered = append(filtered, e)
				}
			}
			if err := n.log.appendEntries(first-1, filtered...); err != nil {
				return nil, err
			}
		}
	}

	// 恢复 FSM 检查点。
	fsmPath := cfg.DataDir + "/fsm.bin"
	fstore, err := newFileFSMStore(fsmPath)
	if err != nil {
		return nil, err
	}
	n.fsmStore = fstore
	appliedIndex, appliedTerm, fsmData, ok, err := fstore.Load()
	if err != nil {
		return nil, err
	}
	switch {
	case ok && snapIndex > 0 && appliedIndex < snapIndex:
		// 极端情况：FSM 检查点落后于快照（例如快照后崩溃在 FSM 落盘前）。
		// 用快照数据安全恢复 FSM 到快照位点，绝不静默丢弃。
		meta, snapData, exists, err := storage.LoadSnapshot()
		if err != nil || !exists {
			return nil, raftErrorf(KindSnapshotMismatch,
				"fsm behind snapshot index %d but snapshot unavailable: %v", snapIndex, err)
		}
		if err := fsm.Restore(snapData); err != nil {
			return nil, err
		}
		n.appliedIdx, appliedTerm = meta.LastIndex, meta.LastTerm
		if err := fstore.Save(n.appliedIdx, appliedTerm, mustSnapshot(fsm)); err != nil {
			return nil, err
		}
	case ok:
		if err := fsm.Restore(fsmData); err != nil {
			return nil, err
		}
		n.appliedIdx = appliedIndex
	}
	// 重启后重新应用快照位点之后、但尚未被 FSM 检查点覆盖的已持久化条目。
	for idx := n.appliedIdx + 1; idx <= n.log.lastIndex(); idx++ {
		e, exists := n.log.entryAt(idx)
		if !exists {
			break
		}
		if err := fsm.Apply(e); err != nil {
			return nil, err
		}
		n.appliedIdx = idx
	}
	if n.appliedIdx > 0 || snapIndex > 0 {
		// 将恢复后的 FSM 状态固化，避免下次重复应用。
		if err := fstore.Save(n.appliedIdx, n.entryTermOrZero(n.appliedIdx), mustSnapshot(fsm)); err != nil {
			return nil, err
		}
	}
	n.commitIdx = n.appliedIdx
	n.role = RoleFollower
	n.logState("node-restored")
	return n, nil
}

func mustSnapshot(fsm StateMachine) []byte {
	data, err := fsm.Snapshot()
	if err != nil {
		return nil
	}
	return data
}

func (n *Node) entryTermOrZero(index uint64) uint64 {
	if t, ok := n.log.termAt(index); ok {
		return t
	}
	return 0
}

// Start 启动后台选举与复制循环。
func (n *Node) Start(ctx context.Context) {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.runLoop(ctx)
	}()
}

// Stop 优雅停止。
func (n *Node) Stop() {
	select {
	case <-n.stopCh:
		return // 已关闭
	default:
		close(n.stopCh)
	}
	n.wg.Wait()
}

// Propose 由客户端发起一次写入。
// 返回错误分类：
//   - KindNotLeader：当前不是领导者（调用方可带退避重试）
//   - KindQuorumUnavailable：选举中/多数派不可达，在 ctx 前未获确认
//   - KindStaleTerm：写入期间领导者被替换
func (n *Node) Propose(ctx context.Context, data []byte) error {
	ch := make(chan error, 1)
	req := clientProposal{data: data, ch: ch}
	select {
	case n.proposals <- req:
	case <-n.stopCh:
		return raftErrorf(KindShutdown, "node %s stopped", n.cfg.NodeID)
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-ch:
		return err
	case <-n.stopCh:
		return raftErrorf(KindShutdown, "node %s stopped", n.cfg.NodeID)
	case <-ctx.Done():
		return raftErrorf(KindQuorumUnavailable,
			"proposal not acknowledged by a quorum before deadline: %v", ctx.Err())
	}
}

// ForceSnapshot 对当前已应用位点做快照并压缩日志。
func (n *Node) ForceSnapshot(ctx context.Context) error {
	ch := make(chan error, 1)
	select {
	case n.snapReq <- snapRequest{ch: ch}:
	case <-n.stopCh:
		return raftErrorf(KindShutdown, "node %s stopped", n.cfg.NodeID)
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-ch:
		return err
	case <-n.stopCh:
		return raftErrorf(KindShutdown, "node %s stopped", n.cfg.NodeID)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// State 返回节点当前观测状态（线程安全快照）。
func (n *Node) State() StateInfo {
	ch := make(chan StateInfo, 1)
	select {
	case n.queryReq <- queryRequest{ch: ch}:
	case <-n.stopCh:
		n.wg.Wait()
		return n.snapshotState()
	}
	select {
	case s := <-ch:
		return s
	case <-n.stopCh:
		n.wg.Wait()
		return n.snapshotState()
	}
}

// FSMView 在事件循环内对状态机执行只读查询，返回 fn 的结果。
// 这是访问活动节点 FSM 的唯一线程安全方式。
func (n *Node) FSMView(ctx context.Context, fn func(StateMachine) any) (any, error) {
	ch := make(chan any, 1)
	req := fsmQuery{fn: fn, ch: ch}
	select {
	case n.fsmQuery <- req:
	case <-n.stopCh:
		return nil, raftErrorf(KindShutdown, "node %s stopped", n.cfg.NodeID)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case v := <-ch:
		return v, nil
	case <-n.stopCh:
		return nil, raftErrorf(KindShutdown, "node %s stopped", n.cfg.NodeID)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (n *Node) snapshotState() StateInfo {
	return StateInfo{
		NodeID: n.cfg.NodeID, Role: n.role, Term: n.currentTerm,
		VotedFor: n.votedFor, CommitIndex: n.commitIdx,
		AppliedIndex: n.appliedIdx, LastLogIndex: n.log.lastIndex(),
		LeaderHint: n.leaderID, Peers: append([]string(nil), n.peers...),
	}
}

func (n *Node) quorum() int { return len(n.peers)/2 + 1 }

// addPeerLocked 在事件循环内扩展集群视图并初始化复制位点。
func (n *Node) addPeerLocked(id string) error {
	for _, p := range n.peers {
		if p == id {
			return nil // 幂等
		}
	}
	n.peers = append(n.peers, id)
	if n.role == RoleLeader {
		n.nextIndex[id] = 1
		n.matchIndex[id] = 0
		n.sendHeartbeats()
	}
	n.logState("peer-added:" + id)
	return nil
}

// AddPeer 将新成员加入运行时集群视图（仅内存配置变更；快照与日志复制
// 随后会自动覆盖新节点）。必须在事件循环外安全调用，因此通过 query
// 通道串行化。
func (n *Node) AddPeer(ctx context.Context, id string) error {
	ch := make(chan error, 1)
	req := peerRequest{id: id, ch: ch}
	select {
	case n.peerReq <- req:
	case <-n.stopCh:
		return raftErrorf(KindShutdown, "node %s stopped", n.cfg.NodeID)
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-ch:
		return err
	case <-n.stopCh:
		return raftErrorf(KindShutdown, "node %s stopped", n.cfg.NodeID)
	}
}

// ---- 事件循环 ----

func (n *Node) runLoop(ctx context.Context) {
	defer close(n.doneCh)
	n.resetElectionTimer()
	for {
		select {
		case <-ctx.Done():
			n.shutdown()
			return
		case <-n.stopCh:
			n.shutdown()
			return
		case <-n.timer.C:
			switch n.timerKind {
			case timerElection:
				// 先发起 PreVote：只有多数派愿意与我们对话时，才真正自增任期。
				// 这样被分区的节点不会产生一串高任期，恢复时无法立刻扰乱
				// 在任领导者。
				n.startPreVote()
			case timerHeartbeat:
				n.sendHeartbeats()
				n.resetHeartbeatTimer()
			}
		case msg := <-n.transport.Recv():
			if msg.From != "" {
				n.handleMessage(msg)
			}
		case p := <-n.proposals:
			n.handleProposal(p)
		case r := <-n.snapReq:
			r.ch <- n.compactLogTo(n.appliedIdx)
		case r := <-n.queryReq:
			r.ch <- n.snapshotState()
		case r := <-n.peerReq:
			r.ch <- n.addPeerLocked(r.id)
		case r := <-n.fsmQuery:
			r.ch <- r.fn(n.fsm)
		}
	}
}

func (n *Node) shutdown() {
	if n.stopped {
		return
	}
	n.stopped = true
	n.timer.Stop()
	n.notifyWaiters(^uint64(0), raftErrorf(KindShutdown, "node %s stopped", n.cfg.NodeID))
	if c, ok := n.storage.(interface{ Close() error }); ok {
		_ = c.Close()
	}
	if n.fsmStore != nil {
		_ = n.fsmStore.Close()
	}
}

func (n *Node) resetTimer(d time.Duration, kind timerKind) {
	n.timerKind = kind
	n.lastTimerReset = time.Now()
	n.timer.Reset(d)
}

func (n *Node) resetHeartbeatTimer() {
	n.resetTimer(n.cfg.HeartbeatInterval, timerHeartbeat)
}

func (n *Node) resetElectionTimer() {
	d := n.cfg.ElectionTimeout
	if n.cfg.RandomElection {
		// [1.0, 1.4) 倍抖动，避免选票瓜分并防止风暴。
		extra := time.Duration(rand.Int63n(int64(2 * n.cfg.ElectionTimeout / 5)))
		d += extra
	}
	n.resetTimer(d, timerElection)
}

// ---- 角色切换 ----

func (n *Node) persistVote(term uint64, candidate string) error {
	n.currentTerm = term
	n.votedFor = candidate
	return n.storage.SaveState(PersistentState{CurrentTerm: term, VotedFor: candidate})
}

func (n *Node) becomeFollower(term uint64, leaderID string) {
	wasLeader := n.role == RoleLeader
	if term > n.currentTerm {
		// term 推进必须先持久化（含本任期尚未投票的事实）再响应，
		// 保证崩溃恢复后不会在同一任期投出两张票。
		if err := n.persistVote(term, ""); err != nil {
			n.logger.Printf("[%s] term=%d persist follower state failed: %v", n.cfg.NodeID, term, err)
		}
	}
	n.role = RoleFollower
	if leaderID != "" {
		n.leaderID = leaderID
	}
	if wasLeader {
		// 旧领导者被降级，其未确认提案全部失败，调用方须重试。
		n.failEpochWaiters(raftErrorf(KindStaleTerm,
			"leadership lost at term %d", n.currentTerm))
	}
	n.resetElectionTimer()
	n.logState("become-follower")
}

func (n *Node) becomeCandidate() {
	n.currentTerm++
	// 先投票给自己并 fsync，再发出 RequestVote（论文 Figure 8 要求）。
	if err := n.persistVote(n.currentTerm, n.cfg.NodeID); err != nil {
		n.logger.Printf("[%s] term=%d persist vote failed: %v", n.cfg.NodeID, n.currentTerm, err)
		n.resetElectionTimer()
		return
	}
	n.role = RoleCandidate
	n.preVoting = false
	n.leaderID = ""
	n.votesGranted = map[string]bool{n.cfg.NodeID: true}
	n.resetElectionTimer()
	lastTerm, _ := n.log.termAt(n.log.lastIndex())
	req := Message{
		Type: MsgVoteRequest, From: n.cfg.NodeID, Term: n.currentTerm,
		LastLogIndex: n.log.lastIndex(), LastLogTerm: lastTerm,
	}
	for _, p := range n.peers {
		if p == n.cfg.NodeID {
			continue
		}
		m := req
		m.To = p
		n.transport.Send(m)
	}
	n.logState("become-candidate")
	if len(n.votesGranted) >= n.quorum() {
		// 单节点集群。
		n.becomeLeader()
	}
}

// ---- PreVote（防止分区节点用高任期扰乱集群 / 选票瓜分）----

func (n *Node) startPreVote() {
	nextTerm := n.currentTerm + 1
	n.preVoting = true
	n.preVotes = map[string]bool{n.cfg.NodeID: true}
	lastTerm, _ := n.log.termAt(n.log.lastIndex())
	req := Message{
		Type: MsgPreVoteRequest, From: n.cfg.NodeID, Term: nextTerm,
		LastLogIndex: n.log.lastIndex(), LastLogTerm: lastTerm,
	}
	for _, p := range n.peers {
		if p == n.cfg.NodeID {
			continue
		}
		m := req
		m.To = p
		n.transport.Send(m)
	}
	n.resetElectionTimer()
	n.logger.Printf("[%s] term=%d pre-vote started (proposed term %d)",
		n.cfg.NodeID, n.currentTerm, nextTerm)
}

func (n *Node) handlePreVoteRequest(msg Message) {
	resp := Message{Type: MsgPreVoteResponse, Term: n.currentTerm}
	upToDate := n.log.upToDate(msg.LastLogIndex, msg.LastLogTerm)
	// PreVote 不改变任期；只有提议任期高于本地、且候选日志足够新时才支持。
	shouldGrant := msg.Term > n.currentTerm && upToDate
	resp.Reject = !shouldGrant
	if !shouldGrant {
		resp.RejectKind = KindRejected
		resp.RejectReason = fmt.Sprintf(
			"pre-vote denied (proposed %d vs current %d, upToDate=%v)",
			msg.Term, n.currentTerm, upToDate)
		n.send(msg.From, resp)
		return
	}
	n.send(msg.From, resp)
}

func (n *Node) handlePreVoteResponse(msg Message) {
	if !n.preVoting {
		return
	}
	if msg.Reject {
		n.logger.Printf("[%s] term=%d pre-vote rejected by %s: %s",
			n.cfg.NodeID, n.currentTerm, msg.From, msg.RejectReason)
		return
	}
	n.preVotes[msg.From] = true
	n.logger.Printf("[%s] term=%d pre-vote granted by %s (%d/%d)",
		n.cfg.NodeID, n.currentTerm, msg.From, len(n.preVotes), n.quorum())
	if len(n.preVotes) >= n.quorum() {
		n.preVoting = false
		n.becomeCandidate()
	}
}

func (n *Node) becomeLeader() {
	n.role = RoleLeader
	n.leaderID = n.cfg.NodeID
	last := n.log.lastIndex()
	for _, p := range n.peers {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.matchIndex[n.cfg.NodeID] = last
	// 立即广播一轮（空日志作为心跳）并提交 no-op，确立本任期提交权。
	n.appendNoOp()
	n.sendHeartbeats()
	n.resetHeartbeatTimer()
	n.logState("become-leader")
}

func (n *Node) appendNoOp() {
	idx := n.log.lastIndex() + 1
	e := Entry{Index: idx, Term: n.currentTerm}
	if err := n.log.appendEntries(n.log.lastIndex(), e); err != nil {
		n.logger.Printf("[%s] term=%d noop append failed: %v", n.cfg.NodeID, n.currentTerm, err)
		return
	}
	if err := n.storage.AppendEntries([]Entry{e}); err != nil {
		n.logger.Printf("[%s] term=%d noop persist failed: %v", n.cfg.NodeID, n.currentTerm, err)
	}
	n.matchIndex[n.cfg.NodeID] = idx
}

// ---- 客户端提案（仅领导者，在 loop 内调用）----

func (n *Node) handleProposal(p clientProposal) {
	if n.role != RoleLeader {
		p.ch <- raftErrorf(KindNotLeader,
			"node %s is %s in term %d (leader=%q)", n.cfg.NodeID, n.role, n.currentTerm, n.leaderID)
		return
	}
	idx := n.log.lastIndex() + 1
	e := Entry{Index: idx, Term: n.currentTerm, Data: append([]byte(nil), p.data...)}
	if err := n.log.appendEntries(n.log.lastIndex(), e); err != nil {
		p.ch <- raftErrorf(KindInternal, "local append: %v", err)
		return
	}
	if err := n.storage.AppendEntries([]Entry{e}); err != nil {
		p.ch <- raftErrorf(KindInternal, "persist append: %v", err)
		return
	}
	n.matchIndex[n.cfg.NodeID] = idx
	w := &proposalWaiter{index: idx, term: n.currentTerm, ch: p.ch}
	n.waitersMu.Lock()
	n.waiters[idx] = append(n.waiters[idx], w)
	n.waitersMu.Unlock()
	n.advanceCommitIndex()
	n.sendHeartbeats()
}

func (n *Node) failEpochWaiters(err error) {
	n.waitersMu.Lock()
	var pending []*proposalWaiter
	for idx, ws := range n.waiters {
		for _, w := range ws {
			if w.term >= n.currentTerm || w.index > n.commitIdx {
				pending = append(pending, w)
			}
		}
		delete(n.waiters, idx)
	}
	n.waitersMu.Unlock()
	for _, w := range pending {
		w.ch <- err
	}
}

func (n *Node) notifyWaiters(index uint64, err error) {
	n.waitersMu.Lock()
	var toNotify []*proposalWaiter
	for idx, ws := range n.waiters {
		if idx <= index {
			toNotify = append(toNotify, ws...)
			delete(n.waiters, idx)
		}
	}
	n.waitersMu.Unlock()
	for _, w := range toNotify {
		// 提案的任期与当前任期不同：属于旧任期，返回任期过期而非成功。
		if err == nil && w.term != n.currentTerm && n.role == RoleFollower {
			w.ch <- raftErrorf(KindStaleTerm,
				"entry %d from term %d committed under newer term %d", w.index, w.term, n.currentTerm)
			continue
		}
		w.ch <- err
	}
}

// ---- RPC 分发 ----

func (n *Node) handleMessage(msg Message) {
	// PreVote 是“试探”：既不代表发送方已进入该任期，也不得改变接收方
	// 的 currentTerm，因此不参与下面的高任期降级规则。
	if msg.Type == MsgPreVoteRequest || msg.Type == MsgPreVoteResponse {
		if msg.Type == MsgPreVoteRequest {
			n.handlePreVoteRequest(msg)
		} else if n.preVoting {
			n.handlePreVoteResponse(msg)
		}
		return
	}
	// 所有正式 RPC 规则一：遇到更高任期立即降级为跟随者。
	if msg.Term > n.currentTerm {
		n.becomeFollower(msg.Term, "")
	}
	switch msg.Type {
	case MsgVoteRequest:
		n.handleVoteRequest(msg)
	case MsgVoteResponse:
		if n.role == RoleCandidate {
			n.handleVoteResponse(msg)
		}
	case MsgAppendRequest:
		n.handleAppendRequest(msg)
	case MsgAppendResponse:
		if n.role == RoleLeader {
			n.handleAppendResponse(msg)
		}
	case MsgSnapshotRequest:
		n.handleSnapshotRequest(msg)
	case MsgSnapshotResponse:
		if n.role == RoleLeader {
			n.handleSnapshotResponse(msg)
		}
	}
}

func (n *Node) send(to string, resp Message) {
	resp.From = n.cfg.NodeID
	resp.To = to
	n.transport.Send(resp)
}

func (n *Node) handleVoteRequest(msg Message) {
	resp := Message{Type: MsgVoteResponse, Term: n.currentTerm}
	granted := false
	switch {
	case msg.Term < n.currentTerm:
		resp.Reject = true
		resp.RejectKind = KindStaleTerm
		resp.RejectReason = fmt.Sprintf("candidate term %d < %d", msg.Term, n.currentTerm)
	case n.votedFor != "" && n.votedFor != msg.From:
		resp.Reject = true
		resp.RejectKind = KindRejected
		resp.RejectReason = fmt.Sprintf("already voted for %q in term %d", n.votedFor, n.currentTerm)
	case !n.log.upToDate(msg.LastLogIndex, msg.LastLogTerm):
		// 选举限制：候选者日志至少与本地一样新才给票。
		resp.Reject = true
		resp.RejectKind = KindLogInconsistent
		resp.RejectReason = fmt.Sprintf(
			"candidate log (%d,%d) not up-to-date vs local last (%d,%d)",
			msg.LastLogIndex, msg.LastLogTerm, n.log.lastIndex(), mustTerm(n.log))
	default:
		if err := n.persistVote(msg.Term, msg.From); err != nil {
			resp.Reject = true
			resp.RejectKind = KindInternal
			resp.RejectReason = err.Error()
		} else {
			granted = true
			n.role = RoleFollower
			n.leaderID = ""
			n.resetElectionTimer()
		}
	}
	if granted {
		n.logger.Printf("[%s] term=%d GRANTED vote to %s (commit=%d last=%d)",
			n.cfg.NodeID, n.currentTerm, msg.From, n.commitIdx, n.log.lastIndex())
	} else {
		n.logger.Printf("[%s] term=%d REJECTED vote from %s: %s",
			n.cfg.NodeID, n.currentTerm, msg.From, resp.RejectReason)
	}
	n.send(msg.From, resp)
}

func mustTerm(l *raftLog) uint64 {
	t, _ := l.termAt(l.lastIndex())
	return t
}

func (n *Node) handleVoteResponse(msg Message) {
	if msg.Term < n.currentTerm || msg.Reject {
		if msg.Reject {
			n.logger.Printf("[%s] term=%d vote rejected by %s (%s)",
				n.cfg.NodeID, n.currentTerm, msg.From, msg.RejectReason)
		}
		return
	}
	n.votesGranted[msg.From] = true
	n.logger.Printf("[%s] term=%d vote granted by %s (%d/%d)",
		n.cfg.NodeID, n.currentTerm, msg.From, len(n.votesGranted), n.quorum())
	if n.role == RoleCandidate && len(n.votesGranted) >= n.quorum() {
		n.becomeLeader()
	}
}

// ---- 可观测日志：统一打印任期/节点/提交位点等判定依据 ----

func (n *Node) logState(prefix string) {
	n.logger.Printf("[%s] term=%d role=%s commit=%d applied=%d lastIndex=%d leader=%q %s",
		n.cfg.NodeID, n.currentTerm, n.role, n.commitIdx, n.appliedIdx,
		n.log.lastIndex(), n.leaderID, prefix)
}
