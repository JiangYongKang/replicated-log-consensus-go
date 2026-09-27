package raft

import "fmt"

// verifySnapshotFrame 校验在线传输的整帧快照，并核对元数据自洽性：
// 帧内位点必须与 RPC 元数据一致，防止版本错配或张冠李戴的快照被接受。
func verifySnapshotFrame(frameData []byte, meta SnapshotMeta) (uint64, uint64, []byte, error) {
	body, err := readFileCheckedBytes(frameData, magicSnapshot)
	if err != nil {
		return 0, 0, nil, err
	}
	r := byteReader{body}
	idx, err := r.u64()
	if err != nil {
		return 0, 0, nil, raftErrorf(KindSnapshotMismatch, "snapshot body: %v", err)
	}
	term, err := r.u64()
	if err != nil {
		return 0, 0, nil, raftErrorf(KindSnapshotMismatch, "snapshot body: %v", err)
	}
	if idx != meta.LastIndex || term != meta.LastTerm {
		return 0, 0, nil, raftErrorf(KindSnapshotMismatch,
			"snapshot meta mismatch: frame=(%d,%d) wire=(%d,%d)",
			idx, term, meta.LastIndex, meta.LastTerm)
	}
	// 跳过 peers 段。
	np, err := r.u16()
	if err != nil {
		return 0, 0, nil, raftErrorf(KindSnapshotMismatch, "snapshot body: %v", err)
	}
	for i := uint16(0); i < np; i++ {
		if _, err := r.bytes(); err != nil {
			return 0, 0, nil, raftErrorf(KindSnapshotMismatch, "snapshot body: %v", err)
		}
	}
	fsmData, err := r.bytes()
	if err != nil {
		return 0, 0, nil, raftErrorf(KindSnapshotMismatch, "snapshot body: %v", err)
	}
	if len(meta.Peers) > 0 && len(meta.Peers) != int(np) {
		return 0, 0, nil, raftErrorf(KindSnapshotMismatch,
			"snapshot config mismatch: frame peers=%d wire peers=%d", np, len(meta.Peers))
	}
	return idx, term, fsmData, nil
}

// ---- 领导者：复制 ----

// sendHeartbeats 对所有跟随者发送一轮追加（无新条目时即心跳）。
// nextIndex 落后于本地快照位点的跟随者改用 InstallSnapshot 追赶。
func (n *Node) sendHeartbeats() {
	for _, p := range n.peers {
		if p == n.cfg.NodeID {
			continue
		}
		n.replicateTo(p)
	}
}

func (n *Node) replicateTo(peer string) {
	ni := n.nextIndex[peer]
	if ni <= n.log.snapshotIndex {
		n.sendSnapshotTo(peer)
		return
	}
	prevIndex := ni - 1
	prevTerm, ok := n.log.termAt(prevIndex)
	if !ok {
		// 理论上不应发生：快照前的位点已通过 InstallSnapshot 路径处理。
		n.sendSnapshotTo(peer)
		return
	}
	entries := n.log.slice(ni, n.log.lastIndex())
	req := Message{
		Type:         MsgAppendRequest,
		Term:         n.currentTerm,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIdx,
	}
	n.send(peer, req)
}

func (n *Node) sendSnapshotTo(peer string) {
	meta, _, ok, err := n.storage.LoadSnapshot()
	if err != nil || !ok {
		n.logger.Printf("[%s] term=%d cannot snapshot-sync peer %s: snapshot unavailable (%v)",
			n.cfg.NodeID, n.currentTerm, peer, err)
		return
	}
	// 在线传输发送磁盘上的原始整帧（含 magic/版本/CRC），接收方据此
	// 校验完整性；切勿发送解码后的 FSM 负载（那样无法验损）。
	frameData, ferr := readRawSnapshot(n.storage)
	if ferr != nil {
		n.logger.Printf("[%s] term=%d read snapshot frame for %s failed: %v",
			n.cfg.NodeID, n.currentTerm, peer, ferr)
		return
	}
	req := Message{
		Type: MsgSnapshotRequest, Term: n.currentTerm,
		SnapshotMeta: meta, SnapshotData: frameData,
	}
	n.send(peer, req)
}

// ---- 跟随者：AppendEntries ----

func (n *Node) storedLastIndex() uint64 { return n.storage.StoredLastIndex() }

type rawSnapshotReader interface {
	ReadRawSnapshot() ([]byte, error)
}

func readRawSnapshot(s Storage) ([]byte, error) {
	if r, ok := s.(rawSnapshotReader); ok {
		return r.ReadRawSnapshot()
	}
	// 兜底：重新解码再编码一帧（自定义 Storage 实现路径）。
	meta, data, exists, err := s.LoadSnapshot()
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, raftErrorf(KindSnapshotMismatch, "snapshot not available")
	}
	return marshalSnapshot(meta, data), nil
}

func (n *Node) handleAppendRequest(msg Message) {
	resp := Message{Type: MsgAppendResponse, Term: n.currentTerm}
	switch {
	case msg.Term < n.currentTerm:
		resp.Reject = true
		resp.RejectKind = KindStaleTerm
		resp.RejectReason = fmt.Sprintf("leader term %d < %d", msg.Term, n.currentTerm)
		n.send(msg.From, resp)
		return
	case msg.Term >= n.currentTerm:
		// 同任期（或更高任期，已在分发处降级）出现合法领导者：候选者与
		// PreVote 发起者都必须立即让位，保证同一任期不存在两个领导者。
		if n.role == RoleCandidate || n.preVoting {
			n.becomeFollower(n.currentTerm, msg.From)
		} else if n.role == RoleFollower {
			n.leaderID = msg.From
		}
		n.role = RoleFollower
		n.preVoting = false
		n.leaderID = msg.From
		n.resetElectionTimer()
	}

	if msg.PrevLogIndex > 0 && msg.PrevLogIndex > n.log.lastIndex() {
		// 缺日志：明确拒绝并告知对端回退。
		resp.Reject = true
		resp.RejectKind = KindLogInconsistent
		resp.HintIndex = n.log.lastIndex() + 1
		resp.RejectReason = fmt.Sprintf("missing entries up to %d, local last %d",
			msg.PrevLogIndex, n.log.lastIndex())
		n.send(msg.From, resp)
		return
	}
	if msg.PrevLogIndex > 0 {
		got, ok := n.log.termAt(msg.PrevLogIndex)
		if !ok || got != msg.PrevLogTerm {
			// 同一索引任期不一致：日志冲突，拒绝并给出快速回退提示。
			hintTerm := uint64(0)
			if ok {
				hintTerm = got
			}
			resp.Reject = true
			resp.RejectKind = KindLogInconsistent
			resp.HintIndex = n.conflictHintIndex(msg.PrevLogIndex, got, ok)
			resp.HintTerm = hintTerm
			resp.RejectReason = fmt.Sprintf(
				"term mismatch at prevIndex %d: want %d got %d (ok=%v)",
				msg.PrevLogIndex, msg.PrevLogTerm, got, ok)
			n.logger.Printf("[%s] term=%d append conflict at index %d: wantTerm=%d gotTerm=%d -> hint=%d",
				n.cfg.NodeID, n.currentTerm, msg.PrevLogIndex, msg.PrevLogTerm, got, resp.HintIndex)
			n.send(msg.From, resp)
			return
		}
	}

	// 通过一致性检查：写入条目（冲突时 appendEntries 内部截断）并持久化。
	var conflictAt uint64
	if len(msg.Entries) > 0 {
		for i, e := range msg.Entries {
			want := msg.PrevLogIndex + 1 + uint64(i)
			if existing, exists := n.log.entryAt(want); exists && existing.Term != e.Term {
				conflictAt = want
				break
			}
		}
		if err := n.log.appendEntries(msg.PrevLogIndex, msg.Entries...); err != nil {
			resp.Reject = true
			resp.RejectKind = KindLogInconsistent
			resp.RejectReason = err.Error()
			n.send(msg.From, resp)
			return
		}
		if conflictAt > 0 {
			// 发生截断：从冲突点起整段重写持久日志，磁盘与内存严格一致。
			rewritten := n.log.slice(conflictAt, n.log.lastIndex())
			if err := n.storage.ReplaceEntries(conflictAt, rewritten); err != nil {
				n.logger.Printf("[%s] term=%d persist conflict rewrite failed: %v",
					n.cfg.NodeID, n.currentTerm, err)
			}
			n.logger.Printf("[%s] term=%d resolved log conflict: truncated from %d, new last=%d",
				n.cfg.NodeID, n.currentTerm, conflictAt, n.log.lastIndex())
		} else if len(msg.Entries) > 0 {
			// 仅追加真正新增的尾部；重复/乱序 RPC 中已存在的条目幂等跳过。
			stored := n.storedLastIndex()
			start := stored + 1
			if start < n.log.firstIndex() {
				start = n.log.firstIndex()
			}
			if start <= n.log.lastIndex() {
				added := n.log.slice(start, n.log.lastIndex())
				if len(added) > 0 {
					if err := n.storage.AppendEntries(added); err != nil {
						// 追加被拒（说明磁盘状态与内存不一致）：整段重写兜底。
						if err2 := n.storage.ReplaceEntries(n.log.firstIndex(),
							n.log.slice(n.log.firstIndex(), n.log.lastIndex())); err2 != nil {
							n.logger.Printf("[%s] term=%d persist append failed: %v / rewrite: %v",
								n.cfg.NodeID, n.currentTerm, err, err2)
						}
					}
				}
			}
		}
	}

	if msg.LeaderCommit > n.commitIdx {
		target := msg.LeaderCommit
		if target > n.log.lastIndex() {
			target = n.log.lastIndex()
		}
		n.commitIdx = target
	}
	resp.Success = true
	resp.MatchIndex = n.log.lastIndex()
	n.send(msg.From, resp)
	n.applyCommitted()
}

// conflictHintIndex 返回快速回退位点：冲突任期第一个条目的前一索引。
func (n *Node) conflictHintIndex(conflictIndex, conflictTerm uint64, termKnown bool) uint64 {
	if !termKnown || conflictTerm == 0 {
		if conflictIndex > 0 {
			return conflictIndex - 1
		}
		return 0
	}
	for i := conflictIndex; i > n.log.snapshotIndex; i-- {
		if t, ok := n.log.termAt(i); ok && t != conflictTerm {
			return i
		}
	}
	return n.log.snapshotIndex
}

// ---- 领导者：AppendEntries 响应 ----

func (n *Node) handleAppendResponse(msg Message) {
	if msg.Term < n.currentTerm {
		return
	}
	if msg.Reject {
		switch msg.RejectKind {
		case KindStaleTerm:
			// 对端在更高任期：handleMessage 已负责降级。
			return
		case KindLogInconsistent:
			hint := msg.HintIndex
			if hint == 0 {
				hint = n.nextIndex[msg.From] - 1
			}
			if hint < 1 {
				hint = 1
			}
			old := n.nextIndex[msg.From]
			n.nextIndex[msg.From] = hint
			n.logger.Printf("[%s] term=%d peer=%s log backoff nextIndex %d -> %d (%s)",
				n.cfg.NodeID, n.currentTerm, msg.From, old, hint, msg.RejectReason)
			n.replicateTo(msg.From) // 立即用更短前缀重试，可安全重复
		default:
			n.logger.Printf("[%s] term=%d peer=%s rejected append: %s",
				n.cfg.NodeID, n.currentTerm, msg.From, msg.RejectReason)
		}
		return
	}
	match := msg.MatchIndex
	if match > n.matchIndex[msg.From] {
		n.matchIndex[msg.From] = match
	}
	if ni := n.nextIndex[msg.From]; match+1 > ni {
		n.nextIndex[msg.From] = match + 1
	}
	n.advanceCommitIndex()
}

// ---- 提交与应用 ----

// advanceCommitIndex 仅当 N 属于当前任期且存在于多数派 matchIndex 时，
// 才把 commitIndex 推进到 N（论文 8.3.2 节，防止旧任期条目被直接提交）。
func (n *Node) advanceCommitIndex() {
	if n.role != RoleLeader {
		return
	}
	peers := n.peers
	for candidate := n.log.lastIndex(); candidate > n.commitIdx; candidate-- {
		term, ok := n.log.termAt(candidate)
		if !ok || term != n.currentTerm {
			continue
			// 不检查更早的索引也安全：索引单调，旧任期条目会随本任期
			// no-op 被间接提交。
		}
		count := 0
		for _, p := range peers {
			if n.matchIndex[p] >= candidate {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIdx = candidate
			n.logger.Printf("[%s] term=%d commit advanced to %d (quorum=%d/%d)",
				n.cfg.NodeID, n.currentTerm, candidate, count, n.quorum())
			n.applyCommitted()
			return
		}
	}
}

// applyCommitted 将 (appliedIdx, commitIdx] 区间的条目顺序应用到状态机，
// 每批结束后原子保存 FSM 检查点（appliedIndex/term/数据）。
// 重启时从该检查点恢复，因此同一条目不会被重复应用。
func (n *Node) applyCommitted() {
	if n.commitIdx <= n.appliedIdx {
		return
	}
	lastTerm := uint64(0)
	for idx := n.appliedIdx + 1; idx <= n.commitIdx; idx++ {
		e, ok := n.log.entryAt(idx)
		if !ok {
			n.logger.Printf("[%s] term=%d apply gap at %d (snapshot=%d last=%d)",
				n.cfg.NodeID, n.currentTerm, idx, n.log.snapshotIndex, n.log.lastIndex())
			break
		}
		if err := n.fsm.Apply(e); err != nil {
			// 确定性命令损坏：记录明确错误并停止推进，等待人工/快照处理，
			// 绝不跳过造成状态偏移。
			n.logger.Printf("[%s] term=%d apply entry %d failed: %v",
				n.cfg.NodeID, n.currentTerm, idx, err)
			break
		}
		n.appliedIdx = idx
		lastTerm = e.Term
	}
	if n.appliedIdx > 0 {
		snap, err := n.fsm.Snapshot()
		if err != nil {
			n.logger.Printf("[%s] term=%d fsm snapshot failed: %v",
				n.cfg.NodeID, n.currentTerm, err)
			return
		}
		if err := n.fsmStore.Save(n.appliedIdx, lastTerm, snap); err != nil {
			n.logger.Printf("[%s] term=%d checkpoint save failed: %v",
				n.cfg.NodeID, n.currentTerm, err)
		}
	}
	n.logState("applied")
	n.notifyWaiters(n.appliedIdx, nil)
}

// ---- 快照安装（落后/新节点追赶）----

func (n *Node) handleSnapshotRequest(msg Message) {
	resp := Message{Type: MsgSnapshotResponse, Term: n.currentTerm}
	if msg.Term < n.currentTerm {
		resp.Reject = true
		resp.RejectKind = KindStaleTerm
		resp.RejectReason = fmt.Sprintf("snapshot term %d < %d", msg.Term, n.currentTerm)
		n.send(msg.From, resp)
		return
	}
	n.becomeFollower(msg.Term, msg.From)
	meta := msg.SnapshotMeta
	// 1) 在线载荷先做 CRC/版本校验：损坏或版本不符必须明确拒绝、可安全重试，
	//    绝不静默接受为本地状态。
	snapIndex, snapTerm, fsmData, err := verifySnapshotFrame(msg.SnapshotData, meta)
	if err != nil {
		resp.Reject = true
		resp.RejectKind = KindSnapshotMismatch
		resp.RejectReason = err.Error()
		n.logger.Printf("[%s] term=%d REJECTED snapshot: %v", n.cfg.NodeID, n.currentTerm, err)
		n.send(msg.From, resp)
		return
	}
	if snapIndex <= n.log.snapshotIndex || snapIndex <= n.appliedIdx {
		// 过期快照：幂等忽略但报告成功，避免无意义回退。
		resp.Success = true
		resp.MatchIndex = n.appliedIdx
		n.send(msg.From, resp)
		return
	}
	// 2) 先落盘快照文件（含校验），崩溃后也可从磁盘继续恢复。
	if err := n.storage.SaveSnapshot(SnapshotMeta{
		LastIndex: snapIndex, LastTerm: snapTerm, Peers: append([]string(nil), meta.Peers...),
	}, fsmData); err != nil {
		resp.Reject = true
		resp.RejectKind = KindSnapshotMismatch
		resp.RejectReason = err.Error()
		n.send(msg.From, resp)
		return
	}
	// 3) 重置内存日志到快照位点，回收旧条目。
	n.log = newRaftLog(snapIndex, snapTerm)
	if err := n.storage.ReplaceEntries(snapIndex+1, nil); err != nil {
		n.logger.Printf("[%s] term=%d snapshot log reset failed: %v",
			n.cfg.NodeID, n.currentTerm, err)
	}
	// 4) 用快照数据重建 FSM，并固化检查点，保证不重复应用。
	if err := n.fsm.Restore(fsmData); err != nil {
		resp.Reject = true
		resp.RejectKind = KindSnapshotMismatch
		resp.RejectReason = fmt.Sprintf("fsm restore: %v", err)
		n.send(msg.From, resp)
		return
	}
	n.appliedIdx = snapIndex
	if err := n.fsmStore.Save(snapIndex, snapTerm, fsmData); err != nil {
		n.logger.Printf("[%s] term=%d checkpoint after snapshot failed: %v",
			n.cfg.NodeID, n.currentTerm, err)
	}
	if snapIndex > n.commitIdx {
		n.commitIdx = snapIndex
	}
	resp.Success = true
	resp.MatchIndex = snapIndex
	n.send(msg.From, resp)
	n.logState("snapshot-installed")
}

func (n *Node) handleSnapshotResponse(msg Message) {
	if msg.Reject {
		if msg.RejectKind == KindSnapshotMismatch {
			n.logger.Printf("[%s] term=%d snapshot to %s rejected: %s (will retry)",
				n.cfg.NodeID, n.currentTerm, msg.From, msg.RejectReason)
			// 保持 nextIndex 不变；下一轮心跳会重传，传输中断可安全重试。
		}
		return
	}
	match := msg.MatchIndex
	if match > n.matchIndex[msg.From] {
		n.matchIndex[msg.From] = match
	}
	n.nextIndex[msg.From] = match + 1
	n.logger.Printf("[%s] term=%d peer=%s caught up via snapshot at index %d",
		n.cfg.NodeID, n.currentTerm, msg.From, match)
	n.advanceCommitIndex()
	n.replicateTo(msg.From) // 继续发送快照之后的剩余日志
}

// ---- 本地日志压缩 ----

// compactLogTo 对当前已应用位点制作快照并回收旧日志。
// index 必须 <= commitIdx 且对应日志任期可核验。
func (n *Node) compactLogTo(index uint64) error {
	if index == 0 {
		return raftErrorf(KindRejected, "nothing to snapshot")
	}
	if index > n.commitIdx {
		return raftErrorf(KindRejected, "snapshot index %d exceeds commit %d", index, n.commitIdx)
	}
	if index <= n.log.snapshotIndex {
		return raftErrorf(KindRejected, "snapshot index %d already compacted (<= %d)",
			index, n.log.snapshotIndex)
	}
	term, ok := n.log.termAt(index)
	if !ok {
		return raftErrorf(KindLogInconsistent, "no term for index %d", index)
	}
	fsmData, err := n.fsm.Snapshot()
	if err != nil {
		return err
	}
	// 先持久化快照，再回收日志；若中途崩溃，残留的旧日志只是占空间，安全。
	meta := SnapshotMeta{LastIndex: index, LastTerm: term, Peers: append([]string(nil), n.peers...)}
	if err := n.storage.SaveSnapshot(meta, fsmData); err != nil {
		return err
	}
	if err := n.log.compact(index, term); err != nil {
		return err
	}
	if err := n.storage.CompactBefore(index + 1); err != nil {
		return err
	}
	n.logger.Printf("[%s] term=%d compacted log through index %d (term %d), retained %d entries",
		n.cfg.NodeID, n.currentTerm, index, term, len(n.log.entries))
	return nil
}
