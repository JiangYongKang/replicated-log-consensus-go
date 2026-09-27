# replicated-log-consensus-go

一套在**单机多节点**运行、具备故障容错能力的复制日志与领导者选举组件（Raft
模型的教学级、零外部依赖实现）。它在节点崩溃、网络分区、消息乱序/重复/丢失、
日志冲突与快照损坏等情况下，仍能给出**可解释且可分类**的结论。

- 纯 Go 实现，仅依赖标准库；节点间通信使用进程内可控的内存网络。
- 单事件循环（single goroutine event loop）拥有全部协议状态，外部交互走
  channel，天然避免锁竞态（`go test -race` 验证）。
- 持久层使用「魔数 + 版本 + 载荷 + CRC32 + 原子 rename + fsync」格式。
- 故障全部通过本地策略注入（丢包/重复/乱序/分区/快照损坏），不依赖外部服务。

---

## 1. 目录结构

```
raft/
  errors.go        错误分类（ErrorKind）与 RaftError
  types.go         Entry / Message / Config / StateInfo 等核心类型
  log.go           内存日志视图 + Raft 冲突检测/截断
  storage.go       原子文件持久化：state / entries / snapshot / FSM 检查点
  statemachine.go  StateMachine 接口与确定性 KV 状态机
  transport.go     内存网络与故障注入（丢包/重复/乱序/损坏/分区）
  server.go        节点生命周期、事件循环、PreVote 选举、客户端提案
  replicate.go     日志复制、冲突回退、提交、应用、快照安装/追赶/压缩
  *_test.go        覆盖选举/分区/重启/乱序重复/日志冲突/快照等场景
cmd/demo/main.go   本地一键演示：选主 → 写入 → 杀领导者 → 恢复 → 追赶
```

---

## 2. 一致性语义

### 2.1 选举安全（Election Safety）

- 一个任期内至多一个领导者。投票前先把 `(currentTerm, votedFor)` **fsync**，
  崩溃恢复后不会在同一任期投出第二张票。
- 候选者必须通过**选举限制**：其日志 `(lastTerm, lastIndex)` 至少与投票者
  一样新才可能获票，因此被选领导者必然包含所有已提交条目。
- 实现了 **PreVote 预投票**：被分区的节点在确认多数派愿意对话前，不会自增
  真实任期，从而不会用一连串「高任期」在恢复时扰乱在任领导者。
- 候选者在同任期收到合法领导者的心跳时立即退回跟随者。

### 2.2 日志复制与提交（Leader Append / Commit）

- 所有写入经领导者追加；跟随者在 `PrevLogIndex/PrevLogTerm` 一致性检查通过
  后才写入。同索引任期不同即发生**显式冲突截断**，再用领导者日志覆盖。
- 提交推进遵循论文 §8.3.2：只有当某个索引的条目属于**当前任期**且被多数派
  复制时，`commitIndex` 才前进；更早任期的条目随当前任期的 no-op 被间接提交。
- 领导者当选即写入一条空 no-op，快速确立本任期的提交能力。
- **未被多数派确认的条目对客户端不可见**；旧领导者分区期间的写入必然以
  `quorum_unavailable` 失败，恢复后其分叉日志会被明确截断。

### 2.3 应用恰好一次（Apply-once across restart）

- 状态机应用位点 `(appliedIndex, appliedTerm, fsmData)` 以 **FSM 检查点**
  原子落盘（`fsm.bin`）。节点重启后先恢复检查点，只重新应用其后的条目，
  因此同一条目**不会丢失，也不会重复应用**。
- 重复或乱序的 AppendEntries 是幂等的：已存在且任期相同的前缀直接保留，
  不会重复追加、重复应用，状态机不发生偏移。

### 2.4 快照与追赶（Snapshot / Catch-up）

- 可对**已提交**位点制作快照：先原子落盘快照，再回收旧日志（顺序保证崩溃
  安全）。
- 落后或新加入的节点，当 `nextIndex` 早于领导者快照位点时，走
  `InstallSnapshot`：先 CRC/版本/元数据校验，再落盘、重置日志、重建 FSM、
  固化检查点，之后继续复制快照之后的剩余日志。
- 快照**损坏或版本不匹配一律被明确拒绝**（`snapshot_mismatch`），不会静默
  接受为本地状态；领导者心跳会持续重传，**可安全重试**。

---

## 3. 错误分类

所有对外错误都是 `*raft.RaftError`，通过 `raft.IsKind(err, kind)` 或
`raft.AsRaftError(err)` 按原因判定，而不是解析错误文本。

| ErrorKind              | 含义与典型触发 |
|------------------------|----------------|
| `quorum_unavailable`   | 多数派不可达：分区/选举期间写入在超时前未获多数派确认 |
| `stale_term`           | 请求来自过期任期（旧领导者被降级、过期投票/追加） |
| `log_inconsistent`     | 追加位置任期/索引不匹配，或压缩位点无法核验 |
| `snapshot_mismatch`    | 快照 CRC 失败、魔数/版本不符、元数据自相矛盾 |
| `not_leader`           | 向跟随者/候选者发起写入（调用方应带退避重试） |
| `shutdown`             | 节点已关闭 |
| `rejected`             | 对端明确拒绝（已在本任期投过票、快照过期等） |
| `internal`             | 不应发生的不变量破坏 |

网络层拒绝消息（投票/追加/快照）在 `Message.RejectKind` 与
`Message.RejectReason` 中携带同样的分类，便于按原因处理与测试断言。

---

## 4. 持久化与文件格式

数据目录（默认每个节点一个子目录）包含：

| 文件         | 内容 | 何时写入 |
|--------------|------|----------|
| `state.bin`  | `currentTerm`、`votedFor` | 任期推进/投票时（先 fsync 再响应） |
| `entries.bin`| 快照位点之后的日志条目 | 追加 / 冲突重写 / 压缩回收 |
| `snapshot.bin`| 快照元数据（lastIndex/lastTerm/peers）+ FSM 数据 | `ForceSnapshot` / 安装快照 |
| `fsm.bin`    | `appliedIndex/appliedTerm` + FSM 数据检查点 | 每批应用后 |

所有文件共用统一二进制帧：

```
+---------+---------+----------------+------------+
| magic 4 | version |      body      | CRC32 (LE) |
+---------+---------+----------------+------------+
```

- `magic`：`RFST`(state) / `RFEN`(entries) / `RFSN`(snapshot) / `RFCP`(fsm)
- 写入流程：写临时文件 → `fsync` → 原子 `rename` → `fsync` 目录。
  崩溃后要么看到完整旧文件，要么看到完整新文件，绝不会读到半写。
- 读取时校验魔数、版本与 CRC32；任一不符返回 `snapshot_mismatch`。
- **在线传输的快照就是 `snapshot.bin` 的原始整帧**，接收方据此验损；测试中
  翻转首字节会使魔数/CRC 必然失败。

整数均为小端；字符串/字节段以 `uint32` 长度前缀编码；条目编码为
`index(u64) term(u64) data(bytes)`。

---

## 5. 本地复现与验证

需要 Go（在 go1.26 上验证；`go.mod` 声明 1.25）。无需任何外部服务。

```bash
# 构建
go build ./...

# 全部单测（故障全部在进程内注入）
go test ./... -v

# 竞态检测
go test ./... -race

# 多轮稳定性
go test ./raft/ -count=10
```

单测日志会打印判定依据，例如：

```
[n3] term=2 GRANTED vote to n1 (commit=9 last=13)
[n1] term=2 append conflict at index 6: wantTerm=1 gotTerm=3 -> hint=3
[n1] term=2 commit advanced to 13 (quorum=3/3)
[n2] term=2 REJECTED snapshot: raft: snapshot_mismatch: frame checksum mismatch
```

每行包含**节点标识 `[nX]`、`term`、`role`、`commit`、`applied`、
`lastIndex`、`leader`** 等关键字段。

### 测试场景

| 测试 | 覆盖 |
|------|------|
| `TestLeaderElection` | 冷启动选主、同任期唯一领导者 |
| `TestNetworkPartition` | 隔离领导者、多数派换任推进、旧领导者写入被拒、恢复后降级收敛 |
| `TestProposeAndVisibility` | 正常复制、跟随者写入返回 `not_leader` |
| `TestConcurrentWrites` | 200 条并发写全序一致、无丢失/重复应用 |
| `TestRestartPersistence` | 单节点重启与全集群冷启动后已提交条目不丢、不重复应用 |
| `TestDuplicateAndReorderMessages` | 50% 重复 + 乱序下仍一致收敛 |
| `TestMessageLossThenRecovery` | 60% 丢包期间无同任期双领导者，恢复后收敛 |
| `TestLogConflictResolution` | 分区分叉日志被显式截断、未提交分叉不可见 |
| `TestSnapshotCompactionAndCatchUp` | 压缩后落后节点经快照+日志完整追赶 |
| `TestSnapshotCatchUpFreshNode` | 全新空节点经快照获得全部已提交状态 |
| `TestCorruptSnapshotRejected` | 损坏快照被拒、状态不被污染、清除故障后自动重传成功 |
| `TestSnapshotFileVersionAndCRCCheck` | 磁盘快照 CRC/魔数损坏返回 `snapshot_mismatch` |

### 演示程序

```bash
go run ./cmd/demo
# 或保留数据目录以便检查 state.bin / entries.bin / snapshot.bin / fsm.bin
go run ./cmd/demo -dir ./demo-data
```

演示会：选出领导者 → 写入 10 个键 → 做快照 → 杀掉领导者触发故障转移 →
写入新键 → 重启死亡节点并完成追赶。

---

## 6. 设计取舍与边界

- 成员变更以「运行时内存视图扩展」（`AddPeer`）支持新节点快照追赶的演示，
  未实现论文中的联合共识（Joint Consensus）配置变更条目；生产环境应补充。
- 持久层为简单可靠的「整文件原子重写」，适合教学与中小规模日志；超大日志应
  改为分段 + 增量刷盘。
- 网络为内存实现，延迟/丢包模型用于可重复的故障注入，不模拟真实 TCP 语义。
- 线性一致读通过 `Node.FSMView` 在事件循环内读取状态机快照实现，避免读到
  旧领导者的过期数据（需结合领导者确认；演示与测试均在已确认提交后读取）。
