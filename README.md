# replicated-log-consensus-go

单机多节点、可注入故障的复制日志与领导者选举组件（Raft 风格共识），纯 Go 标准库实现，不依赖外部服务。支持运行中动态调整集群成员（扩缩容），且调整过程不破坏已建立的一致性。

## 设计概览

```
raft/
  types.go       消息、日志条目与成员变更类型
  errors.go      分类错误定义
  node.go        共识核心：选举、预投票、复制、提交、应用、快照
  membership.go  成员集模型与动态成员变更（学习者、提升、移除）
  storage.go     持久化接口与内存实现
  snapshot.go    快照格式、编解码与校验（携带成员配置）
  transport.go   可控网络：丢包/乱序/重复/分区注入
```

- **确定性驱动**：节点由 `Tick()` 推进计时，消息经 `Network` 排队后由测试显式投递，故障注入完全本地可控、可复现。
- **持久化优先**：任期、投票、日志在每次变更后同步写入 `Storage`；崩溃恢复只依据持久化状态。成员配置不单独持久化——它由快照（携带配置）+ 日志中的变更条目重放推导，任何节点重启后都收敛到同一份配置。

## 一致性语义

- **选举安全**：同一任期至多一个领导者。投票需满足 (a) 该任期未投过票（`votedFor` 持久化，重启不丢失），(b) 候选人日志至少与本地一样新，(c) 候选人在本地当前成员集合内。
- **提交安全**：领导者只对**当前任期**的条目按多数派计数提交（Raft 论文 5.4.2）；当选时追加 no-op 条目以间接提交前任期条目。被多数派确认的条目在任意节点重启后不丢失。
- **应用安全**：`commitIdx` 单调前进，`lastApply` 单调跟随；每个位点在同一节点生命周期内**恰好应用一次**。快照覆盖的位点重启后绝不重复应用。未提交条目对客户端不可见。
- **冲突解决**：`AppendEntries` 的 `prevLog` 校验失败即拒绝并附带回退提示（`RejectHint`）；冲突条目及其后缀被删除，以领导者日志为准。重复/乱序消息幂等处理，状态机不偏移。
- **领导者租约**：领导者在一个选举超时内收不到多数派回应即退位（CheckQuorum），分区中的旧领导者无法继续接受写入。
- **预投票（PreVote）**：选举超时后先在不抬升任期的前提下探测自己是否可能当选，得到多数派预投票同意后才真正抬升任期发起选举。分区或被移除的节点因此无法抬高自己的任期，重连后不会打断集群。
- **投票租约**：刚收到过有效领导者消息的跟随者、以及多数派活跃的领导者，在租约窗口内拒绝一切投票类请求（包括高任期的），且不抬升任期。被移除节点残留的拉票消息无法动摇活跃领导者。

## 成员变更（动态扩缩容）

成员配置本身作为特殊日志条目（`EntryConfChange`）复制与提交，每条变更条目**携带变更后的完整成员集**，任何节点仅凭最后一条变更条目（或快照）即可还原完整配置。

### API

```go
idx, err := leader.AddNode("n4")     // 以学习者身份加入，追平后自动提升为投票成员
idx, err := leader.RemoveNode("n3")  // 移除投票成员或学习者
idx, err := leader.ProposeConfChange(raft.ConfChange{Type: raft.ConfPromote, NodeID: "n4"}) // 手动提升
voters, learners := node.Membership() // 当前成员视图
removed := node.Removed()             // 本节点是否已被移除（转为被动）
```

### 流程与规则

- **加节点**：`AddNode` 先把新节点加为**学习者**（参与复制、不计入多数派、不参与选举）；学习者追平已提交位点后，领导者**自动发起提升**为投票成员。新节点以空配置启动即可，初始配置从复制来的变更条目/快照中学到。
- **减节点**：`RemoveNode` 提交后集群立即按新成员集合工作。领导者会在一个宽限期（2 个选举超时）内继续向被移除节点复制，让它得知结果并**转为完全被动**（不选举、不响应）；宽限期后停止复制。
- **单变更串行**：任意时刻至多一个未提交的变更（`ErrConfChangeInProgress`），每次只增删一个节点——这是单节点变更安全性的前提。变更条目**追加即生效**（新配置立即用于多数派判定），**提交即确定**；若条目被新领导者覆盖截断，节点从快照+剩余日志重放自动回滚到一致视图。
- **领导者移除自己**：变更提交后旧领导者退位转为被动，剩余成员选出新领导者。

### 安全边界（调整期间对外的语义）

- 已确认的写入在任何成员变更路径下**不丢、不重**；同一任期始终至多一个领导者。
- 变更中途领导者崩溃：悬置（未提交）的变更可能生效也可能回滚，取决于谁当选，但**所有节点最终收敛到唯一一份配置**，已提交的变更必然存活。
- 被移除节点即使仍在运行、持有旧状态：它收不到多数派选票（非成员）、抬不了任期（预投票被成员拒绝）、打不断领导者（投票租约），无法影响集群的提交与选主。若它在分区中错过移除通知，会无害地持续预投票；重新上线需先清空状态再以新身份加入。
- 缩容到单节点是合法配置：单节点即多数派，可独立提交；之后可再扩容回来。
- 多数派不可达时，`Propose`/`ProposeConfChange` 明确返回 `ErrNoMajority` 并说明可达节点数，不会含糊挂起。

## 错误分类

| 错误 | 含义 | 典型场景 |
|---|---|---|
| `ErrNotLeader` | 当前节点不是领导者 | 客户端写到跟随者 |
| `ErrNoMajority` | 多数派不足 | 分区中提交/维持领导权失败 |
| `ErrStaleTerm` | 消息任期过期 | 旧领导者的延迟消息 |
| `ErrLogInconsistent` | prevLog 冲突/位点缺失 | 日志分叉、压缩未提交条目 |
| `ErrSnapshotCorrupt` | 快照校验和失败 | 传输损坏（可安全重试） |
| `ErrVersionMismatch` | 快照格式版本不兼容 | 跨版本集群 |
| `ErrCompacted` | 条目已被快照回收 | 重复压缩同一区间 |
| `ErrInvalidConfChange` | 成员变更不合法 | 重复添加、移除不存在的节点 |
| `ErrConfChangeInProgress` | 上一个变更尚未提交 | 并发发起多个成员变更 |

`Propose`/`ProposeConfChange` 直接返回分类错误；消息级拒绝原因记录在 `Node.LastError()`，测试据此按原因断言。

## 持久化与快照格式

**持久化状态**（`PersistentState`）：`CurrentTerm`、`VotedFor`、快照之后的 `Log`。提交位点不持久化，重启后由领导者重新推进；快照边界即已应用边界。成员配置由快照成员集 + 日志变更条目重放推导。

**快照编码**（大端序，v2 起携带成员配置）：

```
version(8) | lastIncludedIndex(8) | lastIncludedTerm(8) | dataLen(8) | data |
voterCount(8) | voter(len(8)+bytes)... | learnerCount(8) | learner(len(8)+bytes)... |
checksum(4)
```

`checksum` 为对 `version..learners` 的 CRC32(IEEE)。解码时先验版本（不符 → `ErrVersionMismatch`）再验校验和（不符 → `ErrSnapshotCorrupt`），任何损坏都被明确拒绝而非静默降级。

## 落后节点追赶

领导者按 `nextIdx` 向跟随者（含学习者）发送 `AppendEntries`；若所需条目已被本地快照回收（`nextIdx <= snapIndex`），改发 `InstallSnapshot`（快照携带成员配置，新节点可经快照直接加入）。跟随者安装快照时：若快照末位点在本地日志中且任期一致则保留其后缀，否则整体替换；`commitIdx`/`lastApply` 前进到快照边界，状态机经 `ApplySnapshotFunc` 整体重建。传输中断后由心跳/重试继续，重复安装幂等。

## 本地复现与验证

```bash
go build ./...        # 编译
go vet ./...          # 静态检查
go test ./... -count=1 -v          # 全部测试（日志含任期/节点/提交位点/成员集）
go test ./... -race -count=1       # 竞态检测
go test ./... -run 'ConfChange|AddNode|RemoveNode|Shrink|SnapshotCarries' -v  # 只看成员变更场景
```

测试场景覆盖（`-v` 日志可见各场景的任期、提交位点与成员集变化）：

- **选举**：`TestElectSingleLeader`、`TestNoLeaderWithoutMajority`、`TestIsolatedLeaderStepsDown`、`TestPartitionHealsAndReelects`、`TestStaleTermRejected`、`TestSimultaneousStartElectsQuickly`（多节点同时启动快速选主 + 心跳抖动不换主）
- **复制与恢复**：`TestReplicateAndCommit`、`TestDuplicateAndReorderedMessages`、`TestDroppedMessagesRecover`、`TestRestartRecovery`、`TestRestartPreservesVote`、`TestConflictingLogsResolved`、`TestLogInconsistencyClassified`、`TestCommittedWriteSurvivesFullRestart`（全量崩溃重启后已确认写入存活）
- **快照**：`TestSnapshotCatchUp`、`TestCorruptSnapshotRejected`、`TestSnapshotVersionMismatch`、`TestCompactUncommittedRejected`、`TestSnapshotSurvivesRestart`、`TestSnapshotCarriesMembership`（新节点经快照加入并还原成员集）
- **成员变更**：`TestAddNodeLearnerThenPromote`（学习者→自动提升→补齐历史）、`TestRemoveNode`、`TestRemoveLeaderItself`、`TestConfChangeRejectedWhilePending`（并发/非法变更拒绝）、`TestConfChangeCrashMidway`（变更中途崩溃→回滚）、`TestConfChangeCrashLeaderReelected`（中途崩溃→旧领导者连任→变更生效）、`TestConfChangeCommittedThenLeaderCrash`（已提交变更耐崩溃）、`TestRemovedNodeCannotDisruptAfterHeal`（被移除节点残留消息无害）、`TestRemovedNodeRestartStaysPassive`（被移除节点重启保持被动）、`TestPartitionDuringAddNode`（扩容途中分区）、`TestShrinkToSingleNodeAndGrowBack`（缩容到单节点再扩回）

故障注入通过 `Network` 完成：`Partition`/`Cut`/`Heal`（分区）、`DropFirst`（丢包）、`DuplicateFirst`（重复）、`ReorderFirstToEnd`（乱序），配合 `crash`/`restart`（同一 `Storage` 重建节点）模拟崩溃恢复，`join`（空配置启动）模拟新节点加入。
