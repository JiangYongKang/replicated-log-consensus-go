# replicated-log-consensus-go

单机多节点、可注入故障的复制日志与领导者选举组件（Raft 风格共识），纯 Go 标准库实现，不依赖外部服务。

## 设计概览

```
raft/
  types.go      消息与日志条目类型
  errors.go     分类错误定义
  node.go       共识核心：选举、复制、提交、应用、快照
  storage.go    持久化接口与内存实现
  snapshot.go   快照格式、编解码与校验
  transport.go  可控网络：丢包/乱序/重复/分区注入
```

- **确定性驱动**：节点由 `Tick()` 推进计时，消息经 `Network` 排队后由测试显式投递，故障注入完全本地可控、可复现。
- **持久化优先**：任期、投票、日志在每次变更后同步写入 `Storage`；崩溃恢复只依据持久化状态。

## 一致性语义

- **选举安全**：同一任期至多一个领导者。投票需满足 (a) 该任期未投过票（`votedFor` 持久化，重启不丢失），(b) 候选人日志至少与本地一样新。
- **提交安全**：领导者只对**当前任期**的条目按多数派计数提交（Raft 论文 5.4.2）；当选时追加 no-op 条目以间接提交前任期条目。被多数派确认的条目在任意节点重启后不丢失。
- **应用安全**：`commitIdx` 单调前进，`lastApply` 单调跟随；每个位点在同一节点生命周期内**恰好应用一次**。快照覆盖的位点重启后绝不重复应用。未提交条目对客户端不可见。
- **冲突解决**：`AppendEntries` 的 `prevLog` 校验失败即拒绝并附带回退提示（`RejectHint`）；冲突条目及其后缀被删除，以领导者日志为准。重复/乱序消息幂等处理，状态机不偏移。
- **领导者租约**：领导者在一个选举超时内收不到多数派回应即退位（CheckQuorum），分区中的旧领导者无法继续接受写入。

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

`Propose` 直接返回 `ErrNotLeader` / `ErrNoMajority`；消息级拒绝原因记录在 `Node.LastError()`，测试据此按原因断言。

## 持久化与快照格式

**持久化状态**（`PersistentState`）：`CurrentTerm`、`VotedFor`、快照之后的 `Log`。提交位点不持久化，重启后由领导者重新推进；快照边界即已应用边界。

**快照编码**（大端序）：

```
version(8) | lastIncludedIndex(8) | lastIncludedTerm(8) | dataLen(8) | data | checksum(4)
```

`checksum` 为对 `version..data` 的 CRC32(IEEE)。解码时先验版本（不符 → `ErrVersionMismatch`）再验校验和（不符 → `ErrSnapshotCorrupt`），任何损坏都被明确拒绝而非静默降级。

## 落后节点追赶

领导者按 `nextIdx` 向跟随者发送 `AppendEntries`；若所需条目已被本地快照回收（`nextIdx <= snapIndex`），改发 `InstallSnapshot`。跟随者安装快照时：若快照末位点在本地日志中且任期一致则保留其后缀，否则整体替换；`commitIdx`/`lastApply` 前进到快照边界，状态机经 `ApplySnapshotFunc` 整体重建。传输中断后由心跳/重试继续，重复安装幂等。

## 本地复现与验证

```bash
go build ./...        # 编译
go vet ./...          # 静态检查
go test ./... -count=1 -v          # 全部测试（日志含任期/节点/提交位点）
go test ./... -race -count=1       # 竞态检测
```

测试场景覆盖：

- **选举**：`TestElectSingleLeader`、`TestNoLeaderWithoutMajority`、`TestIsolatedLeaderStepsDown`、`TestPartitionHealsAndReelects`、`TestStaleTermRejected`
- **复制与恢复**：`TestReplicateAndCommit`、`TestDuplicateAndReorderedMessages`、`TestDroppedMessagesRecover`、`TestRestartRecovery`、`TestRestartPreservesVote`、`TestConflictingLogsResolved`、`TestLogInconsistencyClassified`
- **快照**：`TestSnapshotCatchUp`、`TestCorruptSnapshotRejected`、`TestSnapshotVersionMismatch`、`TestCompactUncommittedRejected`、`TestSnapshotSurvivesRestart`

故障注入通过 `Network` 完成：`Partition`/`Cut`/`Heal`（分区）、`DropFirst`（丢包）、`DuplicateFirst`（重复）、`ReorderFirstToEnd`（乱序），配合 `crash`/`restart`（同一 `Storage` 重建节点）模拟崩溃恢复。
