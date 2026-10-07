# replicated-log-consensus-go

单机多节点、可注入故障的复制日志与领导者选举组件（Raft 风格共识），纯 Go 标准库实现，不依赖外部服务。

支持**运行中在线成员变更**：不停机加节点、踢节点，变更过程崩溃/分区/残留消息下仍保证一致性与配置唯一。

## 设计概览

```
raft/
  types.go           消息与日志条目类型（含配置条目、PreVote）
  errors.go          分类错误定义
  node.go            共识核心：选举(PreVote)、复制、提交、应用、快照、成员配置
  membership.go      Configuration：投票集合/学习者、联合多数派判定
  membership_api.go  对外成员变更 API：AddLearner/PromoteMember/RemoveMember
  storage.go         持久化接口与内存实现
  snapshot.go        快照格式(v2,含配置)、编解码与校验
  transport.go       可控网络：丢包/乱序/重复/分区注入
```

- **确定性驱动**：节点由 `Tick()` 推进计时，消息经 `Network` 排队后由测试显式投递，故障注入完全本地可控、可复现。
- **持久化优先**：任期、投票、日志（含成员配置条目）在每次变更后同步写入 `Storage`；崩溃恢复只依据持久化状态。
- **随机化选举超时**：`[ElectionTimeout, ElectionTimeoutMax]` 内以 (节点ID, 任期) 为种子确定性抖动，多个配置完全一致的节点同时启动也能稳定选出唯一领导，不长期分票。

## 一致性语义

- **选举安全（PreVote 预投票）**：候选人先以 `term+1` 发起不落地、不抬高任期的 PreVote，拿到当前配置多数派认可后才真正 `term++` 竞选。被踢节点/分区节点的预投票得不到多数派，无法用高任期打断在任领导。正式 `RequestVote` 在**步进任期之前**先做成员守卫：非当前配置成员的拉票直接拒绝，不会抬高在任成员任期。同一任期至多一个领导者。
- **提交安全**：领导者只对**当前任期**的条目按多数派计数提交（Raft 论文 5.4.2）；当选时追加 no-op 条目以间接提交前任期条目。被多数派确认的条目在任意节点重启后不丢失。多数派够不到时 `Propose` 立即返回 `ErrNoMajority`，不含糊等待。
- **应用安全**：`commitIdx` 单调前进，`lastApply` 单调跟随；每个位点在同一节点生命周期内**恰好应用一次**。快照覆盖的位点重启后绝不重复应用。配置条目与 no-op 不下发给业务状态机。未提交条目对客户端不可见。
- **冲突解决**：`AppendEntries` 的 `prevLog` 校验失败即拒绝并附带回退提示（`RejectHint`）；冲突条目及其后缀被删除，以领导者日志为准，随后配置从快照基线重放重算。重复/乱序消息幂等处理，状态机不偏移。
- **领导者租约（CheckQuorum）**：领导者在一个选举超时内收不到当前配置多数派回应即退位；联合配置下必须 C_old、C_new 两套多数派都满足。分区中的旧领导者无法继续接受写入。

## 运行中成员变更

### 用法

成员配置（`Configuration{Voters, VotersNew, Learners}`）作为**特殊配置条目**走正常日志复制：一旦追加到日志即生效，提交规则与普通条目一致。新节点先用包含原集群的引导配置启动，再分三步加入：

```go
// 1) 以学习者身份加入：只接收 AppendEntries/Snapshot 追赶，不投票、不计入多数派
leader.AddLearner("n4")
// 2) 等待其追平（matchIndex == lastLogIndex），再提升为投票成员
leader.PromoteMember("n4")
//    内部自动走两阶段： C ──(joint entry)──▶ C_old,new ──(final entry)──▶ C_new
// 3) 踢人：投票成员走同样两阶段；学习者用单条配置直接移除
leader.RemoveMember("n2")
```

- **学习者（Learner）先补齐再参与**：新节点作为学习者把落后日志（必要时经快照）追平，`PromoteMember` 在未追平时返回 `ErrMembershipChange`，杜绝"未同步新成员拖垮多数派可用性"。
- **联合共识两阶段**：投票成员的增/删先生成联合配置 `C_old,new`，此期间**任何提交与当选都必须同时在 C_old 和 C_new 两个多数派通过**；联合条目提交后领导者自动追加仅含 C_new 的最终条目。这保证任意时刻集群不会分裂成两拨各自合法、各自能提交的配置。
- **串行化**：上一次投票成员变更未提交完成时，新的变更返回 `ErrMembershipChange`，配置切换严格串行。
- **领导退位规则**：联合配置下领导者若不在 C_new（无法凑齐 C_new 多数派推进提交）立即退位；最终 C_new 中保留的领导负责把最终配置条目提交（缩容到单节点也能立即提交）。
- 可通过 `node.Configuration()` 读取当前生效配置。

### 变更期间的安全边界与对外语义

- **变更做到一半领导崩溃**：联合配置尚未在双方多数派提交时，集群宁可**暂时无领导**，也不会让只持有 C_old 或只持有 C_new 的一拨人当选/提交——这正是防止两个配置分叉的关键。新成员或旧领导恢复后，集群选出唯一领导并把变更收敛到**唯一确定**的 C_new（见 `TestMembershipLeaderCrashMidChange`）。
- **网络分区**：变更期间分区，两侧都不会出现稳定领导；恢复后收敛到同一领导、同一最终配置（`TestMembershipWithPartition`）。
- **被踢节点仍存活/残留消息**：它的 PreVote、正式拉票都会被新配置成员拒绝，任期无法压过在任领导；它收到的旧 `AppendEntries` 响应也不被采纳。即使多个被踢节点彼此连通、凑成"旧配置多数派"，也选不出领导、提交不了写入（`TestRemovedNodeCannotDisrupt`、`TestRemovedQuorumCannotCommit`）。
- **已确认写入不丢不重**：成员增删前后被多数派确认、并已持久化的写入，在崩溃重启后仍在；配置/快照追赶不会让业务条目被重复应用。
- **缩容到极小**：支持缩容到单个投票成员。单节点多数派为 1，可独立选主与提交，重启后经新任期 no-op 重新提交旧条目，写入不丢（`TestShrinkToOneNode`）。
- **不可用即明确失败**：当前配置多数派不可达时 `Propose` 立即返回 `ErrNotLeader` 或 `ErrNoMajority`；恢复后写入重新成功（`TestProposalFailureClassified`）。
- **配置随快照恢复**：快照内嵌其位点处生效的成员配置。落后/重启节点通过 `InstallSnapshot` 一次拿到最新配置，不会回退到旧成员集合（`TestConfigInSnapshot`）。

## 错误分类

| 错误 | 含义 | 典型场景 |
|---|---|---|
| `ErrNotLeader` | 当前节点不是领导者 | 写到跟随者 / 已被移出配置的旧领导 |
| `ErrNoMajority` | 多数派不足 | 分区中提交/维持领导权失败 |
| `ErrStaleTerm` | 消息任期过期 | 旧领导者的延迟消息 |
| `ErrLogInconsistent` | prevLog 冲突/位点缺失 | 日志分叉、压缩未提交条目 |
| `ErrSnapshotCorrupt` | 快照校验和失败 | 传输损坏（可安全重试） |
| `ErrVersionMismatch` | 快照格式版本不兼容 | 跨版本集群 |
| `ErrCompacted` | 条目已被快照回收 | 重复压缩同一区间 |
| `ErrMembershipChange` | 变更未完成即再变更 / 学习者未追平 | 重叠变更、过早提升 |
| `ErrUnknownMember` | 目标不在成员集合 | 移除不存在的节点 |
| `ErrMemberExists` | 目标已在成员集合 | 重复加入 |
| `ErrNotLearner` | 目标不是学习者 | 直接提升一个投票成员 |
| `ErrRejectVote` | 非配置成员的拉票被拒 | 被踢节点残留的竞选消息 |

`Propose`/`AddLearner`/`PromoteMember`/`RemoveMember` 直接返回上述分类错误；消息级拒绝原因记录在 `Node.LastError()`，测试据此按原因断言。

## 持久化与快照格式

**持久化状态**（`PersistentState`）：`CurrentTerm`、`VotedFor`、快照之后的 `Log`（其中配置条目的 `Config` 字段一并深拷贝持久化）。提交位点不持久化，重启后由领导者重新推进；快照边界即已应用边界。

**快照 v2 编码**（大端序）：

```
version(8) | lastIncludedIndex(8) | lastIncludedTerm(8) | dataLen(8) | data
| cfgLen(8) | cfgJSON | checksum(4)
```

`cfgJSON` 为快照位点处生效 `Configuration` 的 JSON 编码（空集群引导时 cfgLen=0）。`checksum` 为对其之前全部字节的 CRC32(IEEE)。解码时先验版本（不符 → `ErrVersionMismatch`）再验校验和（不符 → `ErrSnapshotCorrupt`），任何损坏都被明确拒绝而非静默降级。压缩日志时（`Compact`）自动把位点处配置写入快照。

## 落后节点追赶

领导者按 `nextIdx` 向跟随者与学习者发送 `AppendEntries`；若所需条目已被本地快照回收（`nextIdx <= snapIndex`），改发 `InstallSnapshot`（含最新配置）。跟随者安装快照时：若快照末位点在本地日志中且任期一致则保留其后缀，否则整体替换；配置以快照为基线、叠加其后配置条目重算；`commitIdx`/`lastApply` 前进到快照边界，状态机经 `ApplySnapshotFunc` 整体重建。传输中断后由心跳/重试继续，重复安装幂等。

## 本地复现与验证

```bash
go build ./...                          # 编译
go vet ./...                            # 静态检查
go test ./... -count=1 -v               # 全部测试（-v 日志含场景名/任期/节点/提交位点/最终配置）
go test ./... -race -count=1            # 竞态检测
go test ./... -count=30                 # 多轮重复，排查抖动/时序偶发
go test ./raft/ -run TestShrinkToOneNode -v   # 只跑某个场景
```

测试场景覆盖（`-v` 下每个用例会打印所处阶段与结果）：

- **选举**：`TestElectSingleLeader`、`TestNoLeaderWithoutMajority`、`TestIsolatedLeaderStepsDown`、`TestPartitionHealsAndReelects`、`TestStaleTermRejected`、`TestProposeOnFollower`、`TestIdenticalConfigsElectStably`
- **复制与恢复**：`TestReplicateAndCommit`、`TestDuplicateAndReorderedMessages`、`TestDroppedMessagesRecover`、`TestRestartRecovery`、`TestRestartPreservesVote`、`TestConflictingLogsResolved`、`TestLogInconsistencyClassified`、`TestProposalFailureClassified`
- **快照**：`TestSnapshotCatchUp`、`TestCorruptSnapshotRejected`、`TestSnapshotVersionMismatch`、`TestCompactUncommittedRejected`、`TestSnapshotSurvivesRestart`
- **成员变更（本轮新增）**：
  - 在线增删：`TestAddNodeOnline`、`TestRemoveVoterOnline`
  - 变更中途崩溃重启：`TestMembershipLeaderCrashMidChange`
  - 网络分区：`TestMembershipWithPartition`
  - 被踢节点残留消息 / 旧多数派复活：`TestRemovedNodeCannotDisrupt`、`TestRemovedQuorumCannotCommit`
  - 缩容到极小：`TestShrinkToOneNode`
  - 配置随快照恢复：`TestConfigInSnapshot`
  - 变更消息重复/乱序：`TestDuplicatedConfigMessages`
  - API 校验与串行化：`TestMembershipAPIValidation`、`TestConcurrentChangesRejected`、`TestPromoteBeforeCaughtUp`

故障注入通过 `Network` 完成：`Partition`/`Cut`/`Heal`/`HealPair`（分区）、`DropFirst`（丢包）、`DuplicateFirst`（重复）、`ReorderFirstToEnd`（乱序）、`PeekFirst`（窥探队首），配合 `crash`/`restart`（同一 `Storage` 重建节点）模拟崩溃恢复。
