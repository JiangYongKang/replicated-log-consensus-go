package raft

import (
	"math/rand"
	"sync"
	"time"
)

// Transport 负责节点间 RPC 投递。
type Transport interface {
	Send(msg Message)
	Recv() <-chan Message
	NodeID() string
}

// EdgePolicy 描述一条有向链路 (from -> to) 的故障注入策略。
type EdgePolicy struct {
	DropRate        float64 // 丢包概率 [0,1]，模拟不可达/分区
	DuplicateRate   float64 // 以该概率额外投递一次副本（模拟重复消息）
	Reorder         bool    // 随机短延迟，制造乱序
	CorruptSnapshot bool    // 对快照 RPC 的载荷注入字节损坏
}

// memTransport 是节点持有的网络句柄。
type memTransport struct {
	id  string
	net *Network
	ch  chan Message
}

func (t *memTransport) Send(msg Message)     { t.net.send(msg) }
func (t *memTransport) Recv() <-chan Message { return t.ch }
func (t *memTransport) NodeID() string       { return t.id }

// Network 是单机可控的内存网络：消息经策略过滤后异步投递到对端缓冲。
// 所有故障都在进程内注入，不依赖外部服务。
type Network struct {
	mu     sync.Mutex
	chans  map[string]*memTransport
	policy map[string]EdgePolicy
	rng    *rand.Rand
	buffer int
}

func NewNetwork(buffer int) *Network {
	if buffer <= 0 {
		buffer = 256
	}
	return &Network{
		chans:  make(map[string]*memTransport),
		policy: make(map[string]EdgePolicy),
		rng:    rand.New(rand.NewSource(1)),
		buffer: buffer,
	}
}

// Seed 重置随机源，便于确定性测试。
func (n *Network) Seed(seed int64) {
	n.mu.Lock()
	n.rng = rand.New(rand.NewSource(seed))
	n.mu.Unlock()
}

func (n *Network) AddNode(id string) Transport {
	n.mu.Lock()
	defer n.mu.Unlock()
	if t, ok := n.chans[id]; ok {
		// 节点重启：复用同一投递通道，避免重启期间消息进错通道。
		return t
	}
	t := &memTransport{id: id, net: n, ch: make(chan Message, n.buffer)}
	n.chans[id] = t
	return t
}

func edgeKey(from, to string) string { return from + "->" + to }

func (n *Network) SetPolicy(from, to string, p EdgePolicy) {
	n.mu.Lock()
	n.policy[edgeKey(from, to)] = p
	n.mu.Unlock()
}

// Partition 将节点与集群其余部分双向隔离（isolated=true），
// 或恢复通信（isolated=false）。
func (n *Network) Partition(id string, isolated bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for other := range n.chans {
		if other == id {
			continue
		}
		key1, key2 := edgeKey(id, other), edgeKey(other, id)
		if isolated {
			n.policy[key1] = EdgePolicy{DropRate: 1}
			n.policy[key2] = EdgePolicy{DropRate: 1}
		} else {
			delete(n.policy, key1)
			delete(n.policy, key2)
		}
	}
}

func (n *Network) ResetPolicy() {
	n.mu.Lock()
	n.policy = make(map[string]EdgePolicy)
	n.mu.Unlock()
}

func (n *Network) send(msg Message) {
	n.mu.Lock()
	dst, ok := n.chans[msg.To]
	p := n.policy[edgeKey(msg.From, msg.To)]
	r := n.rng.Float64()
	n.mu.Unlock()
	if !ok {
		return // 未知接收方：静默丢弃（模拟无路由）
	}
	if p.DropRate > 0 && r < p.DropRate {
		return // 注入丢包/分区
	}
	out := msg
	if p.CorruptSnapshot && msg.Type == MsgSnapshotRequest && len(msg.SnapshotData) > 0 {
		// 深拷贝后再翻转字节，避免污染发送方持有的共享底层数组。
		corrupted := make([]byte, len(msg.SnapshotData))
		copy(corrupted, msg.SnapshotData)
		corrupted[0] ^= 0xFF
		out.SnapshotData = corrupted
	}
	delay := time.Duration(0)
	if p.Reorder {
		n.mu.Lock()
		delay = time.Duration(1+n.rng.Int63n(12)) * time.Millisecond
		n.mu.Unlock()
	}
	n.deliver(dst.ch, out, delay)
	if p.DuplicateRate > 0 {
		n.mu.Lock()
		again := n.rng.Float64()
		n.mu.Unlock()
		if again < p.DuplicateRate {
			n.deliver(dst.ch, out, delay+3*time.Millisecond)
		}
	}
}

// deliver 将消息放入对端缓冲；缓冲满时用短时 goroutine 兜底，
// 慢节点不会让发送方 goroutine 整体卡死。
func (n *Network) deliver(ch chan Message, msg Message, delay time.Duration) {
	if delay <= 0 {
		select {
		case ch <- msg:
		default:
			go func() { ch <- msg }()
		}
		return
	}
	time.AfterFunc(delay, func() {
		select {
		case ch <- msg:
		default:
			go func() { ch <- msg }()
		}
	})
}
