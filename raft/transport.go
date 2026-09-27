package raft

import "sync"

// Transport 消息发送接口；实现负责把消息投递到目标节点。
type Transport interface {
	Send(msg Message)
}

// Network 测试用的可控网络：消息先入队，由测试显式驱动投递，
// 可注入丢包、乱序、重复与分区。非并发安全假设由 mu 保证。
type Network struct {
	mu       sync.Mutex
	handlers map[NodeID]func(Message)
	queue    []Message
	// cut[a][b] 为 true 表示 a->b 的链路被分区切断。
	cut map[NodeID]map[NodeID]bool
}

func NewNetwork() *Network {
	return &Network{
		handlers: map[NodeID]func(Message){},
		cut:      map[NodeID]map[NodeID]bool{},
	}
}

// Attach 注册节点的消息处理函数。
func (n *Network) Attach(id NodeID, h func(Message)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

// Send 实现 Transport：消息入队，等待显式投递。
// 被分区切断的链路直接丢弃（模拟网络不可达）。
func (n *Network) Send(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cut[msg.From][msg.To] {
		return
	}
	n.queue = append(n.queue, msg)
}

// Partition 切断 id 与所有已知节点之间的双向链路。
func (n *Network) Partition(id NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for other := range n.handlers {
		if other == id {
			continue
		}
		n.setCut(id, other, true)
		n.setCut(other, id, true)
	}
}

// Cut 切断 from->to 的单向链路。
func (n *Network) Cut(from, to NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.setCut(from, to, true)
}

func (n *Network) setCut(a, b NodeID, v bool) {
	if n.cut[a] == nil {
		n.cut[a] = map[NodeID]bool{}
	}
	n.cut[a][b] = v
}

// Heal 恢复所有链路。
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cut = map[NodeID]map[NodeID]bool{}
}

// Pending 返回队列中待投递消息数。
func (n *Network) Pending() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.queue)
}

// DeliverOne 投递队列中的第一条消息；返回 false 表示队列为空。
func (n *Network) DeliverOne() bool {
	n.mu.Lock()
	if len(n.queue) == 0 {
		n.mu.Unlock()
		return false
	}
	msg := n.queue[0]
	n.queue = n.queue[1:]
	h := n.handlers[msg.To]
	n.mu.Unlock()
	if h != nil {
		h(msg)
	}
	return true
}

// DeliverAll 循环投递直到队列为空（含投递过程中新产生的消息）。
func (n *Network) DeliverAll() {
	for n.DeliverOne() {
	}
}

// DropFirst 丢弃队列中第一条消息（丢包注入）。
func (n *Network) DropFirst() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.queue) == 0 {
		return false
	}
	n.queue = n.queue[1:]
	return true
}

// DuplicateFirst 复制队列中第一条消息（重复消息注入）。
func (n *Network) DuplicateFirst() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.queue) == 0 {
		return false
	}
	n.queue = append([]Message{n.queue[0]}, n.queue...)
	return true
}

// ReorderFirstToEnd 把第一条消息移到队尾（乱序注入）。
func (n *Network) ReorderFirstToEnd() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.queue) < 2 {
		return false
	}
	m := n.queue[0]
	copy(n.queue, n.queue[1:])
	n.queue[len(n.queue)-1] = m
	return true
}
