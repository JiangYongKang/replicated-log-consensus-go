package raft

import (
	"fmt"
	"testing"
)

// cluster 测试集群：通过可控网络驱动消息，逐 tick 推进。
type cluster struct {
	t         *testing.T
	net       *Network
	storages  map[NodeID]*MemoryStorage
	nodes     map[NodeID]*Node
	applied   map[NodeID][]string // 各节点状态机已应用的命令（按序）
	snaps     map[NodeID][]byte   // 各节点状态机已安装的快照
	ids       []NodeID
	bootPeers []NodeID // 初始成员（仅用于无任何持久化状态时的引导）
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	c := &cluster{
		t:        t,
		net:      NewNetwork(),
		storages: map[NodeID]*MemoryStorage{},
		nodes:    map[NodeID]*Node{},
		applied:  map[NodeID][]string{},
		snaps:    map[NodeID][]byte{},
	}
	for i := 0; i < n; i++ {
		id := NodeID(fmt.Sprintf("n%d", i+1))
		c.ids = append(c.ids, id)
		c.storages[id] = NewMemoryStorage()
	}
	c.bootPeers = append([]NodeID(nil), c.ids...)
	for _, id := range c.ids {
		c.startNode(id)
	}
	return c
}

// join 以空配置启动一个全新的节点（等待通过成员变更加入集群）。
func (c *cluster) join(id NodeID) {
	c.ids = append(c.ids, id)
	c.storages[id] = NewMemoryStorage()
	c.startNodeWithPeers(id, nil)
}

// startNode 用同一存储（重）建节点，模拟启动或崩溃恢复。
func (c *cluster) startNode(id NodeID) { c.startNodeWithPeers(id, c.bootPeers) }

func (c *cluster) startNodeWithPeers(id NodeID, peers []NodeID) {
	cfg := Config{
		ID:               id,
		Peers:            peers,
		ElectionTimeout:  10 + indexOf(c.ids, id)*5, // 错峰超时，保证选举确定性
		HeartbeatTimeout: 2,
	}
	// 状态机随进程重启而重建：已应用列表清零；
	// 若存储中有快照，则状态机从快照恢复（快照覆盖的条目不会被重复应用）。
	c.applied[id] = nil
	if snap, ok, _ := c.storages[id].LoadSnapshot(); ok {
		c.snaps[id] = append([]byte(nil), snap.Data...)
	}
	node := NewNode(cfg, c.storages[id], c.net, func(e LogEntry) {
		c.applied[id] = append(c.applied[id], string(e.Data))
	})
	node.SetApplySnapshotFunc(func(s *Snapshot) {
		c.snaps[id] = append([]byte(nil), s.Data...)
	})
	c.nodes[id] = node
	c.net.Attach(id, node.Handle)
}

func indexOf(ids []NodeID, id NodeID) int {
	for i, x := range ids {
		if x == id {
			return i
		}
	}
	return -1
}

// crash 模拟节点崩溃：仅从集群摘除（存储保留）。
func (c *cluster) crash(id NodeID) {
	delete(c.nodes, id)
	c.net.Attach(id, func(Message) {}) // 消息被丢弃
}

// restart 用保留的存储重建节点，模拟崩溃恢复。
func (c *cluster) restart(id NodeID) { c.startNode(id) }

// step 所有存活节点推进一个 tick 并投递全部消息。
func (c *cluster) step() {
	for _, id := range c.ids {
		if n, ok := c.nodes[id]; ok {
			n.Tick()
		}
	}
	c.net.DeliverAll()
}

func (c *cluster) steps(k int) {
	for i := 0; i < k; i++ {
		c.step()
	}
}

// leader 返回当前唯一领导者；无或不止一个返回 ""。
func (c *cluster) leader() NodeID {
	found := NodeID("")
	terms := map[uint64]int{}
	for _, id := range c.ids {
		if n, ok := c.nodes[id]; ok {
			r, term := n.State()
			if r == Leader {
				if found != "" {
					return ""
				}
				found = id
			}
			terms[term]++
		}
	}
	return found
}

// waitLeader 推进集群直到选出领导者，最多 maxSteps 步。
func (c *cluster) waitLeader(maxSteps int) NodeID {
	for i := 0; i < maxSteps; i++ {
		if l := c.leader(); l != "" {
			_, term := c.nodes[l].State()
			c.t.Logf("leader elected: id=%s term=%d commit=%d", l, term, c.nodes[l].Committed())
			return l
		}
		c.step()
	}
	c.t.Fatalf("no leader after %d steps", maxSteps)
	return ""
}

// checkSameTermSingleLeader 断言任意任期至多一个领导者。
func (c *cluster) checkSameTermSingleLeader() {
	seen := map[uint64]NodeID{}
	for _, id := range c.ids {
		n, ok := c.nodes[id]
		if !ok {
			continue
		}
		r, term := n.State()
		if r != Leader {
			continue
		}
		if prev, dup := seen[term]; dup {
			c.t.Fatalf("two leaders in term %d: %s and %s", term, prev, id)
		}
		seen[term] = id
	}
}

// checkAppliedConsistent 断言所有节点已应用前缀一致（无偏移、无丢失）。
func (c *cluster) checkAppliedConsistent() {
	var base []string
	var baseID NodeID
	for _, id := range c.ids {
		if _, ok := c.nodes[id]; !ok {
			continue
		}
		if base == nil || len(c.applied[id]) < len(base) {
			base = c.applied[id]
			baseID = id
		}
	}
	for _, id := range c.ids {
		if _, ok := c.nodes[id]; !ok {
			continue
		}
		a := c.applied[id]
		for i := range base {
			if a[i] != base[i] {
				c.t.Fatalf("applied diverge at %d: %s=%q %s=%q", i, baseID, base[i], id, a[i])
			}
		}
	}
	c.t.Logf("applied consistent across nodes, prefix len=%d (node=%s commit=%d)",
		len(base), baseID, c.nodes[baseID].Committed())
}
