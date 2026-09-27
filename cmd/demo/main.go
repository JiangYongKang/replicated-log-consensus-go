// Command demo 在单机上启动一个 3 节点 Raft 集群，演示：
//   - 领导者选举与唯一领导
//   - 客户端写入并复制
//   - 杀掉领导者后的故障转移与重连恢复
//   - 快照压缩与落后节点追赶
//
// 不依赖任何外部服务，仅使用内存网络与临时文件目录。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/highcumontoa/replicated-log-consensus-go/raft"
)

type stdLogger struct{}

func (stdLogger) Printf(format string, args ...any) {
	log.Printf(format, args...)
}

func main() {
	dir := flag.String("dir", "", "data directory (default: temp dir)")
	flag.Parse()

	if *dir == "" {
		var err error
		*dir, err = os.MkdirTemp("", "raft-demo-")
		if err != nil {
			log.Fatal(err)
		}
	}
	log.Printf("data dir: %s", *dir)

	net := raft.NewNetwork(256)
	peers := []string{"n1", "n2", "n3"}
	nodes := map[string]*raft.Node{}
	fsms := map[string]*raft.KVStateMachine{}

	start := func(id string) {
		d := filepath.Join(*dir, id)
		st, err := raft.NewFileStorage(d)
		if err != nil {
			log.Fatal(err)
		}
		fsm := raft.NewKVStateMachine()
		tr := net.AddNode(id)
		cfg := raft.Config{
			NodeID: id, Peers: peers, DataDir: d,
			ElectionTimeout:   400 * time.Millisecond,
			HeartbeatInterval: 80 * time.Millisecond,
			RandomElection:    true,
		}
		n, err := raft.NewNode(cfg, st, fsm, tr, stdLogger{})
		if err != nil {
			log.Fatal(err)
		}
		n.Start(context.Background())
		nodes[id] = n
		fsms[id] = fsm
	}
	for _, id := range peers {
		start(id)
	}

	waitLeader := func() string {
		for i := 0; i < 100; i++ {
			for id, n := range nodes {
				if n.State().Role == raft.RoleLeader {
					return id
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		log.Fatal("no leader")
		return ""
	}

	ldr := waitLeader()
	log.Printf("=== elected leader: %s (term=%d) ===", ldr, nodes[ldr].State().Term)

	ctx := context.Background()
	for i := 1; i <= 10; i++ {
		key := fmt.Sprintf("key%d", i)
		if err := nodes[ldr].Propose(ctx, raft.KVSetCommand(key, "v"+fmt.Sprint(i))); err != nil {
			log.Fatalf("propose %s: %v", key, err)
		}
	}
	time.Sleep(300 * time.Millisecond)
	for _, id := range peers {
		v, _ := view(fsms[id], nodes[id], "key10")
		log.Printf("after writes: node=%s role=%s commit=%d applied=%d key10=%s",
			id, nodes[id].State().Role, nodes[id].State().CommitIndex,
			nodes[id].State().AppliedIndex, v)
	}

	// 快照压缩。
	if err := nodes[ldr].ForceSnapshot(ctx); err != nil {
		log.Printf("snapshot: %v", err)
	} else {
		log.Printf("=== leader snapshot taken at applied=%d ===", nodes[ldr].State().AppliedIndex)
	}

	// 杀掉领导者，验证故障转移。
	log.Printf("=== killing leader %s ===", ldr)
	nodes[ldr].Stop()
	dead := ldr
	delete(nodes, ldr)
	time.Sleep(100 * time.Millisecond)
	newLeader := waitLeader()
	log.Printf("=== new leader after failover: %s (term=%d) ===",
		newLeader, nodes[newLeader].State().Term)

	if err := nodes[newLeader].Propose(ctx, raft.KVSetCommand("after-failover", "ok")); err != nil {
		log.Fatalf("post-failover propose: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	// 重启死亡节点，验证追赶。
	log.Printf("=== restarting %s ===", dead)
	start(dead)
	time.Sleep(500 * time.Millisecond)
	val, ok := view(fsms[dead], nodes[dead], "after-failover")
	log.Printf("restarted %s: role=%s commit=%d applied=%d after-failover=%q(ok=%v)",
		dead, nodes[dead].State().Role, nodes[dead].State().CommitIndex,
		nodes[dead].State().AppliedIndex, val, ok)

	for _, n := range nodes {
		n.Stop()
	}
	log.Printf("=== demo complete; data retained under %s ===", *dir)
}

func view(fsm *raft.KVStateMachine, n *raft.Node, key string) (string, bool) {
	res, err := n.FSMView(context.Background(), func(sm raft.StateMachine) any {
		v, ok := sm.(*raft.KVStateMachine).Get(key)
		return [2]any{v, ok}
	})
	if err != nil {
		return "", false
	}
	pair := res.([2]any)
	return pair[0].(string), pair[1].(bool)
}
