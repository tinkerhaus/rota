package integration

import (
	"net"
	"testing"
	"time"

	"github.com/tinkerhaus/rota/internal/node"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a := l.Addr().String()
	_ = l.Close()
	return a
}

func leaderOf(nodes map[string]*node.Node, timeout time.Duration) (string, *node.Node) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, n := range nodes {
			if n.IsLeader() {
				return id, n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "", nil
}

// A 3-node TCP cluster replicates with quorum; killing the leader elects a new
// one that still holds every committed-but-unacked message (no loss).
func TestClusterFailover(t *testing.T) {
	a1, a2, a3 := freeAddr(t), freeAddr(t), freeAddr(t)
	peers := []node.Peer{{ID: "n1", Addr: a1}, {ID: "n2", Addr: a2}, {ID: "n3", Addr: a3}}

	mk := func(id, bind string, boot bool, init []node.Peer) *node.Node {
		n, err := node.Open(node.Config{
			DataDir: t.TempDir(), NodeID: id, RaftBind: bind,
			Bootstrap: boot, InitialPeers: init, VisibilityMs: 60_000,
		})
		if err != nil {
			t.Fatalf("open %s: %v", id, err)
		}
		return n
	}

	nodes := map[string]*node.Node{
		"n1": mk("n1", a1, true, peers),
		"n2": mk("n2", a2, false, nil),
		"n3": mk("n3", a3, false, nil),
	}
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()

	if err := nodes["n1"].WaitClusterLeader(20 * time.Second); err != nil {
		t.Fatal(err)
	}
	leaderID, leader := leaderOf(nodes, 15*time.Second)
	if leader == nil {
		t.Fatal("no initial leader")
	}

	const lane = "c"
	const total = 50
	for i := 0; i < total; i++ {
		if _, err := leader.Publish(node.PublishReq{Lane: lane, GroupID: "g", Payload: []byte("x")}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	// Consume + ack 10 before the failover.
	for acked := 0; acked < 10; acked++ {
		lr, ok, err := leader.LeaseOne(lane, "c")
		if err != nil {
			t.Fatalf("lease: %v", err)
		}
		if !ok {
			t.Fatal("no work before failover")
		}
		if err := leader.Ack(lr.LeaseID); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}

	// Kill the leader.
	leader.Close()
	delete(nodes, leaderID)

	// A survivor must take over.
	_, newLeader := leaderOf(nodes, 25*time.Second)
	if newLeader == nil {
		t.Fatal("no new leader after failover")
	}
	if err := newLeader.Barrier(10 * time.Second); err != nil {
		t.Fatalf("barrier: %v", err)
	}

	// Drain the survivors: every un-acked message must still be there.
	got := 0
	for empty := 0; empty < 5; {
		lr, ok, err := newLeader.LeaseOne(lane, "c")
		if err != nil {
			t.Fatalf("lease after failover: %v", err)
		}
		if !ok {
			empty++
			time.Sleep(100 * time.Millisecond)
			continue
		}
		empty = 0
		_ = newLeader.Ack(lr.LeaseID)
		got++
	}
	if got != total-10 {
		t.Fatalf("after failover drained %d messages, want %d (no loss)", got, total-10)
	}
}
