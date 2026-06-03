package integration

import (
	"testing"
	"time"

	"github.com/tinkerhaus/rota/internal/node"
)

// A lane rate limit throttles dequeue: with burst=2 only ~2 messages are leasable
// in a tight loop, even though more are queued.
func TestLaneRateLimit(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "rl"
	pubN(t, n, lane, "g", 10, 1)
	if err := n.SetLaneRateLimit(lane, 10, 2); err != nil {
		t.Fatalf("set rate limit: %v", err)
	}
	got := 0
	for i := 0; i < 10; i++ {
		if lr, ok := leaseOne(t, n, lane); ok {
			_ = n.Ack(lr.LeaseID)
			got++
		}
	}
	if got < 1 || got > 3 {
		t.Fatalf("rate-limited burst leased %d, want ~2 (1..3)", got)
	}
	// Tokens refill over time, so more become leasable after a wait.
	time.Sleep(600 * time.Millisecond)
	more := 0
	for i := 0; i < 10; i++ {
		if lr, ok := leaseOne(t, n, lane); ok {
			_ = n.Ack(lr.LeaseID)
			more++
		}
	}
	if more < 1 {
		t.Fatal("rate limiter never refilled tokens")
	}
}

// PauseLane stops leasing; ResumeLane restores it.
func TestPauseResumeLane(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "pl"
	pubN(t, n, lane, "g", 5, 1)
	n.PauseLane(lane, 0)
	if _, ok := leaseOne(t, n, lane); ok {
		t.Fatal("a paused lane must not lease")
	}
	n.ResumeLane(lane)
	got := 0
	for {
		lr, ok := leaseOne(t, n, lane)
		if !ok {
			break
		}
		_ = n.Ack(lr.LeaseID)
		if got++; got > 10 {
			break
		}
	}
	if got != 5 {
		t.Fatalf("after resume leased %d, want 5", got)
	}
}

// A fully-drained, idle group is reaped (its metadata row is removed).
func TestIdleReap(t *testing.T) {
	n, err := node.Open(node.Config{DataDir: t.TempDir(), NodeID: "reap", VisibilityMs: 60_000, IdleReapMs: 300})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	if err := n.WaitLeader(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	const lane = "rp"
	if _, err := n.Publish(node.PublishReq{Lane: lane, GroupID: "g", Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	lr, ok := leaseOne(t, n, lane)
	if !ok {
		t.Fatal("expected a lease")
	}
	_ = n.Ack(lr.LeaseID) // group now fully drained
	if !n.GroupConfig(lane, "g").Exists {
		t.Fatal("group metadata should still exist immediately after draining")
	}
	// The reap loop ticks every ~2s and reaps groups idle past IdleReapMs.
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if !n.GroupConfig(lane, "g").Exists {
			return // reaped
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("drained idle group was never reaped")
}

// TeardownGroup drops a group across every lane it appears in.
func TestTeardownGroup(t *testing.T) {
	n := openNode(t, 60_000)
	pubN(t, n, "L1", "shared", 5, 1)
	pubN(t, n, "L2", "shared", 5, 1)
	lanes, affected, err := n.TeardownGroup("shared")
	if err != nil {
		t.Fatalf("teardown: %v", err)
	}
	if affected != 10 || len(lanes) != 2 {
		t.Fatalf("teardown affected %d msgs across %v, want 10 across 2 lanes", affected, lanes)
	}
	if _, ok := leaseOne(t, n, "L1"); ok {
		t.Fatal("L1 still has the torn-down group")
	}
	if _, ok := leaseOne(t, n, "L2"); ok {
		t.Fatal("L2 still has the torn-down group")
	}
}
