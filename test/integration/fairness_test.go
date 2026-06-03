package integration

import (
	"testing"
	"time"

	"github.com/tinkerhaus/rota/internal/node"
)

// TestCrossGroupFairness is the Phase 0 smoke test: a large group (500) must not
// head-of-line-block a small group (20) sharing one lane. With equal weights the
// DRR scheduler interleaves them ~1:1, so group B finishes within the first ~40
// deliveries, not after all 500 of group A.
func TestCrossGroupFairness(t *testing.T) {
	dir := t.TempDir()
	n, err := node.Open(node.Config{DataDir: dir, NodeID: "test"})
	if err != nil {
		t.Fatalf("open node: %v", err)
	}
	defer n.Close()
	if err := n.WaitLeader(10 * time.Second); err != nil {
		t.Fatalf("wait leader: %v", err)
	}

	const lane = "t"
	const nA, nB = 500, 20
	for i := 0; i < nA; i++ {
		if _, err := n.Publish(node.PublishReq{Lane: lane, GroupID: "A", Payload: []byte("a")}); err != nil {
			t.Fatalf("publish A: %v", err)
		}
	}
	for i := 0; i < nB; i++ {
		if _, err := n.Publish(node.PublishReq{Lane: lane, GroupID: "B", Payload: []byte("b")}); err != nil {
			t.Fatalf("publish B: %v", err)
		}
	}

	order := make([]string, 0, nA+nB)
	for len(order) < nA+nB {
		lr, ok, err := n.LeaseOne(lane, "c")
		if err != nil {
			t.Fatalf("lease: %v", err)
		}
		if !ok {
			t.Fatalf("expected work but lease returned empty at %d/%d", len(order), nA+nB)
		}
		order = append(order, lr.GroupID)
		if err := n.Ack(lr.LeaseID); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}

	lastB := -1
	for i, g := range order {
		if g == "B" {
			lastB = i
		}
	}
	if lastB < 0 {
		t.Fatal("group B was never served")
	}
	// Equal weights ⇒ ~1:1 interleave ⇒ B drained by ~2*nB. Allow generous slack.
	if lastB+1 > 3*nB {
		t.Fatalf("head-of-line blocking: group B finished at delivery %d (want <= %d)", lastB+1, 3*nB)
	}
	// Sanity: the first two deliveries should not both be from the same large group.
	if order[0] == order[1] && order[0] == "A" && order[2] == "A" {
		t.Logf("note: first three deliveries all A (order[:6]=%v)", order[:6])
	}
}
