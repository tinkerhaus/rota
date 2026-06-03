package integration

import (
	"testing"
	"time"

	"github.com/tinkerhaus/rota/internal/fsm"
	"github.com/tinkerhaus/rota/internal/node"
)

func openNode(t *testing.T, visMs uint64) *node.Node {
	t.Helper()
	n, err := node.Open(node.Config{DataDir: t.TempDir(), NodeID: "test", VisibilityMs: visMs})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { n.Close() })
	if err := n.WaitLeader(10 * time.Second); err != nil {
		t.Fatalf("wait leader: %v", err)
	}
	return n
}

func leaseOne(t *testing.T, n *node.Node, lane string) (*fsm.LeaseResult, bool) {
	t.Helper()
	lr, ok, err := n.LeaseOne(lane, "c")
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	return lr, ok
}

// Delayed publish: a message with not_before in the future is not leasable until
// the Chronos READY_AT timer fires.
func TestDelayedPublish(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "d"
	nb := uint64(time.Now().UnixMilli()) + 300
	if _, err := n.Publish(node.PublishReq{Lane: lane, GroupID: "g", Payload: []byte("x"), NotBeforeMs: nb}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, ok := leaseOne(t, n, lane); ok {
		t.Fatal("message was leasable before its not_before time")
	}
	time.Sleep(550 * time.Millisecond)
	lr, ok := leaseOne(t, n, lane)
	if !ok {
		t.Fatal("delayed message never became leasable after not_before")
	}
	if err := n.Ack(lr.LeaseID); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

// Visibility timeout: a leased-but-unacked message is automatically redelivered
// after its visibility deadline (crash recovery).
func TestVisibilityTimeoutRedelivery(t *testing.T) {
	n := openNode(t, 250)
	const lane = "v"
	if _, err := n.Publish(node.PublishReq{Lane: lane, GroupID: "g", Payload: []byte("x")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	lr1, ok := leaseOne(t, n, lane)
	if !ok {
		t.Fatal("expected a lease")
	}
	// Do not ack; wait past the visibility deadline.
	time.Sleep(700 * time.Millisecond)
	lr2, ok := leaseOne(t, n, lane)
	if !ok {
		t.Fatal("message was not redelivered after visibility timeout")
	}
	if lr2.MsgID != lr1.MsgID {
		t.Fatalf("redelivered a different message: got %d want %d", lr2.MsgID, lr1.MsgID)
	}
	if lr2.LeaseID == lr1.LeaseID {
		t.Fatal("expected a fresh lease id on redelivery")
	}
	if err := n.Ack(lr2.LeaseID); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

// No-penalty requeue: REQUEUE_NO_PENALTY returns the message immediately without
// incrementing the attempt counter.
func TestNoPenaltyRequeue(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "np"
	if _, err := n.Publish(node.PublishReq{Lane: lane, GroupID: "g", Payload: []byte("x")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	lr1, ok := leaseOne(t, n, lane)
	if !ok {
		t.Fatal("expected a lease")
	}
	if lr1.Attempt != 0 {
		t.Fatalf("first attempt should be 0, got %d", lr1.Attempt)
	}
	if _, err := n.Nack(lr1.LeaseID, fsm.NackRequeueNoPenalty, 0, nil); err != nil {
		t.Fatalf("nack: %v", err)
	}
	lr2, ok := leaseOne(t, n, lane)
	if !ok {
		t.Fatal("message was not requeued")
	}
	if lr2.MsgID != lr1.MsgID || lr2.Attempt != 0 {
		t.Fatalf("no-penalty requeue changed identity/attempt: msg %d attempt %d", lr2.MsgID, lr2.Attempt)
	}
	_ = n.Ack(lr2.LeaseID)
}

// Retry to DLQ: with max_attempts=1, a single RETRY nack promotes the message to
// the dead-letter queue.
func TestRetryToDLQ(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "r"
	if _, err := n.Publish(node.PublishReq{Lane: lane, GroupID: "g", Payload: []byte("x"), MaxAttempts: 1}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	lr, ok := leaseOne(t, n, lane)
	if !ok {
		t.Fatal("expected a lease")
	}
	dl, err := n.Nack(lr.LeaseID, fsm.NackRetry, 0, map[string]string{"err": "boom"})
	if err != nil {
		t.Fatalf("nack: %v", err)
	}
	if !dl {
		t.Fatal("expected the retry to dead-letter at max_attempts=1")
	}
	if c, _ := n.DLQCount(lane); c != 1 {
		t.Fatalf("DLQ count = %d, want 1", c)
	}
	if _, ok := leaseOne(t, n, lane); ok {
		t.Fatal("dead-lettered message should not be leasable")
	}
}

// Terminal dead-letter: DEAD_LETTER nack moves the message to the DLQ directly.
func TestTerminalDeadLetter(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "t"
	if _, err := n.Publish(node.PublishReq{Lane: lane, GroupID: "g", Payload: []byte("x")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	lr, ok := leaseOne(t, n, lane)
	if !ok {
		t.Fatal("expected a lease")
	}
	dl, err := n.Nack(lr.LeaseID, fsm.NackDeadLetter, 0, nil)
	if err != nil || !dl {
		t.Fatalf("terminal nack: dl=%v err=%v", dl, err)
	}
	if c, _ := n.DLQCount(lane); c != 1 {
		t.Fatalf("DLQ count = %d, want 1", c)
	}
}
