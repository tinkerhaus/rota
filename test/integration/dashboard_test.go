package integration

import (
	"testing"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/fsm"
	"github.com/tinkerhaus/rota/internal/node"
)

// The dashboard group grid pages through a lane's groups and reports per-group depth.
func TestListGroupsPagination(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "dash"
	pubN(t, n, lane, "A", 3, 1)
	pubN(t, n, lane, "B", 2, 1)
	pubN(t, n, lane, "C", 1, 1)

	page1, tok, err := n.ListGroupStats(lane, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 2 || tok == "" {
		t.Fatalf("page1 = %d groups, tok=%q; want 2 groups + a continuation token", len(page1), tok)
	}
	page2, tok2, err := n.ListGroupStats(lane, 2, tok)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 1 || tok2 != "" {
		t.Fatalf("page2 = %d groups, tok=%q; want 1 group + empty token", len(page2), tok2)
	}
	ready := map[string]uint64{}
	for _, g := range append(page1, page2...) {
		ready[g.GroupId] = g.Ready
	}
	if ready["A"] != 3 || ready["B"] != 2 || ready["C"] != 1 {
		t.Fatalf("group ready counts = %+v, want A:3 B:2 C:1", ready)
	}
}

// The DLQ inspector surfaces a dead letter with its reason, payload, and failure headers.
func TestListDeadLetters(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "dlq"
	if _, err := n.Publish(node.PublishReq{Lane: lane, GroupID: "g", Payload: []byte("boom"), MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	lr, ok := leaseOne(t, n, lane)
	if !ok {
		t.Fatal("expected a lease")
	}
	// A retry-nack on a max_attempts=1 message dead-letters it.
	if _, err := n.Nack(lr.LeaseID, fsm.NackRetry, 0, map[string]string{"err": "boom"}); err != nil {
		t.Fatal(err)
	}

	dls, tok, err := n.ListDeadLetters(lane, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(dls) != 1 || tok != "" {
		t.Fatalf("ListDeadLetters = %d entries (tok=%q), want 1", len(dls), tok)
	}
	dl := dls[0]
	if dl.GroupId != "g" || dl.Reason != "max_attempts" || string(dl.Payload) != "boom" {
		t.Fatalf("dead letter = %+v, want group=g reason=max_attempts payload=boom", dl)
	}
	if dl.FailureHeaders["err"] != "boom" {
		t.Fatalf("failure headers = %+v, want err=boom", dl.FailureHeaders)
	}
}

// The lease inspector pages through a lane's in-flight leases, reporting each
// lease's coordinates (group, msg id, consumer, attempt, epoch). It must surface
// ONLY the requested lane even though leases are keyed globally by lease id.
func TestListLeases(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "lz"
	pubN(t, n, lane, "g", 3, 1)
	// A second lane's lease must not leak into the first lane's listing.
	pubN(t, n, "other", "g", 1, 1)

	// Lease all three of lane "lz" (left in-flight) plus the one in "other".
	var leased []*fsm.LeaseResult
	for i := 0; i < 3; i++ {
		lr, ok := leaseOne(t, n, lane)
		if !ok {
			t.Fatalf("expected lease %d", i)
		}
		leased = append(leased, lr)
	}
	if _, ok := leaseOne(t, n, "other"); !ok {
		t.Fatal("expected a lease in lane other")
	}

	page1, tok, err := n.ListLeases(lane, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 2 || tok == "" {
		t.Fatalf("page1 = %d leases (tok=%q), want 2 + a continuation token", len(page1), tok)
	}
	page2, tok2, err := n.ListLeases(lane, 2, tok)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 1 || tok2 != "" {
		t.Fatalf("page2 = %d leases (tok=%q), want 1 + empty token", len(page2), tok2)
	}

	all := append(page1, page2...)
	for _, li := range all {
		if li.Lane != lane {
			t.Fatalf("lease from wrong lane leaked in: %+v", li)
		}
		if li.GroupId != "g" || li.ConsumerId != "c" {
			t.Fatalf("lease coordinates = %+v, want group=g consumer=c", li)
		}
	}
	// The three listed leases must match the three we hold (by lease id).
	got := map[uint64]bool{}
	for _, li := range all {
		got[li.LeaseId] = true
	}
	for _, lr := range leased {
		if !got[lr.LeaseID] {
			t.Fatalf("held lease %d missing from ListLeases output", lr.LeaseID)
		}
	}
}

// PeekMessages is a non-destructive read of a group's head: it returns the
// messages with their state but leaves them leasable (no state change). A capped
// limit bounds the read.
func TestPeekMessages(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "pk"
	pubN(t, n, lane, "g", 5, 1)

	peek, err := n.PeekMessages(lane, "g", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(peek) != 3 {
		t.Fatalf("PeekMessages limit=3 returned %d, want 3", len(peek))
	}
	// Head messages are the lowest msg ids, all READY, attempt 0.
	for i, mp := range peek {
		wantID := uint64(i + 1)
		if mp.MsgId != wantID {
			t.Fatalf("peek[%d].MsgId = %d, want %d (head ordering)", i, mp.MsgId, wantID)
		}
		if mp.State != rotav1.MessageState_READY || mp.Attempt != 0 {
			t.Fatalf("peek[%d] = %+v, want READY attempt=0", i, mp)
		}
	}

	// The peek was non-destructive: all five are still leasable.
	for i := 0; i < 5; i++ {
		if _, ok := leaseOne(t, n, lane); !ok {
			t.Fatalf("message %d not leasable after peek (peek mutated state)", i)
		}
	}
}

// RedriveDeadLetter re-publishes a dead letter onto its lane as a fresh READY
// message (attempt reset) and deletes the DLQ row. A second redrive of the same
// msg id is an idempotent no-op (ok=false).
func TestRedriveDeadLetter(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "rd"
	if _, err := n.Publish(node.PublishReq{Lane: lane, GroupID: "g", Payload: []byte("revive"), MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	lr, ok := leaseOne(t, n, lane)
	if !ok {
		t.Fatal("expected a lease")
	}
	// Dead-letter it (max_attempts=1 ⇒ a retry-nack dead-letters).
	if _, err := n.Nack(lr.LeaseID, fsm.NackRetry, 0, map[string]string{"why": "boom"}); err != nil {
		t.Fatal(err)
	}

	dls, _, err := n.ListDeadLetters(lane, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(dls) != 1 {
		t.Fatalf("want 1 dead letter before redrive, got %d", len(dls))
	}
	deadID := dls[0].MsgId

	okRD, newID, err := n.RedriveDeadLetter(lane, "g", deadID)
	if err != nil {
		t.Fatal(err)
	}
	if !okRD || newID == 0 {
		t.Fatalf("redrive = (ok=%v, newID=%d), want ok=true + a new msg id", okRD, newID)
	}

	// The DLQ row is gone.
	dls2, _, err := n.ListDeadLetters(lane, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(dls2) != 0 {
		t.Fatalf("DLQ should be empty after redrive, got %d", len(dls2))
	}

	// The re-published message is leasable again, with attempt reset to 0.
	lr2, ok := leaseOne(t, n, lane)
	if !ok {
		t.Fatal("redriven message not leasable")
	}
	if lr2.MsgID != newID {
		t.Fatalf("leased msg id = %d, want the redriven %d", lr2.MsgID, newID)
	}
	if lr2.Attempt != 0 {
		t.Fatalf("redriven message attempt = %d, want 0 (reset)", lr2.Attempt)
	}
	if string(lr2.Payload) != "revive" {
		t.Fatalf("redriven payload = %q, want revive", lr2.Payload)
	}
	if err := n.Ack(lr2.LeaseID); err != nil {
		t.Fatal(err)
	}

	// Idempotent: redriving the same (now-absent) DLQ row again is a no-op.
	okRD2, newID2, err := n.RedriveDeadLetter(lane, "g", deadID)
	if err != nil {
		t.Fatal(err)
	}
	if okRD2 || newID2 != 0 {
		t.Fatalf("second redrive = (ok=%v, newID=%d), want ok=false + no new message", okRD2, newID2)
	}
}
