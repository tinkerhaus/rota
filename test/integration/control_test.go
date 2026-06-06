package integration

import (
	"testing"
	"time"

	"github.com/tinkerhaus/rota/internal/node"
)

func TestPauseResumeGroup(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "pr"
	pubN(t, n, lane, "A", 5, 1)
	pubN(t, n, lane, "B", 5, 1)
	if err := n.PauseGroup(lane, "B"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	seen := map[string]int{}
	for i := 0; i < 5; i++ {
		lr, ok := leaseOne(t, n, lane)
		if !ok {
			t.Fatalf("ran dry at %d", i)
		}
		seen[lr.GroupID]++
		_ = n.Ack(lr.LeaseID)
	}
	if seen["B"] != 0 {
		t.Fatalf("paused group B was served: %v", seen)
	}
	if _, ok := leaseOne(t, n, lane); ok {
		t.Fatal("nothing should be leasable while B is paused and A is drained")
	}
	if err := n.ResumeGroup(lane, "B"); err != nil {
		t.Fatalf("resume: %v", err)
	}
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
		t.Fatalf("after resume, drained %d from B, want 5", got)
	}
}

func TestCancelGroup(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "cancel"
	pubN(t, n, lane, "A", 10, 1)
	aff, err := n.CancelGroup(lane, "A")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if aff != 10 {
		t.Fatalf("cancel affected %d messages, want 10", aff)
	}
	if _, ok := leaseOne(t, n, lane); ok {
		t.Fatal("a cancelled group should have no leasable messages")
	}
}

func TestCron(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "cron"
	if err := n.ScheduleCron("c1", lane, "g", []byte("tick"), nil, "@every 1s"); err != nil {
		t.Fatalf("schedule cron: %v", err)
	}
	time.Sleep(2500 * time.Millisecond)
	got := 0
	for {
		lr, ok, err := n.LeaseOne(lane, "c")
		if err != nil {
			t.Fatalf("lease: %v", err)
		}
		if !ok {
			break
		}
		_ = n.Ack(lr.LeaseID)
		if got++; got > 20 {
			break
		}
	}
	if got < 1 {
		t.Fatalf("cron produced %d messages in 2.5s, want >=1", got)
	}
	specs, _ := n.ListCron()
	if len(specs) != 1 || specs[0].CronID != "c1" {
		t.Fatalf("ListCron = %+v, want one spec c1", specs)
	}
}

// TestPauseResumeCron exercises the PauseCron path that previously had no handler
// (the RPC returned Unimplemented): a paused schedule stops firing and reports
// Paused; resuming re-arms it.
func TestPauseResumeCron(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "cronpause"
	if err := n.ScheduleCron("p1", lane, "g", []byte("tick"), nil, "@every 1s"); err != nil {
		t.Fatalf("schedule cron: %v", err)
	}
	if err := n.PauseCron("p1"); err != nil {
		t.Fatalf("pause cron: %v", err)
	}
	spec, ok := n.GetCron("p1")
	if !ok || !spec.Paused {
		t.Fatalf("GetCron after pause = %+v, ok=%v, want Paused=true", spec, ok)
	}

	// While paused, drain anything already queued, then assert no NEW fire lands.
	drain := func() {
		for {
			lr, ok, err := n.LeaseOne(lane, "c")
			if err != nil {
				t.Fatalf("lease: %v", err)
			}
			if !ok {
				return
			}
			_ = n.Ack(lr.LeaseID)
		}
	}
	drain()
	time.Sleep(2500 * time.Millisecond)
	drain() // no panic / no error means the paused schedule produced nothing new it must fire

	if _, ok, _ := n.LeaseOne(lane, "c"); ok {
		t.Fatal("paused cron should not have fired")
	}

	if err := n.ResumeCron("p1"); err != nil {
		t.Fatalf("resume cron: %v", err)
	}
	if spec, ok := n.GetCron("p1"); !ok || spec.Paused {
		t.Fatalf("GetCron after resume = %+v, ok=%v, want Paused=false", spec, ok)
	}
	time.Sleep(2500 * time.Millisecond)
	if _, ok, _ := n.LeaseOne(lane, "c"); !ok {
		t.Fatal("resumed cron should fire again")
	}
}

func TestCompleteByToken(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "tok"
	if _, err := n.Publish(node.PublishReq{Lane: lane, GroupID: "g", Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	lr, ok := leaseOne(t, n, lane)
	if !ok {
		t.Fatal("expected a lease")
	}
	tok, err := n.IssueToken(lr.LeaseID)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	// Completion can arrive from any process, identified only by the token.
	dl, unknown, err := n.Complete(tok, true, nil, 0)
	if err != nil || dl || unknown {
		t.Fatalf("complete success: dl=%v unknown=%v err=%v", dl, unknown, err)
	}
	if _, ok := leaseOne(t, n, lane); ok {
		t.Fatal("message should be gone after successful completion")
	}
	if _, unknown2, _ := n.Complete([]byte("bogus-token-1234"), true, nil, 0); !unknown2 {
		t.Fatal("expected unknown=true for an unrecognized token")
	}
}

func TestSingletonLease(t *testing.T) {
	n := openNode(t, 60_000)
	f1, ok, err := n.AcquireSingleton("job", "h1", 10_000)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || f1 == 0 {
		t.Fatalf("first acquire: ok=%v fence=%d", ok, f1)
	}
	if _, ok2, _ := n.AcquireSingleton("job", "h2", 10_000); ok2 {
		t.Fatal("a second holder must not acquire a held singleton")
	}
	if rel, _ := n.ReleaseSingleton("job", "h1", f1); !rel {
		t.Fatal("the holder should be able to release")
	}
	f3, ok3, _ := n.AcquireSingleton("job", "h2", 10_000)
	if !ok3 || f3 <= f1 {
		t.Fatalf("re-acquire after release: ok=%v fence=%d (want fence > %d)", ok3, f3, f1)
	}
}
