package fsm

import (
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/storage"
)

// An unknown CmdType must halt the node (panic) rather than silently no-op while
// still advancing applied_index. Without the halt, a node running an older binary
// would skip a command it does not understand yet advance its index, diverging
// permanently from the rest of the cluster on a mixed-version rollout.
func TestApplyUnknownCmdTypeHaltsWithoutAdvancing(t *testing.T) {
	s, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	f, _ := New(s)

	// A normal command advances the applied index to 1.
	applyCmd(t, f, 1, Command{Type: CmdPublish, Publish: &PublishCmd{
		Lane: "l", GroupID: "g", Payload: []byte("x"), NowMs: 1000,
	}})
	if got := f.AppliedIndex(); got != 1 {
		t.Fatalf("applied index = %d, want 1", got)
	}

	// An unknown command type at index 2 must panic, releasing the FSM lock via the
	// deferred unwind, and must NOT advance the applied index.
	const unknown = CmdType(250)
	data, _ := json.Marshal(Command{Type: unknown})
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Apply did not panic on unknown CmdType %d", unknown)
			}
		}()
		f.Apply(&raft.Log{Index: 2, Type: raft.LogCommand, Data: data})
	}()

	if got := f.AppliedIndex(); got != 1 {
		t.Fatalf("applied index advanced to %d on unknown command; want 1 (no advance)", got)
	}
	if v, ok, _ := s.GetRaw(storage.MetaKey("applied_index")); !ok || beU64(v) != 1 {
		t.Fatalf("persisted applied_index changed on unknown command; want 1")
	}
}

// A dedup_key makes a re-publish within the window a no-op that returns the
// ORIGINAL message id; a different key is independent; and once the window
// expires the same key publishes anew.
func TestPublishDedupKey(t *testing.T) {
	s, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	f, _ := New(s)

	const win = uint64(300_000)
	now := uint64(1_000_000)
	pub := func(idx uint64, key string, nowMs uint64) *PublishResult {
		t.Helper()
		data, _ := json.Marshal(Command{Type: CmdPublish, Publish: &PublishCmd{
			Lane: "l", GroupID: "g", Payload: []byte("x"), NowMs: nowMs,
			DedupKey: key, DedupExpiryMs: nowMs + win,
		}})
		r := f.Apply(&raft.Log{Index: idx, Type: raft.LogCommand, Data: data})
		if e, ok := r.(error); ok {
			t.Fatalf("apply: %v", e)
		}
		pr, _ := r.(*PublishResult)
		if pr == nil {
			t.Fatalf("no PublishResult: %T", r)
		}
		return pr
	}

	first := pub(1, "dk1", now)
	if first.Duplicate {
		t.Fatal("first publish must not be a duplicate")
	}
	if dup := pub(2, "dk1", now+1000); !dup.Duplicate || dup.MsgID != first.MsgID {
		t.Fatalf("re-publish within window = %+v, want Duplicate with MsgID %d", dup, first.MsgID)
	}
	if other := pub(3, "dk2", now+1000); other.Duplicate {
		t.Fatal("a different dedup key must be independent")
	}
	if third := pub(4, "dk1", now+win+1); third.Duplicate || third.MsgID == first.MsgID {
		t.Fatalf("publish after the window expires = %+v, want a fresh non-duplicate id", third)
	}
}

// A completion token is cleaned up when its lease is torn down (here, a direct
// ack), so it cannot leak and a late Complete against it reports Unknown.
func TestCompletionTokenClearedOnLeaseTeardown(t *testing.T) {
	s, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	f, _ := New(s)

	apply := func(idx uint64, c Command) interface{} {
		t.Helper()
		data, _ := json.Marshal(c)
		r := f.Apply(&raft.Log{Index: idx, Type: raft.LogCommand, Data: data})
		if e, ok := r.(error); ok {
			t.Fatalf("apply: %v", e)
		}
		return r
	}

	apply(1, Command{Type: CmdPublish, Publish: &PublishCmd{Lane: "l", GroupID: "g", Payload: []byte("x"), NowMs: 1000}})
	lr, _ := apply(2, Command{Type: CmdLease, Lease: &LeaseCmd{Lane: "l", GroupID: "g", ConsumerID: "c", DeadlineMs: 60_000}}).(*LeaseResult)
	if lr == nil || lr.Empty {
		t.Fatal("expected a lease")
	}
	tokenHash := []byte("0123456789abcdef0123456789abcdef")
	apply(3, Command{Type: CmdIssueToken, IssueToken: &IssueTokenCmd{LeaseID: lr.LeaseID, TokenHash: tokenHash}})
	if _, ok, _ := s.GetRaw(storage.TokenKey(tokenHash)); !ok {
		t.Fatal("token row should exist after IssueToken")
	}

	// Direct ack (not via Complete) must still clean up the token row.
	apply(4, Command{Type: CmdAck, Ack: &AckCmd{LeaseID: lr.LeaseID}})
	if _, ok, _ := s.GetRaw(storage.TokenKey(tokenHash)); ok {
		t.Fatal("token row should be deleted after the lease is acked")
	}

	cr, _ := apply(5, Command{Type: CmdComplete, Complete: &CompleteCmd{TokenHash: tokenHash, Success: true, NowMs: 2000}}).(*CompleteResult)
	if cr == nil || !cr.Unknown {
		t.Fatalf("late complete on a cleaned token = %+v, want Unknown", cr)
	}
}

// The max-lease-lifetime cap force-fails a lease at its absolute deadline even
// after an ExtendVisibility that pushed the visibility deadline well past the cap.
func TestMaxLeaseLifetimeBackstop(t *testing.T) {
	s, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	f, _ := New(s)

	apply := func(idx uint64, c Command) interface{} {
		t.Helper()
		data, _ := json.Marshal(c)
		r := f.Apply(&raft.Log{Index: idx, Type: raft.LogCommand, Data: data})
		if e, ok := r.(error); ok {
			t.Fatalf("apply: %v", e)
		}
		return r
	}

	apply(1, Command{Type: CmdPublish, Publish: &PublishCmd{Lane: "l", GroupID: "g", Payload: []byte("x"), NowMs: 1000}})
	const maxLife = uint64(50_000)
	lr, _ := apply(2, Command{Type: CmdLease, Lease: &LeaseCmd{
		Lane: "l", GroupID: "g", ConsumerID: "c", DeadlineMs: 30_000, MaxLifeMs: maxLife,
	}}).(*LeaseResult)
	if lr == nil || lr.Empty {
		t.Fatal("expected a lease")
	}
	// Extend the visibility deadline FAR past the cap — must not rescue the lease.
	apply(3, Command{Type: CmdExtend, Extend: &ExtendCmd{LeaseID: lr.LeaseID, NewDeadlineMs: 1_000_000}})

	// Fire the lifetime-cap timer (as the sweep would).
	apply(4, Command{Type: CmdFireTimer, Fire: &FireTimerCmd{
		Kind: storage.TimerLeaseMaxLife, DueTs: maxLife, Ref: storage.LeaseDeadlineRef(lr.LeaseID), FireAt: maxLife,
	}})

	if ok, _ := s.GetProto(storage.LeaseKey(lr.LeaseID), &rotav1.Lease{}); ok {
		t.Fatal("lease should be reclaimed at the lifetime cap")
	}
	if c, _ := s.CountDLQ("l"); c != 1 {
		t.Fatalf("DLQ depth = %d, want 1 (max_lease_lifetime)", c)
	}
	gm := &rotav1.GroupMeta{}
	if _, err := s.GetProto(storage.GroupMetaKey("l", "g"), gm); err != nil {
		t.Fatal(err)
	}
	if gm.InflightCount != 0 {
		t.Fatalf("inflight = %d, want 0 after the backstop fired", gm.InflightCount)
	}
}
