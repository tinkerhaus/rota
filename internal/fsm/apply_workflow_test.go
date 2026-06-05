package fsm

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/storage"
)

func wfApply(t *testing.T, f *FSM, idx uint64, c Command) interface{} {
	t.Helper()
	data, _ := json.Marshal(c)
	r := f.Apply(&raft.Log{Index: idx, Type: raft.LogCommand, Data: data})
	if e, ok := r.(error); ok {
		t.Fatalf("apply: %v", e)
	}
	return r
}

func loadRun(t *testing.T, s *storage.Store, runID uint64) *rotav1.WorkflowRun {
	t.Helper()
	run := &rotav1.WorkflowRun{}
	if ok, _ := s.GetProto(storage.WFRunKey(runID), run); !ok {
		t.Fatalf("run %d not found", runID)
	}
	return run
}

// A run starts RUNNING with a seeded WORKFLOW_STARTED event at seq 1; a workflow
// task's events append under the OCC fence, advancing seq and epoch; a terminal
// event closes the run.
func TestWFStartAndAppend(t *testing.T) {
	s, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	f, _ := New(s)

	sr, _ := wfApply(t, f, 1, Command{Type: CmdWFStartRun, WFStart: &WFStartRunCmd{
		WorkflowType: "order", TenantID: "tenant-A", Input: []byte("{}"), NowMs: 1000,
	}}).(*WFStartRunResult)
	if sr == nil || sr.RunID == 0 {
		t.Fatalf("start run result = %+v", sr)
	}
	run := loadRun(t, s, sr.RunID)
	if run.Status != rotav1.WorkflowStatus_WF_RUNNING || run.CurHistorySeq != 1 || run.RunEpoch != 0 {
		t.Fatalf("after start: status=%v seq=%d epoch=%d, want RUNNING/1/0", run.Status, run.CurHistorySeq, run.RunEpoch)
	}

	// Append one workflow-task's events against the current (epoch=0, seq=1).
	ar, _ := wfApply(t, f, 2, Command{Type: CmdWFAppendEvents, WFAppend: &WFAppendEventsCmd{
		RunID: sr.RunID, RunEpoch: 0, HistorySeq: 1, NowMs: 2000,
		Events: []WFEventIn{
			{Type: int32(rotav1.HistoryEventType_HET_WORKFLOW_TASK_COMPLETED)},
			{Type: int32(rotav1.HistoryEventType_HET_ACTIVITY_SCHEDULED), Attrs: []byte("charge")},
		},
	}}).(*WFAppendResult)
	if ar == nil || !ar.Applied || ar.NewSeq != 3 {
		t.Fatalf("append = %+v, want Applied with NewSeq 3", ar)
	}
	run = loadRun(t, s, sr.RunID)
	if run.CurHistorySeq != 3 || run.RunEpoch != 1 {
		t.Fatalf("after append: seq=%d epoch=%d, want 3/1", run.CurHistorySeq, run.RunEpoch)
	}

	// A terminal event closes the run.
	wfApply(t, f, 3, Command{Type: CmdWFAppendEvents, WFAppend: &WFAppendEventsCmd{
		RunID: sr.RunID, RunEpoch: 1, HistorySeq: 3, NowMs: 3000,
		Events: []WFEventIn{{Type: int32(rotav1.HistoryEventType_HET_WORKFLOW_COMPLETED)}},
	}})
	if run = loadRun(t, s, sr.RunID); run.Status != rotav1.WorkflowStatus_WF_COMPLETED {
		t.Fatalf("after terminal event: status=%v, want WF_COMPLETED", run.Status)
	}
}

// THE LINCHPIN: two appends against the SAME (run_epoch, history_seq) — a duplicate
// dispatch or a failover re-propose — must NOT both commit. The first wins; the
// second is a benign no-op, so the run cannot diverge into two histories.
func TestWFAppendOCCFenceRejectsStaleSecondCommit(t *testing.T) {
	s, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	f, _ := New(s)

	sr, _ := wfApply(t, f, 1, Command{Type: CmdWFStartRun, WFStart: &WFStartRunCmd{
		WorkflowType: "order", TenantID: "t", NowMs: 1000,
	}}).(*WFStartRunResult)

	// First append against (epoch=0, seq=1): commits, run advances to (epoch=1, seq=2).
	first, _ := wfApply(t, f, 2, Command{Type: CmdWFAppendEvents, WFAppend: &WFAppendEventsCmd{
		RunID: sr.RunID, RunEpoch: 0, HistorySeq: 1, NowMs: 2000,
		Events: []WFEventIn{{Type: int32(rotav1.HistoryEventType_HET_ACTIVITY_SCHEDULED), Attrs: []byte("path-A")}},
	}}).(*WFAppendResult)
	if !first.Applied {
		t.Fatal("first append should commit")
	}

	// Second append against the SAME stale (epoch=0, seq=1) carrying a DIVERGENT
	// event (path-B). It must be rejected as a benign no-op — not committed.
	second, _ := wfApply(t, f, 3, Command{Type: CmdWFAppendEvents, WFAppend: &WFAppendEventsCmd{
		RunID: sr.RunID, RunEpoch: 0, HistorySeq: 1, NowMs: 2500,
		Events: []WFEventIn{{Type: int32(rotav1.HistoryEventType_HET_ACTIVITY_SCHEDULED), Attrs: []byte("path-B")}},
	}}).(*WFAppendResult)
	if second.Applied || second.Reason != "stale" {
		t.Fatalf("stale second append = %+v, want Applied=false Reason=stale", second)
	}

	// The committed history must contain exactly the WINNER (path-A) at seq 2 — the
	// divergent path-B must never have been written.
	run := loadRun(t, s, sr.RunID)
	if run.CurHistorySeq != 2 || run.RunEpoch != 1 {
		t.Fatalf("run = seq %d epoch %d, want 2/1 (only the first append applied)", run.CurHistorySeq, run.RunEpoch)
	}
	ev := &rotav1.HistoryEvent{}
	if ok, _ := s.GetProto(storage.WFHistoryKey(sr.RunID, 2), ev); !ok || string(ev.Attrs) != "path-A" {
		t.Fatalf("event 2 attrs = %q (ok=%v), want path-A — the divergent commit must have been fenced out", ev.Attrs, ok)
	}
}

// BUG-1 regression: an activity terminal is idempotent by scheduled_event_id. A
// redelivered/duplicate completion must NOT append a second ACTIVITY_COMPLETED, and
// completing an unscheduled activity is invalid.
func TestWFActivityCompletionIsIdempotent(t *testing.T) {
	s, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	f, _ := New(s)

	sr, _ := wfApply(t, f, 1, Command{Type: CmdWFStartRun, WFStart: &WFStartRunCmd{
		WorkflowType: "w", TenantID: "t", NowMs: 1000,
	}}).(*WFStartRunResult)

	// Schedule an activity — it lands at event_id 3 (after STARTED@1, WFTC@2).
	asa, _ := proto.Marshal(&rotav1.ActivityScheduledAttrs{ActivityType: "charge", TenantId: "t", Input: []byte("1")})
	wfApply(t, f, 2, Command{Type: CmdWFAppendEvents, WFAppend: &WFAppendEventsCmd{
		RunID: sr.RunID, RunEpoch: 0, HistorySeq: 1, NowMs: 2000, Events: []WFEventIn{
			{Type: int32(rotav1.HistoryEventType_HET_WORKFLOW_TASK_COMPLETED)},
			{Type: int32(rotav1.HistoryEventType_HET_ACTIVITY_SCHEDULED), Attrs: asa},
		},
	}})

	r1, _ := wfApply(t, f, 3, Command{Type: CmdWFCompleteActivity, WFCompleteActivity: &WFCompleteActivityCmd{
		RunID: sr.RunID, ScheduledEventID: 3, Success: true, Result: []byte("ok"), NowMs: 3000,
	}}).(*WFAppendResult)
	if r1 == nil || !r1.Applied {
		t.Fatalf("first completion = %+v, want Applied", r1)
	}
	r2, _ := wfApply(t, f, 4, Command{Type: CmdWFCompleteActivity, WFCompleteActivity: &WFCompleteActivityCmd{
		RunID: sr.RunID, ScheduledEventID: 3, Success: true, Result: []byte("ok-again"), NowMs: 3500,
	}}).(*WFAppendResult)
	if r2.Applied || r2.Reason != "duplicate_completion" {
		t.Fatalf("duplicate completion = %+v, want duplicate_completion no-op", r2)
	}

	run := loadRun(t, s, sr.RunID)
	completed := 0
	for id := uint64(1); id <= run.CurHistorySeq; id++ {
		ev := &rotav1.HistoryEvent{}
		if ok, _ := s.GetProto(storage.WFHistoryKey(sr.RunID, id), ev); ok && ev.EventType == rotav1.HistoryEventType_HET_ACTIVITY_COMPLETED {
			completed++
		}
	}
	if completed != 1 {
		t.Fatalf("ACTIVITY_COMPLETED count = %d, want exactly 1 (redelivery deduped)", completed)
	}

	r3, _ := wfApply(t, f, 5, Command{Type: CmdWFCompleteActivity, WFCompleteActivity: &WFCompleteActivityCmd{
		RunID: sr.RunID, ScheduledEventID: 999, Success: true, NowMs: 4000,
	}}).(*WFAppendResult)
	if r3.Applied || r3.Reason != "invalid_completion" {
		t.Fatalf("completing an unscheduled activity = %+v, want invalid_completion", r3)
	}
}

// Workflow state (run record, per-run history, and activity dedup markers) must
// survive a Raft FSM snapshot/restore — the crash-recovery / new-follower path.
func TestSnapshotRestoreWorkflowState(t *testing.T) {
	s1, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()
	f1, _ := New(s1)

	sr, _ := wfApply(t, f1, 1, Command{Type: CmdWFStartRun, WFStart: &WFStartRunCmd{
		WorkflowType: "w", TenantID: "t", NowMs: 1000,
	}}).(*WFStartRunResult)
	asa, _ := proto.Marshal(&rotav1.ActivityScheduledAttrs{ActivityType: "a", TenantId: "t"})
	wfApply(t, f1, 2, Command{Type: CmdWFAppendEvents, WFAppend: &WFAppendEventsCmd{
		RunID: sr.RunID, RunEpoch: 0, HistorySeq: 1, NowMs: 2000, Events: []WFEventIn{
			{Type: int32(rotav1.HistoryEventType_HET_WORKFLOW_TASK_COMPLETED)},
			{Type: int32(rotav1.HistoryEventType_HET_ACTIVITY_SCHEDULED), Attrs: asa},
		},
	}})
	wfApply(t, f1, 3, Command{Type: CmdWFCompleteActivity, WFCompleteActivity: &WFCompleteActivityCmd{
		RunID: sr.RunID, ScheduledEventID: 3, Success: true, NowMs: 3000,
	}})
	before := loadRun(t, s1, sr.RunID)

	snap, err := f1.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{buf: &buf}); err != nil {
		t.Fatal(err)
	}
	snap.Release()

	s2, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	f2, _ := New(s2)
	if err := f2.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatal(err)
	}

	after := loadRun(t, s2, sr.RunID)
	if after.CurHistorySeq != before.CurHistorySeq || after.RunEpoch != before.RunEpoch || after.Status != before.Status {
		t.Fatalf("run did not survive snapshot: before=%+v after=%+v", before, after)
	}
	for id := uint64(1); id <= after.CurHistorySeq; id++ {
		ev := &rotav1.HistoryEvent{}
		if ok, _ := s2.GetProto(storage.WFHistoryKey(sr.RunID, id), ev); !ok {
			t.Fatalf("history event %d lost across snapshot/restore", id)
		}
	}
	if _, ok, _ := s2.GetRaw(storage.WFActivityDoneKey(sr.RunID, 3)); !ok {
		t.Fatal("activity dedup marker lost across snapshot/restore (would re-run the activity)")
	}
}
