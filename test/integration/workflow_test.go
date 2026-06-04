package integration

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/fsm"
	"github.com/tinkerhaus/rota/internal/node"
)

// End-to-end durable workflow on the broker's own primitives: start → decide
// (schedule activity) → the activity dispatches as a REAL lease on __act/<type> →
// a worker leases + completes it → the result appends to history → decide (complete)
// → run CLOSED. Proves activities are leases and the gate drives the loop.
func TestWorkflowEndToEndHappyPath(t *testing.T) {
	n := openNode(t, 60_000)

	runID, err := n.StartWorkflow("order", "tenant-A", []byte("{}"))
	if err != nil {
		t.Fatalf("start workflow: %v", err)
	}

	// Decision 1: the worker replayed history to (epoch, seq) and decides to charge.
	decide := func(cmds []node.WorkflowCommand) *fsm.WFAppendResult {
		t.Helper()
		run, ok := n.GetRun(runID)
		if !ok {
			t.Fatal("run vanished")
		}
		cs, err := n.RunPrefixChecksum(runID, run.CurHistorySeq)
		if err != nil {
			t.Fatal(err)
		}
		res, err := n.CompleteWorkflowTask(runID, run.RunEpoch, run.CurHistorySeq, cs, cmds)
		if err != nil {
			t.Fatalf("complete workflow task: %v", err)
		}
		if !res.Applied {
			t.Fatalf("decision rejected: %+v", res)
		}
		return res
	}

	decide([]node.WorkflowCommand{{Kind: "schedule_activity", ActivityType: "charge", Input: []byte("100")}})

	// The activity must have dispatched as a leasable message on __act/charge.
	lr, ok := leaseOne(t, n, "__act/charge")
	if !ok {
		t.Fatal("activity was not dispatched as a lease on __act/charge")
	}
	task := &rotav1.ActivityTask{}
	if err := proto.Unmarshal(lr.Payload, task); err != nil {
		t.Fatalf("activity payload: %v", err)
	}
	if task.GetRunId() != runID || task.GetActivityType() != "charge" || string(task.GetInput()) != "100" {
		t.Fatalf("activity task = %+v, want run %d / charge / 100", task, runID)
	}
	_ = n.Ack(lr.LeaseID)

	// The worker reports the activity result; the engine appends ACTIVITY_COMPLETED.
	if r, err := n.CompleteActivityTask(task.GetRunId(), task.GetScheduledEventId(), true, []byte("charged")); err != nil || !r.Applied {
		t.Fatalf("complete activity = %+v err=%v", r, err)
	}

	// Decision 2: complete the workflow.
	decide([]node.WorkflowCommand{{Kind: "complete_workflow", Result: []byte("done")}})

	run, _ := n.GetRun(runID)
	if run.GetStatus() != rotav1.WorkflowStatus_WF_COMPLETED {
		t.Fatalf("final status = %v, want WF_COMPLETED", run.GetStatus())
	}

	// History: STARTED, WFTC, ACT_SCHEDULED, ACT_COMPLETED, WFTC, WF_COMPLETED.
	hist, _ := n.GetRunHistory(runID)
	want := []rotav1.HistoryEventType{
		rotav1.HistoryEventType_HET_WORKFLOW_STARTED,
		rotav1.HistoryEventType_HET_WORKFLOW_TASK_COMPLETED,
		rotav1.HistoryEventType_HET_ACTIVITY_SCHEDULED,
		rotav1.HistoryEventType_HET_ACTIVITY_COMPLETED,
		rotav1.HistoryEventType_HET_WORKFLOW_TASK_COMPLETED,
		rotav1.HistoryEventType_HET_WORKFLOW_COMPLETED,
	}
	if len(hist) != len(want) {
		t.Fatalf("history has %d events, want %d: %+v", len(hist), len(want), hist)
	}
	for i, ty := range want {
		if hist[i].GetEventType() != ty || hist[i].GetEventId() != uint64(i+1) {
			t.Fatalf("event %d = id %d type %v, want id %d type %v", i, hist[i].GetEventId(), hist[i].GetEventType(), i+1, ty)
		}
	}
}

// The leader validation gate rejects a worker whose replayed-prefix checksum does
// not match committed history (a divergent/stale replay) — proposing NOTHING, so
// the run is left uncorrupted.
func TestWorkflowGateRejectsDivergentReplay(t *testing.T) {
	n := openNode(t, 60_000)
	runID, err := n.StartWorkflow("w", "t", nil)
	if err != nil {
		t.Fatal(err)
	}
	run, _ := n.GetRun(runID)

	res, err := n.CompleteWorkflowTask(runID, run.RunEpoch, run.CurHistorySeq, []byte("not-the-real-checksum"),
		[]node.WorkflowCommand{{Kind: "complete_workflow"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied || res.Reason != "non_determinism" {
		t.Fatalf("gate result = %+v, want rejected with non_determinism", res)
	}
	after, _ := n.GetRun(runID)
	if after.GetCurHistorySeq() != run.GetCurHistorySeq() || after.GetRunEpoch() != run.GetRunEpoch() || after.GetStatus() != rotav1.WorkflowStatus_WF_RUNNING {
		t.Fatalf("run mutated after gate rejection: %+v (want unchanged, RUNNING)", after)
	}
}

// BUG-2 regression: a nil prefix checksum must NOT bypass the determinism gate.
func TestWorkflowGateRequiresChecksum(t *testing.T) {
	n := openNode(t, 60_000)
	runID, err := n.StartWorkflow("w", "t", nil)
	if err != nil {
		t.Fatal(err)
	}
	run, _ := n.GetRun(runID)
	res, err := n.CompleteWorkflowTask(runID, run.RunEpoch, run.CurHistorySeq, nil,
		[]node.WorkflowCommand{{Kind: "complete_workflow"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied || res.Reason != "checksum_required" {
		t.Fatalf("nil-checksum decision = %+v, want rejected with checksum_required", res)
	}
}

// BUG-3 regression: an activity that dead-letters must surface to its run as an
// ACTIVITY_FAILED event (liveness), instead of leaving the run wedged forever.
func TestActivityDeadLetterBridgesToRun(t *testing.T) {
	n := openNode(t, 60_000)
	runID, err := n.StartWorkflow("order", "t", nil)
	if err != nil {
		t.Fatal(err)
	}
	run, _ := n.GetRun(runID)
	cs, _ := n.RunPrefixChecksum(runID, run.CurHistorySeq)
	if r, err := n.CompleteWorkflowTask(runID, run.RunEpoch, run.CurHistorySeq, cs,
		[]node.WorkflowCommand{{Kind: "schedule_activity", ActivityType: "charge"}}); err != nil || !r.Applied {
		t.Fatalf("schedule activity = %+v err=%v", r, err)
	}

	lr, ok := leaseOne(t, n, "__act/charge")
	if !ok {
		t.Fatal("no activity lease")
	}
	// Terminal-nack the activity -> dead-letter -> the bridge must append ACTIVITY_FAILED.
	if _, err := n.Nack(lr.LeaseID, fsm.NackDeadLetter, 0, map[string]string{"why": "boom"}); err != nil {
		t.Fatal(err)
	}

	hist, _ := n.GetRunHistory(runID)
	failed := false
	for _, e := range hist {
		if e.GetEventType() == rotav1.HistoryEventType_HET_ACTIVITY_FAILED {
			failed = true
		}
	}
	if !failed {
		t.Fatalf("dead-lettered activity did not bridge to ACTIVITY_FAILED; history = %d events", len(hist))
	}
}

// A durable timer (workflow.sleep) arms on the unified time wheel and, when it
// fires via the leader's chronos sweep, appends TIMER_FIRED to the run — after
// which the workflow can make progress and complete.
func TestWorkflowDurableTimer(t *testing.T) {
	n := openNode(t, 60_000)
	runID, err := n.StartWorkflow("sleeper", "t", nil)
	if err != nil {
		t.Fatal(err)
	}

	run, _ := n.GetRun(runID)
	cs, _ := n.RunPrefixChecksum(runID, run.CurHistorySeq)
	if r, err := n.CompleteWorkflowTask(runID, run.RunEpoch, run.CurHistorySeq, cs,
		[]node.WorkflowCommand{{Kind: "start_timer", DelayMs: 200}}); err != nil || !r.Applied {
		t.Fatalf("start_timer = %+v err=%v", r, err)
	}

	// Wait for the chronos sweep to fire the timer into the run's history.
	fired := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !fired {
		hist, _ := n.GetRunHistory(runID)
		for _, e := range hist {
			if e.GetEventType() == rotav1.HistoryEventType_HET_TIMER_FIRED {
				fired = true
			}
		}
		if !fired {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if !fired {
		t.Fatal("durable timer never fired TIMER_FIRED into the run history")
	}

	run, _ = n.GetRun(runID)
	cs2, _ := n.RunPrefixChecksum(runID, run.CurHistorySeq)
	if r, err := n.CompleteWorkflowTask(runID, run.RunEpoch, run.CurHistorySeq, cs2,
		[]node.WorkflowCommand{{Kind: "complete_workflow"}}); err != nil || !r.Applied {
		t.Fatalf("complete after timer = %+v err=%v", r, err)
	}
	if run, _ = n.GetRun(runID); run.GetStatus() != rotav1.WorkflowStatus_WF_COMPLETED {
		t.Fatalf("status = %v, want WF_COMPLETED", run.GetStatus())
	}
}
