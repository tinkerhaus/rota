package integration

import (
	"context"
	"strconv"
	"sync/atomic"
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

// The full loop driven by REAL polling workers (no hand-driven decisions): a
// workflow worker leases tasks and decides; an activity worker runs the activity.
// StartWorkflow alone must carry the run to completion, with the activity run once.
func TestWorkflowWorkerDriven(t *testing.T) {
	n := openNode(t, 60_000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var charged int32
	go n.RunActivityWorker(ctx, "charge", "act-worker", func(task *rotav1.ActivityTask) ([]byte, bool) {
		atomic.AddInt32(&charged, 1)
		return []byte("charged"), true
	})

	// Deterministic decider: schedule the charge once, then complete once it's done.
	decide := func(runID uint64, history []*rotav1.HistoryEvent) []node.WorkflowCommand {
		var scheduled, completed bool
		for _, e := range history {
			switch e.GetEventType() {
			case rotav1.HistoryEventType_HET_ACTIVITY_SCHEDULED:
				scheduled = true
			case rotav1.HistoryEventType_HET_ACTIVITY_COMPLETED:
				completed = true
			}
		}
		switch {
		case !scheduled:
			return []node.WorkflowCommand{{Kind: "schedule_activity", ActivityType: "charge", Input: []byte("100")}}
		case completed:
			return []node.WorkflowCommand{{Kind: "complete_workflow", Result: []byte("done")}}
		default:
			return nil // activity in flight: wait
		}
	}
	go n.RunWorkflowWorker(ctx, "order", "wf-worker", decide)

	runID, err := n.StartWorkflow("order", "tenant-A", []byte("{}"))
	if err != nil {
		t.Fatal(err)
	}

	done := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !done {
		if run, ok := n.GetRun(runID); ok && run.GetStatus() == rotav1.WorkflowStatus_WF_COMPLETED {
			done = true
		} else {
			time.Sleep(40 * time.Millisecond)
		}
	}
	if !done {
		t.Fatal("workflow did not complete under worker drive within 10s")
	}
	if c := atomic.LoadInt32(&charged); c != 1 {
		t.Fatalf("activity ran %d times, want exactly 1 (idempotent under the loop)", c)
	}
}

// Robustness: many concurrent runs driven by the same worker pool must each
// complete independently (per-run head-of-line via group = run_id), with their
// activities each running exactly once.
func TestWorkflowConcurrentRuns(t *testing.T) {
	n := openNode(t, 60_000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var activityRuns int32
	go n.RunActivityWorker(ctx, "step", "act-worker", func(task *rotav1.ActivityTask) ([]byte, bool) {
		atomic.AddInt32(&activityRuns, 1)
		return []byte("ok"), true
	})
	decide := func(runID uint64, history []*rotav1.HistoryEvent) []node.WorkflowCommand {
		var scheduled, completed bool
		for _, e := range history {
			switch e.GetEventType() {
			case rotav1.HistoryEventType_HET_ACTIVITY_SCHEDULED:
				scheduled = true
			case rotav1.HistoryEventType_HET_ACTIVITY_COMPLETED:
				completed = true
			}
		}
		switch {
		case !scheduled:
			return []node.WorkflowCommand{{Kind: "schedule_activity", ActivityType: "step"}}
		case completed:
			return []node.WorkflowCommand{{Kind: "complete_workflow"}}
		default:
			return nil
		}
	}
	// Two workflow workers to exercise concurrent task processing.
	go n.RunWorkflowWorker(ctx, "batch", "wf-worker-1", decide)
	go n.RunWorkflowWorker(ctx, "batch", "wf-worker-2", decide)

	const N = 25
	runIDs := make([]uint64, N)
	for i := 0; i < N; i++ {
		id, err := n.StartWorkflow("batch", "tenant-"+strconv.Itoa(i%4), nil)
		if err != nil {
			t.Fatalf("start %d: %v", i, err)
		}
		runIDs[i] = id
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		allDone := true
		for _, id := range runIDs {
			if run, ok := n.GetRun(id); !ok || run.GetStatus() != rotav1.WorkflowStatus_WF_COMPLETED {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, id := range runIDs {
		run, _ := n.GetRun(id)
		if run.GetStatus() != rotav1.WorkflowStatus_WF_COMPLETED {
			t.Fatalf("run %d status = %v, want WF_COMPLETED", id, run.GetStatus())
		}
	}
	if c := atomic.LoadInt32(&activityRuns); c != N {
		t.Fatalf("activities ran %d times, want exactly %d (one per run)", c, N)
	}
}

// A workflow that waits for a signal stays RUNNING until SignalWorkflow delivers
// SIGNAL_RECEIVED, which dispatches a task that lets it complete.
func TestWorkflowSignal(t *testing.T) {
	n := openNode(t, 60_000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	decide := func(runID uint64, history []*rotav1.HistoryEvent) []node.WorkflowCommand {
		for _, e := range history {
			if e.GetEventType() == rotav1.HistoryEventType_HET_SIGNAL_RECEIVED {
				return []node.WorkflowCommand{{Kind: "complete_workflow"}}
			}
		}
		return nil // no signal yet: wait
	}
	go n.RunWorkflowWorker(ctx, "approval", "wf-worker", decide)

	runID, err := n.StartWorkflow("approval", "t", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Let the worker process the initial task (it decides to wait).
	time.Sleep(400 * time.Millisecond)
	if run, _ := n.GetRun(runID); run.GetStatus() != rotav1.WorkflowStatus_WF_RUNNING {
		t.Fatalf("run should be RUNNING before the signal, got %v", run.GetStatus())
	}

	if err := n.SignalWorkflow(runID, "proceed", []byte("yes")); err != nil {
		t.Fatal(err)
	}

	done := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !done {
		if run, ok := n.GetRun(runID); ok && run.GetStatus() == rotav1.WorkflowStatus_WF_COMPLETED {
			done = true
		} else {
			time.Sleep(40 * time.Millisecond)
		}
	}
	if !done {
		t.Fatal("workflow did not complete after the signal was delivered")
	}
}

// continue-as-new closes a run and spawns a successor with carried input and fresh
// history. A counting workflow loops via continue-as-new until done; each run's
// history stays bounded, and the chain links parent→child.
func TestWorkflowContinueAsNew(t *testing.T) {
	n := openNode(t, 60_000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// count carried in WORKFLOW_STARTED input (history[0]); loop 0→1→2→3 then complete.
	decide := func(runID uint64, history []*rotav1.HistoryEvent) []node.WorkflowCommand {
		count := 0
		if len(history) > 0 {
			count, _ = strconv.Atoi(string(history[0].GetAttrs()))
		}
		if count < 3 {
			return []node.WorkflowCommand{{Kind: "continue_as_new", Input: []byte(strconv.Itoa(count + 1))}}
		}
		return []node.WorkflowCommand{{Kind: "complete_workflow"}}
	}
	go n.RunWorkflowWorker(ctx, "counter", "wf-worker", decide)

	first, err := n.StartWorkflow("counter", "t", []byte("0"))
	if err != nil {
		t.Fatal(err)
	}

	// Wait until a COMPLETED run exists (the end of the chain).
	var completedID uint64
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && completedID == 0 {
		runs, _, _ := n.ListWorkflowRuns(0, false, 100, "")
		for _, r := range runs {
			if r.GetStatus() == rotav1.WorkflowStatus_WF_COMPLETED {
				completedID = r.GetRunId()
			}
		}
		if completedID == 0 {
			time.Sleep(40 * time.Millisecond)
		}
	}
	if completedID == 0 {
		t.Fatal("continue-as-new chain never reached a COMPLETED run")
	}

	// 4 runs total (counts 0,1,2 CONTINUED + count 3 COMPLETED), each with bounded
	// history (a CONTINUED run is just STARTED, WFTC, CONTINUED_AS_NEW = 3 events).
	runs, _, _ := n.ListWorkflowRuns(0, false, 100, "")
	if len(runs) != 4 {
		t.Fatalf("chain produced %d runs, want 4", len(runs))
	}
	var continued int
	for _, r := range runs {
		if r.GetStatus() == rotav1.WorkflowStatus_WF_CONTINUED {
			continued++
			if r.GetCurHistorySeq() > 4 {
				t.Fatalf("run %d history seq = %d, want bounded (<=4)", r.GetRunId(), r.GetCurHistorySeq())
			}
		}
	}
	if continued != 3 {
		t.Fatalf("got %d CONTINUED runs, want 3", continued)
	}
	// The first run is the root (no parent); successors link to a parent.
	firstRun, _ := n.GetRun(first)
	if firstRun.GetParentRunId() != 0 {
		t.Fatalf("root run has parent %d, want 0", firstRun.GetParentRunId())
	}
	completed, _ := n.GetRun(completedID)
	if completed.GetParentRunId() == 0 {
		t.Fatal("final run should link to a parent via continue-as-new")
	}
}
