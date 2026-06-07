package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/fsm"
	"github.com/tinkerhaus/rota/internal/observe"
	"github.com/tinkerhaus/rota/internal/storage"
)

// Durable-execution engine (Phase 8, node layer). User workflow code runs in a
// WORKER, never here and never in Apply. A worker replays a run's history, decides,
// and submits a command list to CompleteWorkflowTask — the leader-side validation
// gate — which validates BEFORE proposing the single CmdWFAppendEvents that the FSM
// applies under its OCC fence. Activities dispatch as ordinary fair-scheduled leases.

// SignalWorkflow delivers a named signal (with an optional payload) to a running
// run. It appends a SIGNAL_RECEIVED event and dispatches a workflow task so the
// workflow reacts. A signal to a missing/closed run is a benign no-op.
func (n *Node) SignalWorkflow(runID uint64, signalName string, payload []byte) error {
	_, err := n.apply(fsm.Command{Type: fsm.CmdWFSignal, WFSignal: &fsm.WFSignalCmd{
		RunID: runID, SignalName: signalName, Payload: payload, NowMs: nowMs(),
	}})
	return err
}

// CancelWorkflow terminally cancels a running run. Returns true if it was running
// (and is now CANCELED), false if it was missing or already closed.
func (n *Node) CancelWorkflow(runID uint64, reason []byte) (bool, error) {
	res, err := n.apply(fsm.Command{Type: fsm.CmdWFCancel, WFCancel: &fsm.WFCancelCmd{
		RunID: runID, Reason: reason, NowMs: nowMs(),
	}})
	if err != nil {
		return false, err
	}
	if r, ok := res.(*fsm.WFAppendResult); ok {
		return r.Applied, nil
	}
	return false, nil
}

// ListWorkflowRuns pages through workflow runs (newest-tag order = ascending run id),
// optionally filtered by status. nextToken is "" on the last page.
func (n *Node) ListWorkflowRuns(statusFilter rotav1.WorkflowStatus, withFilter bool, pageSize uint32, pageToken string) ([]*rotav1.WorkflowRun, string, error) {
	lo, hi := storage.WFRunBounds()
	it, err := n.store.DB.NewIter(&pebble.IterOptions{LowerBound: pageStart(lo, pageToken), UpperBound: hi})
	if err != nil {
		return nil, "", err
	}
	defer it.Close()
	limit := clampPage(pageSize)
	var out []*rotav1.WorkflowRun
	var lastKey []byte
	for it.First(); it.Valid(); it.Next() {
		run := &rotav1.WorkflowRun{}
		if proto.Unmarshal(it.Value(), run) != nil {
			continue
		}
		if withFilter && run.Status != statusFilter {
			continue
		}
		out = append(out, run)
		lastKey = append(lastKey[:0], it.Key()...)
		if len(out) >= limit {
			it.Next()
			if it.Valid() {
				return out, hex.EncodeToString(lastKey), nil
			}
			break
		}
	}
	return out, "", nil
}

// StartWorkflow creates a run (RUNNING, seeded WORKFLOW_STARTED@1) and returns its id.
func (n *Node) StartWorkflow(workflowType, tenantID string, input []byte) (uint64, error) {
	res, err := n.apply(fsm.Command{Type: fsm.CmdWFStartRun, WFStart: &fsm.WFStartRunCmd{
		WorkflowType: workflowType, TenantID: tenantID, Input: input, NowMs: nowMs(),
	}})
	if err != nil {
		return 0, err
	}
	if r, ok := res.(*fsm.WFStartRunResult); ok {
		return r.RunID, nil
	}
	return 0, fmt.Errorf("start workflow: no result")
}

// WorkflowCommand is a worker's decision, produced by replaying history.
type WorkflowCommand struct {
	Kind         string // "schedule_activity" | "start_timer" | "complete_workflow" | "fail_workflow"
	ActivityType string
	Input        []byte
	Result       []byte
	DelayMs      uint64 // for start_timer: sleep duration; leader stamps the absolute fire time
}

// GetRun returns a run's RunMeta.
func (n *Node) GetRun(runID uint64) (*rotav1.WorkflowRun, bool) {
	run := &rotav1.WorkflowRun{}
	ok, _ := n.store.GetProto(storage.WFRunKey(runID), run)
	return run, ok
}

// GetRunHistory returns a run's events in event_id order.
func (n *Node) GetRunHistory(runID uint64) ([]*rotav1.HistoryEvent, error) {
	lo := storage.WFHistoryPrefix(runID)
	hi := storage.PrefixEnd(lo)
	it, err := n.store.DB.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []*rotav1.HistoryEvent
	for it.First(); it.Valid(); it.Next() {
		ev := &rotav1.HistoryEvent{}
		if proto.Unmarshal(it.Value(), ev) == nil {
			out = append(out, ev)
		}
	}
	return out, nil
}

// RunPrefixChecksum is the rolling hash over a run's committed history [1..uptoSeq]
// — the value a replaying worker echoes so the gate can detect a divergent replay.
func (n *Node) RunPrefixChecksum(runID, uptoSeq uint64) ([]byte, error) {
	return historyChecksum(n.store, runID, uptoSeq)
}

func historyChecksum(s *storage.Store, runID, uptoSeq uint64) ([]byte, error) {
	events := make([]*rotav1.HistoryEvent, 0, uptoSeq)
	for id := uint64(1); id <= uptoSeq; id++ {
		ev := &rotav1.HistoryEvent{}
		ok, err := s.GetProto(storage.WFHistoryKey(runID, id), ev)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		events = append(events, ev)
	}
	return PrefixChecksumOf(events), nil
}

// PrefixChecksumOf computes the determinism checksum a worker echoes, from the
// history events it replayed. The server's gate recomputes the same value over the
// committed prefix and rejects on mismatch (a divergent replay). It hashes
// (event_id, event_type, attrs) per event — an algorithm any SDK can reproduce.
func PrefixChecksumOf(events []*rotav1.HistoryEvent) []byte {
	h := sha256.New()
	var buf [8]byte
	for _, ev := range events {
		binary.BigEndian.PutUint64(buf[:], ev.GetEventId())
		h.Write(buf[:])
		binary.BigEndian.PutUint32(buf[:4], uint32(ev.GetEventType()))
		h.Write(buf[:4])
		h.Write(ev.GetAttrs())
	}
	return h.Sum(nil)
}

// CompleteWorkflowTask is the LEADER-SIDE VALIDATION GATE. A worker that replayed
// the run to (epoch, seq) submits its decision plus the prefix checksum it computed.
// BEFORE proposing anything to Raft the gate: (1) requires the run RUNNING; (2) OCC —
// rejects if (epoch, seq) no longer match committed state; (3) determinism — rejects
// if the worker's prefix checksum != the committed prefix (a divergent replay); then
// translates the commands to events and proposes ONE CmdWFAppendEvents. A rejection
// proposes NOTHING, so the run stays exactly at (epoch, seq), uncorrupted.
func (n *Node) CompleteWorkflowTask(runID uint64, epoch uint32, seq uint64, prefixChecksum []byte, cmds []WorkflowCommand) (*fsm.WFAppendResult, error) {
	run, ok := n.GetRun(runID)
	if !ok {
		return nil, fmt.Errorf("run %d not found", runID)
	}
	if run.Status != rotav1.WorkflowStatus_WF_RUNNING {
		return &fsm.WFAppendResult{Applied: false, Reason: "closed"}, nil
	}
	if epoch != run.RunEpoch || seq != run.CurHistorySeq {
		return &fsm.WFAppendResult{Applied: false, Reason: "stale"}, nil
	}
	// The determinism check is MANDATORY — a nil checksum must not silently bypass
	// it (that would let an unvalidated forward decision commit cluster-wide).
	if prefixChecksum == nil {
		observe.WorkflowTaskRejections.WithLabelValues("checksum_required").Inc()
		return &fsm.WFAppendResult{Applied: false, Reason: "checksum_required"}, nil
	}
	want, err := historyChecksum(n.store, runID, seq)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(prefixChecksum, want) {
		observe.WorkflowTaskRejections.WithLabelValues("non_determinism").Inc()
		return &fsm.WFAppendResult{Applied: false, Reason: "non_determinism"}, nil
	}
	events, err := translateCommands(cmds, run.TenantId)
	if err != nil {
		observe.WorkflowTaskRejections.WithLabelValues("invalid_command").Inc()
		return nil, err
	}
	return n.proposeAppend(runID, epoch, seq, events)
}

// translateCommands turns a worker's decision into history events: a
// WORKFLOW_TASK_COMPLETED frame, then one event per command.
func translateCommands(cmds []WorkflowCommand, tenantID string) ([]fsm.WFEventIn, error) {
	events := []fsm.WFEventIn{{Type: int32(rotav1.HistoryEventType_HET_WORKFLOW_TASK_COMPLETED)}}
	for _, c := range cmds {
		switch c.Kind {
		case "schedule_activity":
			attrs, _ := proto.Marshal(&rotav1.ActivityScheduledAttrs{
				ActivityType: c.ActivityType, TenantId: tenantID, Input: c.Input,
			})
			events = append(events, fsm.WFEventIn{Type: int32(rotav1.HistoryEventType_HET_ACTIVITY_SCHEDULED), Attrs: attrs})
		case "start_timer":
			// Leader-stamps the absolute fire time so every node arms the same instant.
			attrs, _ := proto.Marshal(&rotav1.TimerStartedAttrs{FireAtMs: nowMs() + c.DelayMs})
			events = append(events, fsm.WFEventIn{Type: int32(rotav1.HistoryEventType_HET_TIMER_STARTED), Attrs: attrs})
		case "continue_as_new":
			attrs, _ := proto.Marshal(&rotav1.ContinueAsNewAttrs{Input: c.Input})
			events = append(events, fsm.WFEventIn{Type: int32(rotav1.HistoryEventType_HET_WORKFLOW_CONTINUED_AS_NEW), Attrs: attrs})
		case "complete_workflow":
			events = append(events, fsm.WFEventIn{Type: int32(rotav1.HistoryEventType_HET_WORKFLOW_COMPLETED), Attrs: c.Result})
		case "fail_workflow":
			events = append(events, fsm.WFEventIn{Type: int32(rotav1.HistoryEventType_HET_WORKFLOW_FAILED), Attrs: c.Result})
		default:
			return nil, fmt.Errorf("unknown workflow command kind %q", c.Kind)
		}
	}
	return events, nil
}

// CompleteActivityTask records an activity result as an EXTERNAL event (appended on
// the leader's own authority — there is no worker decision to validate). It is
// IDEMPOTENT by scheduled_event_id: the FSM dedups on a per-activity done-marker, so
// an at-least-once redelivery or a race with the dead-letter bridge records the
// terminal exactly once. It appends at the run's current version (no epoch/seq fence
// needed — the marker is the idempotency, not the OCC generation).
func (n *Node) CompleteActivityTask(runID, scheduledEventID uint64, success bool, result []byte) (*fsm.WFAppendResult, error) {
	res, err := n.apply(fsm.Command{Type: fsm.CmdWFCompleteActivity, WFCompleteActivity: &fsm.WFCompleteActivityCmd{
		RunID: runID, ScheduledEventID: scheduledEventID, Success: success, Result: result, NowMs: nowMs(),
	}})
	if err != nil {
		return nil, err
	}
	if r, ok := res.(*fsm.WFAppendResult); ok {
		return r, nil
	}
	return nil, fmt.Errorf("complete activity: no result")
}

// proposeAppend proposes one CmdWFAppendEvents against a specific (epoch, seq).
func (n *Node) proposeAppend(runID uint64, epoch uint32, seq uint64, events []fsm.WFEventIn) (*fsm.WFAppendResult, error) {
	res, err := n.apply(fsm.Command{Type: fsm.CmdWFAppendEvents, WFAppend: &fsm.WFAppendEventsCmd{
		RunID: runID, RunEpoch: epoch, HistorySeq: seq, Events: events, NowMs: nowMs(),
	}})
	if err != nil {
		return nil, err
	}
	if r, ok := res.(*fsm.WFAppendResult); ok {
		return r, nil
	}
	return nil, fmt.Errorf("append: no result")
}

// ─── Worker loops (the engine drivers) ──────────────────────────────────────────

// WorkflowDecider replays a run's committed history and returns the commands for
// the current decision. It MUST be deterministic in history (idempotent on replay):
// given the same history it returns the same commands, emitting NEW commands only at
// the current decision point. Returning nil means "no new work — wait".
type WorkflowDecider func(runID uint64, history []*rotav1.HistoryEvent) []WorkflowCommand

// ActivityHandler executes an activity and returns (result, success).
type ActivityHandler func(task *rotav1.ActivityTask) (result []byte, success bool)

// RunWorkflowWorker polls workflow tasks for a workflow type and drives decisions
// through the leader validation gate until ctx is cancelled. Leader-only in effect
// (LeaseOne only yields on the leader); a follower simply leases nothing and idles.
func (n *Node) RunWorkflowWorker(ctx context.Context, workflowType, consumerID string, decide WorkflowDecider) {
	lane := fsm.WorkflowLanePrefix + workflowType
	for ctx.Err() == nil {
		lr, ok, err := n.LeaseOne(lane, consumerID)
		if err != nil || !ok {
			sleepCtx(ctx, 15*time.Millisecond)
			continue
		}
		n.settleWorkflowTask(lr.LeaseID, n.driveWorkflowTask(lr, decide))
	}
}

// WorkflowTaskRetryDelayMs is how long a workflow task waits before redelivery
// when it could not be consumed, instead of being dropped (which wedges the run).
const WorkflowTaskRetryDelayMs uint64 = 1000

// wfTaskResult is the disposition of one driven workflow task. Exactly one of the
// three branches is meaningful:
//   - drop:  a benign no-op (malformed payload, or a stale/closed run already
//     superseded by a fresh task) — safe to consume the lease.
//   - retry: the task could NOT be processed (a transient error, OR the decider
//     returned an invalid command) — it must be requeued, never dropped.
//   - res:   the gate ran; ack on applied/stale/closed, else requeue.
type wfTaskResult struct {
	res   *fsm.WFAppendResult
	drop  bool
	retry bool
}

// settleWorkflowTask consumes a workflow-task lease only when it is safe: the
// decision applied, the task is a benign no-op, or it was superseded/closed. A
// rejection, a transient error, or an invalid decision REQUEUES the task (with a
// delay) rather than acking it — acking there would delete the only task while
// the run still has wf_task_pending=true, wedging it forever.
func (n *Node) settleWorkflowTask(leaseID uint64, r wfTaskResult) {
	switch {
	case r.drop:
		_ = n.Ack(leaseID)
	case r.retry:
		_, _ = n.Nack(leaseID, fsm.NackRequeueNoPenalty, WorkflowTaskRetryDelayMs, nil)
	case r.res != nil && (r.res.Applied || r.res.Reason == "stale" || r.res.Reason == "closed"):
		_ = n.Ack(leaseID)
	default:
		_, _ = n.Nack(leaseID, fsm.NackRequeueNoPenalty, WorkflowTaskRetryDelayMs, nil)
	}
}

// driveWorkflowTask handles one leased workflow task: load the run, replay history,
// decide, and submit through the gate (which validates OCC + prefix checksum). It
// returns the task's disposition. Crucially, a gate ERROR (e.g. the decider
// returned an unknown command kind, which fails command translation) is reported
// as retry — NOT a droppable no-op — so an invalid decision cannot silently delete
// the task and wedge the run.
func (n *Node) driveWorkflowTask(lr *fsm.LeaseResult, decide WorkflowDecider) wfTaskResult {
	ref := &rotav1.WorkflowTaskRef{}
	if proto.Unmarshal(lr.Payload, ref) != nil {
		return wfTaskResult{drop: true} // malformed task payload: can never be processed
	}
	run, found := n.GetRun(ref.RunId)
	if !found || run.Status != rotav1.WorkflowStatus_WF_RUNNING {
		return wfTaskResult{drop: true} // stale/closed: a fresh task covers the new state
	}
	history, err := n.GetRunHistory(ref.RunId)
	if err != nil {
		return wfTaskResult{retry: true} // transient store error: don't drop
	}
	cmds := decide(ref.RunId, history)
	cs, err := n.RunPrefixChecksum(ref.RunId, run.CurHistorySeq)
	if err != nil {
		return wfTaskResult{retry: true}
	}
	res, err := n.CompleteWorkflowTask(ref.RunId, run.RunEpoch, run.CurHistorySeq, cs, cmds)
	if err != nil {
		return wfTaskResult{retry: true} // invalid command / append error: don't drop
	}
	return wfTaskResult{res: res}
}

// RunActivityWorker polls activity tasks for an activity type, runs the handler, and
// records the result (idempotently, by scheduled_event_id) until ctx is cancelled.
func (n *Node) RunActivityWorker(ctx context.Context, activityType, consumerID string, handle ActivityHandler) {
	lane := fsm.ActivityLanePrefix + activityType
	for ctx.Err() == nil {
		lr, ok, err := n.LeaseOne(lane, consumerID)
		if err != nil || !ok {
			sleepCtx(ctx, 15*time.Millisecond)
			continue
		}
		task := &rotav1.ActivityTask{}
		if proto.Unmarshal(lr.Payload, task) == nil && task.GetRunId() != 0 {
			result, success := handle(task)
			_, _ = n.CompleteActivityTask(task.GetRunId(), task.GetScheduledEventId(), success, result)
		}
		_ = n.Ack(lr.LeaseID)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
