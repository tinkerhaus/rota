package fsm

import (
	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/storage"
)

// ActivityLanePrefix is the lane namespace for engine-dispatched activity tasks.
// An activity of type T dispatches onto lane "__act/T", group = tenant, so the
// existing fair scheduler interleaves activities per tenant just like any message.
const ActivityLanePrefix = "__act/"

// Durable-execution kernel (Phase 8). The engine's system of record is a per-run,
// append-only, totally-ordered event HISTORY plus a RunMeta record, both written in
// the FSM's one atomic batch alongside applied_index. User workflow code never runs
// here — Apply only appends events a leader has already validated (the off-FSM
// validation gate is a later layer); this handler does zero non-deterministic work.

// nextRunID assigns a cluster-wide monotonic run id (mirrors nextLeaseID).
func (f *FSM) nextRunID(b *pebble.Batch) uint64 {
	var id uint64 = 1
	if v, ok, _ := f.s.GetRaw(storage.MetaKey("next_run_id")); ok {
		id = beU64(v)
	}
	_ = b.Set(storage.MetaKey("next_run_id"), u64b(id+1), nil)
	return id
}

// applyWFStartRun creates a run: assigns the run id, writes RunMeta RUNNING, and
// seeds the history with a WORKFLOW_STARTED event at event_id 1. Deterministic —
// the leader stamps NowMs and every node applies the identical committed outcome.
func (f *FSM) applyWFStartRun(b *pebble.Batch, c *WFStartRunCmd) (interface{}, error) {
	runID := f.nextRunID(b)
	run := &rotav1.WorkflowRun{
		RunId: runID, WorkflowType: c.WorkflowType, TenantId: c.TenantID,
		Status: rotav1.WorkflowStatus_WF_RUNNING, RunEpoch: 0,
		Input: c.Input, StartedMs: c.NowMs, LastEventMs: c.NowMs, ParentRunId: c.ParentRunID,
	}
	started := &rotav1.HistoryEvent{
		EventId:     1,
		EventType:   rotav1.HistoryEventType_HET_WORKFLOW_STARTED,
		EventTimeMs: c.NowMs,
		Attrs:       c.Input,
	}
	if err := putProto(b, storage.WFHistoryKey(runID, 1), started); err != nil {
		return nil, err
	}
	run.CurHistorySeq = 1
	if err := putProto(b, storage.WFRunKey(runID), run); err != nil {
		return nil, err
	}
	return &WFStartRunResult{RunID: runID}, nil
}

// applyWFAppendEvents is the COMMITTED-DIVERGENCE LINCHPIN. It re-checks the
// optimistic-concurrency fence against COMMITTED state inside the one atomic batch,
// exactly as applyLease re-validates headReady even though Pick is advisory: the
// first append against a given (run_epoch, history_seq) wins and bumps both; any
// second append against the SAME version — duplicate dispatch, a stale-cache worker,
// or a failover re-propose — fails the re-check as a benign no-op that still advances
// applied_index. No interleaving can commit two divergent batches against one version.
// A gap-free event_id guard makes an already-written id impossible to rewrite.
func (f *FSM) applyWFAppendEvents(b *pebble.Batch, c *WFAppendEventsCmd) (interface{}, error) {
	run := &rotav1.WorkflowRun{}
	if found, _ := f.s.GetProto(storage.WFRunKey(c.RunID), run); !found {
		return &WFAppendResult{Applied: false, Reason: "missing"}, nil
	}
	if run.Status != rotav1.WorkflowStatus_WF_RUNNING {
		return &WFAppendResult{Applied: false, Reason: "closed"}, nil
	}
	if c.RunEpoch != run.RunEpoch || c.HistorySeq != run.CurHistorySeq {
		return &WFAppendResult{Applied: false, Reason: "stale"}, nil
	}

	next := run.CurHistorySeq
	for i := range c.Events {
		next++
		ev := &rotav1.HistoryEvent{
			EventId:     next,
			EventType:   rotav1.HistoryEventType(c.Events[i].Type),
			EventTimeMs: c.NowMs,
			Attrs:       c.Events[i].Attrs,
		}
		if err := putProto(b, storage.WFHistoryKey(c.RunID, next), ev); err != nil {
			return nil, err
		}
		switch ev.EventType {
		case rotav1.HistoryEventType_HET_ACTIVITY_SCHEDULED:
			// Atomic dispatch: publish the activity as a leasable message in the SAME
			// batch as its history event, so the schedule and the dispatch commit
			// together (no orphaned event, no lost task on a crash between them).
			if err := f.dispatchActivity(b, c.RunID, next, ev.Attrs); err != nil {
				return nil, err
			}
		case rotav1.HistoryEventType_HET_TIMER_STARTED:
			// Durable timer (workflow.sleep): arm a TimerWFFired entry on the unified
			// time wheel at the leader-stamped fire time, in the same batch.
			if err := f.armWFTimer(b, c.RunID, next, ev.Attrs); err != nil {
				return nil, err
			}
		case rotav1.HistoryEventType_HET_WORKFLOW_COMPLETED:
			run.Status = rotav1.WorkflowStatus_WF_COMPLETED
		case rotav1.HistoryEventType_HET_WORKFLOW_FAILED:
			run.Status = rotav1.WorkflowStatus_WF_FAILED
		case rotav1.HistoryEventType_HET_WORKFLOW_CANCELED:
			run.Status = rotav1.WorkflowStatus_WF_CANCELED
		case rotav1.HistoryEventType_HET_WORKFLOW_CONTINUED_AS_NEW:
			run.Status = rotav1.WorkflowStatus_WF_CONTINUED
		}
	}
	run.CurHistorySeq = next
	run.RunEpoch++ // every committed append advances the OCC generation
	run.LastEventMs = c.NowMs
	if err := putProto(b, storage.WFRunKey(c.RunID), run); err != nil {
		return nil, err
	}
	return &WFAppendResult{Applied: true, NewSeq: next}, nil
}

// applyWFCompleteActivity records an activity terminal idempotently (see
// recordActivityTerminal). A duplicate (already-recorded scheduled_event_id) or a
// completion for an unscheduled/closed activity is a benign no-op that still advances
// applied_index.
func (f *FSM) applyWFCompleteActivity(b *pebble.Batch, c *WFCompleteActivityCmd) (interface{}, error) {
	appended, reason, newSeq, err := f.recordActivityTerminal(b, c.RunID, c.ScheduledEventID, !c.Success, c.Result, c.NowMs)
	if err != nil {
		return nil, err
	}
	return &WFAppendResult{Applied: appended, Reason: reason, NewSeq: newSeq}, nil
}

// recordActivityTerminal appends an ACTIVITY_COMPLETED/FAILED event for
// schedEventID into the run, idempotently. It is the SINGLE path both the worker
// completion (CmdWFCompleteActivity) and the dead-letter bridge use, so the two can
// race and still record the outcome exactly once:
//   - run absent / not RUNNING                  -> no-op ("closed")
//   - schedEventID not a real ACTIVITY_SCHEDULED -> no-op ("invalid_completion")
//   - done-marker already present               -> no-op ("duplicate_completion")
//   - otherwise: append at cur_seq+1, write the marker, bump (seq, epoch), all in b.
//
// Returns (appended, reason, newSeq, err).
func (f *FSM) recordActivityTerminal(b *pebble.Batch, runID, schedEventID uint64, failed bool, result []byte, nowMs uint64) (bool, string, uint64, error) {
	run := &rotav1.WorkflowRun{}
	if ok, _ := f.s.GetProto(storage.WFRunKey(runID), run); !ok {
		return false, "missing", 0, nil
	}
	if run.Status != rotav1.WorkflowStatus_WF_RUNNING {
		return false, "closed", 0, nil
	}
	if _, present, _ := f.s.GetRaw(storage.WFActivityDoneKey(runID, schedEventID)); present {
		return false, "duplicate_completion", 0, nil // already recorded ⇒ idempotent no-op
	}
	sched := &rotav1.HistoryEvent{}
	if ok, _ := f.s.GetProto(storage.WFHistoryKey(runID, schedEventID), sched); !ok ||
		sched.EventType != rotav1.HistoryEventType_HET_ACTIVITY_SCHEDULED {
		return false, "invalid_completion", 0, nil // completing an unscheduled activity
	}

	seq := run.CurHistorySeq + 1
	typ := rotav1.HistoryEventType_HET_ACTIVITY_COMPLETED
	if failed {
		typ = rotav1.HistoryEventType_HET_ACTIVITY_FAILED
	}
	attrs, _ := proto.Marshal(&rotav1.ActivityCompletedAttrs{
		ScheduledEventId: schedEventID, Success: !failed, Result: result,
	})
	ev := &rotav1.HistoryEvent{EventId: seq, EventType: typ, EventTimeMs: nowMs, Attrs: attrs}
	if err := putProto(b, storage.WFHistoryKey(runID, seq), ev); err != nil {
		return false, "", 0, err
	}
	if err := b.Set(storage.WFActivityDoneKey(runID, schedEventID), []byte{1}, nil); err != nil {
		return false, "", 0, err
	}
	run.CurHistorySeq = seq
	run.RunEpoch++
	run.LastEventMs = nowMs
	if err := putProto(b, storage.WFRunKey(runID), run); err != nil {
		return false, "", 0, err
	}
	return true, "", seq, nil
}

// armWFTimer arms a durable workflow timer on the unified time wheel for a
// TIMER_STARTED event, firing at the leader-stamped fire_at. Epoch 0 — a WF timer
// is not message-epoch-fenced; the tidx-row presence is its idempotency guard.
func (f *FSM) armWFTimer(b *pebble.Batch, runID, startedEventID uint64, attrs []byte) error {
	tsa := &rotav1.TimerStartedAttrs{}
	if proto.Unmarshal(attrs, tsa) != nil || tsa.GetFireAtMs() == 0 {
		return nil // malformed/zero ⇒ no timer (the event still stands)
	}
	return addTimer(b, tsa.GetFireAtMs(), storage.TimerWFFired, storage.WFTimerRef(runID, startedEventID), 0)
}

// recordTimerFired appends a TIMER_FIRED event to a run when its durable timer
// fires. Idempotency comes from the firing tidx row being consumed exactly once
// (applyFireTimer deletes it), so a re-proposed fire after failover is a no-op.
func (f *FSM) recordTimerFired(b *pebble.Batch, runID, startedEventID, nowMs uint64) error {
	run := &rotav1.WorkflowRun{}
	if ok, _ := f.s.GetProto(storage.WFRunKey(runID), run); !ok {
		return nil
	}
	if run.Status != rotav1.WorkflowStatus_WF_RUNNING {
		return nil
	}
	seq := run.CurHistorySeq + 1
	attrs, _ := proto.Marshal(&rotav1.TimerFiredAttrs{StartedEventId: startedEventID})
	ev := &rotav1.HistoryEvent{
		EventId: seq, EventType: rotav1.HistoryEventType_HET_TIMER_FIRED,
		EventTimeMs: nowMs, Attrs: attrs,
	}
	if err := putProto(b, storage.WFHistoryKey(runID, seq), ev); err != nil {
		return err
	}
	run.CurHistorySeq = seq
	run.RunEpoch++
	run.LastEventMs = nowMs
	return putProto(b, storage.WFRunKey(runID), run)
}

// dispatchActivity publishes the activity task message for an ACTIVITY_SCHEDULED
// event onto __act/<type> (group = tenant), carrying run_id + scheduled_event_id so
// the worker's completion routes back into this run's history. Reuses publishReady,
// so the activity is an ordinary fair-scheduled message.
func (f *FSM) dispatchActivity(b *pebble.Batch, runID, schedEventID uint64, attrs []byte) error {
	asa := &rotav1.ActivityScheduledAttrs{}
	if proto.Unmarshal(attrs, asa) != nil || asa.GetActivityType() == "" {
		return nil // malformed/empty attrs ⇒ nothing to dispatch (the event still stands)
	}
	task := &rotav1.ActivityTask{
		RunId: runID, ScheduledEventId: schedEventID,
		ActivityType: asa.GetActivityType(), Input: asa.GetInput(),
	}
	payload, err := proto.Marshal(task)
	if err != nil {
		return err
	}
	lane := ActivityLanePrefix + asa.GetActivityType()
	_, err = f.publishReady(b, lane, asa.GetTenantId(), payload, nil, 0)
	return err
}
