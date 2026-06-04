package node

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/fsm"
	"github.com/tinkerhaus/rota/internal/storage"
)

// Durable-execution engine (Phase 8, node layer). User workflow code runs in a
// WORKER, never here and never in Apply. A worker replays a run's history, decides,
// and submits a command list to CompleteWorkflowTask — the leader-side validation
// gate — which validates BEFORE proposing the single CmdWFAppendEvents that the FSM
// applies under its OCC fence. Activities dispatch as ordinary fair-scheduled leases.

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
	h := sha256.New()
	var buf [8]byte
	for id := uint64(1); id <= uptoSeq; id++ {
		ev := &rotav1.HistoryEvent{}
		ok, err := s.GetProto(storage.WFHistoryKey(runID, id), ev)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		binary.BigEndian.PutUint64(buf[:], ev.EventId)
		h.Write(buf[:])
		binary.BigEndian.PutUint32(buf[:4], uint32(ev.EventType))
		h.Write(buf[:4])
		h.Write(ev.Attrs)
	}
	return h.Sum(nil), nil
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
		return &fsm.WFAppendResult{Applied: false, Reason: "checksum_required"}, nil
	}
	want, err := historyChecksum(n.store, runID, seq)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(prefixChecksum, want) {
		return &fsm.WFAppendResult{Applied: false, Reason: "non_determinism"}, nil
	}
	events, err := translateCommands(cmds, run.TenantId)
	if err != nil {
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
