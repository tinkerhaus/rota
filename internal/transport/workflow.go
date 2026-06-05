package transport

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/fsm"
	"github.com/tinkerhaus/rota/internal/node"
)

// WorkflowService implements the rota.v1 Workflow (durable execution) plane.
// Mutating RPCs (Start/Signal/Cancel) are leader-guarded via mutatingMethods; the
// read RPCs (GetRun/GetHistory/ListRuns) are follower-servable.
type WorkflowService struct {
	rotav1.UnimplementedWorkflowServer
	n *node.Node
}

func NewWorkflow(n *node.Node) *WorkflowService { return &WorkflowService{n: n} }

func (w *WorkflowService) StartWorkflow(ctx context.Context, req *rotav1.StartWorkflowRequest) (*rotav1.StartWorkflowResponse, error) {
	if req.GetWorkflowType() == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow_type is required")
	}
	id, err := w.n.StartWorkflow(req.GetWorkflowType(), req.GetTenantId(), req.GetInput())
	if err != nil {
		return nil, err
	}
	return &rotav1.StartWorkflowResponse{RunId: id}, nil
}

func (w *WorkflowService) SignalWorkflow(ctx context.Context, req *rotav1.SignalWorkflowRequest) (*rotav1.SignalWorkflowResponse, error) {
	if err := w.n.SignalWorkflow(req.GetRunId(), req.GetSignalName(), req.GetPayload()); err != nil {
		return nil, err
	}
	return &rotav1.SignalWorkflowResponse{}, nil
}

func (w *WorkflowService) CancelWorkflow(ctx context.Context, req *rotav1.CancelWorkflowRequest) (*rotav1.CancelWorkflowResponse, error) {
	ok, err := w.n.CancelWorkflow(req.GetRunId(), req.GetReason())
	if err != nil {
		return nil, err
	}
	return &rotav1.CancelWorkflowResponse{Canceled: ok}, nil
}

func (w *WorkflowService) GetWorkflowRun(ctx context.Context, req *rotav1.WorkflowRunRef) (*rotav1.WorkflowRun, error) {
	run, ok := w.n.GetRun(req.GetRunId())
	if !ok {
		return nil, status.Error(codes.NotFound, "workflow run not found")
	}
	return run, nil
}

func (w *WorkflowService) GetWorkflowHistory(ctx context.Context, req *rotav1.WorkflowRunRef) (*rotav1.GetWorkflowHistoryResponse, error) {
	hist, err := w.n.GetRunHistory(req.GetRunId())
	if err != nil {
		return nil, err
	}
	return &rotav1.GetWorkflowHistoryResponse{Events: hist}, nil
}

func (w *WorkflowService) ListWorkflowRuns(ctx context.Context, req *rotav1.ListWorkflowRunsRequest) (*rotav1.ListWorkflowRunsResponse, error) {
	runs, next, err := w.n.ListWorkflowRuns(req.GetStatus(), req.GetHasStatus(), req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	return &rotav1.ListWorkflowRunsResponse{Runs: runs, NextPageToken: next}, nil
}

// ─── Worker protocol ────────────────────────────────────────────────────────────

func (w *WorkflowService) PollWorkflowTask(ctx context.Context, req *rotav1.PollTaskRequest) (*rotav1.PolledWorkflowTask, error) {
	lr, ok, err := w.n.LeaseOne(fsm.WorkflowLanePrefix+req.GetTaskType(), req.GetConsumerId())
	if err != nil {
		return nil, err
	}
	if !ok {
		return &rotav1.PolledWorkflowTask{Empty: true}, nil
	}
	ref := &rotav1.WorkflowTaskRef{}
	run, found := (*rotav1.WorkflowRun)(nil), false
	if proto.Unmarshal(lr.Payload, ref) == nil {
		run, found = w.n.GetRun(ref.GetRunId())
	}
	if !found || run.GetStatus() != rotav1.WorkflowStatus_WF_RUNNING {
		_ = w.n.Ack(lr.LeaseID) // stale/closed task: drop it
		return &rotav1.PolledWorkflowTask{Empty: true}, nil
	}
	history, err := w.n.GetRunHistory(ref.GetRunId())
	if err != nil {
		return nil, err
	}
	return &rotav1.PolledWorkflowTask{
		RunId: ref.GetRunId(), LeaseId: lr.LeaseID,
		RunEpoch: run.GetRunEpoch(), HistorySeq: run.GetCurHistorySeq(), History: history,
	}, nil
}

func (w *WorkflowService) RespondWorkflowTask(ctx context.Context, req *rotav1.RespondWorkflowTaskRequest) (*rotav1.RespondWorkflowTaskResponse, error) {
	cmds := make([]node.WorkflowCommand, 0, len(req.GetCommands()))
	for _, c := range req.GetCommands() {
		cmds = append(cmds, node.WorkflowCommand{
			Kind: c.GetKind(), ActivityType: c.GetActivityType(),
			Input: c.GetInput(), Result: c.GetResult(), DelayMs: c.GetDelayMs(),
		})
	}
	res, err := w.n.CompleteWorkflowTask(req.GetRunId(), req.GetRunEpoch(), req.GetHistorySeq(), req.GetPrefixChecksum(), cmds)
	if err != nil {
		return nil, err
	}
	_ = w.n.Ack(req.GetLeaseId())
	return &rotav1.RespondWorkflowTaskResponse{Applied: res.Applied, Reason: res.Reason}, nil
}

func (w *WorkflowService) PollActivityTask(ctx context.Context, req *rotav1.PollTaskRequest) (*rotav1.PolledActivityTask, error) {
	lr, ok, err := w.n.LeaseOne(fsm.ActivityLanePrefix+req.GetTaskType(), req.GetConsumerId())
	if err != nil {
		return nil, err
	}
	if !ok {
		return &rotav1.PolledActivityTask{Empty: true}, nil
	}
	task := &rotav1.ActivityTask{}
	if proto.Unmarshal(lr.Payload, task) != nil {
		_ = w.n.Ack(lr.LeaseID)
		return &rotav1.PolledActivityTask{Empty: true}, nil
	}
	return &rotav1.PolledActivityTask{
		RunId: task.GetRunId(), LeaseId: lr.LeaseID, ScheduledEventId: task.GetScheduledEventId(),
		ActivityType: task.GetActivityType(), Input: task.GetInput(),
	}, nil
}

func (w *WorkflowService) RespondActivityTask(ctx context.Context, req *rotav1.RespondActivityTaskRequest) (*rotav1.RespondActivityTaskResponse, error) {
	if _, err := w.n.CompleteActivityTask(req.GetRunId(), req.GetScheduledEventId(), req.GetSuccess(), req.GetResult()); err != nil {
		return nil, err
	}
	_ = w.n.Ack(req.GetLeaseId())
	return &rotav1.RespondActivityTaskResponse{}, nil
}
