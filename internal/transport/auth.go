package transport

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/fsm"
	"github.com/tinkerhaus/rota/internal/node"
)

// AuthUnaryInterceptor enforces replicated principal/grant auth for Rota service
// methods. Non-Rota services such as gRPC health and reflection remain open so
// probes and tooling can still discover a node.
func AuthUnaryInterceptor(n *node.Node) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if err := requireAuth(ctx, n, info.FullMethod, checksForUnary(n, info.FullMethod, req)); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// AuthStreamInterceptor is the streaming twin of AuthUnaryInterceptor. For Work,
// lane/group authorization is checked on each LeaseRequest frame; settlement
// frames are accepted only after the stream has made an authorized lease request.
func AuthStreamInterceptor(n *node.Node) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !strings.HasPrefix(info.FullMethod, "/rota.v1.") {
			return handler(srv, stream)
		}
		return handler(srv, &authzServerStream{ServerStream: stream, n: n, method: info.FullMethod})
	}
}

func requireAuth(ctx context.Context, n *node.Node, method string, checks []node.AuthCheck) error {
	if !strings.HasPrefix(method, "/rota.v1.") {
		return nil
	}
	decision, err := n.AuthorizeToken(authTokenFromContext(ctx), checks)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if !decision.Enabled || decision.Allowed {
		return nil
	}
	if !decision.Known {
		return status.Error(codes.Unauthenticated, "missing or invalid rota auth token")
	}
	if decision.Reason != "" {
		return status.Error(codes.PermissionDenied, "rota auth token lacks permission: "+decision.Reason)
	}
	return status.Error(codes.PermissionDenied, "rota auth token lacks permission")
}

func authTokenFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	for _, got := range md.Get("x-rota-token") {
		if got = strings.TrimSpace(got); got != "" {
			return got
		}
	}
	for _, got := range md.Get("authorization") {
		got = strings.TrimSpace(got)
		if strings.HasPrefix(got, "Bearer ") {
			if token := strings.TrimSpace(strings.TrimPrefix(got, "Bearer ")); token != "" {
				return token
			}
		}
	}
	return ""
}

type authzServerStream struct {
	grpc.ServerStream
	n              *node.Node
	method         string
	workAuthorized bool
}

func (s *authzServerStream) RecvMsg(m interface{}) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	checks, requiresPriorWorkAuth := checksForStreamMessage(s.method, m)
	if requiresPriorWorkAuth && !s.workAuthorized {
		enabled, err := s.n.AuthEnabled()
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		if enabled {
			return status.Error(codes.PermissionDenied, "work settlement requires an authorized lease request first")
		}
	}
	if len(checks) == 0 {
		return nil
	}
	if err := requireAuth(s.Context(), s.n, s.method, checks); err != nil {
		return err
	}
	if s.method == rotav1.Broker_Work_FullMethodName {
		s.workAuthorized = true
	}
	return nil
}

func checksForStreamMessage(method string, msg interface{}) ([]node.AuthCheck, bool) {
	if method != rotav1.Broker_Work_FullMethodName {
		return []node.AuthCheck{{Action: rotav1.AuthAction_AUTH_ADMIN}}, false
	}
	cm, ok := msg.(*rotav1.WorkClientMsg)
	if !ok || cm == nil {
		return nil, false
	}
	switch x := cm.Msg.(type) {
	case *rotav1.WorkClientMsg_LeaseRequest:
		req := x.LeaseRequest
		if req == nil {
			return nil, false
		}
		if len(req.GetGroupAllow()) == 0 {
			return []node.AuthCheck{{Action: rotav1.AuthAction_AUTH_CONSUME, Lane: req.GetLane()}}, false
		}
		checks := make([]node.AuthCheck, 0, len(req.GetGroupAllow()))
		for _, group := range req.GetGroupAllow() {
			checks = append(checks, node.AuthCheck{Action: rotav1.AuthAction_AUTH_CONSUME, Lane: req.GetLane(), Group: group})
		}
		return checks, false
	case *rotav1.WorkClientMsg_Ack, *rotav1.WorkClientMsg_Nack, *rotav1.WorkClientMsg_Extend, *rotav1.WorkClientMsg_Complete:
		return nil, true
	default:
		return nil, false
	}
}

func checksForUnary(n *node.Node, method string, req interface{}) []node.AuthCheck {
	switch method {
	case rotav1.Broker_Publish_FullMethodName:
		r, _ := req.(*rotav1.PublishRequest)
		return checksForMessageSpec(rotav1.AuthAction_AUTH_PUBLISH, r.GetMessage())
	case rotav1.Broker_PublishBatch_FullMethodName:
		r, _ := req.(*rotav1.PublishBatchRequest)
		var checks []node.AuthCheck
		for _, msg := range r.GetMessages() {
			checks = append(checks, checksForMessageSpec(rotav1.AuthAction_AUTH_PUBLISH, msg)...)
		}
		return checks

	case rotav1.Control_GetGroupConfig_FullMethodName:
		return checksForGroupRef(rotav1.AuthAction_AUTH_READ, req)
	case rotav1.Control_SetGroupConfig_FullMethodName:
		r, _ := req.(*rotav1.SetGroupConfigRequest)
		return one(rotav1.AuthAction_AUTH_CONFIGURE, r.GetLane(), r.GetGroupId())
	case rotav1.Control_PauseGroup_FullMethodName, rotav1.Control_ResumeGroup_FullMethodName,
		rotav1.Control_CancelGroup_FullMethodName, rotav1.Control_PurgeGroup_FullMethodName,
		rotav1.Control_ReapGroup_FullMethodName:
		return checksForGroupRef(rotav1.AuthAction_AUTH_CONFIGURE, req)
	case rotav1.Control_TeardownGroup_FullMethodName:
		r, _ := req.(*rotav1.TeardownRequest)
		return one(rotav1.AuthAction_AUTH_CONFIGURE, "", r.GetGroupId())
	case rotav1.Control_GetPolicy_FullMethodName, rotav1.Control_GetLaneFairness_FullMethodName,
		rotav1.Control_GetPolicyHealth_FullMethodName:
		return checksForLaneRef(rotav1.AuthAction_AUTH_READ, req)
	case rotav1.Control_SetPolicy_FullMethodName, rotav1.Control_ValidatePolicy_FullMethodName:
		r, _ := req.(*rotav1.SetPolicyRequest)
		return one(rotav1.AuthAction_AUTH_CONFIGURE, r.GetLane(), "")
	case rotav1.Control_SetLaneConfig_FullMethodName:
		r, _ := req.(*rotav1.SetLaneConfigRequest)
		return one(rotav1.AuthAction_AUTH_CONFIGURE, r.GetLane(), "")
	case rotav1.Control_PauseLane_FullMethodName:
		r, _ := req.(*rotav1.PauseLaneRequest)
		return one(rotav1.AuthAction_AUTH_CONFIGURE, r.GetLane(), "")
	case rotav1.Control_ResumeLane_FullMethodName:
		return checksForLaneRef(rotav1.AuthAction_AUTH_CONFIGURE, req)
	case rotav1.Control_ScheduleCron_FullMethodName:
		r, _ := req.(*rotav1.ScheduleCronRequest)
		return one(rotav1.AuthAction_AUTH_CONFIGURE, r.GetLane(), r.GetGroupId())
	case rotav1.Control_ListCron_FullMethodName:
		r, _ := req.(*rotav1.ListCronRequest)
		return one(rotav1.AuthAction_AUTH_READ, r.GetLane(), "")
	case rotav1.Control_DeleteCron_FullMethodName, rotav1.Control_PauseCron_FullMethodName:
		r, _ := req.(*rotav1.CronRef)
		return one(rotav1.AuthAction_AUTH_CONFIGURE, "__cron", r.GetCronId())
	case rotav1.Control_AcquireSingletonLease_FullMethodName:
		r, _ := req.(*rotav1.AcquireSingletonRequest)
		return one(rotav1.AuthAction_AUTH_CONFIGURE, "__singleton/"+r.GetName(), r.GetHolder())
	case rotav1.Control_RenewSingletonLease_FullMethodName:
		r, _ := req.(*rotav1.RenewSingletonRequest)
		return one(rotav1.AuthAction_AUTH_CONFIGURE, "__singleton/"+r.GetName(), r.GetHolder())
	case rotav1.Control_ReleaseSingletonLease_FullMethodName:
		r, _ := req.(*rotav1.ReleaseSingletonRequest)
		return one(rotav1.AuthAction_AUTH_CONFIGURE, "__singleton/"+r.GetName(), r.GetHolder())
	case rotav1.Control_CompleteByToken_FullMethodName:
		return one(rotav1.AuthAction_AUTH_COMPLETE, "", "")
	case rotav1.Control_GetStats_FullMethodName:
		r, _ := req.(*rotav1.GetStatsRequest)
		return one(rotav1.AuthAction_AUTH_READ, r.GetLane(), r.GetGroupId())
	case rotav1.Control_DescribeCluster_FullMethodName, rotav1.Control_Health_FullMethodName:
		return one(rotav1.AuthAction_AUTH_READ, "", "")
	case rotav1.Control_ListGroups_FullMethodName:
		r, _ := req.(*rotav1.ListGroupsRequest)
		return one(rotav1.AuthAction_AUTH_READ, r.GetLane(), "")
	case rotav1.Control_ListDeadLetters_FullMethodName:
		r, _ := req.(*rotav1.ListDeadLettersRequest)
		return one(rotav1.AuthAction_AUTH_READ, r.GetLane(), "")
	case rotav1.Control_ListLeases_FullMethodName:
		r, _ := req.(*rotav1.ListLeasesRequest)
		return one(rotav1.AuthAction_AUTH_READ, r.GetLane(), "")
	case rotav1.Control_PeekMessages_FullMethodName:
		r, _ := req.(*rotav1.PeekMessagesRequest)
		return one(rotav1.AuthAction_AUTH_READ, r.GetLane(), r.GetGroupId())
	case rotav1.Control_RedriveDeadLetter_FullMethodName:
		r, _ := req.(*rotav1.RedriveDeadLetterRequest)
		return one(rotav1.AuthAction_AUTH_CONFIGURE, r.GetLane(), r.GetGroupId())
	case rotav1.Control_CreatePrincipal_FullMethodName, rotav1.Control_RotatePrincipalToken_FullMethodName,
		rotav1.Control_SetPrincipalDisabled_FullMethodName, rotav1.Control_GrantPrincipal_FullMethodName,
		rotav1.Control_RevokePrincipalGrant_FullMethodName, rotav1.Control_ListPrincipals_FullMethodName:
		return one(rotav1.AuthAction_AUTH_ADMIN, "", "")

	case rotav1.Workflow_StartWorkflow_FullMethodName:
		r, _ := req.(*rotav1.StartWorkflowRequest)
		return one(rotav1.AuthAction_AUTH_WORKFLOW, r.GetWorkflowType(), r.GetTenantId())
	case rotav1.Workflow_PollWorkflowTask_FullMethodName:
		r, _ := req.(*rotav1.PollTaskRequest)
		return one(rotav1.AuthAction_AUTH_WORKFLOW, r.GetTaskType(), "")
	case rotav1.Workflow_PollActivityTask_FullMethodName:
		r, _ := req.(*rotav1.PollTaskRequest)
		return one(rotav1.AuthAction_AUTH_WORKFLOW, fsm.ActivityLanePrefix+r.GetTaskType(), "")
	case rotav1.Workflow_GetWorkflowRun_FullMethodName, rotav1.Workflow_GetWorkflowHistory_FullMethodName:
		r, _ := req.(*rotav1.WorkflowRunRef)
		return checksForRun(n, rotav1.AuthAction_AUTH_READ, r.GetRunId())
	case rotav1.Workflow_SignalWorkflow_FullMethodName:
		r, _ := req.(*rotav1.SignalWorkflowRequest)
		return checksForRun(n, rotav1.AuthAction_AUTH_WORKFLOW, r.GetRunId())
	case rotav1.Workflow_CancelWorkflow_FullMethodName:
		r, _ := req.(*rotav1.CancelWorkflowRequest)
		return checksForRun(n, rotav1.AuthAction_AUTH_WORKFLOW, r.GetRunId())
	case rotav1.Workflow_RespondWorkflowTask_FullMethodName:
		r, _ := req.(*rotav1.RespondWorkflowTaskRequest)
		return checksForRun(n, rotav1.AuthAction_AUTH_WORKFLOW, r.GetRunId())
	case rotav1.Workflow_RespondActivityTask_FullMethodName:
		r, _ := req.(*rotav1.RespondActivityTaskRequest)
		return checksForRun(n, rotav1.AuthAction_AUTH_WORKFLOW, r.GetRunId())
	case rotav1.Workflow_ListWorkflowRuns_FullMethodName:
		return one(rotav1.AuthAction_AUTH_READ, "", "")
	default:
		return one(rotav1.AuthAction_AUTH_ADMIN, "", "")
	}
}

func checksForMessageSpec(action rotav1.AuthAction, msg *rotav1.MessageSpec) []node.AuthCheck {
	if msg == nil {
		return one(action, "", "")
	}
	return one(action, msg.GetLane(), msg.GetGroupId())
}

func checksForGroupRef(action rotav1.AuthAction, req interface{}) []node.AuthCheck {
	r, _ := req.(*rotav1.GroupRef)
	return one(action, r.GetLane(), r.GetGroupId())
}

func checksForLaneRef(action rotav1.AuthAction, req interface{}) []node.AuthCheck {
	r, _ := req.(*rotav1.LaneRef)
	return one(action, r.GetLane(), "")
}

func checksForRun(n *node.Node, action rotav1.AuthAction, runID uint64) []node.AuthCheck {
	run, ok := n.GetRun(runID)
	if !ok {
		return one(action, "", "")
	}
	return one(action, run.GetWorkflowType(), run.GetTenantId())
}

func one(action rotav1.AuthAction, lane, group string) []node.AuthCheck {
	return []node.AuthCheck{{Action: action, Lane: lane, Group: group}}
}
