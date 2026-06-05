package transport

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/node"
)

// notLeaderMetaKey carries a serialized NotLeader detail in the gRPC trailer so
// a client can redial the leader. A "-bin" suffix marks binary metadata.
const notLeaderMetaKey = "not-leader-bin"

// requireLeader returns a NOT_LEADER gRPC error if this node is not the raft
// leader, else nil. The error is FAILED_PRECONDITION with a NotLeader detail
// (the leader's gRPC address) in the trailer AND in the message string, so both
// structured and string-parsing clients can redirect. Mutating RPCs guard on it
// so a follower redirects instead of failing opaquely.
func requireLeader(ctx context.Context, n *node.Node) error {
	if n.IsLeader() {
		return nil
	}
	addr, id := n.LeaderHint()
	if raw, err := proto.Marshal(&rotav1.NotLeader{LeaderAddr: addr, LeaderId: id}); err == nil {
		_ = grpc.SetTrailer(ctx, metadata.Pairs(notLeaderMetaKey, string(raw)))
	}
	return status.Errorf(codes.FailedPrecondition, "not_leader: %s", addr)
}

// notLeaderFrame builds an in-stream NOT_LEADER error frame for the Work stream,
// carrying the leader's gRPC address so the consumer can reconnect to it.
func notLeaderFrame(n *node.Node) *rotav1.WorkServerMsg {
	addr, _ := n.LeaderHint()
	return &rotav1.WorkServerMsg{Msg: &rotav1.WorkServerMsg_Error{
		Error: &rotav1.StreamError{Code: rotav1.ErrorCode_NOT_LEADER, LeaderAddr: addr, Detail: "not leader"},
	}}
}

// mutatingMethods are the unary RPCs that change replicated or leader-local
// state and therefore must run on the leader. Reads are intentionally absent
// (followers can serve them from their local replica / report their own state).
var mutatingMethods = map[string]bool{
	"/rota.v1.Broker/Publish":                true,
	"/rota.v1.Broker/PublishBatch":           true,
	"/rota.v1.Control/SetGroupConfig":        true,
	"/rota.v1.Control/PauseGroup":            true,
	"/rota.v1.Control/ResumeGroup":           true,
	"/rota.v1.Control/CancelGroup":           true,
	"/rota.v1.Control/PurgeGroup":            true,
	"/rota.v1.Control/ReapGroup":             true,
	"/rota.v1.Control/TeardownGroup":         true,
	"/rota.v1.Control/SetPolicy":             true,
	"/rota.v1.Control/ScheduleCron":          true,
	"/rota.v1.Control/DeleteCron":            true,
	"/rota.v1.Control/PauseCron":             true,
	"/rota.v1.Control/AcquireSingletonLease": true,
	"/rota.v1.Control/RenewSingletonLease":   true,
	"/rota.v1.Control/ReleaseSingletonLease": true,
	"/rota.v1.Control/CompleteByToken":       true,
	"/rota.v1.Control/SetLaneConfig":         true,
	"/rota.v1.Control/PauseLane":             true,
	"/rota.v1.Control/ResumeLane":            true,
	"/rota.v1.Control/RedriveDeadLetter":     true,
	"/rota.v1.Workflow/StartWorkflow":        true,
	"/rota.v1.Workflow/SignalWorkflow":       true,
	"/rota.v1.Workflow/CancelWorkflow":       true,
	"/rota.v1.Workflow/PollWorkflowTask":     true,
	"/rota.v1.Workflow/RespondWorkflowTask":  true,
	"/rota.v1.Workflow/PollActivityTask":     true,
	"/rota.v1.Workflow/RespondActivityTask":  true,
}

// LeaderGuardInterceptor redirects mutating unary RPCs to the leader (NOT_LEADER
// status + redirect detail) when this node is a follower. Centralizes the guard
// so individual handlers stay focused on their logic.
func LeaderGuardInterceptor(n *node.Node) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if mutatingMethods[info.FullMethod] {
			if err := requireLeader(ctx, n); err != nil {
				return nil, err
			}
		}
		return handler(ctx, req)
	}
}
