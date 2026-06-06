// Package transport implements the rota.v1 Broker gRPC service over a Node.
package transport

import (
	"context"
	"io"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/fsm"
	"github.com/tinkerhaus/rota/internal/node"
)

type BrokerService struct {
	rotav1.UnimplementedBrokerServer
	n *node.Node
}

func NewBroker(n *node.Node) *BrokerService { return &BrokerService{n: n} }

func (b *BrokerService) Publish(ctx context.Context, req *rotav1.PublishRequest) (*rotav1.PublishResponse, error) {
	id, err := b.n.Publish(publishReqFromSpec(req.GetMessage()))
	if err != nil {
		return nil, err
	}
	return &rotav1.PublishResponse{MessageId: id}, nil
}

// publishReqFromSpec maps a wire MessageSpec to a node.PublishReq.
func publishReqFromSpec(m *rotav1.MessageSpec) node.PublishReq {
	r := node.PublishReq{
		Lane:          m.GetLane(),
		GroupID:       m.GetGroupId(),
		Payload:       m.GetPayload(),
		Headers:       m.GetHeaders(),
		MaxAttempts:   m.GetMaxAttempts(),
		NotBeforeMs:   resolveNotBefore(m),
		TtlMs:         durationMs(m.GetTtl()),
		IssueToken:    m.GetIssueToken(),
		ExternalToken: m.GetExternalToken(),
		DedupKey:      m.GetDedupKey(),
	}
	if m.Weight != nil {
		w := m.GetWeight()
		r.Weight = &w
	}
	if m.BatchSize != nil {
		bs := m.GetBatchSize()
		r.BatchSize = &bs
	}
	return r
}

// PublishBatch applies many publishes in one Raft entry (atomic) or best-effort.
func (b *BrokerService) PublishBatch(ctx context.Context, req *rotav1.PublishBatchRequest) (*rotav1.PublishBatchResponse, error) {
	reqs := make([]node.PublishReq, 0, len(req.GetMessages()))
	for _, m := range req.GetMessages() {
		reqs = append(reqs, publishReqFromSpec(m))
	}
	items, err := b.n.PublishBatch(reqs, req.GetAtomic())
	if err != nil {
		return nil, err
	}
	resp := &rotav1.PublishBatchResponse{}
	for _, it := range items {
		res := &rotav1.PublishItemResult{MessageId: it.MsgID}
		if !it.OK {
			res.Code = rotav1.ErrorCode_INTERNAL
			res.Detail = it.Err
		}
		resp.Results = append(resp.Results, res)
	}
	return resp, nil
}

// resolveNotBefore turns the not_before oneof (relative delay OR absolute at)
// into a single absolute unix-ms instant. 0 ⇒ eligible now.
func resolveNotBefore(m *rotav1.MessageSpec) uint64 {
	if d := m.GetDelay(); d != nil {
		ms := d.AsDuration().Milliseconds()
		if ms < 0 {
			ms = 0
		}
		return uint64(time.Now().UnixMilli()) + uint64(ms)
	}
	if at := m.GetAt(); at != nil {
		t := at.AsTime().UnixMilli()
		if t < 0 {
			t = 0
		}
		return uint64(t)
	}
	return 0
}

// durationMs converts an optional protobuf Duration to milliseconds (0 if unset
// or non-positive).
func durationMs(d *durationpb.Duration) uint64 {
	if d == nil {
		return 0
	}
	if ms := d.AsDuration().Milliseconds(); ms > 0 {
		return uint64(ms)
	}
	return 0
}

// Work is the bidirectional stream: the client advertises credit and acks/nacks;
// the server fair-leases and streams messages. credit=1 ⇒ strictly serial.
func (b *BrokerService) Work(stream rotav1.Broker_WorkServer) error {
	ctx := stream.Context()
	var lane string
	consumerID := "consumer"
	var groupAllow, groupDeny []string
	credit := 0
	inflight := 0
	lastPaused := false

	recvCh := make(chan *rotav1.WorkClientMsg, 16)
	errCh := make(chan error, 1)
	go func() {
		for {
			m, err := stream.Recv()
			if err != nil {
				errCh <- err
				return
			}
			recvCh <- m
		}
	}()

	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			if err == io.EOF {
				return nil
			}
			return err
		case cm := <-recvCh:
			switch x := cm.Msg.(type) {
			case *rotav1.WorkClientMsg_LeaseRequest:
				// Only the leader can serve work; redirect a follower's consumer.
				if !b.n.IsLeader() {
					_ = stream.Send(notLeaderFrame(b.n))
					return nil
				}
				if l := x.LeaseRequest.GetLane(); l != "" {
					lane = l
				}
				if c := x.LeaseRequest.GetConsumerId(); c != "" {
					consumerID = c
				}
				// Group filters apply for the life of the stream; the latest
				// LeaseRequest wins (nil leaves the prior filters unchanged).
				if ga := x.LeaseRequest.GetGroupAllow(); ga != nil {
					groupAllow = ga
				}
				if gd := x.LeaseRequest.GetGroupDeny(); gd != nil {
					groupDeny = gd
				}
				credit += int(x.LeaseRequest.GetCredit())
			case *rotav1.WorkClientMsg_Ack:
				if inflight > 0 {
					inflight--
				}
				_ = b.n.Ack(x.Ack.GetLeaseId())
			case *rotav1.WorkClientMsg_Nack:
				if inflight > 0 {
					inflight--
				}
				var delayMs uint64
				if d := x.Nack.GetDelay(); d != nil {
					if ms := d.AsDuration().Milliseconds(); ms > 0 {
						delayMs = uint64(ms)
					}
				}
				_, _ = b.n.Nack(x.Nack.GetLeaseId(), fsm.NackMode(x.Nack.GetMode()), delayMs, x.Nack.GetFailureMeta())
			case *rotav1.WorkClientMsg_Extend:
				var ttl uint64
				if d := x.Extend.GetTtl(); d != nil {
					if ms := d.AsDuration().Milliseconds(); ms > 0 {
						ttl = uint64(ms)
					}
				}
				_ = b.n.Extend(x.Extend.GetLeaseId(), ttl)
			case *rotav1.WorkClientMsg_Complete:
				// In-stream complete-by-token: resolve the message by its token.
				if inflight > 0 {
					inflight--
				}
				_, _, _ = b.n.Complete(
					x.Complete.GetExternalToken(),
					x.Complete.GetOutcome() == rotav1.Outcome_SUCCESS,
					x.Complete.GetResultMeta(),
					durationMs(x.Complete.GetDelay()),
				)
			}
		case <-ticker.C:
		}

		// Tell the consumer to back off (or resume) when the lane's pause flips,
		// so a circuit breaker driving PauseLane is observable, not just silent.
		if lane != "" {
			if paused := b.n.LanePaused(lane); paused != lastPaused {
				lastPaused = paused
				kind := rotav1.ControlKind_RESUME_LANE
				if paused {
					kind = rotav1.ControlKind_PAUSE_LANE
				}
				_ = stream.Send(&rotav1.WorkServerMsg{Msg: &rotav1.WorkServerMsg_Control{
					Control: &rotav1.ControlFrame{Kind: kind, Lane: lane},
				}})
			}
		}

		// Deliver while we have credit and there is fair work to hand out.
		for lane != "" && inflight < credit {
			lr, ok, err := b.n.LeaseOneFiltered(lane, consumerID, groupAllow, groupDeny)
			if err != nil {
				// Leadership lost mid-stream: redirect rather than fail opaquely.
				if !b.n.IsLeader() {
					_ = stream.Send(notLeaderFrame(b.n))
					return nil
				}
				return err
			}
			if !ok {
				break
			}
			out := &rotav1.WorkServerMsg{Msg: &rotav1.WorkServerMsg_Lease{Lease: &rotav1.LeasedMessage{
				LeaseId:            lr.LeaseID,
				Lane:               lr.Lane,
				GroupId:            lr.GroupID,
				Payload:            lr.Payload,
				Attempt:            lr.Attempt,
				Headers:            lr.Headers,
				MessageId:          lr.MsgID,
				ExternalToken:      lr.ExternalToken, // complete-by-token: empty unless requested
				VisibilityDeadline: timestamppb.New(time.UnixMilli(int64(lr.DeadlineMs))),
			}}}
			if err := stream.Send(out); err != nil {
				return err
			}
			inflight++
		}
	}
}
