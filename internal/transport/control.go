package transport

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/fsm"
	"github.com/tinkerhaus/rota/internal/node"
	"github.com/tinkerhaus/rota/internal/policy"
)

// ControlService implements the rota.v1 Control plane over a Node.
type ControlService struct {
	rotav1.UnimplementedControlServer
	n *node.Node
}

func NewControl(n *node.Node) *ControlService { return &ControlService{n: n} }

// ─── Group config + lifecycle ──────────────────────────────────────────────────

func (c *ControlService) SetGroupConfig(ctx context.Context, req *rotav1.SetGroupConfigRequest) (*rotav1.GroupConfig, error) {
	var w *float64
	if req.Weight != nil {
		v := req.GetWeight()
		w = &v
	}
	var bs *uint32
	if req.BatchSize != nil {
		v := req.GetBatchSize()
		bs = &v
	}
	if err := c.n.SetGroupConfig(req.GetLane(), req.GetGroupId(), w, bs); err != nil {
		return nil, err
	}
	return c.groupConfig(req.GetLane(), req.GetGroupId()), nil
}

func (c *ControlService) GetGroupConfig(ctx context.Context, req *rotav1.GroupRef) (*rotav1.GroupConfig, error) {
	return c.groupConfig(req.GetLane(), req.GetGroupId()), nil
}

func (c *ControlService) groupConfig(lane, group string) *rotav1.GroupConfig {
	gc := c.n.GroupConfig(lane, group)
	return &rotav1.GroupConfig{Lane: lane, GroupId: group, Weight: gc.Weight, BatchSize: gc.BatchSize, Paused: gc.Paused}
}

func (c *ControlService) PauseGroup(ctx context.Context, req *rotav1.GroupRef) (*rotav1.GroupConfig, error) {
	if err := c.n.PauseGroup(req.GetLane(), req.GetGroupId()); err != nil {
		return nil, err
	}
	return c.groupConfig(req.GetLane(), req.GetGroupId()), nil
}

func (c *ControlService) ResumeGroup(ctx context.Context, req *rotav1.GroupRef) (*rotav1.GroupConfig, error) {
	if err := c.n.ResumeGroup(req.GetLane(), req.GetGroupId()); err != nil {
		return nil, err
	}
	return c.groupConfig(req.GetLane(), req.GetGroupId()), nil
}

func (c *ControlService) CancelGroup(ctx context.Context, req *rotav1.GroupRef) (*rotav1.GroupOpResult, error) {
	aff, err := c.n.CancelGroup(req.GetLane(), req.GetGroupId())
	if err != nil {
		return nil, err
	}
	return &rotav1.GroupOpResult{AffectedMessages: aff}, nil
}

func (c *ControlService) PurgeGroup(ctx context.Context, req *rotav1.GroupRef) (*rotav1.GroupOpResult, error) {
	aff, err := c.n.PurgeGroup(req.GetLane(), req.GetGroupId())
	if err != nil {
		return nil, err
	}
	return &rotav1.GroupOpResult{AffectedMessages: aff}, nil
}

// ─── Policy ─────────────────────────────────────────────────────────────────────

func bindingFromProto(src *rotav1.PolicySource) policy.Binding {
	var k policy.Kind
	switch src.GetKind() {
	case rotav1.PolicyKind_DRR:
		k = policy.KindDRR
	case rotav1.PolicyKind_WFQ:
		k = policy.KindWFQ
	case rotav1.PolicyKind_STRICT_PRIORITY:
		k = policy.KindStrictPriority
	case rotav1.PolicyKind_LOTTERY:
		k = policy.KindLottery
	case rotav1.PolicyKind_COMPLETION_AWARE:
		k = policy.KindCompletionAware
	default: // CUSTOM
		if src.GetEngine() == "wasm" {
			k = policy.KindWASM
		} else {
			k = policy.KindCEL
		}
	}
	return policy.Binding{Kind: k, Source: src.GetCode()}
}

func protoFromBinding(b policy.Binding) *rotav1.PolicySource {
	src := &rotav1.PolicySource{Code: b.Source, Engine: "builtin"}
	switch b.Kind {
	case policy.KindWFQ:
		src.Kind = rotav1.PolicyKind_WFQ
	case policy.KindStrictPriority:
		src.Kind = rotav1.PolicyKind_STRICT_PRIORITY
	case policy.KindLottery:
		src.Kind = rotav1.PolicyKind_LOTTERY
	case policy.KindCompletionAware:
		src.Kind = rotav1.PolicyKind_COMPLETION_AWARE
	case policy.KindCEL:
		src.Kind, src.Engine = rotav1.PolicyKind_CUSTOM, "cel"
	case policy.KindWASM:
		src.Kind, src.Engine = rotav1.PolicyKind_CUSTOM, "wasm"
	default:
		src.Kind = rotav1.PolicyKind_DRR
	}
	return src
}

func (c *ControlService) SetPolicy(ctx context.Context, req *rotav1.SetPolicyRequest) (*rotav1.PolicyInfo, error) {
	if err := c.n.SetPolicy(req.GetLane(), bindingFromProto(req.GetSource())); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return c.GetPolicy(ctx, &rotav1.LaneRef{Lane: req.GetLane()})
}

func (c *ControlService) GetPolicy(ctx context.Context, req *rotav1.LaneRef) (*rotav1.PolicyInfo, error) {
	b, ok := c.n.GetPolicy(req.GetLane())
	if !ok {
		return &rotav1.PolicyInfo{Lane: req.GetLane(), Source: &rotav1.PolicySource{Kind: rotav1.PolicyKind_DRR}}, nil
	}
	return &rotav1.PolicyInfo{
		Lane: req.GetLane(), Source: protoFromBinding(b), PolicyVersion: b.Version,
		SourceHash: b.Hash, Quarantined: c.n.PolicyQuarantined(req.GetLane()),
	}, nil
}

func (c *ControlService) ValidatePolicy(ctx context.Context, req *rotav1.SetPolicyRequest) (*rotav1.ValidatePolicyResult, error) {
	if err := c.n.ValidatePolicy(bindingFromProto(req.GetSource())); err != nil {
		return &rotav1.ValidatePolicyResult{Ok: false, Diagnostics: []string{err.Error()}}, nil
	}
	return &rotav1.ValidatePolicyResult{Ok: true}, nil
}

// ─── Cron ───────────────────────────────────────────────────────────────────────

func (c *ControlService) ScheduleCron(ctx context.Context, req *rotav1.ScheduleCronRequest) (*rotav1.CronInfo, error) {
	if err := c.n.ScheduleCron(req.GetCronId(), req.GetLane(), req.GetGroupId(), req.GetPayload(), req.GetHeaders(), req.GetSchedule()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &rotav1.CronInfo{CronId: req.GetCronId(), Lane: req.GetLane(), Schedule: req.GetSchedule()}, nil
}

func (c *ControlService) ListCron(ctx context.Context, req *rotav1.ListCronRequest) (*rotav1.ListCronResponse, error) {
	specs, err := c.n.ListCron()
	if err != nil {
		return nil, err
	}
	resp := &rotav1.ListCronResponse{}
	for _, s := range specs {
		if req.GetLane() != "" && s.Lane != req.GetLane() {
			continue
		}
		resp.Crons = append(resp.Crons, cronInfo(s))
	}
	return resp, nil
}

func (c *ControlService) DeleteCron(ctx context.Context, req *rotav1.CronRef) (*rotav1.CronOpResult, error) {
	if err := c.n.DeleteCron(req.GetCronId()); err != nil {
		return nil, err
	}
	return &rotav1.CronOpResult{Existed: true}, nil
}

func (c *ControlService) PauseCron(ctx context.Context, req *rotav1.CronRef) (*rotav1.CronInfo, error) {
	if err := c.n.PauseCron(req.GetCronId()); err != nil {
		return nil, err
	}
	s, ok := c.n.GetCron(req.GetCronId())
	if !ok {
		return nil, status.Error(codes.NotFound, "cron not found")
	}
	return cronInfo(s), nil
}

func cronInfo(s fsm.CronSpec) *rotav1.CronInfo {
	return &rotav1.CronInfo{
		CronId: s.CronID, Lane: s.Lane, Schedule: s.Schedule,
		NextFireMs: s.NextFireMs, LastFireMs: s.LastFireMs, Paused: s.Paused,
	}
}

// ─── Singleton ──────────────────────────────────────────────────────────────────

func (c *ControlService) AcquireSingletonLease(ctx context.Context, req *rotav1.AcquireSingletonRequest) (*rotav1.SingletonLease, error) {
	ttl := uint64(req.GetTtl().AsDuration().Milliseconds())
	fence, ok, err := c.n.AcquireSingleton(req.GetName(), req.GetHolder(), ttl)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "singleton lease is held by another holder")
	}
	return &rotav1.SingletonLease{Name: req.GetName(), Holder: req.GetHolder(), Fence: fence}, nil
}

func (c *ControlService) RenewSingletonLease(ctx context.Context, req *rotav1.RenewSingletonRequest) (*rotav1.SingletonLease, error) {
	ttl := uint64(req.GetTtl().AsDuration().Milliseconds())
	ok, err := c.n.RenewSingleton(req.GetName(), req.GetHolder(), req.GetFence(), ttl)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "renew rejected (stale holder or fence)")
	}
	return &rotav1.SingletonLease{Name: req.GetName(), Holder: req.GetHolder(), Fence: req.GetFence()}, nil
}

func (c *ControlService) ReleaseSingletonLease(ctx context.Context, req *rotav1.ReleaseSingletonRequest) (*rotav1.SingletonOpResult, error) {
	ok, err := c.n.ReleaseSingleton(req.GetName(), req.GetHolder(), req.GetFence())
	if err != nil {
		return nil, err
	}
	return &rotav1.SingletonOpResult{Released: ok}, nil
}

// ─── Async completion + introspection ──────────────────────────────────────────

func (c *ControlService) CompleteByToken(ctx context.Context, req *rotav1.CompleteByTokenRequest) (*rotav1.CompleteResult, error) {
	_, unknown, err := c.n.Complete(req.GetExternalToken(), req.GetOutcome() == rotav1.Outcome_SUCCESS, req.GetResultMeta(), durationMs(req.GetDelay()))
	if err != nil {
		return nil, err
	}
	return &rotav1.CompleteResult{Resolved: !unknown, UnknownToken: unknown}, nil
}

func (c *ControlService) GetStats(ctx context.Context, req *rotav1.GetStatsRequest) (*rotav1.StatsResponse, error) {
	stats, err := c.n.Stats(req.GetLane())
	if err != nil {
		return nil, err
	}
	resp := &rotav1.StatsResponse{}
	for _, s := range stats {
		// publish_rate/lease_rate are smoothed (EWMA) events/sec from the leader's
		// per-lane meter; depths are a point-in-time aggregate over group metadata.
		// ack_rate/oldest_age_ms remain 0 (not yet metered).
		resp.Lanes = append(resp.Lanes, &rotav1.LaneStats{
			Lane: s.Lane, Leasable: s.Leasable, Delayed: s.Delayed, Inflight: s.Inflight,
			DlqDepth: s.DLQ, GroupCount: s.GroupCount, PolicyVersion: s.PolicyVersion,
			PublishRate: s.PublishRate, LeaseRate: s.LeaseRate,
		})
	}
	return resp, nil
}

func (c *ControlService) ListGroups(ctx context.Context, req *rotav1.ListGroupsRequest) (*rotav1.ListGroupsResponse, error) {
	groups, next, err := c.n.ListGroupStats(req.GetLane(), req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	return &rotav1.ListGroupsResponse{Groups: groups, NextPageToken: next}, nil
}

func (c *ControlService) ListDeadLetters(ctx context.Context, req *rotav1.ListDeadLettersRequest) (*rotav1.ListDeadLettersResponse, error) {
	dls, next, err := c.n.ListDeadLetters(req.GetLane(), req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	return &rotav1.ListDeadLettersResponse{DeadLetters: dls, NextPageToken: next}, nil
}

func (c *ControlService) ListLeases(ctx context.Context, req *rotav1.ListLeasesRequest) (*rotav1.ListLeasesResponse, error) {
	leases, next, err := c.n.ListLeases(req.GetLane(), req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	return &rotav1.ListLeasesResponse{Leases: leases, NextPageToken: next}, nil
}

func (c *ControlService) PeekMessages(ctx context.Context, req *rotav1.PeekMessagesRequest) (*rotav1.PeekMessagesResponse, error) {
	msgs, err := c.n.PeekMessages(req.GetLane(), req.GetGroupId(), req.GetLimit())
	if err != nil {
		return nil, err
	}
	return &rotav1.PeekMessagesResponse{Messages: msgs}, nil
}

func (c *ControlService) GetLaneFairness(ctx context.Context, req *rotav1.LaneRef) (*rotav1.LaneFairness, error) {
	return c.n.LaneFairness(req.GetLane())
}

func (c *ControlService) GetPolicyHealth(ctx context.Context, req *rotav1.LaneRef) (*rotav1.PolicyHealth, error) {
	return c.n.PolicyHealth(req.GetLane()), nil
}

func (c *ControlService) RedriveDeadLetter(ctx context.Context, req *rotav1.RedriveDeadLetterRequest) (*rotav1.RedriveDeadLetterResponse, error) {
	ok, newID, err := c.n.RedriveDeadLetter(req.GetLane(), req.GetGroupId(), req.GetMsgId())
	if err != nil {
		return nil, err
	}
	return &rotav1.RedriveDeadLetterResponse{Ok: ok, NewMsgId: newID}, nil
}

func (c *ControlService) DescribeCluster(ctx context.Context, req *rotav1.DescribeClusterRequest) (*rotav1.ClusterInfo, error) {
	ci := c.n.ClusterInfo()
	out := &rotav1.ClusterInfo{LeaderId: ci.LeaderID, LeaderAddr: ci.LeaderAddr, Term: ci.Term, AppliedIndex: ci.AppliedIndex}
	for _, p := range ci.Peers {
		out.Peers = append(out.Peers, &rotav1.PeerInfo{Id: p.ID, Addr: p.Addr, Suffrage: p.Suffrage})
	}
	return out, nil
}

func (c *ControlService) Health(ctx context.Context, req *rotav1.HealthRequest) (*rotav1.HealthResponse, error) {
	serving, quorum, leader := c.n.Health()
	return &rotav1.HealthResponse{Serving: serving, HasQuorum: quorum, IsLeader: leader}, nil
}

// ─── Lane config + back-pressure + reap/teardown ───────────────────────────────

func (c *ControlService) SetLaneConfig(ctx context.Context, req *rotav1.SetLaneConfigRequest) (*rotav1.LaneConfig, error) {
	if err := c.n.SetLaneRateLimit(req.GetLane(), req.GetRatePerSec(), req.GetBurst()); err != nil {
		return nil, err
	}
	return &rotav1.LaneConfig{
		Lane: req.GetLane(), RatePerSec: req.GetRatePerSec(), Burst: req.GetBurst(),
		Paused: c.n.LanePaused(req.GetLane()),
	}, nil
}

func (c *ControlService) PauseLane(ctx context.Context, req *rotav1.PauseLaneRequest) (*rotav1.LaneOpResult, error) {
	c.n.PauseLane(req.GetLane(), uint64(req.GetDuration().AsDuration().Milliseconds()))
	return &rotav1.LaneOpResult{Ok: true}, nil
}

func (c *ControlService) ResumeLane(ctx context.Context, req *rotav1.LaneRef) (*rotav1.LaneOpResult, error) {
	c.n.ResumeLane(req.GetLane())
	return &rotav1.LaneOpResult{Ok: true}, nil
}

func (c *ControlService) ReapGroup(ctx context.Context, req *rotav1.GroupRef) (*rotav1.GroupOpResult, error) {
	ok, err := c.n.ReapGroup(req.GetLane(), req.GetGroupId())
	if err != nil {
		return nil, err
	}
	var aff uint64
	if ok {
		aff = 1
	}
	return &rotav1.GroupOpResult{AffectedMessages: aff}, nil
}

func (c *ControlService) TeardownGroup(ctx context.Context, req *rotav1.TeardownRequest) (*rotav1.TeardownResult, error) {
	lanes, aff, err := c.n.TeardownGroup(req.GetGroupId())
	if err != nil {
		return nil, err
	}
	return &rotav1.TeardownResult{AffectedLanes: lanes, AffectedMessages: aff}, nil
}
