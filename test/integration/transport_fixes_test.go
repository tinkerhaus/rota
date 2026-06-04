package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/node"
	"github.com/tinkerhaus/rota/internal/transport"
)

// ─── PublishBatch (atomic + best-effort) ────────────────────────────────────

func TestPublishBatchGRPC(t *testing.T) {
	bc, cc := startServer(t)
	ctx := context.Background()

	mk := func(group string) *rotav1.MessageSpec {
		return &rotav1.MessageSpec{Lane: "b", GroupId: group, Payload: []byte("x")}
	}
	// Atomic batch: 6 messages across two groups in ONE Raft write.
	resp, err := bc.PublishBatch(ctx, &rotav1.PublishBatchRequest{
		Atomic:   true,
		Messages: []*rotav1.MessageSpec{mk("g1"), mk("g1"), mk("g1"), mk("g2"), mk("g2"), mk("g2")},
	})
	if err != nil {
		t.Fatalf("PublishBatch atomic: %v", err)
	}
	if len(resp.GetResults()) != 6 {
		t.Fatalf("results = %d, want 6", len(resp.GetResults()))
	}
	// Same-group items must get DISTINCT per-group ids (1,2,3), not collisions.
	seen := map[uint64]bool{}
	for i, r := range resp.GetResults() {
		if r.GetCode() != rotav1.ErrorCode_OK {
			t.Fatalf("item %d not OK: %v", i, r.GetDetail())
		}
		if i < 3 { // first three are g1
			if r.GetMessageId() == 0 || seen[r.GetMessageId()] {
				t.Fatalf("g1 item %d duplicate/zero id %d", i, r.GetMessageId())
			}
			seen[r.GetMessageId()] = true
		}
	}

	// Best-effort batch: 4 more.
	if _, err := bc.PublishBatch(ctx, &rotav1.PublishBatchRequest{
		Messages: []*rotav1.MessageSpec{mk("g1"), mk("g2"), mk("g3"), mk("g3")},
	}); err != nil {
		t.Fatalf("PublishBatch best-effort: %v", err)
	}

	st, _ := cc.GetStats(ctx, &rotav1.GetStatsRequest{Lane: "b"})
	if len(st.GetLanes()) != 1 || st.GetLanes()[0].GetLeasable() != 10 {
		t.Fatalf("lane b leasable = %+v, want 10", st.GetLanes())
	}
}

// ─── Complete-by-token over the Work stream ─────────────────────────────────

func TestCompleteByTokenOverWork(t *testing.T) {
	bc, cc := startServer(t)
	ctx := context.Background()

	// (a) issue_token: the broker mints a token, delivered on the lease; resolve
	// it off-stream via the Control CompleteByToken RPC.
	if _, err := bc.Publish(ctx, &rotav1.PublishRequest{Message: &rotav1.MessageSpec{
		Lane: "ct", GroupId: "g", Payload: []byte("async"), IssueToken: true,
	}}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	stream, err := bc.Work(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(&rotav1.WorkClientMsg{Msg: &rotav1.WorkClientMsg_LeaseRequest{
		LeaseRequest: &rotav1.LeaseRequest{Lane: "ct", Credit: 1, ConsumerId: "c"},
	}})
	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("work recv: %v", err)
	}
	lease := msg.GetLease()
	if lease == nil || len(lease.GetExternalToken()) == 0 {
		t.Fatalf("no completion token delivered on lease: %+v", msg)
	}
	res, err := cc.CompleteByToken(ctx, &rotav1.CompleteByTokenRequest{
		ExternalToken: lease.GetExternalToken(), Outcome: rotav1.Outcome_SUCCESS,
	})
	if err != nil || !res.GetResolved() {
		t.Fatalf("CompleteByToken: err=%v resolved=%v", err, res.GetResolved())
	}
	if got := laneInflight(t, cc, "ct"); got != 0 {
		t.Fatalf("inflight after complete = %d, want 0", got)
	}

	// (b) in-stream Complete frame on the Work stream resolves another message.
	if _, err := bc.Publish(ctx, &rotav1.PublishRequest{Message: &rotav1.MessageSpec{
		Lane: "ct", GroupId: "g", Payload: []byte("async2"), IssueToken: true,
	}}); err != nil {
		t.Fatalf("publish2: %v", err)
	}
	_ = stream.Send(&rotav1.WorkClientMsg{Msg: &rotav1.WorkClientMsg_LeaseRequest{
		LeaseRequest: &rotav1.LeaseRequest{Lane: "ct", Credit: 1, ConsumerId: "c"},
	}})
	msg2, err := stream.Recv()
	if err != nil {
		t.Fatalf("work recv2: %v", err)
	}
	tok2 := msg2.GetLease().GetExternalToken()
	if len(tok2) == 0 {
		t.Fatal("no token on second lease")
	}
	_ = stream.Send(&rotav1.WorkClientMsg{Msg: &rotav1.WorkClientMsg_Complete{
		Complete: &rotav1.Complete{ExternalToken: tok2, Outcome: rotav1.Outcome_SUCCESS},
	}})
	// The in-stream complete is async; poll until inflight drains.
	deadline := time.Now().Add(3 * time.Second)
	for laneInflight(t, cc, "ct") != 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := laneInflight(t, cc, "ct"); got != 0 {
		t.Fatalf("inflight after in-stream complete = %d, want 0", got)
	}
}

func laneInflight(t *testing.T, cc rotav1.ControlClient, lane string) uint64 {
	t.Helper()
	st, _ := cc.GetStats(context.Background(), &rotav1.GetStatsRequest{Lane: lane})
	for _, l := range st.GetLanes() {
		if l.GetLane() == lane {
			return l.GetInflight()
		}
	}
	return 0
}

// ─── Delayed publish reflected in the `delayed` stat ────────────────────────

func TestDelayedStat(t *testing.T) {
	bc, cc := startServer(t)
	ctx := context.Background()

	if _, err := bc.Publish(ctx, &rotav1.PublishRequest{Message: &rotav1.MessageSpec{
		Lane: "d", GroupId: "g", Payload: []byte("later"),
		NotBefore: &rotav1.MessageSpec_Delay{Delay: durationpb.New(time.Second)},
	}}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	st, _ := cc.GetStats(ctx, &rotav1.GetStatsRequest{Lane: "d"})
	if len(st.GetLanes()) != 1 || st.GetLanes()[0].GetDelayed() != 1 || st.GetLanes()[0].GetLeasable() != 0 {
		t.Fatalf("after delayed publish: %+v, want delayed=1 leasable=0", st.GetLanes())
	}

	// The leader's timer sweep promotes it to READY after the delay.
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		st, _ = cc.GetStats(ctx, &rotav1.GetStatsRequest{Lane: "d"})
		if len(st.GetLanes()) == 1 && st.GetLanes()[0].GetDelayed() == 0 && st.GetLanes()[0].GetLeasable() == 1 {
			return // PASS
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("delayed message never became leasable: %+v", st.GetLanes())
}

// ─── Leader redirect (NOT_LEADER) on a 3-node cluster ───────────────────────

type clusterNode struct {
	id       string
	n        *node.Node
	grpcAddr string
	bc       rotav1.BrokerClient
	cc       rotav1.ControlClient
}

func startGRPCCluster(t *testing.T, size int) map[string]*clusterNode {
	t.Helper()
	ids := []string{"n1", "n2", "n3"}[:size]
	grpcAddrs := map[string]string{}
	lisByID := map[string]net.Listener{}
	var peers []node.Peer
	for _, id := range ids {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lisByID[id] = lis
		grpcAddrs[id] = lis.Addr().String()
		peers = append(peers, node.Peer{ID: id, Addr: freeAddr(t)})
	}
	raftByID := map[string]string{}
	for _, p := range peers {
		raftByID[p.ID] = p.Addr
	}

	out := map[string]*clusterNode{}
	for i, id := range ids {
		cfg := node.Config{
			DataDir: t.TempDir(), NodeID: id, RaftBind: raftByID[id],
			Bootstrap: i == 0, VisibilityMs: 60_000, GRPCAddrs: grpcAddrs,
		}
		if i == 0 {
			cfg.InitialPeers = peers
		}
		n, err := node.Open(cfg)
		if err != nil {
			t.Fatalf("open %s: %v", id, err)
		}
		srv := grpc.NewServer(grpc.UnaryInterceptor(transport.LeaderGuardInterceptor(n)))
		rotav1.RegisterBrokerServer(srv, transport.NewBroker(n))
		rotav1.RegisterControlServer(srv, transport.NewControl(n))
		go func(l net.Listener) { _ = srv.Serve(l) }(lisByID[id])
		conn, err := grpc.NewClient(grpcAddrs[id], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { srv.Stop(); _ = conn.Close(); n.Close() })
		out[id] = &clusterNode{id: id, n: n, grpcAddr: grpcAddrs[id],
			bc: rotav1.NewBrokerClient(conn), cc: rotav1.NewControlClient(conn)}
	}
	if err := out[ids[0]].n.WaitClusterLeader(20 * time.Second); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestLeaderRedirect(t *testing.T) {
	nodes := startGRPCCluster(t, 3)
	ctx := context.Background()

	var leader, follower *clusterNode
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		leader, follower = nil, nil
		for _, cn := range nodes {
			if cn.n.IsLeader() {
				leader = cn
			} else {
				follower = cn
			}
		}
		if leader != nil && follower != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if leader == nil || follower == nil {
		t.Fatal("no stable leader/follower")
	}

	// Unary write to a follower => FAILED_PRECONDITION with the leader's gRPC
	// address in the NotLeader trailer detail.
	var md metadata.MD
	_, err := follower.bc.Publish(ctx, &rotav1.PublishRequest{Message: &rotav1.MessageSpec{
		Lane: "r", GroupId: "g", Payload: []byte("x"),
	}}, grpc.Trailer(&md))
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("follower Publish code = %v (err=%v), want FailedPrecondition", status.Code(err), err)
	}
	addr := ""
	if vals := md.Get("not-leader-bin"); len(vals) > 0 {
		nl := &rotav1.NotLeader{}
		if proto.Unmarshal([]byte(vals[0]), nl) == nil {
			addr = nl.GetLeaderAddr()
		}
	}
	if addr != leader.grpcAddr {
		t.Fatalf("redirect addr = %q, want leader gRPC %q", addr, leader.grpcAddr)
	}

	// Following the redirect (publishing to the leader) succeeds.
	if _, err := leader.bc.Publish(ctx, &rotav1.PublishRequest{Message: &rotav1.MessageSpec{
		Lane: "r", GroupId: "g", Payload: []byte("x"),
	}}); err != nil {
		t.Fatalf("leader Publish after redirect: %v", err)
	}

	// Work stream to a follower => an in-stream NOT_LEADER frame with the addr.
	stream, err := follower.bc.Work(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(&rotav1.WorkClientMsg{Msg: &rotav1.WorkClientMsg_LeaseRequest{
		LeaseRequest: &rotav1.LeaseRequest{Lane: "r", Credit: 1, ConsumerId: "c"},
	}})
	wm, err := stream.Recv()
	if err != nil {
		t.Fatalf("work recv: %v", err)
	}
	se := wm.GetError()
	if se == nil || se.GetCode() != rotav1.ErrorCode_NOT_LEADER || se.GetLeaderAddr() != leader.grpcAddr {
		t.Fatalf("work frame = %+v, want NOT_LEADER -> %s", wm, leader.grpcAddr)
	}
}
