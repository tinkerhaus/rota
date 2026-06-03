package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/node"
	"github.com/tinkerhaus/rota/internal/transport"
)

func startServer(t *testing.T) (rotav1.BrokerClient, rotav1.ControlClient) {
	t.Helper()
	n, err := node.Open(node.Config{DataDir: t.TempDir(), NodeID: "ctl"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	if err := n.WaitLeader(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	rotav1.RegisterBrokerServer(srv, transport.NewBroker(n))
	rotav1.RegisterControlServer(srv, transport.NewControl(n))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return rotav1.NewBrokerClient(conn), rotav1.NewControlClient(conn)
}

func TestGRPCControlPlane(t *testing.T) {
	bc, cc := startServer(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := bc.Publish(ctx, &rotav1.PublishRequest{Message: &rotav1.MessageSpec{
			Lane: "g", GroupId: "x", Payload: []byte("m"),
		}}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	pi, err := cc.SetPolicy(ctx, &rotav1.SetPolicyRequest{
		Lane: "g", Source: &rotav1.PolicySource{Kind: rotav1.PolicyKind_STRICT_PRIORITY},
	})
	if err != nil {
		t.Fatalf("SetPolicy: %v", err)
	}
	if pi.GetPolicyVersion() != 1 {
		t.Fatalf("policy version = %d, want 1", pi.GetPolicyVersion())
	}
	gp, _ := cc.GetPolicy(ctx, &rotav1.LaneRef{Lane: "g"})
	if gp.GetSource().GetKind() != rotav1.PolicyKind_STRICT_PRIORITY {
		t.Fatalf("GetPolicy kind = %v", gp.GetSource().GetKind())
	}

	vp, _ := cc.ValidatePolicy(ctx, &rotav1.SetPolicyRequest{
		Lane: "g", Source: &rotav1.PolicySource{Kind: rotav1.PolicyKind_CUSTOM, Engine: "cel", Code: []byte("backlog +")},
	})
	if vp.GetOk() {
		t.Fatal("ValidatePolicy accepted a malformed CEL expression")
	}

	h, _ := cc.Health(ctx, &rotav1.HealthRequest{})
	if !h.GetServing() || !h.GetIsLeader() {
		t.Fatalf("Health = %+v", h)
	}

	st, _ := cc.GetStats(ctx, &rotav1.GetStatsRequest{})
	var found bool
	for _, l := range st.GetLanes() {
		if l.GetLane() == "g" && l.GetLeasable() == 3 {
			found = true
		}
	}
	if !found {
		t.Fatalf("GetStats missing lane g with leasable=3: %+v", st.GetLanes())
	}

	ci, _ := cc.DescribeCluster(ctx, &rotav1.DescribeClusterRequest{})
	if ci.GetLeaderId() == "" || len(ci.GetPeers()) != 1 {
		t.Fatalf("DescribeCluster = %+v", ci)
	}

	if _, err := cc.ScheduleCron(ctx, &rotav1.ScheduleCronRequest{
		CronId: "k", Lane: "cl", GroupId: "g", Schedule: "@every 1s", Payload: []byte("t"),
	}); err != nil {
		t.Fatalf("ScheduleCron: %v", err)
	}
	lc, _ := cc.ListCron(ctx, &rotav1.ListCronRequest{})
	if len(lc.GetCrons()) != 1 {
		t.Fatalf("ListCron = %+v", lc.GetCrons())
	}

	sl, err := cc.AcquireSingletonLease(ctx, &rotav1.AcquireSingletonRequest{
		Name: "s", Holder: "h", Ttl: durationpb.New(10 * time.Second),
	})
	if err != nil || sl.GetFence() == 0 {
		t.Fatalf("AcquireSingletonLease: err=%v lease=%+v", err, sl)
	}
}
