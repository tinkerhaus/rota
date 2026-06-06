package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/node"
	"github.com/tinkerhaus/rota/internal/transport"
)

func startCLITestServer(t *testing.T) (string, rotav1.BrokerClient, rotav1.ControlClient, rotav1.WorkflowClient) {
	t.Helper()
	n, err := node.Open(node.Config{DataDir: t.TempDir(), NodeID: "cli"})
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
	rotav1.RegisterWorkflowServer(srv, transport.NewWorkflow(n))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		done := make(chan struct{})
		go func() {
			srv.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			srv.Stop()
		}
	})

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return lis.Addr().String(), rotav1.NewBrokerClient(conn), rotav1.NewControlClient(conn), rotav1.NewWorkflowClient(conn)
}

func TestDoctorDetectsPausedBacklog(t *testing.T) {
	_, bc, cc, wc := startCLITestServer(t)
	ctx := context.Background()
	if _, err := bc.Publish(ctx, &rotav1.PublishRequest{Message: &rotav1.MessageSpec{
		Lane: "ops", GroupId: "tenant-a", Payload: []byte("work"),
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := cc.PauseGroup(ctx, &rotav1.GroupRef{Lane: "ops", GroupId: "tenant-a"}); err != nil {
		t.Fatal(err)
	}

	report, err := collectDoctorReport(ctx, cc, wc, doctorOptions{pageSize: 50, leaseWarnAfter: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "WARN" {
		t.Fatalf("status = %s, want WARN", report.Status)
	}
	if !hasFinding(report, "paused_group_backlog") {
		t.Fatalf("findings = %+v, want paused_group_backlog", report.Findings)
	}

	var out bytes.Buffer
	if err := writeDoctorReport(&out, report, false); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	if !strings.Contains(text, "Rota Doctor: WARN") || !strings.Contains(text, "paused_group_backlog") {
		t.Fatalf("human report missing key content:\n%s", text)
	}
}

func TestLeasesForceExpireRequeuesLease(t *testing.T) {
	addr, bc, _, _ := startCLITestServer(t)
	ctx := context.Background()
	if _, err := bc.Publish(ctx, &rotav1.PublishRequest{Message: &rotav1.MessageSpec{
		Lane: "ops-force", GroupId: "g", Payload: []byte("work"),
	}}); err != nil {
		t.Fatal(err)
	}
	stream, err := bc.Work(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&rotav1.WorkClientMsg{Msg: &rotav1.WorkClientMsg_LeaseRequest{
		LeaseRequest: &rotav1.LeaseRequest{Lane: "ops-force", Credit: 1, ConsumerId: "test"},
	}}); err != nil {
		t.Fatal(err)
	}
	msg, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	lease := msg.GetLease()
	if lease == nil {
		t.Fatalf("server message = %+v, want lease", msg)
	}
	closeWorkStream(t, stream)

	if err := cmdLeasesForceExpire([]string{"--grpc", addr, "--lease-id", fmt.Sprint(lease.GetLeaseId())}); err != nil {
		t.Fatal(err)
	}

	stream2, err := bc.Work(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream2.Send(&rotav1.WorkClientMsg{Msg: &rotav1.WorkClientMsg_LeaseRequest{
		LeaseRequest: &rotav1.LeaseRequest{Lane: "ops-force", Credit: 1, ConsumerId: "test2"},
	}}); err != nil {
		t.Fatal(err)
	}
	msg2, err := stream2.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if msg2.GetLease() == nil || msg2.GetLease().GetMessageId() != lease.GetMessageId() {
		t.Fatalf("post-force-expire message = %+v, want original message id %d", msg2, lease.GetMessageId())
	}
	closeWorkStream(t, stream2)
}

func closeWorkStream(t *testing.T, stream rotav1.Broker_WorkClient) {
	t.Helper()
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := stream.Recv(); err != nil {
			return
		}
	}
}

func TestBenchPublishesAndDrains(t *testing.T) {
	addr, _, _, _ := startCLITestServer(t)
	report, err := runBench(benchOptions{
		clientOptions: clientOptions{grpcAddr: addr, timeout: 10 * time.Second},
		lane:          "bench-test",
		messages:      20,
		groups:        4,
		workers:       2,
		batchSize:     5,
		payloadBytes:  16,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Acked != 20 || report.PublishFailures != 0 || report.DrainPerSec <= 0 {
		t.Fatalf("bench report = %+v", report)
	}
}

func TestSoakPublishesAndDrains(t *testing.T) {
	addr, _, _, _ := startCLITestServer(t)
	report, err := runSoak(soakOptions{
		clientOptions: clientOptions{grpcAddr: addr, timeout: 10 * time.Second},
		duration:      300 * time.Millisecond,
		drain:         300 * time.Millisecond,
		lanePrefix:    "soak-test",
		lanes:         1,
		groups:        2,
		publishers:    1,
		workers:       2,
		publishRate:   20,
		payloadBytes:  16,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Published == 0 || report.Leased == 0 || report.Acked == 0 {
		t.Fatalf("soak report = %+v, want published/leased/acked > 0", report)
	}
}

func TestDoctorJSONRenderer(t *testing.T) {
	report := &doctorReport{
		CheckedAtMs: 1,
		Status:      "OK",
		Cluster:     doctorCluster{Serving: true, HasQuorum: true, IsLeader: true, LeaderID: "n1"},
	}
	var out bytes.Buffer
	if err := writeDoctorReport(&out, report, true); err != nil {
		t.Fatal(err)
	}
	var decoded doctorReport
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatalf("invalid JSON %q: %v", out.String(), err)
	}
	if decoded.Status != "OK" || !decoded.Cluster.Serving {
		t.Fatalf("decoded report = %+v", decoded)
	}
}

func hasFinding(report *doctorReport, code string) bool {
	for _, f := range report.Findings {
		if f.Code == code {
			return true
		}
	}
	return false
}
