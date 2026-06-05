package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/node"
	"github.com/tinkerhaus/rota/internal/transport"
)

func startWorkflowServer(t *testing.T) (rotav1.WorkflowClient, *node.Node) {
	t.Helper()
	n, err := node.Open(node.Config{DataDir: t.TempDir(), NodeID: "wf"})
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
	rotav1.RegisterWorkflowServer(srv, transport.NewWorkflow(n))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return rotav1.NewWorkflowClient(conn), n
}

// Drives a durable workflow entirely over the gRPC Workflow service: start, read
// run + history + list, and cancel — with in-process workers executing decisions.
func TestGRPCWorkflowService(t *testing.T) {
	wc, n := startWorkflowServer(t)
	ctx := context.Background()

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go n.RunActivityWorker(wctx, "charge", "act", func(task *rotav1.ActivityTask) ([]byte, bool) {
		return []byte("ok"), true
	})
	decide := func(runID uint64, history []*rotav1.HistoryEvent) []node.WorkflowCommand {
		var scheduled, completed bool
		for _, e := range history {
			switch e.GetEventType() {
			case rotav1.HistoryEventType_HET_ACTIVITY_SCHEDULED:
				scheduled = true
			case rotav1.HistoryEventType_HET_ACTIVITY_COMPLETED:
				completed = true
			}
		}
		switch {
		case !scheduled:
			return []node.WorkflowCommand{{Kind: "schedule_activity", ActivityType: "charge"}}
		case completed:
			return []node.WorkflowCommand{{Kind: "complete_workflow"}}
		default:
			return nil
		}
	}
	go n.RunWorkflowWorker(wctx, "order", "wf", decide)

	sr, err := wc.StartWorkflow(ctx, &rotav1.StartWorkflowRequest{WorkflowType: "order", TenantId: "t", Input: []byte("{}")})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	runID := sr.GetRunId()
	if runID == 0 {
		t.Fatal("StartWorkflow returned run id 0")
	}

	done := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !done {
		run, err := wc.GetWorkflowRun(ctx, &rotav1.WorkflowRunRef{RunId: runID})
		if err == nil && run.GetStatus() == rotav1.WorkflowStatus_WF_COMPLETED {
			done = true
		} else {
			time.Sleep(40 * time.Millisecond)
		}
	}
	if !done {
		t.Fatal("workflow not completed via gRPC")
	}

	h, err := wc.GetWorkflowHistory(ctx, &rotav1.WorkflowRunRef{RunId: runID})
	if err != nil {
		t.Fatalf("GetWorkflowHistory: %v", err)
	}
	if len(h.GetEvents()) < 4 {
		t.Fatalf("history has %d events, want >=4", len(h.GetEvents()))
	}

	lr, err := wc.ListWorkflowRuns(ctx, &rotav1.ListWorkflowRunsRequest{})
	if err != nil {
		t.Fatalf("ListWorkflowRuns: %v", err)
	}
	if len(lr.GetRuns()) != 1 {
		t.Fatalf("ListWorkflowRuns = %d runs, want 1", len(lr.GetRuns()))
	}

	// Start a workflow with no worker, then cancel it over gRPC.
	sr2, err := wc.StartWorkflow(ctx, &rotav1.StartWorkflowRequest{WorkflowType: "sleeper", TenantId: "t"})
	if err != nil {
		t.Fatal(err)
	}
	cr, err := wc.CancelWorkflow(ctx, &rotav1.CancelWorkflowRequest{RunId: sr2.GetRunId(), Reason: []byte("operator")})
	if err != nil {
		t.Fatalf("CancelWorkflow: %v", err)
	}
	if !cr.GetCanceled() {
		t.Fatal("CancelWorkflow should report canceled=true for a running run")
	}
	run2, err := wc.GetWorkflowRun(ctx, &rotav1.WorkflowRunRef{RunId: sr2.GetRunId()})
	if err != nil {
		t.Fatal(err)
	}
	if run2.GetStatus() != rotav1.WorkflowStatus_WF_CANCELED {
		t.Fatalf("status after cancel = %v, want WF_CANCELED", run2.GetStatus())
	}
}

// Drives a workflow entirely through the language-agnostic gRPC WORKER PROTOCOL
// (Poll/Respond) — the exact contract an SDK in any language uses. Proves a remote
// worker can replay history, compute the determinism checksum, and pass the gate.
func TestGRPCWorkerProtocol(t *testing.T) {
	wc, _ := startWorkflowServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Activity worker over the wire.
	go func() {
		for ctx.Err() == nil {
			at, err := wc.PollActivityTask(ctx, &rotav1.PollTaskRequest{TaskType: "charge", ConsumerId: "a"})
			if err != nil || at.GetEmpty() {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			_, _ = wc.RespondActivityTask(ctx, &rotav1.RespondActivityTaskRequest{
				RunId: at.GetRunId(), LeaseId: at.GetLeaseId(), ScheduledEventId: at.GetScheduledEventId(),
				Success: true, Result: []byte("ok"),
			})
		}
	}()
	// Workflow worker over the wire: replay history, decide, echo the checksum.
	go func() {
		for ctx.Err() == nil {
			wt, err := wc.PollWorkflowTask(ctx, &rotav1.PollTaskRequest{TaskType: "order", ConsumerId: "w"})
			if err != nil || wt.GetEmpty() {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			var scheduled, completed bool
			for _, e := range wt.GetHistory() {
				switch e.GetEventType() {
				case rotav1.HistoryEventType_HET_ACTIVITY_SCHEDULED:
					scheduled = true
				case rotav1.HistoryEventType_HET_ACTIVITY_COMPLETED:
					completed = true
				}
			}
			var cmds []*rotav1.WorkflowCommandProto
			switch {
			case !scheduled:
				cmds = []*rotav1.WorkflowCommandProto{{Kind: "schedule_activity", ActivityType: "charge"}}
			case completed:
				cmds = []*rotav1.WorkflowCommandProto{{Kind: "complete_workflow"}}
			}
			_, _ = wc.RespondWorkflowTask(ctx, &rotav1.RespondWorkflowTaskRequest{
				RunId: wt.GetRunId(), LeaseId: wt.GetLeaseId(), RunEpoch: wt.GetRunEpoch(),
				HistorySeq: wt.GetHistorySeq(), PrefixChecksum: node.PrefixChecksumOf(wt.GetHistory()),
				Commands: cmds,
			})
		}
	}()

	sr, err := wc.StartWorkflow(ctx, &rotav1.StartWorkflowRequest{WorkflowType: "order", TenantId: "t"})
	if err != nil {
		t.Fatal(err)
	}
	done := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !done {
		run, err := wc.GetWorkflowRun(ctx, &rotav1.WorkflowRunRef{RunId: sr.GetRunId()})
		if err == nil && run.GetStatus() == rotav1.WorkflowStatus_WF_COMPLETED {
			done = true
		} else {
			time.Sleep(40 * time.Millisecond)
		}
	}
	if !done {
		t.Fatal("workflow not completed via the gRPC worker protocol")
	}
}
