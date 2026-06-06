package integration

import (
	"context"
	"testing"
	"time"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/node"
)

// CR-001: a validation-rejected workflow-task response must NOT drop the task and
// wedge the run. After a non-determinism rejection the task is requeued (with a
// delay), so a subsequent poll can still retrieve it and the run can complete.
func TestWorkflowRejectDoesNotWedgeRun(t *testing.T) {
	wc, n := startWorkflowServer(t)
	ctx := context.Background()

	sr, err := wc.StartWorkflow(ctx, &rotav1.StartWorkflowRequest{WorkflowType: "order", TenantId: "t"})
	if err != nil {
		t.Fatal(err)
	}
	runID := sr.GetRunId()

	// Poll the initial workflow task.
	task := pollWFTask(t, wc, "order", 5*time.Second)
	if task.GetRunId() != runID {
		t.Fatalf("polled run %d, want %d", task.GetRunId(), runID)
	}

	// Respond with a BOGUS checksum: the gate must reject it.
	resp, err := wc.RespondWorkflowTask(ctx, &rotav1.RespondWorkflowTaskRequest{
		RunId: runID, LeaseId: task.GetLeaseId(), RunEpoch: task.GetRunEpoch(),
		HistorySeq: task.GetHistorySeq(), PrefixChecksum: []byte("not-the-real-checksum"),
		Commands: []*rotav1.WorkflowCommandProto{{Kind: "complete_workflow"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetApplied() || resp.GetReason() != "non_determinism" {
		t.Fatalf("reject result = %+v, want Applied=false Reason=non_determinism", resp)
	}

	// The run must NOT be wedged: a later poll retrieves the (requeued) task again.
	task2 := pollWFTask(t, wc, "order", 5*time.Second)
	if task2.GetRunId() != runID {
		t.Fatalf("after reject, polled run %d, want %d (task was dropped → wedged)", task2.GetRunId(), runID)
	}

	// Responding correctly now completes the run — proving recovery.
	cs, err := n.RunPrefixChecksum(runID, task2.GetHistorySeq())
	if err != nil {
		t.Fatal(err)
	}
	resp2, err := wc.RespondWorkflowTask(ctx, &rotav1.RespondWorkflowTaskRequest{
		RunId: runID, LeaseId: task2.GetLeaseId(), RunEpoch: task2.GetRunEpoch(),
		HistorySeq: task2.GetHistorySeq(), PrefixChecksum: cs,
		Commands: []*rotav1.WorkflowCommandProto{{Kind: "complete_workflow", Result: []byte("ok")}},
	})
	if err != nil || !resp2.GetApplied() {
		t.Fatalf("correct response = %+v err=%v, want Applied=true", resp2, err)
	}
	run, _ := n.GetRun(runID)
	if run.GetStatus() != rotav1.WorkflowStatus_WF_COMPLETED {
		t.Fatalf("status = %v, want WF_COMPLETED", run.GetStatus())
	}
}

// pollWFTask polls until a non-empty workflow task arrives (or fails after to).
func pollWFTask(t *testing.T, wc rotav1.WorkflowClient, wfType string, to time.Duration) *rotav1.PolledWorkflowTask {
	t.Helper()
	deadline := time.Now().Add(to)
	for time.Now().Before(deadline) {
		task, err := wc.PollWorkflowTask(context.Background(), &rotav1.PollTaskRequest{TaskType: wfType, ConsumerId: "w"})
		if err != nil {
			t.Fatalf("poll: %v", err)
		}
		if !task.GetEmpty() {
			return task
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no workflow task within %s", to)
	return nil
}

// CR-001 (embedded helper edge): the in-process RunWorkflowWorker must NOT drop a
// task when the decider returns an INVALID command (which fails command
// translation inside the gate). The task must be requeued, so a later good worker
// can still complete the run.
func TestInProcessWorkerRequeuesInvalidCommand(t *testing.T) {
	n := openNode(t, 60_000)

	runID, err := n.StartWorkflow("ord", "t", nil)
	if err != nil {
		t.Fatal(err)
	}

	// A worker whose decider always returns an unknown command kind. With the bug
	// it would ack (drop) the task on the first failed decision and wedge the run.
	buggyCtx, stopBuggy := context.WithCancel(context.Background())
	go n.RunWorkflowWorker(buggyCtx, "ord", "buggy", func(uint64, []*rotav1.HistoryEvent) []node.WorkflowCommand {
		return []node.WorkflowCommand{{Kind: "not_a_real_command"}}
	})
	time.Sleep(1500 * time.Millisecond) // let it fail (and requeue) at least once
	stopBuggy()
	time.Sleep(100 * time.Millisecond) // let the goroutine exit

	// A correct worker must still be able to drive the run to completion — only
	// possible if the task was requeued rather than dropped.
	goodCtx, stopGood := context.WithCancel(context.Background())
	defer stopGood()
	go n.RunWorkflowWorker(goodCtx, "ord", "good", func(uint64, []*rotav1.HistoryEvent) []node.WorkflowCommand {
		return []node.WorkflowCommand{{Kind: "complete_workflow", Result: []byte("ok")}}
	})

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if run, ok := n.GetRun(runID); ok && run.GetStatus() == rotav1.WorkflowStatus_WF_COMPLETED {
			return // recovered — the invalid-command task was requeued, not dropped
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("run never completed: an invalid-command decision dropped the task (wedged)")
}

// CR-002: Work-stream group filters must be enforced by the server.
func TestLeaseGroupFilters(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "gf"
	pub := func(group string) {
		if _, err := n.Publish(node.PublishReq{Lane: lane, GroupID: group, Payload: []byte("x")}); err != nil {
			t.Fatalf("publish %s: %v", group, err)
		}
	}
	pub("a")
	pub("b")

	// allow=[a] must only ever lease group a.
	lr, ok, err := n.LeaseOneFiltered(lane, "c", []string{"a"}, nil)
	if err != nil || !ok || lr.GroupID != "a" {
		t.Fatalf("allow=[a] leased %+v ok=%v err=%v, want group a", lr, ok, err)
	}
	_ = n.Ack(lr.LeaseID)
	// a is now empty; allow=[a] must lease nothing (b is excluded by the filter).
	if _, ok, _ := n.LeaseOneFiltered(lane, "c", []string{"a"}, nil); ok {
		t.Fatal("allow=[a] leased a non-allowed group")
	}

	// deny=[a] must skip a and lease b.
	pub("a") // refill a so deny has something to skip
	lr2, ok, err := n.LeaseOneFiltered(lane, "c", nil, []string{"a"})
	if err != nil || !ok || lr2.GroupID != "b" {
		t.Fatalf("deny=[a] leased %+v ok=%v err=%v, want group b", lr2, ok, err)
	}
}

// CR-003: a message published with a TTL is auto-dead-lettered if still
// undelivered when the TTL elapses; a delivered (leased) message is not.
func TestMessageTTLExpiry(t *testing.T) {
	n := openNode(t, 60_000)

	// Undelivered message expires.
	if _, err := n.Publish(node.PublishReq{Lane: "ttl", GroupID: "g", Payload: []byte("x"), TtlMs: 80}); err != nil {
		t.Fatal(err)
	}
	// Delayed message whose TTL is shorter than its delay: never becomes leasable.
	nb := uint64(time.Now().UnixMilli()) + 2000
	if _, err := n.Publish(node.PublishReq{Lane: "ttl2", GroupID: "g", Payload: []byte("y"), NotBeforeMs: nb, TtlMs: 80}); err != nil {
		t.Fatal(err)
	}
	// Leased-in-time message is NOT expired by its TTL.
	if _, err := n.Publish(node.PublishReq{Lane: "ttl3", GroupID: "g", Payload: []byte("z"), TtlMs: 120}); err != nil {
		t.Fatal(err)
	}
	lr, ok := leaseOne(t, n, "ttl3")
	if !ok {
		t.Fatal("ttl3 should be leasable immediately")
	}

	time.Sleep(400 * time.Millisecond) // let the TTL timers fire

	if c, _ := n.DLQCount("ttl"); c != 1 {
		t.Fatalf("ttl DLQ = %d, want 1 (undelivered message should expire)", c)
	}
	if _, ok := leaseOne(t, n, "ttl"); ok {
		t.Fatal("expired message must not be leasable")
	}
	if c, _ := n.DLQCount("ttl2"); c != 1 {
		t.Fatalf("ttl2 DLQ = %d, want 1 (delayed message expired before becoming ready)", c)
	}
	if c, _ := n.DLQCount("ttl3"); c != 0 {
		t.Fatalf("ttl3 DLQ = %d, want 0 (a leased message is not TTL-expired)", c)
	}
	_ = n.Ack(lr.LeaseID)
}

// CR-004: PublishBatch must collapse duplicate dedup keys WITHIN the same batch.
func TestPublishBatchWithinBatchDedup(t *testing.T) {
	n := openNode(t, 60_000)
	items := []node.PublishReq{
		{Lane: "bd", GroupID: "g", Payload: []byte("a"), DedupKey: "k"},
		{Lane: "bd", GroupID: "g", Payload: []byte("b"), DedupKey: "k"}, // same key in-batch
		{Lane: "bd2", GroupID: "g", Payload: []byte("c"), DedupKey: "k"}, // different lane: independent
	}
	res, err := n.PublishBatch(items, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 3 || !res[0].OK || !res[1].OK || !res[2].OK {
		t.Fatalf("results = %+v, want 3 OK", res)
	}
	if !res[1].Duplicate || res[1].MsgID != res[0].MsgID {
		t.Fatalf("item 1 = %+v, want Duplicate with same id as item 0 (%d)", res[1], res[0].MsgID)
	}
	if res[2].Duplicate {
		t.Fatalf("item 2 (different lane) = %+v, want a fresh non-duplicate", res[2])
	}
	// Only ONE message landed in lane bd group g.
	if _, ok := leaseOne(t, n, "bd"); !ok {
		t.Fatal("bd should have one leasable message")
	}
	if _, ok := leaseOne(t, n, "bd"); ok {
		t.Fatal("bd should have only ONE message (within-batch dup was not collapsed)")
	}
}

// CR-006: a failed complete-by-token honors the caller-supplied redelivery delay
// instead of the (much longer) default exponential backoff.
func TestCompleteByTokenFailureDelay(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "cd"
	if _, err := n.Publish(node.PublishReq{Lane: lane, GroupID: "g", Payload: []byte("x"), IssueToken: true}); err != nil {
		t.Fatal(err)
	}
	lr, ok := leaseOne(t, n, lane)
	if !ok || len(lr.ExternalToken) == 0 {
		t.Fatalf("expected a leased message with a token, got ok=%v token=%d bytes", ok, len(lr.ExternalToken))
	}
	// Fail with an explicit 200ms delay. The first-retry default backoff is ~1s,
	// so becoming leasable within ~500ms proves the caller delay was honored.
	if _, _, err := n.Complete(lr.ExternalToken, false, nil, 200); err != nil {
		t.Fatal(err)
	}
	if _, ok := leaseOne(t, n, lane); ok {
		t.Fatal("message should not be immediately leasable after a delayed retry")
	}
	time.Sleep(500 * time.Millisecond)
	if _, ok := leaseOne(t, n, lane); !ok {
		t.Fatal("message should be leasable ~200ms after the failed completion (delay ignored?)")
	}
}
