package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestBenchSuiteSmoke(t *testing.T) {
	report, err := runBenchSuite(benchSuiteOptions{
		profile:          "smoke",
		messages:         12,
		groups:           2,
		workers:          1,
		batchSize:        4,
		payloadBytes:     8,
		latencySamples:   4,
		soakDuration:     700 * time.Millisecond,
		soakRate:         30,
		retryEvery:       2,
		deadletterEvery:  3,
		workflowDuration: 700 * time.Millisecond,
		workflowRate:     10,
		backupMessages:   6,
		timeout:          20 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.SingleNode == nil || report.SingleNode.Throughput.Acked != 12 {
		t.Fatalf("single-node report = %+v", report.SingleNode)
	}
	if report.SingleNode.Latency == nil || report.SingleNode.Latency.Samples != 4 {
		t.Fatalf("latency report = %+v", report.SingleNode.Latency)
	}
	if report.Soak == nil || report.Soak.Published == 0 || report.Soak.Leased == 0 {
		t.Fatalf("soak report = %+v", report.Soak)
	}
	if report.Soak.Retried == 0 && report.Soak.DeadLettered == 0 {
		t.Fatalf("soak report = %+v, want retry or dead-letter pressure", report.Soak)
	}
	if report.Workflow == nil || report.Workflow.Started == 0 || report.Workflow.WorkflowTasks == 0 {
		t.Fatalf("workflow report = %+v", report.Workflow)
	}
	if report.Cluster == nil || report.Cluster.Throughput.Acked != 12 {
		t.Fatalf("cluster report = %+v", report.Cluster)
	}
	if report.Cluster.Failover.InitialLeader == "" || report.Cluster.Failover.NewLeader == "" ||
		report.Cluster.Failover.InitialLeader == report.Cluster.Failover.NewLeader {
		t.Fatalf("failover report = %+v", report.Cluster.Failover)
	}
	if report.Cluster.Failover.DrainedAfterFailover !=
		report.Cluster.Failover.CommittedBeforeFailover-report.Cluster.Failover.AckedBeforeFailover {
		t.Fatalf("failover report = %+v, want all unacked work drained after failover", report.Cluster.Failover)
	}
	if report.Backup == nil || report.Backup.Validation == nil || !report.Backup.Validation.OK {
		t.Fatalf("backup report = %+v", report.Backup)
	}
	if report.Backup.Validation.Messages != 6 {
		t.Fatalf("backup validation messages = %d, want 6", report.Backup.Validation.Messages)
	}
}

func TestWriteBenchSuiteReportJSON(t *testing.T) {
	report := &benchSuiteReport{
		StartedAtMs:  1,
		FinishedAtMs: 2,
		Profile:      "smoke",
		RunDir:       "/tmp/rota-bench-suite-test",
		Config:       benchSuiteConfigReport{Messages: 10, Groups: 2, Workers: 1},
		SingleNode: &benchSuiteNodeReport{Throughput: &benchReport{
			Lane: "bench", Messages: 10, Groups: 2, Workers: 1, Acked: 10,
		}},
	}
	var out bytes.Buffer
	if err := writeBenchSuiteReport(&out, report, true); err != nil {
		t.Fatal(err)
	}
	var decoded benchSuiteReport
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatalf("invalid JSON %q: %v", out.String(), err)
	}
	if decoded.Profile != "smoke" || decoded.SingleNode.Throughput.Acked != 10 {
		t.Fatalf("decoded report = %+v", decoded)
	}
}

func TestWriteBenchSuiteReportHuman(t *testing.T) {
	report := &benchSuiteReport{
		Profile: "smoke",
		RunDir:  "/tmp/rota-bench-suite-test",
		SingleNode: &benchSuiteNodeReport{
			Throughput: &benchReport{TotalMs: 1, EndToEndPerSec: 10, PublishPerSec: 10, DrainPerSec: 10, Acked: 1},
			Latency: &benchLatencyReport{
				PublishLatencyMs: latencyStats{P50Ms: 1, P95Ms: 2, P99Ms: 3},
				LeaseWaitMs:      latencyStats{P50Ms: 1, P95Ms: 2, P99Ms: 3},
			},
		},
		Soak: &soakReport{Published: 1, Leased: 1, Acked: 1},
	}
	var out bytes.Buffer
	if err := writeBenchSuiteReport(&out, report, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "single-node throughput") ||
		!strings.Contains(out.String(), "single-node latency") ||
		!strings.Contains(out.String(), "soak pressure") {
		t.Fatalf("human report missing sections:\n%s", out.String())
	}
}
