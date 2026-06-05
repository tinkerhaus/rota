package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/node"
)

// Drives a durable workflow over the HTTP/JSON dashboard gateway: start, poll the
// run, read history, list, and cancel — with in-process workers executing it.
func TestHTTPWorkflowRoutes(t *testing.T) {
	n := openNode(t, 60_000)
	srv := startGateway(t, n)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go n.RunActivityWorker(ctx, "charge", "a", func(task *rotav1.ActivityTask) ([]byte, bool) {
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
	go n.RunWorkflowWorker(ctx, "order", "w", decide)

	// Start via HTTP. protojson encodes uint64 as a string.
	body, _ := json.Marshal(map[string]string{"workflowType": "order", "tenantId": "t"})
	resp, err := http.Post(srv.URL+"/api/workflows", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var sr struct {
		RunID string `json:"runId"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()
	if sr.RunID == "" || sr.RunID == "0" {
		t.Fatalf("start returned runId %q", sr.RunID)
	}
	runPath := "/api/workflows/" + sr.RunID

	done := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !done {
		if getJSON(t, srv, runPath)["status"] == "WF_COMPLETED" {
			done = true
		} else {
			time.Sleep(40 * time.Millisecond)
		}
	}
	if !done {
		t.Fatal("workflow not completed via HTTP")
	}

	if events, _ := getJSON(t, srv, runPath+"/history")["events"].([]any); len(events) < 4 {
		t.Fatalf("history events = %d, want >=4", len(events))
	}
	if runs, _ := getJSON(t, srv, "/api/workflows")["runs"].([]any); len(runs) < 1 {
		t.Fatal("list returned no runs")
	}

	// Start another and cancel it over HTTP.
	body2, _ := json.Marshal(map[string]string{"workflowType": "sleeper", "tenantId": "t"})
	resp2, err := http.Post(srv.URL+"/api/workflows", "application/json", bytes.NewReader(body2))
	if err != nil {
		t.Fatal(err)
	}
	var sr2 struct {
		RunID string `json:"runId"`
	}
	_ = json.NewDecoder(resp2.Body).Decode(&sr2)
	resp2.Body.Close()
	cresp, err := http.Post(srv.URL+"/api/workflows/"+sr2.RunID+"/cancel?reason=op", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	cresp.Body.Close()
	if st := getJSON(t, srv, "/api/workflows/"+sr2.RunID)["status"]; st != "WF_CANCELED" {
		t.Fatalf("status after cancel = %v, want WF_CANCELED", st)
	}
}
