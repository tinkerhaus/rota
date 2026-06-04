package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tinkerhaus/rota/internal/fsm"
	"github.com/tinkerhaus/rota/internal/httpapi"
	"github.com/tinkerhaus/rota/internal/node"
	"github.com/tinkerhaus/rota/internal/transport"
)

// startGateway brings up the in-process HTTP/JSON dashboard gateway over a node,
// returning an httptest server. The static dir is intentionally empty (no
// frontend build) to prove the Go path never depends on web/dist content.
func startGateway(t *testing.T, n *node.Node) *httptest.Server {
	t.Helper()
	h := httpapi.Handler(n, transport.NewControl(n), t.TempDir())
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func getJSON(t *testing.T, srv *httptest.Server, path string) map[string]any {
	t.Helper()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s status = %d, want 200", path, resp.StatusCode)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return out
}

// The HTTP gateway serves the dashboard read routes as protojson (camelCase
// field names) and the mutating redrive POST end-to-end.
func TestHTTPGateway(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "http"
	pubN(t, n, lane, "g", 2, 1)

	srv := startGateway(t, n)

	// GET /api/stats — protojson uses camelCase (dlqDepth), proving the encoder.
	stats := getJSON(t, srv, "/api/stats")
	lanes, _ := stats["lanes"].([]any)
	if len(lanes) == 0 {
		t.Fatalf("/api/stats returned no lanes: %v", stats)
	}
	first, _ := lanes[0].(map[string]any)
	if _, ok := first["dlqDepth"]; !ok {
		t.Fatalf("/api/stats not camelCase (no dlqDepth field): %v", first)
	}

	// GET /api/lanes/{lane}/groups — paginated group grid.
	groups := getJSON(t, srv, "/api/lanes/"+lane+"/groups?page_size=10")
	if gs, _ := groups["groups"].([]any); len(gs) != 1 {
		t.Fatalf("/groups = %v, want exactly 1 group", groups["groups"])
	}

	// GET /api/lanes/{lane}/groups/{group}/messages — non-destructive peek.
	peek := getJSON(t, srv, "/api/lanes/"+lane+"/groups/g/messages?limit=5")
	if ms, _ := peek["messages"].([]any); len(ms) != 2 {
		t.Fatalf("/messages = %v, want 2 head messages", peek["messages"])
	}

	// Dead-letter a message in its own lane so we have a DLQ row to redrive over
	// HTTP. A max_attempts=1 message dead-letters on a single retry-nack; isolating
	// it in lane "dl" keeps the lease deterministic regardless of fairness order.
	const dlLane = "httpdl"
	if _, err := n.Publish(node.PublishReq{Lane: dlLane, GroupID: "g2", Payload: []byte("x"), MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	lr, ok := leaseOne(t, n, dlLane)
	if !ok {
		t.Fatal("expected a lease")
	}
	if _, err := n.Nack(lr.LeaseID, fsm.NackRetry, 0, nil); err != nil {
		t.Fatal(err)
	}
	dls, _, err := n.ListDeadLetters(dlLane, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(dls) == 0 {
		t.Fatal("no dead letter to redrive")
	}
	dl := dls[0]

	// GET /api/lanes/{lane}/dlq — DLQ inspector.
	dlq := getJSON(t, srv, "/api/lanes/"+dlLane+"/dlq")
	if dd, _ := dlq["deadLetters"].([]any); len(dd) == 0 {
		t.Fatalf("/dlq returned no dead letters: %v", dlq)
	}

	// GET /api/lanes/{lane}/leases — lease inspector (the original lane still holds none).
	_ = getJSON(t, srv, "/api/lanes/"+lane+"/leases")

	// POST /api/lanes/{lane}/dlq/redrive — the mutating route.
	body, _ := json.Marshal(map[string]any{"group_id": dl.GroupId, "msg_id": dl.MsgId})
	resp, err := http.Post(srv.URL+"/api/lanes/"+dlLane+"/dlq/redrive", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST redrive: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST redrive status = %d, want 200", resp.StatusCode)
	}
	var rd map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&rd); err != nil {
		t.Fatalf("decode redrive: %v", err)
	}
	if ok, _ := rd["ok"].(bool); !ok {
		t.Fatalf("redrive response = %v, want ok=true", rd)
	}

	// After redrive the DLQ row is gone.
	dlq2 := getJSON(t, srv, "/api/lanes/"+dlLane+"/dlq")
	if dd, _ := dlq2["deadLetters"].([]any); len(dd) != 0 {
		t.Fatalf("/dlq after redrive = %v, want empty", dd)
	}
}
