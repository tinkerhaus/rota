package integration

import (
	"bufio"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/policy"
	"github.com/tinkerhaus/rota/internal/transport"
)

// GetLaneFairness joins durable GroupMeta with the leader's in-memory served
// projection. After publishing to two equal-weight groups and leasing several
// times, every active group must report a sane expected share (≈ its weight
// fraction), an actual share matching its served fraction, and the served counts
// must sum to the lane total.
func TestLaneFairness(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "fair"
	pubN(t, n, lane, "A", 10, 1)
	pubN(t, n, lane, "B", 10, 1)

	// Lease+ack a batch. Equal weights ⇒ the scheduler interleaves A and B, so
	// both groups accrue served counts.
	const leases = 12
	served := map[string]int{}
	for i := 0; i < leases; i++ {
		lr, ok := leaseOne(t, n, lane)
		if !ok {
			t.Fatalf("ran dry at lease %d", i)
		}
		served[lr.GroupID]++
		if err := n.Ack(lr.LeaseID); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}

	ctrl := transport.NewControl(n)
	lf, err := ctrl.GetLaneFairness(context.Background(), &rotav1.LaneRef{Lane: lane})
	if err != nil {
		t.Fatalf("GetLaneFairness: %v", err)
	}
	if lf.GetTotalServed() != leases {
		t.Fatalf("totalServed = %d, want %d", lf.GetTotalServed(), leases)
	}
	if len(lf.GetGroups()) != 2 {
		t.Fatalf("want 2 groups, got %d", len(lf.GetGroups()))
	}

	var sumServed uint64
	var sumExpected, sumActual float64
	for _, g := range lf.GetGroups() {
		sumServed += g.GetServed()
		sumExpected += g.GetExpectedShare()
		sumActual += g.GetActualShare()

		// Each group's reported served count must match what we observed.
		if int(g.GetServed()) != served[g.GetGroupId()] {
			t.Fatalf("group %s served = %d, want %d", g.GetGroupId(), g.GetServed(), served[g.GetGroupId()])
		}
		// actualShare == served / totalServed.
		wantActual := float64(g.GetServed()) / float64(leases)
		if diff := g.GetActualShare() - wantActual; diff > 1e-9 || diff < -1e-9 {
			t.Fatalf("group %s actualShare = %v, want %v", g.GetGroupId(), g.GetActualShare(), wantActual)
		}
		// starvation score is normalized to [0,1].
		if g.GetStarvationScore() < 0 || g.GetStarvationScore() > 1 {
			t.Fatalf("group %s starvation = %v, want [0,1]", g.GetGroupId(), g.GetStarvationScore())
		}
	}
	if sumServed != leases {
		t.Fatalf("served counts sum to %d, want %d", sumServed, leases)
	}
	// Two equal-weight active groups ⇒ each expects ~0.5; the shares sum to ~1.
	if diff := sumExpected - 1.0; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("expected shares sum = %v, want 1.0", sumExpected)
	}
	for _, g := range lf.GetGroups() {
		if diff := g.GetExpectedShare() - 0.5; diff > 1e-9 || diff < -1e-9 {
			t.Fatalf("group %s expectedShare = %v, want 0.5 (equal weights)", g.GetGroupId(), g.GetExpectedShare())
		}
	}
	if diff := sumActual - 1.0; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("actual shares sum = %v, want 1.0", sumActual)
	}
}

// A heavier group claims a proportionally larger expected share of the lane.
func TestLaneFairnessWeightedExpectedShare(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "fairw"
	pubN(t, n, lane, "big", 5, 4)   // weight 4
	pubN(t, n, lane, "small", 5, 1) // weight 1

	// Touch the projection so both groups are "active".
	for i := 0; i < 5; i++ {
		lr, ok := leaseOne(t, n, lane)
		if !ok {
			t.Fatalf("ran dry at %d", i)
		}
		_ = n.Ack(lr.LeaseID)
	}

	ctrl := transport.NewControl(n)
	lf, err := ctrl.GetLaneFairness(context.Background(), &rotav1.LaneRef{Lane: lane})
	if err != nil {
		t.Fatalf("GetLaneFairness: %v", err)
	}
	exp := map[string]float64{}
	for _, g := range lf.GetGroups() {
		exp[g.GetGroupId()] = g.GetExpectedShare()
	}
	// weight 4 vs 1 ⇒ expected 0.8 vs 0.2.
	if diff := exp["big"] - 0.8; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("big expectedShare = %v, want 0.8", exp["big"])
	}
	if diff := exp["small"] - 0.2; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("small expectedShare = %v, want 0.2", exp["small"])
	}
}

// GetPolicyHealth reports the bound policy version, engine, and quarantine state.
// Installing the completion_aware built-in bumps the version and reports engine
// "builtin", not quarantined.
func TestPolicyHealth(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "ph"
	pubN(t, n, lane, "g", 1, 1)

	if err := n.SetPolicy(lane, policy.Binding{Kind: policy.KindCompletionAware}); err != nil {
		t.Fatalf("set completion_aware policy: %v", err)
	}

	ctrl := transport.NewControl(n)
	h, err := ctrl.GetPolicyHealth(context.Background(), &rotav1.LaneRef{Lane: lane})
	if err != nil {
		t.Fatalf("GetPolicyHealth: %v", err)
	}
	if h.GetLane() != lane {
		t.Fatalf("lane = %q, want %q", h.GetLane(), lane)
	}
	if h.GetVersion() != 1 {
		t.Fatalf("version = %d, want 1", h.GetVersion())
	}
	if h.GetEngine() != "builtin" {
		t.Fatalf("engine = %q, want builtin", h.GetEngine())
	}
	if h.GetQuarantined() {
		t.Fatal("policy should not be quarantined after a clean install")
	}

	// The completion_aware policy actually drives leasing (proving it is wired as a
	// selectable built-in, not just stored): the lone message leases fine.
	if lr, ok := leaseOne(t, n, lane); !ok {
		t.Fatal("completion_aware policy did not lease the available message")
	} else {
		_ = n.Ack(lr.LeaseID)
	}
}

// The SSE fairness stream emits at least one `served` event after lease activity.
// A short read deadline bounds the test: we read the immediate snapshot frame
// plus a frame produced by post-connect activity.
func TestFairnessSSEStream(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "sse"
	pubN(t, n, lane, "A", 20, 1)
	pubN(t, n, lane, "B", 20, 1)

	srv := startGateway(t, n)

	// Generate some service so the projection is non-empty before we connect.
	for i := 0; i < 6; i++ {
		lr, ok := leaseOne(t, n, lane)
		if !ok {
			t.Fatalf("ran dry at %d", i)
		}
		_ = n.Ack(lr.LeaseID)
	}

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/api/lanes/"+lane+"/fairness/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET fairness/stream: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stream status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}

	// Drive more service after connecting so a coalesced frame is produced too.
	// The driver stops on `stop` AND we wait for it to fully exit before the test
	// returns, so it never touches the node after t.Cleanup closes the store (a
	// late LeaseOne against a closed pebble would panic).
	stop := make(chan struct{})
	driverDone := make(chan struct{})
	defer func() { close(stop); <-driverDone }()
	go func() {
		defer close(driverDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			lr, ok, err := n.LeaseOne(lane, "c2")
			if err != nil {
				return
			}
			if ok {
				_ = n.Ack(lr.LeaseID)
			}
			select {
			case <-stop:
				return
			case <-time.After(40 * time.Millisecond):
			}
		}
	}()

	// Read lines until we see a `served` event with a data payload, or time out.
	done := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		var sawServedEvent bool
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "event: served") {
				sawServedEvent = true
				continue
			}
			if sawServedEvent && strings.HasPrefix(line, "data: ") {
				done <- strings.TrimPrefix(line, "data: ")
				return
			}
		}
		done <- ""
	}()

	select {
	case data := <-done:
		if data == "" {
			t.Fatal("stream closed before a served event arrived")
		}
		if !strings.Contains(data, "ribbon") || !strings.Contains(data, "totalServed") {
			t.Fatalf("served frame missing expected fields: %q", data)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("no served event within deadline")
	}
}
