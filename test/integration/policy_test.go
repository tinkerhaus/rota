package integration

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/tinkerhaus/rota/internal/node"
	"github.com/tinkerhaus/rota/internal/policy"
)

func drainOrder(t *testing.T, n *node.Node, lane string, count int) []string {
	t.Helper()
	order := make([]string, 0, count)
	for len(order) < count {
		lr, ok := leaseOne(t, n, lane)
		if !ok {
			t.Fatalf("ran dry at %d/%d", len(order), count)
		}
		order = append(order, lr.GroupID)
		if err := n.Ack(lr.LeaseID); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	return order
}

func pubN(t *testing.T, n *node.Node, lane, group string, count int, weight float64) {
	t.Helper()
	w := weight
	for i := 0; i < count; i++ {
		if _, err := n.Publish(node.PublishReq{Lane: lane, GroupID: group, Payload: []byte("x"), Weight: &w}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
}

// A strict-priority policy serves the higher-weight group to exhaustion first,
// overriding the default weighted-fair interleave.
func TestNodeStrictPriorityPolicy(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "sp"
	pubN(t, n, lane, "A", 20, 1)
	pubN(t, n, lane, "B", 20, 5)
	if err := n.SetPolicy(lane, policy.Binding{Kind: policy.KindStrictPriority}); err != nil {
		t.Fatalf("set policy: %v", err)
	}
	order := drainOrder(t, n, lane, 40)
	for i := 0; i < 20; i++ {
		if order[i] != "B" {
			t.Fatalf("strict priority: expected the first 20 to be group B, got order[:24]=%v", order[:24])
		}
	}
}

// A CEL "shortest queue first" policy drains the smaller group before the larger.
func TestNodeCELPolicy(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "cel"
	pubN(t, n, lane, "A", 30, 1)
	pubN(t, n, lane, "B", 5, 1)
	if err := n.SetPolicy(lane, policy.Binding{Kind: policy.KindCEL, Source: []byte("-backlog")}); err != nil {
		t.Fatalf("set policy: %v", err)
	}
	order := drainOrder(t, n, lane, 35)
	for i := 0; i < 5; i++ {
		if order[i] != "B" {
			t.Fatalf("shortest-queue CEL: expected first 5 to be B, got %v", order[:8])
		}
	}
}

// The same behavior, but the policy is a WASM module (proving any-language policies).
func TestNodeWASMPolicy(t *testing.T) {
	out := filepath.Join(t.TempDir(), "p.wasm")
	cmd := exec.Command("go", "build", "-buildmode=c-shared", "-o", out, "./internal/policy/testdata/shortestqueue")
	cmd.Dir = filepath.Join("..", "..")
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("cannot build wasip1 WASM policy: %v\n%s", err, b)
	}
	code, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}

	n := openNode(t, 60_000)
	const lane = "wasm"
	pubN(t, n, lane, "A", 30, 1)
	pubN(t, n, lane, "B", 5, 1)
	if err := n.SetPolicy(lane, policy.Binding{Kind: policy.KindWASM, Source: code}); err != nil {
		t.Fatalf("set wasm policy: %v", err)
	}
	order := drainOrder(t, n, lane, 35)
	for i := 0; i < 5; i++ {
		if order[i] != "B" {
			t.Fatalf("WASM shortest-queue: expected first 5 to be B, got %v", order[:8])
		}
	}
}

// Hot-reload: re-installing a policy bumps the version and takes effect without
// restart.
func TestNodePolicyHotReload(t *testing.T) {
	n := openNode(t, 60_000)
	const lane = "hr"
	pubN(t, n, lane, "A", 1, 1)
	if err := n.SetPolicy(lane, policy.Binding{Kind: policy.KindStrictPriority}); err != nil {
		t.Fatal(err)
	}
	b1, ok := n.GetPolicy(lane)
	if !ok || b1.Version != 1 || b1.Kind != policy.KindStrictPriority {
		t.Fatalf("after first set: %+v ok=%v", b1, ok)
	}
	if err := n.SetPolicy(lane, policy.Binding{Kind: policy.KindCEL, Source: []byte("-backlog")}); err != nil {
		t.Fatal(err)
	}
	b2, _ := n.GetPolicy(lane)
	if b2.Version != 2 || b2.Kind != policy.KindCEL {
		t.Fatalf("hot reload did not take: %+v", b2)
	}
}
