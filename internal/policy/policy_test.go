package policy

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestBuiltinPolicies(t *testing.T) {
	view := LaneView{Groups: []GroupView{
		{ID: "a", Weight: 1, VirtualTime: 2},
		{ID: "b", Weight: 5, VirtualTime: 0},
	}}
	strict, _ := Compile(Binding{Kind: KindStrictPriority})
	sc, _ := strict.Score(view, ConsumerView{})
	if !(sc[1] > sc[0]) {
		t.Fatalf("strict priority: B (weight 5) should outrank A (weight 1): %v", sc)
	}
	wfq, _ := Compile(Binding{Kind: KindWFQ})
	sc2, _ := wfq.Score(view, ConsumerView{})
	if !(sc2[1] > sc2[0]) {
		t.Fatalf("wfq: B (vt 0) should outrank A (vt 2): %v", sc2)
	}
}

// completion_aware de-prioritizes a tenant hogging in-flight capacity. Two
// equal-weight groups at the same virtual time differ ONLY in outstanding
// (leased-but-unacked) work: the high-inflight group must score LOWER (served
// later) than the low-inflight one. Plain WFQ, which sees only virtual time,
// would tie them — so this is exactly the behavior the policy adds.
func TestCompletionAwarePolicy(t *testing.T) {
	view := LaneView{Groups: []GroupView{
		{ID: "hog", Weight: 1, VirtualTime: 0, InFlight: 8},
		{ID: "lean", Weight: 1, VirtualTime: 0, InFlight: 0},
	}}
	c, err := Compile(Binding{Kind: KindCompletionAware})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	defer c.Close()
	sc, err := c.Score(view, ConsumerView{Credit: 1})
	if err != nil {
		t.Fatalf("score: %v", err)
	}
	if !(sc[1] > sc[0]) {
		t.Fatalf("completion_aware: lean (inflight 0) should outrank hog (inflight 8) at equal weight/vt: %v", sc)
	}

	// Weight normalizes the in-flight penalty: a heavier group tolerates more
	// in-flight before it is throttled. Same raw in-flight, higher weight ⇒
	// higher score (less penalized).
	view2 := LaneView{Groups: []GroupView{
		{ID: "light", Weight: 1, VirtualTime: 0, InFlight: 4},
		{ID: "heavy", Weight: 4, VirtualTime: 0, InFlight: 4},
	}}
	sc2, _ := c.Score(view2, ConsumerView{Credit: 1})
	if !(sc2[1] > sc2[0]) {
		t.Fatalf("completion_aware: heavier group should absorb in-flight better: %v", sc2)
	}

	// It must survive policy validation (smoke fixtures) so it can be installed.
	if err := Validate(Binding{Kind: KindCompletionAware}); err != nil {
		t.Fatalf("completion_aware failed validation: %v", err)
	}
}

func TestCELEngine(t *testing.T) {
	c, err := Compile(Binding{Kind: KindCEL, Source: []byte("-backlog")})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	defer c.Close()
	view := LaneView{Groups: []GroupView{{ID: "a", Backlog: 30}, {ID: "b", Backlog: 5}}}
	sc, err := c.Score(view, ConsumerView{})
	if err != nil {
		t.Fatalf("score: %v", err)
	}
	if sc[0] != -30 || sc[1] != -5 {
		t.Fatalf("shortest-queue scores = %v, want [-30 -5]", sc)
	}
	if err := Validate(Binding{Kind: KindCEL, Source: []byte("backlog +")}); err == nil {
		t.Fatal("expected validation to reject a malformed CEL expression")
	}
}

// buildPolicyWASM compiles the testdata shortest-queue policy to a wasip1 reactor.
func buildPolicyWASM(t *testing.T) []byte {
	t.Helper()
	out := filepath.Join(t.TempDir(), "p.wasm")
	cmd := exec.Command("go", "build", "-buildmode=c-shared", "-o", out, "./testdata/shortestqueue")
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("cannot build wasip1 WASM policy (needs Go 1.24+): %v\n%s", err, b)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestWASMEngine(t *testing.T) {
	c, err := Compile(Binding{Kind: KindWASM, Source: buildPolicyWASM(t)})
	if err != nil {
		t.Fatalf("compile wasm: %v", err)
	}
	defer c.Close()
	view := LaneView{Groups: []GroupView{{ID: "a", Weight: 1, Backlog: 30}, {ID: "b", Weight: 1, Backlog: 5}}}
	sc, err := c.Score(view, ConsumerView{})
	if err != nil {
		t.Fatalf("score: %v", err)
	}
	if sc[0] != -30 || sc[1] != -5 {
		t.Fatalf("wasm shortest-queue scores = %v, want [-30 -5]", sc)
	}
}
