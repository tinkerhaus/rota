package scheduler

import (
	"errors"
	"testing"

	"github.com/tinkerhaus/rota/internal/policy"
)

type faultingPolicy struct{}

func (faultingPolicy) Score(policy.LaneView, policy.ConsumerView) ([]float64, error) {
	return nil, errors.New("boom")
}
func (faultingPolicy) Close() {}

func TestWFQFairness(t *testing.T) {
	s := New()
	active := []GroupStat{{ID: "A", Backlog: 100, Weight: 1}, {ID: "B", Backlog: 100, Weight: 1}}
	counts := map[string]int{}
	for i := 0; i < 20; i++ {
		gid, _, ok := s.Pick("l", active, int64(i))
		if !ok {
			t.Fatal("no pick")
		}
		counts[gid]++
	}
	if counts["A"] == 0 || counts["B"] == 0 {
		t.Fatalf("WFQ should interleave equal-weight groups: %v", counts)
	}
	if d := counts["A"] - counts["B"]; d > 2 || d < -2 {
		t.Fatalf("equal weight should be ~1:1, got %v", counts)
	}
}

func TestWeightedFairness(t *testing.T) {
	s := New()
	active := []GroupStat{{ID: "A", Backlog: 1000, Weight: 1}, {ID: "B", Backlog: 1000, Weight: 3}}
	counts := map[string]int{}
	for i := 0; i < 40; i++ {
		gid, _, _ := s.Pick("l", active, int64(i))
		counts[gid]++
	}
	// B has 3x the weight, so it should be served roughly 3x as often.
	ratio := float64(counts["B"]) / float64(counts["A"])
	if ratio < 2.3 || ratio > 3.7 {
		t.Fatalf("weight-3 group should get ~3x service, got B/A=%.2f (%v)", ratio, counts)
	}
}

func TestFaultFallbackAndQuarantine(t *testing.T) {
	s := New()
	s.SetPolicy("l", faultingPolicy{})
	active := []GroupStat{{ID: "A", Backlog: 10, Weight: 1}}
	fallbacks := 0
	for i := 0; i < 10; i++ {
		_, used, ok := s.Pick("l", active, int64(i))
		if !ok {
			t.Fatal("a faulting policy must still serve via WFQ fallback")
		}
		if used {
			fallbacks++
		}
	}
	if fallbacks == 0 {
		t.Fatal("expected WFQ fallback while the policy was faulting")
	}
	if !s.Quarantined("l") {
		t.Fatal("expected the lane to be quarantined after repeated faults")
	}
}
