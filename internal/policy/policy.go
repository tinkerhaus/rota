package policy

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
)

type Kind string

const (
	KindDRR             Kind = "drr" // alias for the WFQ default
	KindWFQ             Kind = "wfq"
	KindStrictPriority  Kind = "strict_priority"
	KindLottery         Kind = "lottery"
	KindCompletionAware Kind = "completion_aware"
	KindCEL             Kind = "cel"
	KindWASM            Kind = "wasm"
)

// Binding is the durable, replicated description of a lane's policy.
type Binding struct {
	Kind    Kind   `json:"kind"`
	Source  []byte `json:"source,omitempty"` // CEL expression or WASM module bytes
	Version uint64 `json:"version"`
	Hash    string `json:"hash,omitempty"`
}

// Compiled is a ready-to-run pure scoring policy. The scheduler calls Score
// serially (under its lock), so implementations need not be call-concurrent.
type Compiled interface {
	Score(lane LaneView, consumer ConsumerView) ([]float64, error)
	Close()
}

// ServeCoster is an OPTIONAL interface a policy may implement to charge a custom
// virtual-time increment when a group is served (the default is the plain WFQ
// 1/weight). Returning a larger value makes the group's virtual clock advance
// faster, so it is served proportionally LESS — a sustained rate effect, not the
// one-time offset an additive score term produces. completion_aware uses this so a
// tenant sitting on in-flight work is genuinely throttled.
type ServeCoster interface {
	ServeCost(g GroupView) float64
}

func HashSource(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// Compile builds a Compiled policy from a binding.
func Compile(b Binding) (Compiled, error) {
	switch b.Kind {
	case KindDRR, KindWFQ, "":
		return builtinWFQ{}, nil
	case KindStrictPriority:
		return builtinStrict{}, nil
	case KindLottery:
		return builtinLottery{}, nil
	case KindCompletionAware:
		return builtinCompletionAware{}, nil
	case KindCEL:
		return compileCEL(string(b.Source))
	case KindWASM:
		return compileWASM(b.Source)
	default:
		return nil, fmt.Errorf("unknown policy kind %q", b.Kind)
	}
}

// Validate compiles a binding and smoke-evaluates it against synthetic fixtures
// (empty / 1-group / N-group lanes) BEFORE it is ever installed on the hot path.
func Validate(b Binding) error {
	c, err := Compile(b)
	if err != nil {
		return err
	}
	defer c.Close()
	for _, v := range smokeFixtures() {
		sc, err := c.Score(v, ConsumerView{Credit: 1})
		if err != nil {
			return fmt.Errorf("smoke eval failed: %w", err)
		}
		if len(sc) != len(v.Groups) {
			return fmt.Errorf("policy returned %d scores for %d groups", len(sc), len(v.Groups))
		}
		for _, s := range sc {
			if math.IsNaN(s) || math.IsInf(s, 0) {
				return fmt.Errorf("policy produced a non-finite score")
			}
		}
	}
	return nil
}

func smokeFixtures() []LaneView {
	mk := func(n int) LaneView {
		gs := make([]GroupView, n)
		for i := range gs {
			gs[i] = GroupView{ID: fmt.Sprintf("g%d", i), Weight: 1, Backlog: i + 1, VirtualTime: float64(i)}
		}
		return LaneView{Lane: "smoke", Groups: gs, NowMs: 1}
	}
	return []LaneView{mk(0), mk(1), mk(5)}
}

// WFQScores is the built-in fairness ranking and the runtime fallback: serve the
// lowest virtual-time group (the one most "behind" its weighted fair share).
func WFQScores(gv []GroupView) []float64 {
	out := make([]float64, len(gv))
	for i, g := range gv {
		out[i] = -g.VirtualTime
	}
	return out
}
