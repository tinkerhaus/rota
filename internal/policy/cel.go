package policy

import (
	"fmt"

	"github.com/google/cel-go/cel"
)

// celPolicy evaluates a user CEL expression once per group. CEL is
// non-Turing-complete (no unbounded loop can hang the tick) and cost-bounded.
// The expression sees: weight, backlog, inflight, deficit, virtual_time, age_ms.
type celPolicy struct {
	prg cel.Program
}

func celEnv() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Variable("weight", cel.DoubleType),
		cel.Variable("backlog", cel.IntType),
		cel.Variable("inflight", cel.IntType),
		cel.Variable("deficit", cel.DoubleType),
		cel.Variable("virtual_time", cel.DoubleType),
		cel.Variable("age_ms", cel.IntType),
	)
}

func compileCEL(src string) (Compiled, error) {
	env, err := celEnv()
	if err != nil {
		return nil, err
	}
	ast, iss := env.Compile(src)
	if iss != nil && iss.Err() != nil {
		return nil, fmt.Errorf("cel compile: %w", iss.Err())
	}
	prg, err := env.Program(ast, cel.EvalOptions(cel.OptOptimize), cel.CostLimit(1_000_000))
	if err != nil {
		return nil, fmt.Errorf("cel program: %w", err)
	}
	return &celPolicy{prg: prg}, nil
}

func (c *celPolicy) Score(l LaneView, _ ConsumerView) ([]float64, error) {
	out := make([]float64, len(l.Groups))
	for i, g := range l.Groups {
		val, _, err := c.prg.Eval(map[string]any{
			"weight":       g.Weight,
			"backlog":      int64(g.Backlog),
			"inflight":     int64(g.InFlight),
			"deficit":      g.Deficit,
			"virtual_time": g.VirtualTime,
			"age_ms":       g.AgeMs,
		})
		if err != nil {
			return nil, err
		}
		switch x := val.Value().(type) {
		case float64:
			out[i] = x
		case int64:
			out[i] = float64(x)
		default:
			return nil, fmt.Errorf("policy expression must yield a number, got %T", val.Value())
		}
	}
	return out, nil
}

func (c *celPolicy) Close() {}
