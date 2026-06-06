package policy

import "hash/fnv"

// builtinWFQ is the default: weighted-fair, no head-of-line blocking.
type builtinWFQ struct{}

func (builtinWFQ) Score(l LaneView, _ ConsumerView) ([]float64, error) {
	return WFQScores(l.Groups), nil
}
func (builtinWFQ) Close() {}

// builtinStrict serves higher-weight groups first (strict priority by weight),
// staying fair (by virtual time) within a weight tier. A heavy group can starve
// a light one — that's the point of strict priority.
type builtinStrict struct{}

func (builtinStrict) Score(l LaneView, _ ConsumerView) ([]float64, error) {
	out := make([]float64, len(l.Groups))
	for i, g := range l.Groups {
		out[i] = g.Weight*1e6 - g.VirtualTime
	}
	return out, nil
}
func (builtinStrict) Close() {}

// builtinCompletionAware is WFQ that also accounts for OUTSTANDING work, not just
// lease-turns. Plain WFQ advances a group's virtual time by 1/weight once per
// lease; a tenant that leases fast but holds work (slow/never acks) looks "fair"
// by turns while hogging in-flight capacity.
//
// It RANKS exactly like WFQ (-VirtualTime), but charges a per-serve virtual-time
// COST of (1 + inflight)/weight instead of the flat 1/weight (see ServeCost). So
// every time a tenant with k in-flight items is served, its virtual clock jumps by
// (1+k)/weight — it is served roughly 1/(1+k) as often as an idle peer for as long
// as it sits on that work. That is a SUSTAINED rate reduction proportional to the
// hoarding, not the one-time offset an additive score term gave. When the tenant
// drains its in-flight, the cost falls back to baseline and WFQ's virtual-time
// catch-up serves it more until it is even again.
type builtinCompletionAware struct{}

func (builtinCompletionAware) Score(l LaneView, _ ConsumerView) ([]float64, error) {
	return WFQScores(l.Groups), nil
}

// ServeCost charges (1 + inflight)/weight per serve: weight-normalized so a heavier
// tenant (entitled to more capacity) tolerates more in-flight before being throttled.
func (builtinCompletionAware) ServeCost(g GroupView) float64 {
	w := g.Weight
	if w <= 0 {
		w = 1
	}
	return (1 + float64(g.InFlight)) / w
}

func (builtinCompletionAware) Close() {}

// builtinLottery weights random-ish selection by group weight (deterministic per
// turn, since only the leader evaluates and only the outcome is replicated).
type builtinLottery struct{}

func (builtinLottery) Score(l LaneView, _ ConsumerView) ([]float64, error) {
	out := make([]float64, len(l.Groups))
	for i, g := range l.Groups {
		h := fnv.New64a()
		_, _ = h.Write([]byte(g.ID))
		var t [8]byte
		for j := 0; j < 8; j++ {
			t[j] = byte(uint64(l.Turn) >> (8 * j))
		}
		_, _ = h.Write(t[:])
		jitter := float64(h.Sum64()%1000) / 1000.0
		out[i] = g.Weight * (1.0 + jitter)
	}
	return out, nil
}
func (builtinLottery) Close() {}
