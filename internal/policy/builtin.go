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
// lease-turns. Plain WFQ advances a group's virtual time once per lease handed
// out; a tenant that leases fast but never completes (acks) looks "fair" by
// turns while hogging in-flight capacity. This policy inflates each group's
// effective virtual time by its in-flight backlog normalized by weight
// (inflight/weight): the more leased-but-unacked work a tenant is sitting on
// relative to its share, the further "ahead" it looks, so it is served later.
// Score is -(VirtualTime + inflight/weight) so the least-ahead group wins, same
// orientation as WFQ. Two equal-weight groups at equal virtual time are split by
// in-flight: the lower-inflight group scores higher and is served first.
type builtinCompletionAware struct{}

func (builtinCompletionAware) Score(l LaneView, _ ConsumerView) ([]float64, error) {
	out := make([]float64, len(l.Groups))
	for i, g := range l.Groups {
		w := g.Weight
		if w <= 0 {
			w = 1
		}
		out[i] = -(g.VirtualTime + float64(g.InFlight)/w)
	}
	return out, nil
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
