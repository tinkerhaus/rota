package node

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/policy"
	"github.com/tinkerhaus/rota/internal/storage"
)

// ─── Leader-only in-memory fairness projection ──────────────────────────────────
//
// The scheduler decides WHO is served and the FSM records the durable counters,
// but neither remembers the recent ORDER of service — and order is what makes
// fairness legible ("A,B,A,C,A,A…"). This projection is a cheap, leader-local,
// bounded record of the last N served (lane,group) events plus a rolling served
// count per (lane,group). It is updated on the hot lease path under a single
// small mutex (one append + one map bump), so it must stay O(1) and allocation-
// free in the steady state. It is intentionally NOT replicated: it is a live
// view, rebuilt naturally on leader change as the new leader serves work.

// ringCap bounds the global served-event ring. 512 events is enough to render a
// ~200-wide fairness "ribbon" per lane while staying tiny and bounded.
const ringCap = 512

// fairnessEvent is one served record: which lane, which group, and when.
type fairnessEvent struct {
	lane     string
	group    string
	servedAt int64 // unix ms
}

// fairnessProjection is the leader-local served-order + rolling-count view.
type fairnessProjection struct {
	mu sync.Mutex

	ring  [ringCap]fairnessEvent
	head  int    // next write index into ring
	count int    // number of valid entries (<= ringCap)
	total uint64 // monotonic total events ever recorded (debug/seq)

	// served counts the rolling window per lane -> group -> count. "Rolling" is
	// defined by the ring: counts decay as old events are overwritten, so they
	// track the same horizon the ribbon shows.
	served map[string]map[string]uint64

	// subscribers receive a non-blocking nudge ("something changed in <lane>")
	// so SSE streams can coalesce and re-render. The channel is buffered=1 and
	// writes are best-effort (a full buffer already means "dirty").
	subs map[int]*fairnessSub
	next int
}

type fairnessSub struct {
	lane string
	ch   chan struct{}
}

func newFairnessProjection() *fairnessProjection {
	return &fairnessProjection{
		served: map[string]map[string]uint64{},
		subs:   map[int]*fairnessSub{},
	}
}

// record appends one served event and bumps the rolling count. Called from the
// hot lease path right after a successful Pick+lease. Cheap: one mutex, one
// array write, one map bump, and a non-blocking notify. When the ring wraps, the
// overwritten event's rolling count is decremented so counts stay windowed.
func (p *fairnessProjection) record(lane, group string) {
	now := time.Now().UnixMilli()
	p.mu.Lock()
	if p.count == ringCap {
		// Evict the oldest (the slot we are about to overwrite) from the counts.
		old := p.ring[p.head]
		if gm := p.served[old.lane]; gm != nil {
			if gm[old.group] > 0 {
				gm[old.group]--
			}
			if gm[old.group] == 0 {
				delete(gm, old.group)
			}
			if len(gm) == 0 {
				delete(p.served, old.lane)
			}
		}
	} else {
		p.count++
	}
	p.ring[p.head] = fairnessEvent{lane: lane, group: group, servedAt: now}
	p.head = (p.head + 1) % ringCap
	p.total++

	gm := p.served[lane]
	if gm == nil {
		gm = map[string]uint64{}
		p.served[lane] = gm
	}
	gm[group]++

	// Notify subscribers of this lane (non-blocking, coalesced by buffer=1).
	for _, s := range p.subs {
		if s.lane != lane {
			continue
		}
		select {
		case s.ch <- struct{}{}:
		default:
		}
	}
	p.mu.Unlock()
}

// LaneFairnessSnapshot is the per-lane projection slice used by the read RPC and
// the SSE stream. ribbon is the last ~maxRibbon served group ids in order; served
// is the rolling per-group count; total is their sum.
type LaneFairnessSnapshot struct {
	Lane   string
	Ribbon []string
	Served map[string]uint64
	Total  uint64
}

// maxRibbon caps the ribbon length emitted to clients (the contract says ~200).
const maxRibbon = 200

// snapshot copies out one lane's served order + rolling counts. Read path: holds
// the mutex only for the copy, never while writing to a client.
func (p *fairnessProjection) snapshot(lane string) LaneFairnessSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := LaneFairnessSnapshot{Lane: lane, Served: map[string]uint64{}}
	if gm := p.served[lane]; gm != nil {
		for g, c := range gm {
			out.Served[g] = c
			out.Total += c
		}
	}
	// Walk the ring oldest→newest, keep only this lane's events, then tail to
	// maxRibbon. start is the oldest valid slot.
	start := (p.head - p.count + ringCap) % ringCap
	for i := 0; i < p.count; i++ {
		e := p.ring[(start+i)%ringCap]
		if e.lane == lane {
			out.Ribbon = append(out.Ribbon, e.group)
		}
	}
	if len(out.Ribbon) > maxRibbon {
		out.Ribbon = out.Ribbon[len(out.Ribbon)-maxRibbon:]
	}
	return out
}

// subscribe registers an SSE listener for one lane. It returns the nudge channel
// and an unsubscribe func. The channel is buffered=1: a pending nudge that has
// not been drained simply absorbs further events (coalescing is the point).
func (p *fairnessProjection) subscribe(lane string) (<-chan struct{}, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	id := p.next
	p.next++
	sub := &fairnessSub{lane: lane, ch: make(chan struct{}, 1)}
	p.subs[id] = sub
	return sub.ch, func() {
		p.mu.Lock()
		delete(p.subs, id)
		p.mu.Unlock()
	}
}

// ─── Node accessors ─────────────────────────────────────────────────────────────

// FairnessSnapshot returns this node's projection of a lane's recent service.
// Leader-only data: on a follower the projection is empty (which is honest — the
// follower has not been serving work).
func (n *Node) FairnessSnapshot(lane string) LaneFairnessSnapshot {
	return n.fairness.snapshot(lane)
}

// SubscribeFairness registers an SSE listener for a lane and returns its coalesced
// nudge channel plus an unsubscribe func. See fairnessProjection.subscribe.
func (n *Node) SubscribeFairness(lane string) (<-chan struct{}, func()) {
	return n.fairness.subscribe(lane)
}

// LaneFairness builds the per-group fairness snapshot for a lane: durable
// GroupMeta (weight, virtual time, deficit, backlog, paused, last-served time)
// joined with the in-memory rolling served counts. Shares are derived here:
//
//	expectedShare = weight / sum(weight over ACTIVE groups)
//	actualShare   = served / totalServed (rolling window)
//	starvation    = (now - lastServed) * backlog * max(0, expected - actual),
//	                normalized to [0,1] across the lane.
//
// A group is "active" for share purposes if it has backlog (ready+inflight) or
// has been served in the window — i.e. it is a live participant, not an empty,
// idle group whose weight would otherwise dilute everyone's expected share.
// Read-only; follower-servable (a follower reports its own — possibly empty —
// projection joined with the replicated GroupMeta it holds).
func (n *Node) LaneFairness(lane string) (*rotav1.LaneFairness, error) {
	snap := n.fairness.snapshot(lane)

	lo := storage.GroupMetaLanePrefix(lane)
	hi := storage.PrefixEnd(lo)
	it, err := n.store.DB.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer it.Close()

	type row struct {
		gm      *rotav1.GroupMeta
		backlog uint64
		served  uint64
	}
	var rows []row
	var weightSum float64
	for it.First(); it.Valid(); it.Next() {
		gm := &rotav1.GroupMeta{}
		if proto.Unmarshal(it.Value(), gm) != nil {
			continue
		}
		backlog := gm.ReadyCount + gm.InflightCount + gm.DelayedCount
		served := snap.Served[gm.GroupId]
		active := !gm.Paused && (backlog > 0 || served > 0)
		if active && gm.Weight > 0 {
			weightSum += gm.Weight
		}
		rows = append(rows, row{gm: gm, backlog: backlog, served: served})
	}
	if weightSum <= 0 {
		weightSum = 1 // avoid div-by-zero; everyone's expected share collapses to 0/weight
	}

	now := time.Now().UnixMilli()
	out := &rotav1.LaneFairness{Lane: lane, TotalServed: snap.Total}

	// First pass: shares + raw starvation pressure; second pass normalizes.
	type calc struct {
		gf  *rotav1.GroupFairness
		raw float64
	}
	calcs := make([]calc, 0, len(rows))
	var maxRaw float64
	for _, r := range rows {
		gm := r.gm
		backlog := r.backlog
		served := r.served
		active := !gm.Paused && (backlog > 0 || served > 0)

		var expected float64
		if active && gm.Weight > 0 {
			expected = gm.Weight / weightSum
		}
		var actual float64
		if snap.Total > 0 {
			actual = float64(served) / float64(snap.Total)
		}

		// Raw starvation pressure: a group that is behind its expected share AND
		// has waiting work AND has not been served recently. lastServedTs is the
		// durable counter; 0 (never served) ⇒ treat the wait as the full window.
		var ageMs float64
		if gm.LastServedTs > 0 {
			ageMs = float64(now - gm.LastServedTs)
		} else if gm.LastActivityMs > 0 {
			ageMs = float64(now - int64(gm.LastActivityMs))
		}
		if ageMs < 0 {
			ageMs = 0
		}
		gap := expected - actual
		if gap < 0 {
			gap = 0
		}
		raw := ageMs * float64(backlog) * gap
		if raw > maxRaw {
			maxRaw = raw
		}

		calcs = append(calcs, calc{
			gf: &rotav1.GroupFairness{
				GroupId: gm.GroupId, Weight: gm.Weight,
				ExpectedShare: expected, ActualShare: actual,
				VirtualTime: gm.VirtualTime, Deficit: gm.Deficit,
				Served: served, Paused: gm.Paused,
			},
			raw: raw,
		})
	}
	for _, c := range calcs {
		if maxRaw > 0 {
			c.gf.StarvationScore = c.raw / maxRaw
		}
		out.Groups = append(out.Groups, c.gf)
	}
	return out, nil
}

// PolicyHealth reports a lane's live scheduling-policy status: bound version,
// quarantine state, engine, and the current fallback-to-WFQ fault count. The
// version/engine come from the replicated binding; quarantine/faults from the
// leader-local scheduler. Read-only; follower-servable.
func (n *Node) PolicyHealth(lane string) *rotav1.PolicyHealth {
	h := &rotav1.PolicyHealth{
		Lane:        lane,
		Quarantined: n.PolicyQuarantined(lane),
		Faults:      uint64(n.sched.Faults(lane)),
		Engine:      "builtin",
	}
	raw, ok, err := n.store.GetRaw(storage.PolicyKey(lane))
	if err != nil || !ok {
		return h
	}
	var b policy.Binding
	if json.Unmarshal(raw, &b) != nil {
		return h
	}
	h.Version = b.Version
	switch b.Kind {
	case policy.KindCEL:
		h.Engine = "cel"
	case policy.KindWASM:
		h.Engine = "wasm"
	default:
		h.Engine = "builtin"
	}
	return h
}
