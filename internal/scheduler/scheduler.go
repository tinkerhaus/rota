// Package scheduler is the broker-owned fairness mechanism. It maintains each
// group's weighted virtual time and asks the lane's Policy to rank groups; the
// highest-scored group with backlog is served next, then its virtual time is
// advanced. The default (and the fallback when a policy faults) is weighted fair
// queueing, which gives weighted fairness with no head-of-line blocking.
package scheduler

import (
	"math"
	"sync"

	"github.com/tinkerhaus/rota/internal/policy"
)

const (
	quarantineThreshold = 5
	quarantineWindowMs  = 10_000
)

// GroupStat is the current view of one active group (Backlog>0) in a lane.
type GroupStat struct {
	ID       string
	Backlog  int
	InFlight int
	Weight   float64
}

type laneState struct {
	vt          map[string]float64
	turn        int64
	pol         policy.Compiled
	faults      int
	windowStart int64
	quarantined bool
}

type Scheduler struct {
	mu    sync.Mutex
	lanes map[string]*laneState
}

func New() *Scheduler { return &Scheduler{lanes: map[string]*laneState{}} }

func (s *Scheduler) lane(name string) *laneState {
	ls := s.lanes[name]
	if ls == nil {
		ls = &laneState{vt: map[string]float64{}}
		s.lanes[name] = ls
	}
	return ls
}

// SetPolicy installs (hot-swaps) a compiled policy for a lane and clears any
// prior quarantine.
func (s *Scheduler) SetPolicy(lane string, c policy.Compiled) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ls := s.lane(lane)
	if ls.pol != nil {
		ls.pol.Close()
	}
	ls.pol = c
	ls.quarantined = false
	ls.faults = 0
}

// ClearPolicy reverts a lane to the built-in WFQ default.
func (s *Scheduler) ClearPolicy(lane string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ls := s.lane(lane)
	if ls.pol != nil {
		ls.pol.Close()
		ls.pol = nil
	}
	ls.quarantined = false
	ls.faults = 0
}

func (s *Scheduler) HasPolicy(lane string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ls := s.lanes[lane]
	return ls != nil && ls.pol != nil
}

func (s *Scheduler) Quarantined(lane string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ls := s.lanes[lane]
	return ls != nil && ls.quarantined
}

// Pick chooses the next group to serve one message from. usedFallback reports
// that the lane's policy faulted and WFQ was used for this tick.
func (s *Scheduler) Pick(lane string, active []GroupStat, nowMs int64) (gid string, usedFallback bool, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(active) == 0 {
		return "", false, false
	}
	ls := s.lane(lane)

	// New groups join at the current minimum virtual time so they neither get a
	// free head start nor are instantly starved.
	minVt := math.Inf(1)
	for _, g := range active {
		if v, ok := ls.vt[g.ID]; ok && v < minVt {
			minVt = v
		}
	}
	if math.IsInf(minVt, 1) {
		minVt = 0
	}
	for _, g := range active {
		if _, ok := ls.vt[g.ID]; !ok {
			ls.vt[g.ID] = minVt
		}
	}
	maxVt := minVt
	for _, g := range active {
		if ls.vt[g.ID] > maxVt {
			maxVt = ls.vt[g.ID]
		}
	}

	gv := make([]policy.GroupView, len(active))
	for i, g := range active {
		gv[i] = policy.GroupView{
			ID: g.ID, Weight: g.Weight, Backlog: g.Backlog, InFlight: g.InFlight,
			VirtualTime: ls.vt[g.ID], Deficit: maxVt - ls.vt[g.ID],
		}
	}
	view := policy.LaneView{Lane: lane, Groups: gv, NowMs: nowMs, Turn: ls.turn}

	var scores []float64
	if ls.pol != nil && !ls.quarantined {
		sc, err := ls.pol.Score(view, policy.ConsumerView{Credit: 1})
		if err != nil || len(sc) != len(gv) || hasNonFinite(sc) {
			usedFallback = true
			s.recordFault(ls, nowMs)
			scores = policy.WFQScores(gv)
		} else {
			scores = sc
		}
	} else {
		scores = policy.WFQScores(gv)
	}

	best := -1
	var bestScore float64
	for i, g := range active {
		if g.Backlog <= 0 {
			continue
		}
		if best < 0 || scores[i] > bestScore || (scores[i] == bestScore && g.ID < active[best].ID) {
			best = i
			bestScore = scores[i]
		}
	}
	if best < 0 {
		return "", usedFallback, false
	}
	gid = active[best].ID
	w := active[best].Weight
	if w <= 0 {
		w = 1
	}
	ls.vt[gid] += 1.0 / w // WFQ mechanism: serving costs 1 unit of weighted virtual time
	ls.turn++
	return gid, usedFallback, true
}

func (s *Scheduler) recordFault(ls *laneState, nowMs int64) {
	if ls.windowStart == 0 || nowMs-ls.windowStart > quarantineWindowMs {
		ls.windowStart = nowMs
		ls.faults = 0
	}
	ls.faults++
	if ls.faults >= quarantineThreshold {
		ls.quarantined = true
	}
}

func hasNonFinite(s []float64) bool {
	for _, x := range s {
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return true
		}
	}
	return false
}
