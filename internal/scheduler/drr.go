// Package scheduler is the broker-owned fairness MECHANISM. Phase 0 ships a
// Deficit Round Robin scheduler in Go (no policy engine yet). It decides which
// group serves the next message so a large group never head-of-line-blocks a
// small one. Deficits and the round-robin cursor live here in memory on the
// leader; later phases move the eligibility signal into the programmable policy.
package scheduler

import "sync"

// GroupStat is the current view of one active group (Ready>0) in a lane.
type GroupStat struct {
	ID     string
	Ready  int
	Weight float64
}

type laneState struct {
	quantum float64
	ring    []string           // active groups, round-robin order
	inring  map[string]bool    // membership set
	deficit map[string]float64 // DRR deficit per group
	granted bool               // has the current head been granted its quantum?
}

type Scheduler struct {
	mu      sync.Mutex
	lanes   map[string]*laneState
	quantum float64
}

func New() *Scheduler { return &Scheduler{lanes: map[string]*laneState{}, quantum: 1.0} }

func (s *Scheduler) lane(name string) *laneState {
	ls := s.lanes[name]
	if ls == nil {
		ls = &laneState{quantum: s.quantum, inring: map[string]bool{}, deficit: map[string]float64{}}
		s.lanes[name] = ls
	}
	return ls
}

// Pick returns the next group to serve one message from, given the lane's
// currently-active groups. Returns ("", false) when there is no servable work
// this instant (the caller polls again, and deficits accumulate across calls).
func (s *Scheduler) Pick(lane string, active []GroupStat) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ls := s.lane(lane)

	stat := make(map[string]GroupStat, len(active))
	for _, g := range active {
		stat[g.ID] = g
		if !ls.inring[g.ID] {
			ls.ring = append(ls.ring, g.ID)
			ls.inring[g.ID] = true
			if _, ok := ls.deficit[g.ID]; !ok {
				ls.deficit[g.ID] = 0
			}
		}
	}

	guard := 0
	for len(ls.ring) > 0 && guard < len(ls.ring)+2 {
		g := ls.ring[0]
		st, ok := stat[g]
		if !ok || st.Ready <= 0 {
			// drained or gone: drop the head and try the next.
			ls.ring = ls.ring[1:]
			delete(ls.inring, g)
			ls.granted = false
			guard++
			continue
		}
		w := st.Weight
		if w <= 0 {
			w = 1.0
		}
		if !ls.granted {
			ls.deficit[g] += ls.quantum * w
			ls.granted = true
		}
		if ls.deficit[g] >= 1.0 {
			ls.deficit[g] -= 1.0
			return g, true // serve one; keep g at the head until its deficit runs out
		}
		// deficit exhausted: rotate g to the back and grant the next head.
		ls.ring = append(ls.ring[1:], g)
		ls.granted = false
		guard++
	}
	return "", false
}
