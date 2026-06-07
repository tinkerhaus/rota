package node

import (
	"context"
	"sync"
	"time"
)

// laneMeter is a leader-local EWMA rate meter per lane. Publish and lease events
// are counted cheaply on the hot path (one map bump under a small mutex); a 1s
// sampler turns the cumulative deltas into smoothed publish/lease rates that the
// dashboard's sparklines and throughput readout consume. Leader-only and not
// replicated — a live view, rebuilt naturally after a leader change.
type laneMeter struct {
	mu   sync.Mutex
	pub  map[string]uint64
	lse  map[string]uint64
	ack  map[string]uint64
	last map[string][3]uint64 // lane -> {pub, lease, ack} at the previous sample
	rate map[string]LaneRate  // smoothed
}

// LaneRate is a lane's smoothed publish/lease/ack rate (events per second).
type LaneRate struct{ Publish, Lease, Ack float64 }

func newLaneMeter() *laneMeter {
	return &laneMeter{
		pub: map[string]uint64{}, lse: map[string]uint64{}, ack: map[string]uint64{},
		last: map[string][3]uint64{}, rate: map[string]LaneRate{},
	}
}

func (m *laneMeter) incPublish(lane string) { m.mu.Lock(); m.pub[lane]++; m.mu.Unlock() }
func (m *laneMeter) incLease(lane string)   { m.mu.Lock(); m.lse[lane]++; m.mu.Unlock() }
func (m *laneMeter) incAck(lane string)     { m.mu.Lock(); m.ack[lane]++; m.mu.Unlock() }

func (m *laneMeter) sample(dt float64) {
	const a = 0.45 // EWMA smoothing
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := map[string]bool{}
	for lane := range m.pub {
		seen[lane] = true
	}
	for lane := range m.lse {
		seen[lane] = true
	}
	for lane := range m.ack {
		seen[lane] = true
	}
	for lane := range seen {
		cum := [3]uint64{m.pub[lane], m.lse[lane], m.ack[lane]}
		prev := m.last[lane]
		ip := float64(cum[0]-prev[0]) / dt
		il := float64(cum[1]-prev[1]) / dt
		ia := float64(cum[2]-prev[2]) / dt
		r := m.rate[lane]
		r.Publish = a*ip + (1-a)*r.Publish
		r.Lease = a*il + (1-a)*r.Lease
		r.Ack = a*ia + (1-a)*r.Ack
		m.rate[lane] = r
		m.last[lane] = cum
	}
}

func (m *laneMeter) rates(lane string) LaneRate {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rate[lane]
}

// meterLoop samples the lane meter once a second to refresh the smoothed rates.
func (n *Node) meterLoop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			dt := now.Sub(last).Seconds()
			last = now
			if dt > 0 {
				n.meter.sample(dt)
			}
		}
	}
}
