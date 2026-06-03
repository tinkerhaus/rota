// Package policy is Rota's programmable fair-scheduler. A policy is a PURE
// scoring function over a read-only view of a lane's groups: it returns one
// score per group (higher = serve sooner). The broker owns the fairness
// arithmetic (virtual-time / deficit) and only asks the policy to rank. This is
// what keeps leader-only evaluation safe: the broker replicates the DECISION,
// never the policy code, and all fairness state lives in the FSM/scheduler.
//
// Engines: built-in (Go), CEL (expressions), and WASM (any language via wazero).
package policy

// GroupView is the read-only snapshot of one candidate group handed to a policy.
type GroupView struct {
	ID          string
	Weight      float64
	BatchSize   uint32
	Backlog     int     // leasable messages
	InFlight    int     // leased-but-unacked
	Deficit     float64 // broker-maintained: how far behind (maxVT - VT)
	VirtualTime float64 // broker-maintained WFQ virtual time
	AgeMs       int64   // age of the oldest leasable message
}

// LaneView is the read-only snapshot of a lane at scheduling time.
type LaneView struct {
	Lane   string
	Groups []GroupView
	NowMs  int64
	Turn   int64
}

// ConsumerView describes the consumer asking for work.
type ConsumerView struct {
	Credit int
}
