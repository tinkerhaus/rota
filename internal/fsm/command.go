package fsm

// Command is the Raft log entry envelope. The leader proposes it; every node
// applies it identically. JSON keeps things simple for now (protobuf later).
type CmdType uint8

const (
	CmdPublish CmdType = iota + 1
	CmdLease
	CmdAck
	CmdNack
	CmdExtend
	CmdFireTimer
	CmdSetPolicy
)

// NackMode mirrors rota.v1.NackMode.
type NackMode uint8

const (
	NackRequeueNoPenalty NackMode = 0
	NackRetry            NackMode = 1
	NackDeadLetter       NackMode = 2
)

type Command struct {
	Type    CmdType       `json:"t"`
	Publish *PublishCmd   `json:"p,omitempty"`
	Lease   *LeaseCmd     `json:"l,omitempty"`
	Ack     *AckCmd       `json:"a,omitempty"`
	Nack    *NackCmd      `json:"n,omitempty"`
	Extend  *ExtendCmd    `json:"e,omitempty"`
	Fire    *FireTimerCmd `json:"f,omitempty"`
	Policy  *PolicyCmd    `json:"pol,omitempty"`
}

// PolicyCmd installs a lane's policy binding (replicated as config; the leader
// then compiles and hot-swaps it into the scheduler).
type PolicyCmd struct {
	Lane    string `json:"lane"`
	Binding []byte `json:"b"` // JSON-encoded policy.Binding
}

type PublishCmd struct {
	Lane        string            `json:"lane"`
	GroupID     string            `json:"g"`
	Payload     []byte            `json:"pl,omitempty"`
	Headers     map[string]string `json:"h,omitempty"`
	Weight      float64           `json:"w,omitempty"`
	HasWeight   bool              `json:"hw,omitempty"`
	BatchSize   uint32            `json:"bs,omitempty"`
	HasBatch    bool              `json:"hb,omitempty"`
	MaxAttempts uint32            `json:"ma,omitempty"`
	NotBeforeMs uint64            `json:"nb,omitempty"` // 0 ⇒ eligible now; else DELAYED until T
	NowMs       uint64            `json:"now,omitempty"`
}

// LeaseCmd carries the leader's DECISION (which group to serve). The FSM picks
// the head READY message of that group and assigns the lease id deterministically.
type LeaseCmd struct {
	Lane       string `json:"lane"`
	GroupID    string `json:"g"`
	ConsumerID string `json:"c"`
	DeadlineMs uint64 `json:"d"` // absolute visibility deadline, leader-stamped
}

type AckCmd struct {
	LeaseID uint64 `json:"lid"`
}

type NackCmd struct {
	LeaseID     uint64            `json:"lid"`
	Mode        NackMode          `json:"m"`
	DelayMs     uint64            `json:"dl,omitempty"` // optional redelivery delay (no-penalty / retry)
	FailureMeta map[string]string `json:"fm,omitempty"`
	NowMs       uint64            `json:"now"` // leader clock, for delay/backoff base
}

type ExtendCmd struct {
	LeaseID       uint64 `json:"lid"`
	NewDeadlineMs uint64 `json:"d"` // absolute, leader-stamped
}

// FireTimerCmd is leader-proposed when the time index sweep finds a due entry.
// It carries the exact timer coordinates (so the FSM deletes the right row) plus
// the leader-stamped fire_at used for any downstream time math (e.g. retry base).
type FireTimerCmd struct {
	Kind   byte   `json:"k"`
	DueTs  uint64 `json:"due"`
	Ref    []byte `json:"r"`
	FireAt uint64 `json:"fa"`
}

// Apply results, returned to the leader's waiting RPC via the raft future.
type PublishResult struct{ MsgID uint64 }

type LeaseResult struct {
	LeaseID    uint64
	MsgID      uint64
	Lane       string
	GroupID    string
	Payload    []byte
	Headers    map[string]string
	Attempt    uint32
	DeadlineMs uint64
	Empty      bool // no leasable message (benign)
}

type AckResult struct{ OK bool }
type NackResult struct {
	OK           bool
	DeadLettered bool
}
type ExtendResult struct{ OK bool }
