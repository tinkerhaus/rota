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
	CmdGroupConfig
	CmdGroupLifecycle
	CmdCron
	CmdFireCron
	CmdIssueToken
	CmdComplete
	CmdSingleton
	CmdSetLaneConfig
	CmdReapGroup
	CmdTeardownGroup
	CmdPublishBatch
)

type GroupOp uint8

const (
	GroupPause GroupOp = iota
	GroupResume
	GroupCancel
	GroupPurge
)

type CronOp uint8

const (
	CronSchedule CronOp = iota
	CronDelete
	CronPause
	CronResume
)

type SingletonOp uint8

const (
	SingletonAcquire SingletonOp = iota
	SingletonRenew
	SingletonRelease
)

// NackMode mirrors rota.v1.NackMode.
type NackMode uint8

const (
	NackRequeueNoPenalty NackMode = 0
	NackRetry            NackMode = 1
	NackDeadLetter       NackMode = 2
)

type Command struct {
	Type         CmdType          `json:"t"`
	Publish      *PublishCmd      `json:"p,omitempty"`
	PublishBatch *PublishBatchCmd `json:"pb,omitempty"`
	Lease        *LeaseCmd        `json:"l,omitempty"`
	Ack          *AckCmd          `json:"a,omitempty"`
	Nack         *NackCmd         `json:"n,omitempty"`
	Extend       *ExtendCmd       `json:"e,omitempty"`
	Fire         *FireTimerCmd    `json:"f,omitempty"`
	Policy       *PolicyCmd       `json:"pol,omitempty"`

	GroupConfig    *GroupConfigCmd    `json:"gc,omitempty"`
	GroupLifecycle *GroupLifecycleCmd `json:"gl,omitempty"`
	Cron           *CronCmd           `json:"cr,omitempty"`
	FireCron       *FireCronCmd       `json:"fc,omitempty"`
	IssueToken     *IssueTokenCmd     `json:"it,omitempty"`
	Complete       *CompleteCmd       `json:"cp,omitempty"`
	Singleton      *SingletonCmd      `json:"sg,omitempty"`
	LaneConfig     *LaneConfigCmd     `json:"lc,omitempty"`
	ReapGroup      *ReapGroupCmd      `json:"rg,omitempty"`
	Teardown       *TeardownCmd       `json:"td,omitempty"`
}

type LaneConfigCmd struct {
	Lane       string  `json:"lane"`
	RatePerSec float64 `json:"r"`
	Burst      uint32  `json:"b"`
}

type ReapGroupCmd struct {
	Lane    string `json:"lane"`
	GroupID string `json:"g"`
}

type TeardownCmd struct {
	GroupID string `json:"g"`
}

type TeardownResult struct {
	AffectedLanes []string
	Affected      uint64
}

type GroupConfigCmd struct {
	Lane      string   `json:"lane"`
	GroupID   string   `json:"g"`
	Weight    *float64 `json:"w,omitempty"`
	BatchSize *uint32  `json:"bs,omitempty"`
}

type GroupLifecycleCmd struct {
	Lane    string  `json:"lane"`
	GroupID string  `json:"g"`
	Op      GroupOp `json:"op"`
}

type CronCmd struct {
	Op         CronOp            `json:"op"`
	CronID     string            `json:"id"`
	Lane       string            `json:"lane,omitempty"`
	GroupID    string            `json:"g,omitempty"`
	Payload    []byte            `json:"pl,omitempty"`
	Headers    map[string]string `json:"h,omitempty"`
	Schedule   string            `json:"s,omitempty"`
	NextFireMs uint64            `json:"nf,omitempty"`
	NowMs      uint64            `json:"now,omitempty"`
}

type FireCronCmd struct {
	CronID     string `json:"id"`
	FireAt     uint64 `json:"fa"`
	NextFireMs uint64 `json:"nf"`
}

type IssueTokenCmd struct {
	LeaseID   uint64 `json:"lid"`
	TokenHash []byte `json:"th"`
}

type CompleteCmd struct {
	TokenHash []byte            `json:"th"`
	Success   bool              `json:"ok"`
	Meta      map[string]string `json:"m,omitempty"`
	NowMs     uint64            `json:"now"`
}

type SingletonCmd struct {
	Op     SingletonOp `json:"op"`
	Name   string      `json:"n"`
	Holder string      `json:"h"`
	TTLms  uint64      `json:"ttl,omitempty"`
	Fence  uint64      `json:"f,omitempty"`
	NowMs  uint64      `json:"now"`
}

type GroupOpResult struct{ Affected uint64 }
type SingletonResult struct {
	OK     bool
	Fence  uint64
	Holder string
}
type CompleteResult struct {
	OK           bool
	DeadLettered bool
	Unknown      bool
}

// PolicyCmd installs a lane's policy binding (replicated as config; the leader
// then compiles and hot-swaps it into the scheduler).
type PolicyCmd struct {
	Lane    string `json:"lane"`
	Binding []byte `json:"b"` // JSON-encoded policy.Binding
}

type PublishCmd struct {
	Lane          string            `json:"lane"`
	GroupID       string            `json:"g"`
	Payload       []byte            `json:"pl,omitempty"`
	Headers       map[string]string `json:"h,omitempty"`
	Weight        float64           `json:"w,omitempty"`
	HasWeight     bool              `json:"hw,omitempty"`
	BatchSize     uint32            `json:"bs,omitempty"`
	HasBatch      bool              `json:"hb,omitempty"`
	MaxAttempts   uint32            `json:"ma,omitempty"`
	NotBeforeMs   uint64            `json:"nb,omitempty"` // 0 ⇒ eligible now; else DELAYED until T
	NowMs         uint64            `json:"now,omitempty"`
	IssueToken    bool              `json:"itk,omitempty"` // mint a completion token at lease time
	ExternalToken []byte            `json:"etk,omitempty"` // producer-supplied completion token (optional)
}

// PublishBatchCmd applies many publishes in one Raft entry. Atomic ⇒ any item
// error aborts the whole batch (nothing commits); best-effort ⇒ good items
// commit and per-item failures are reported.
type PublishBatchCmd struct {
	Items  []PublishCmd `json:"items"`
	Atomic bool         `json:"atomic,omitempty"`
}

type PublishItemResult struct {
	MsgID uint64
	OK    bool
	Err   string
}

type PublishBatchResult struct{ Items []PublishItemResult }

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

	// Complete-by-token intent carried from the message, so the leader can mint
	// (IssueToken) or register (ExternalToken) the token after leasing.
	IssueToken    bool
	ExternalToken []byte
}

type AckResult struct{ OK bool }
type NackResult struct {
	OK           bool
	DeadLettered bool
}
type ExtendResult struct{ OK bool }
