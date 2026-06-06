// Package node wires Pebble + Raft + the FSM + the DRR scheduler into one broker
// node. It supports a single-voter in-memory transport (dev) or a real TCP raft
// cluster (HA), and runs the leader-only Chronos loop that fires due timers.
package node

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"
	"golang.org/x/time/rate"

	"github.com/tinkerhaus/rota/internal/fsm"
	"github.com/tinkerhaus/rota/internal/observe"
	"github.com/tinkerhaus/rota/internal/policy"
	"github.com/tinkerhaus/rota/internal/raftpebble"
	"github.com/tinkerhaus/rota/internal/scheduler"
	"github.com/tinkerhaus/rota/internal/storage"
)

const maxFireBatch = 256

// Peer identifies a raft voter for cluster bootstrap.
type Peer struct {
	ID   string
	Addr string
}

type Config struct {
	DataDir      string
	NodeID       string
	VisibilityMs uint64
	IdleReapMs   uint64 // reap drained groups idle longer than this; 0 = disabled

	// MaxLeaseLifetimeMs caps a single lease's TOTAL lifetime regardless of
	// ExtendVisibility, so a wedged async (complete-by-token) holder cannot pin a
	// message in-flight forever. 0 = disabled (default). Intended for long-lived
	// async leases; enabling it arms one extra timer per lease until the cap.
	MaxLeaseLifetimeMs uint64

	// Clustering. RaftBind == "" selects the in-memory transport (single-node
	// dev/test). Otherwise a TCP transport is created on RaftBind.
	RaftBind      string
	RaftAdvertise string // defaults to RaftBind
	Bootstrap     bool   // this node forms the initial cluster
	InitialPeers  []Peer // voter set for the bootstrapping node (empty ⇒ just self)

	// GRPCAddrs maps node id -> gRPC advertise address, so a follower can tell a
	// client the LEADER's gRPC address on a NOT_LEADER redirect. Passed to every
	// node (raft only knows raft addresses).
	GRPCAddrs map[string]string
}

type Node struct {
	cfg    Config
	store  *storage.Store
	fsm    *fsm.FSM
	raft   *raft.Raft
	sched  *scheduler.Scheduler
	cancel context.CancelFunc
	wg     sync.WaitGroup // background loops; Close waits on it before closing the store

	polMu        sync.Mutex
	loadedPolVer map[string]uint64 // lane -> compiled policy version on this node

	laneMu        sync.Mutex
	limiters      map[string]*rate.Limiter     // leader-local dequeue rate limiters
	loadedLaneCfg map[string]fsm.LaneConfigRec // last-applied lane config per lane
	paused        map[string]int64             // lane -> paused-until unix ms

	// fairness is the leader-only in-memory served-order/served-count projection
	// that powers the Fairness Observatory (GetLaneFairness + the SSE stream).
	fairness *fairnessProjection

	// meter is the leader-local per-lane EWMA publish/lease rate meter that powers
	// the dashboard throughput sparklines.
	meter *laneMeter
}

type PublishReq struct {
	Lane          string
	GroupID       string
	Payload       []byte
	Headers       map[string]string
	Weight        *float64
	BatchSize     *uint32
	NotBeforeMs   uint64
	MaxAttempts   uint32
	IssueToken    bool
	ExternalToken []byte
	DedupKey      string // producer idempotency key; "" disables dedup
}

// dedupWindowMs is how long a dedup_key suppresses re-publishes. Generous enough to
// absorb client retries/failover, bounded so the index self-sweeps.
const dedupWindowMs = 5 * 60 * 1000

func Open(cfg Config) (*Node, error) {
	if cfg.NodeID == "" {
		cfg.NodeID = "node1"
	}
	if cfg.VisibilityMs == 0 {
		cfg.VisibilityMs = 60_000
	}
	st, err := storage.Open(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	f, err := fsm.New(st)
	if err != nil {
		return nil, err
	}
	logStore := raftpebble.New(st)

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)
	rc.SnapshotThreshold = 8192
	rc.SnapshotInterval = 30 * time.Second
	rc.TrailingLogs = 1024
	rc.LogLevel = "ERROR"

	snaps, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, io.Discard)
	if err != nil {
		return nil, err
	}

	transport, selfAddr, err := newTransport(cfg)
	if err != nil {
		return nil, err
	}

	r, err := raft.NewRaft(rc, f, logStore, logStore, snaps, transport)
	if err != nil {
		return nil, err
	}
	hasState, err := raft.HasExistingState(logStore, logStore, snaps)
	if err != nil {
		return nil, err
	}
	// The in-memory single-node dev path auto-bootstraps; a TCP cluster requires
	// exactly one node to set Bootstrap.
	if (cfg.Bootstrap || cfg.RaftBind == "") && !hasState {
		servers := []raft.Server{}
		if len(cfg.InitialPeers) == 0 {
			servers = append(servers, raft.Server{ID: rc.LocalID, Address: selfAddr})
		} else {
			for _, p := range cfg.InitialPeers {
				servers = append(servers, raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.Addr)})
			}
		}
		if err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		cfg: cfg, store: st, fsm: f, raft: r, sched: scheduler.New(),
		cancel: cancel, loadedPolVer: map[string]uint64{},
		limiters: map[string]*rate.Limiter{}, loadedLaneCfg: map[string]fsm.LaneConfigRec{},
		paused: map[string]int64{}, fairness: newFairnessProjection(),
		meter: newLaneMeter(),
	}
	// Track the background loops so Close can drain them before closing the store
	// (they touch Pebble; closing the store from under a mid-iteration sweep panics).
	n.wg.Add(3)
	go func() { defer n.wg.Done(); n.chronosLoop(ctx) }()
	go func() { defer n.wg.Done(); n.reapLoop(ctx) }()
	go func() { defer n.wg.Done(); n.meterLoop(ctx) }()
	return n, nil
}

func newTransport(cfg Config) (raft.Transport, raft.ServerAddress, error) {
	if cfg.RaftBind == "" {
		addr, inm := raft.NewInmemTransport(raft.ServerAddress(cfg.NodeID))
		return inm, addr, nil
	}
	adv := cfg.RaftAdvertise
	if adv == "" {
		adv = cfg.RaftBind
	}
	tcpAddr, err := net.ResolveTCPAddr("tcp", adv)
	if err != nil {
		return nil, "", err
	}
	t, err := raft.NewTCPTransport(cfg.RaftBind, tcpAddr, 3, 10*time.Second, io.Discard)
	if err != nil {
		return nil, "", err
	}
	return t, raft.ServerAddress(adv), nil
}

func (n *Node) WaitLeader(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n.raft.State() == raft.Leader {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("no leader elected within %s", timeout)
}

// WaitClusterLeader waits until SOME node in the cluster is leader (this node may
// be a follower).
func (n *Node) WaitClusterLeader(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if addr, _ := n.raft.LeaderWithID(); addr != "" {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("no cluster leader within %s", timeout)
}

func (n *Node) IsLeader() bool { return n.raft.State() == raft.Leader }

func (n *Node) LeaderAddr() string {
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

// LeaderHint returns the current leader's gRPC advertise address (via the
// configured node-id -> gRPC-addr map; "" if unknown) and its node id. Used to
// populate NOT_LEADER redirects so a client can re-dial the leader directly.
func (n *Node) LeaderHint() (grpcAddr, id string) {
	_, lid := n.raft.LeaderWithID()
	return n.cfg.GRPCAddrs[string(lid)], string(lid)
}

func (n *Node) AddVoter(id, addr string) error {
	return n.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 10*time.Second).Error()
}

func (n *Node) Barrier(timeout time.Duration) error { return n.raft.Barrier(timeout).Error() }

func (n *Node) ForceSnapshot() error { return n.raft.Snapshot().Error() }

func (n *Node) Close() error {
	n.cancel()  // signal the background loops to stop
	n.wg.Wait() // and wait for them to finish touching the store
	_ = n.raft.Shutdown().Error()
	return n.store.Close()
}

func (n *Node) apply(cmd fsm.Command) (interface{}, error) {
	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	fut := n.raft.Apply(data, 5*time.Second)
	if err := fut.Error(); err != nil {
		return nil, err
	}
	if e, ok := fut.Response().(error); ok {
		return nil, e
	}
	return fut.Response(), nil
}

// publishCmd builds a PublishCmd from a PublishReq (one leader clock stamp).
func publishCmd(r PublishReq) *fsm.PublishCmd {
	pc := &fsm.PublishCmd{
		Lane: r.Lane, GroupID: r.GroupID, Payload: r.Payload, Headers: r.Headers,
		MaxAttempts: r.MaxAttempts, NotBeforeMs: r.NotBeforeMs, NowMs: nowMs(),
		IssueToken: r.IssueToken, ExternalToken: r.ExternalToken,
	}
	if r.Weight != nil {
		pc.HasWeight = true
		pc.Weight = *r.Weight
	}
	if r.BatchSize != nil {
		pc.HasBatch = true
		pc.BatchSize = *r.BatchSize
	}
	if r.DedupKey != "" {
		pc.DedupKey = r.DedupKey
		pc.DedupExpiryMs = pc.NowMs + dedupWindowMs
	}
	return pc
}

func (n *Node) Publish(r PublishReq) (uint64, error) {
	res, err := n.apply(fsm.Command{Type: fsm.CmdPublish, Publish: publishCmd(r)})
	if err != nil {
		return 0, err
	}
	pr, _ := res.(*fsm.PublishResult)
	if pr == nil {
		return 0, fmt.Errorf("publish: no result")
	}
	if !pr.Duplicate {
		observe.Publishes.Inc() // a dedup hit enqueued nothing
		n.meter.incPublish(r.Lane)
	}
	return pr.MsgID, nil
}

// PublishBatch applies many publishes in a single Raft entry. With atomic=true
// it is all-or-nothing; otherwise it is best-effort with per-item results.
func (n *Node) PublishBatch(reqs []PublishReq, atomic bool) ([]fsm.PublishItemResult, error) {
	items := make([]fsm.PublishCmd, len(reqs))
	for i, r := range reqs {
		items[i] = *publishCmd(r)
	}
	res, err := n.apply(fsm.Command{Type: fsm.CmdPublishBatch, PublishBatch: &fsm.PublishBatchCmd{Items: items, Atomic: atomic}})
	if err != nil {
		return nil, err
	}
	br, _ := res.(*fsm.PublishBatchResult)
	if br == nil {
		return nil, fmt.Errorf("publish_batch: no result")
	}
	for _, it := range br.Items {
		if it.OK {
			observe.Publishes.Inc()
		}
	}
	return br.Items, nil
}

func (n *Node) LeaseOne(lane, consumerID string) (*fsm.LeaseResult, bool, error) {
	n.ensurePolicy(lane)
	if !n.laneGate(lane) {
		return nil, false, nil // lane paused or rate-limited: backpressure
	}
	groups, err := n.store.ListGroups(lane)
	if err != nil {
		return nil, false, err
	}
	active := make([]scheduler.GroupStat, 0, len(groups))
	for _, g := range groups {
		if g.Ready > 0 && !g.Paused {
			active = append(active, scheduler.GroupStat{
				ID: g.ID, Backlog: int(g.Ready), InFlight: int(g.InFlight), Weight: g.Weight,
			})
		}
	}
	if len(active) == 0 {
		return nil, false, nil
	}
	gid, usedFallback, ok := n.sched.Pick(lane, active, int64(nowMs()))
	if !ok {
		return nil, false, nil
	}
	if usedFallback {
		observe.PolicyFaults.Inc()
	}
	lc := &fsm.LeaseCmd{
		Lane: lane, GroupID: gid, ConsumerID: consumerID, DeadlineMs: nowMs() + n.cfg.VisibilityMs,
	}
	if n.cfg.MaxLeaseLifetimeMs > 0 {
		lc.MaxLifeMs = nowMs() + n.cfg.MaxLeaseLifetimeMs
	}
	res, err := n.apply(fsm.Command{Type: fsm.CmdLease, Lease: lc})
	if err != nil {
		return nil, false, err
	}
	lr, _ := res.(*fsm.LeaseResult)
	if lr == nil || lr.Empty {
		return nil, false, nil
	}
	observe.Leases.Inc()
	n.meter.incLease(lane)
	// Leader-only fairness projection: record the served (lane,group) in order so
	// the Observatory can render the recent service ribbon and rolling shares.
	// Cheap (one append + one map bump under a small mutex); never blocks leasing.
	n.fairness.record(lane, lr.GroupID)
	// Complete-by-token: mint (or register a producer-supplied) token now that the
	// message is leased, and deliver it on the lease so the holder (or an async
	// callback) can resolve the message out-of-band by token.
	if lr.ExternalToken != nil {
		if err := n.registerToken(lr.LeaseID, lr.ExternalToken); err != nil {
			return nil, false, err
		}
	} else if lr.IssueToken {
		tok, err := n.IssueToken(lr.LeaseID)
		if err != nil {
			return nil, false, err
		}
		lr.ExternalToken = tok
	}
	return lr, true, nil
}

func (n *Node) Ack(leaseID uint64) error {
	_, err := n.apply(fsm.Command{Type: fsm.CmdAck, Ack: &fsm.AckCmd{LeaseID: leaseID}})
	if err == nil {
		observe.Acks.Inc()
	}
	return err
}

func (n *Node) Nack(leaseID uint64, mode fsm.NackMode, delayMs uint64, meta map[string]string) (bool, error) {
	res, err := n.apply(fsm.Command{Type: fsm.CmdNack, Nack: &fsm.NackCmd{
		LeaseID: leaseID, Mode: mode, DelayMs: delayMs, FailureMeta: meta, NowMs: nowMs(),
	}})
	if err != nil {
		return false, err
	}
	observe.Nacks.WithLabelValues(nackModeLabel(mode)).Inc()
	nr, _ := res.(*fsm.NackResult)
	if nr == nil {
		return false, nil
	}
	if nr.DeadLettered {
		observe.DeadLetters.Inc()
	}
	return nr.DeadLettered, nil
}

func nackModeLabel(m fsm.NackMode) string {
	switch m {
	case fsm.NackRetry:
		return "retry"
	case fsm.NackDeadLetter:
		return "dead_letter"
	default:
		return "requeue_no_penalty"
	}
}

func (n *Node) Extend(leaseID, ttlMs uint64) error {
	_, err := n.apply(fsm.Command{Type: fsm.CmdExtend, Extend: &fsm.ExtendCmd{
		LeaseID: leaseID, NewDeadlineMs: nowMs() + ttlMs,
	}})
	return err
}

func (n *Node) DLQCount(lane string) (int, error) { return n.store.CountDLQ(lane) }

// SetPolicy validates, replicates, and hot-installs a lane's scheduling policy.
func (n *Node) SetPolicy(lane string, b policy.Binding) error {
	if err := policy.Validate(b); err != nil {
		return fmt.Errorf("policy rejected: %w", err)
	}
	prev := uint64(0)
	if cur, ok := n.GetPolicy(lane); ok {
		prev = cur.Version
	}
	b.Version = prev + 1
	b.Hash = policy.HashSource(b.Source)
	data, err := json.Marshal(b)
	if err != nil {
		return err
	}
	if _, err := n.apply(fsm.Command{Type: fsm.CmdSetPolicy, Policy: &fsm.PolicyCmd{Lane: lane, Binding: data}}); err != nil {
		return err
	}
	c, err := policy.Compile(b)
	if err != nil {
		return err
	}
	n.sched.SetPolicy(lane, c)
	n.polMu.Lock()
	n.loadedPolVer[lane] = b.Version
	n.polMu.Unlock()
	return nil
}

func (n *Node) GetPolicy(lane string) (policy.Binding, bool) {
	raw, ok, err := n.store.GetRaw(storage.PolicyKey(lane))
	if err != nil || !ok {
		return policy.Binding{}, false
	}
	var b policy.Binding
	if json.Unmarshal(raw, &b) != nil {
		return policy.Binding{}, false
	}
	return b, true
}

func (n *Node) ValidatePolicy(b policy.Binding) error { return policy.Validate(b) }

func (n *Node) PolicyQuarantined(lane string) bool { return n.sched.Quarantined(lane) }

// ensurePolicy lazily (re)compiles a lane's policy from the replicated binding
// when the stored version differs from what this node currently has loaded. This
// makes a freshly-elected leader pick up policies set while it was a follower.
func (n *Node) ensurePolicy(lane string) {
	raw, ok, err := n.store.GetRaw(storage.PolicyKey(lane))
	if err != nil {
		return
	}
	n.polMu.Lock()
	defer n.polMu.Unlock()
	if !ok {
		if n.loadedPolVer[lane] != 0 {
			n.sched.ClearPolicy(lane)
			delete(n.loadedPolVer, lane)
		}
		return
	}
	var b policy.Binding
	if json.Unmarshal(raw, &b) != nil {
		return
	}
	if n.loadedPolVer[lane] == b.Version {
		return
	}
	c, err := policy.Compile(b)
	if err != nil {
		return // keep current policy; compile failure is surfaced at SetPolicy time
	}
	n.sched.SetPolicy(lane, c)
	n.loadedPolVer[lane] = b.Version
}

func (n *Node) chronosLoop(ctx context.Context) {
	t := time.NewTicker(25 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if n.raft.State() != raft.Leader {
			continue
		}
		n.sweepTimers()
	}
}

func (n *Node) sweepTimers() {
	now := nowMs()
	it, err := n.store.DB.NewIter(&pebble.IterOptions{
		LowerBound: storage.TimeIndexPrefix(),
		UpperBound: storage.TimeIndexUpTo(now),
	})
	if err != nil {
		return
	}
	type due struct {
		ts   uint64
		kind byte
		ref  []byte
	}
	var batch []due
	for it.First(); it.Valid() && len(batch) < maxFireBatch; it.Next() {
		ts, kind, ref, ok := storage.ParseTimeIndexKey(it.Key())
		if !ok {
			continue
		}
		rc := make([]byte, len(ref))
		copy(rc, ref)
		batch = append(batch, due{ts, kind, rc})
	}
	_ = it.Close()
	for _, e := range batch {
		if e.kind == storage.TimerCronDue {
			n.fireCron(storage.ParseCronDueRef(e.ref), nowMs())
			continue
		}
		_, _ = n.apply(fsm.Command{Type: fsm.CmdFireTimer, Fire: &fsm.FireTimerCmd{
			Kind: e.kind, DueTs: e.ts, Ref: e.ref, FireAt: nowMs(),
		}})
	}
}

func nowMs() uint64 { return uint64(time.Now().UnixMilli()) }
