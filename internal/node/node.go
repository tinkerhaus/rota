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

	// Clustering. RaftBind == "" selects the in-memory transport (single-node
	// dev/test). Otherwise a TCP transport is created on RaftBind.
	RaftBind      string
	RaftAdvertise string // defaults to RaftBind
	Bootstrap     bool   // this node forms the initial cluster
	InitialPeers  []Peer // voter set for the bootstrapping node (empty ⇒ just self)
}

type Node struct {
	cfg    Config
	store  *storage.Store
	fsm    *fsm.FSM
	raft   *raft.Raft
	sched  *scheduler.Scheduler
	cancel context.CancelFunc

	polMu        sync.Mutex
	loadedPolVer map[string]uint64 // lane -> compiled policy version on this node

	laneMu        sync.Mutex
	limiters      map[string]*rate.Limiter     // leader-local dequeue rate limiters
	loadedLaneCfg map[string]fsm.LaneConfigRec // last-applied lane config per lane
	paused        map[string]int64             // lane -> paused-until unix ms
}

type PublishReq struct {
	Lane        string
	GroupID     string
	Payload     []byte
	Headers     map[string]string
	Weight      *float64
	BatchSize   *uint32
	NotBeforeMs uint64
	MaxAttempts uint32
}

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
		paused: map[string]int64{},
	}
	go n.chronosLoop(ctx)
	go n.reapLoop(ctx)
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

func (n *Node) AddVoter(id, addr string) error {
	return n.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 10*time.Second).Error()
}

func (n *Node) Barrier(timeout time.Duration) error { return n.raft.Barrier(timeout).Error() }

func (n *Node) ForceSnapshot() error { return n.raft.Snapshot().Error() }

func (n *Node) Close() error {
	n.cancel()
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

func (n *Node) Publish(r PublishReq) (uint64, error) {
	pc := &fsm.PublishCmd{
		Lane: r.Lane, GroupID: r.GroupID, Payload: r.Payload, Headers: r.Headers,
		MaxAttempts: r.MaxAttempts, NotBeforeMs: r.NotBeforeMs, NowMs: nowMs(),
	}
	if r.Weight != nil {
		pc.HasWeight = true
		pc.Weight = *r.Weight
	}
	if r.BatchSize != nil {
		pc.HasBatch = true
		pc.BatchSize = *r.BatchSize
	}
	res, err := n.apply(fsm.Command{Type: fsm.CmdPublish, Publish: pc})
	if err != nil {
		return 0, err
	}
	pr, _ := res.(*fsm.PublishResult)
	if pr == nil {
		return 0, fmt.Errorf("publish: no result")
	}
	observe.Publishes.Inc()
	return pr.MsgID, nil
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
	res, err := n.apply(fsm.Command{Type: fsm.CmdLease, Lease: &fsm.LeaseCmd{
		Lane: lane, GroupID: gid, ConsumerID: consumerID, DeadlineMs: nowMs() + n.cfg.VisibilityMs,
	}})
	if err != nil {
		return nil, false, err
	}
	lr, _ := res.(*fsm.LeaseResult)
	if lr == nil || lr.Empty {
		return nil, false, nil
	}
	observe.Leases.Inc()
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
