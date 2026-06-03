// Package node wires Pebble + Raft + the FSM + the DRR scheduler into one broker
// node and exposes publish / fair-lease / ack / nack / extend, plus the leader-only
// Chronos loop that fires due timers (delayed publish, retry backoff, visibility).
package node

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"

	"github.com/tinkerhaus/rota/internal/fsm"
	"github.com/tinkerhaus/rota/internal/raftpebble"
	"github.com/tinkerhaus/rota/internal/scheduler"
	"github.com/tinkerhaus/rota/internal/storage"
)

const maxFireBatch = 256

type Config struct {
	DataDir      string
	NodeID       string
	VisibilityMs uint64
}

type Node struct {
	cfg    Config
	store  *storage.Store
	fsm    *fsm.FSM
	raft   *raft.Raft
	sched  *scheduler.Scheduler
	cancel context.CancelFunc
}

// PublishReq is the input to Publish.
type PublishReq struct {
	Lane        string
	GroupID     string
	Payload     []byte
	Headers     map[string]string
	Weight      *float64
	BatchSize   *uint32
	NotBeforeMs uint64 // 0 ⇒ eligible now
	MaxAttempts uint32
}

// Open boots a single-voter raft node on its own Pebble DB and starts Chronos.
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
	rc.SnapshotThreshold = 1 << 60
	rc.SnapshotInterval = 365 * 24 * time.Hour
	rc.LogLevel = "ERROR"

	snaps := raft.NewInmemSnapshotStore()
	_, transport := raft.NewInmemTransport(raft.ServerAddress(cfg.NodeID))

	r, err := raft.NewRaft(rc, f, logStore, logStore, snaps, transport)
	if err != nil {
		return nil, err
	}
	hasState, err := raft.HasExistingState(logStore, logStore, snaps)
	if err != nil {
		return nil, err
	}
	if !hasState {
		conf := raft.Configuration{Servers: []raft.Server{{ID: rc.LocalID, Address: transport.LocalAddr()}}}
		if err := r.BootstrapCluster(conf).Error(); err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{cfg: cfg, store: st, fsm: f, raft: r, sched: scheduler.New(), cancel: cancel}
	go n.chronosLoop(ctx)
	return n, nil
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
	return pr.MsgID, nil
}

// LeaseOne runs the DRR scheduler to choose a group, then leases that group's
// head message. ok=false means there is no leasable work right now.
func (n *Node) LeaseOne(lane, consumerID string) (*fsm.LeaseResult, bool, error) {
	groups, err := n.store.ListGroups(lane)
	if err != nil {
		return nil, false, err
	}
	active := make([]scheduler.GroupStat, 0, len(groups))
	for _, g := range groups {
		if g.Ready > 0 {
			active = append(active, scheduler.GroupStat{ID: g.ID, Ready: int(g.Ready), Weight: g.Weight})
		}
	}
	if len(active) == 0 {
		return nil, false, nil
	}
	gid, ok := n.sched.Pick(lane, active)
	if !ok {
		return nil, false, nil
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
	return lr, true, nil
}

func (n *Node) Ack(leaseID uint64) error {
	_, err := n.apply(fsm.Command{Type: fsm.CmdAck, Ack: &fsm.AckCmd{LeaseID: leaseID}})
	return err
}

// Nack returns deadLettered=true if this nack drove the message to the DLQ.
func (n *Node) Nack(leaseID uint64, mode fsm.NackMode, delayMs uint64, meta map[string]string) (bool, error) {
	res, err := n.apply(fsm.Command{Type: fsm.CmdNack, Nack: &fsm.NackCmd{
		LeaseID: leaseID, Mode: mode, DelayMs: delayMs, FailureMeta: meta, NowMs: nowMs(),
	}})
	if err != nil {
		return false, err
	}
	nr, _ := res.(*fsm.NackResult)
	if nr == nil {
		return false, nil
	}
	return nr.DeadLettered, nil
}

func (n *Node) Extend(leaseID, ttlMs uint64) error {
	_, err := n.apply(fsm.Command{Type: fsm.CmdExtend, Extend: &fsm.ExtendCmd{
		LeaseID: leaseID, NewDeadlineMs: nowMs() + ttlMs,
	}})
	return err
}

// DLQCount reports how many dead letters a lane holds (test/introspection helper).
func (n *Node) DLQCount(lane string) (int, error) { return n.store.CountDLQ(lane) }

// chronosLoop is the leader-only timer sweep: it turns due time-index rows into
// committed FireTimer commands. Followers never fire on their own clock.
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
		// apply blocks until commit, so the row is deleted before the next sweep.
		_, _ = n.apply(fsm.Command{Type: fsm.CmdFireTimer, Fire: &fsm.FireTimerCmd{
			Kind: e.kind, DueTs: e.ts, Ref: e.ref, FireAt: nowMs(),
		}})
	}
}

func nowMs() uint64 { return uint64(time.Now().UnixMilli()) }
