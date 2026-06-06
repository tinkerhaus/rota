package node

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"
	"github.com/robfig/cron/v3"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/fsm"
	"github.com/tinkerhaus/rota/internal/observe"
	"github.com/tinkerhaus/rota/internal/storage"
)

// ─── Group lifecycle ───────────────────────────────────────────────────────────

func (n *Node) SetGroupConfig(lane, group string, weight *float64, batch *uint32) error {
	_, err := n.apply(fsm.Command{Type: fsm.CmdGroupConfig, GroupConfig: &fsm.GroupConfigCmd{
		Lane: lane, GroupID: group, Weight: weight, BatchSize: batch,
	}})
	return err
}

func (n *Node) PauseGroup(lane, group string) error {
	_, e := n.groupOp(lane, group, fsm.GroupPause)
	return e
}
func (n *Node) ResumeGroup(lane, group string) error {
	_, e := n.groupOp(lane, group, fsm.GroupResume)
	return e
}
func (n *Node) CancelGroup(lane, group string) (uint64, error) {
	return n.groupOp(lane, group, fsm.GroupCancel)
}
func (n *Node) PurgeGroup(lane, group string) (uint64, error) {
	return n.groupOp(lane, group, fsm.GroupPurge)
}

func (n *Node) groupOp(lane, group string, op fsm.GroupOp) (uint64, error) {
	res, err := n.apply(fsm.Command{Type: fsm.CmdGroupLifecycle, GroupLifecycle: &fsm.GroupLifecycleCmd{
		Lane: lane, GroupID: group, Op: op,
	}})
	if err != nil {
		return 0, err
	}
	if r, ok := res.(*fsm.GroupOpResult); ok {
		return r.Affected, nil
	}
	return 0, nil
}

// ─── Cron (generic recurring publish) ──────────────────────────────────────────

func (n *Node) ScheduleCron(cronID, lane, group string, payload []byte, headers map[string]string, schedule string) error {
	sched, err := cron.ParseStandard(schedule)
	if err != nil {
		return fmt.Errorf("invalid cron schedule: %w", err)
	}
	next := uint64(sched.Next(time.Now()).UnixMilli())
	_, err = n.apply(fsm.Command{Type: fsm.CmdCron, Cron: &fsm.CronCmd{
		Op: fsm.CronSchedule, CronID: cronID, Lane: lane, GroupID: group,
		Payload: payload, Headers: headers, Schedule: schedule, NextFireMs: next, NowMs: nowMs(),
	}})
	return err
}

func (n *Node) DeleteCron(cronID string) error {
	_, err := n.apply(fsm.Command{Type: fsm.CmdCron, Cron: &fsm.CronCmd{Op: fsm.CronDelete, CronID: cronID}})
	return err
}

// PauseCron stops a schedule from firing (its due-timer is removed) until resumed.
// Idempotent; a pause on an unknown cron is a no-op (GetCron then reports absence).
func (n *Node) PauseCron(cronID string) error {
	_, err := n.apply(fsm.Command{Type: fsm.CmdCron, Cron: &fsm.CronCmd{Op: fsm.CronPause, CronID: cronID}})
	return err
}

// ResumeCron re-arms a paused schedule, leader-stamping the next fire instant from
// the live schedule string.
func (n *Node) ResumeCron(cronID string) error {
	spec, ok := n.loadCronSpec(cronID)
	if !ok {
		return fmt.Errorf("cron %q not found", cronID)
	}
	sched, err := cron.ParseStandard(spec.Schedule)
	if err != nil {
		return fmt.Errorf("invalid cron schedule: %w", err)
	}
	next := uint64(sched.Next(time.Now()).UnixMilli())
	_, err = n.apply(fsm.Command{Type: fsm.CmdCron, Cron: &fsm.CronCmd{
		Op: fsm.CronResume, CronID: cronID, NextFireMs: next, NowMs: nowMs(),
	}})
	return err
}

// GetCron returns a single schedule's replicated spec, if present.
func (n *Node) GetCron(cronID string) (fsm.CronSpec, bool) { return n.loadCronSpec(cronID) }

func (n *Node) ListCron() ([]fsm.CronSpec, error) {
	lo := storage.CronPrefix()
	hi := storage.PrefixEnd(lo)
	it, err := n.store.DB.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []fsm.CronSpec
	for it.First(); it.Valid(); it.Next() {
		var s fsm.CronSpec
		if json.Unmarshal(it.Value(), &s) == nil {
			out = append(out, s)
		}
	}
	return out, nil
}

func (n *Node) loadCronSpec(id string) (fsm.CronSpec, bool) {
	raw, ok, err := n.store.GetRaw(storage.CronKey(id))
	if err != nil || !ok {
		return fsm.CronSpec{}, false
	}
	var s fsm.CronSpec
	if json.Unmarshal(raw, &s) != nil {
		return fsm.CronSpec{}, false
	}
	return s, true
}

// fireCron is called by the leader's timer sweep when a cron is due: it parses
// the schedule, computes the NEXT fire instant (leader-stamped), and proposes a
// FireCron that the FSM applies deterministically.
func (n *Node) fireCron(cronID string, fireAt uint64) {
	spec, ok := n.loadCronSpec(cronID)
	if !ok {
		return
	}
	sched, err := cron.ParseStandard(spec.Schedule)
	if err != nil {
		return
	}
	next := uint64(sched.Next(time.UnixMilli(int64(fireAt))).UnixMilli())
	_, _ = n.apply(fsm.Command{Type: fsm.CmdFireCron, FireCron: &fsm.FireCronCmd{
		CronID: cronID, FireAt: fireAt, NextFireMs: next,
	}})
}

// ─── Complete-by-token (async completion) ──────────────────────────────────────

func (n *Node) IssueToken(leaseID uint64) ([]byte, error) {
	tok := make([]byte, 16)
	if _, err := rand.Read(tok); err != nil {
		return nil, err
	}
	h := sha256.Sum256(tok)
	res, err := n.apply(fsm.Command{Type: fsm.CmdIssueToken, IssueToken: &fsm.IssueTokenCmd{LeaseID: leaseID, TokenHash: h[:]}})
	if err != nil {
		return nil, err
	}
	if r, ok := res.(*fsm.AckResult); ok && !r.OK {
		return nil, fmt.Errorf("lease %d not found", leaseID)
	}
	return tok, nil
}

// registerToken binds a producer-supplied completion token to a lease (the
// hash is replicated; the raw token is held only by the producer/consumer).
func (n *Node) registerToken(leaseID uint64, token []byte) error {
	h := sha256.Sum256(token)
	res, err := n.apply(fsm.Command{Type: fsm.CmdIssueToken, IssueToken: &fsm.IssueTokenCmd{LeaseID: leaseID, TokenHash: h[:]}})
	if err != nil {
		return err
	}
	if r, ok := res.(*fsm.AckResult); ok && !r.OK {
		return fmt.Errorf("lease %d not found", leaseID)
	}
	return nil
}

func (n *Node) Complete(token []byte, success bool, meta map[string]string, delayMs uint64) (deadLettered, unknown bool, err error) {
	h := sha256.Sum256(token)
	res, e := n.apply(fsm.Command{Type: fsm.CmdComplete, Complete: &fsm.CompleteCmd{
		TokenHash: h[:], Success: success, Meta: meta, DelayMs: delayMs, NowMs: nowMs(),
	}})
	if e != nil {
		return false, false, e
	}
	if r, ok := res.(*fsm.CompleteResult); ok {
		return r.DeadLettered, r.Unknown, nil
	}
	return false, false, nil
}

// ─── Singleton leases (cluster-wide single-instance coordination) ──────────────

func (n *Node) AcquireSingleton(name, holder string, ttlMs uint64) (fence uint64, ok bool, err error) {
	res, e := n.apply(fsm.Command{Type: fsm.CmdSingleton, Singleton: &fsm.SingletonCmd{
		Op: fsm.SingletonAcquire, Name: name, Holder: holder, TTLms: ttlMs, NowMs: nowMs(),
	}})
	if e != nil {
		return 0, false, e
	}
	if r, k := res.(*fsm.SingletonResult); k {
		return r.Fence, r.OK, nil
	}
	return 0, false, nil
}

func (n *Node) RenewSingleton(name, holder string, fence, ttlMs uint64) (bool, error) {
	res, e := n.apply(fsm.Command{Type: fsm.CmdSingleton, Singleton: &fsm.SingletonCmd{
		Op: fsm.SingletonRenew, Name: name, Holder: holder, Fence: fence, TTLms: ttlMs, NowMs: nowMs(),
	}})
	if e != nil {
		return false, e
	}
	if r, k := res.(*fsm.SingletonResult); k {
		return r.OK, nil
	}
	return false, nil
}

func (n *Node) ReleaseSingleton(name, holder string, fence uint64) (bool, error) {
	res, e := n.apply(fsm.Command{Type: fsm.CmdSingleton, Singleton: &fsm.SingletonCmd{
		Op: fsm.SingletonRelease, Name: name, Holder: holder, Fence: fence, NowMs: nowMs(),
	}})
	if e != nil {
		return false, e
	}
	if r, k := res.(*fsm.SingletonResult); k {
		return r.OK, nil
	}
	return false, nil
}

// ─── Lane rate-limit + dequeue-pause (back-pressure hooks) ──────────────────────

// SetLaneRateLimit replicates a lane's dequeue rate limit and rebuilds this
// leader's token bucket. ratePerSec <= 0 means unlimited.
func (n *Node) SetLaneRateLimit(lane string, ratePerSec float64, burst uint32) error {
	if _, err := n.apply(fsm.Command{Type: fsm.CmdSetLaneConfig, LaneConfig: &fsm.LaneConfigCmd{
		Lane: lane, RatePerSec: ratePerSec, Burst: burst,
	}}); err != nil {
		return err
	}
	n.ensureLaneConfig(lane)
	return nil
}

// PauseLane stops leasing from a lane for durMs (0 = until ResumeLane). This is
// the hook a consumer-side circuit breaker drives; it is leader-local.
func (n *Node) PauseLane(lane string, durMs uint64) {
	n.laneMu.Lock()
	defer n.laneMu.Unlock()
	if durMs == 0 {
		n.paused[lane] = math.MaxInt64
	} else {
		n.paused[lane] = int64(nowMs()) + int64(durMs)
	}
}

func (n *Node) ResumeLane(lane string) {
	n.laneMu.Lock()
	defer n.laneMu.Unlock()
	delete(n.paused, lane)
}

func (n *Node) LanePaused(lane string) bool {
	n.laneMu.Lock()
	defer n.laneMu.Unlock()
	until, ok := n.paused[lane]
	return ok && int64(nowMs()) < until
}

// laneGate reports whether leasing from a lane is currently allowed (not paused
// and within its dequeue rate limit). It consumes a rate token when it returns true.
func (n *Node) laneGate(lane string) bool {
	n.ensureLaneConfig(lane)
	n.laneMu.Lock()
	defer n.laneMu.Unlock()
	if until, ok := n.paused[lane]; ok {
		if int64(nowMs()) < until {
			return false
		}
		delete(n.paused, lane)
	}
	if lim := n.limiters[lane]; lim != nil {
		return lim.Allow()
	}
	return true
}

// ensureLaneConfig lazily (re)builds this leader's rate limiter from the
// replicated lane config (so a new leader picks up limits set while a follower).
func (n *Node) ensureLaneConfig(lane string) {
	raw, ok, err := n.store.GetRaw(storage.LaneConfigKey(lane))
	if err != nil {
		return
	}
	n.laneMu.Lock()
	defer n.laneMu.Unlock()
	if !ok {
		if _, had := n.loadedLaneCfg[lane]; had {
			delete(n.limiters, lane)
			delete(n.loadedLaneCfg, lane)
		}
		return
	}
	var rec fsm.LaneConfigRec
	if json.Unmarshal(raw, &rec) != nil {
		return
	}
	if cur, had := n.loadedLaneCfg[lane]; had && cur == rec {
		return
	}
	n.loadedLaneCfg[lane] = rec
	if rec.RatePerSec <= 0 {
		delete(n.limiters, lane)
		return
	}
	burst := int(rec.Burst)
	if burst < 1 {
		burst = 1
	}
	n.limiters[lane] = rate.NewLimiter(rate.Limit(rec.RatePerSec), burst)
}

// ─── Idle reap + teardown ───────────────────────────────────────────────────────

func (n *Node) ReapGroup(lane, group string) (bool, error) {
	res, err := n.apply(fsm.Command{Type: fsm.CmdReapGroup, ReapGroup: &fsm.ReapGroupCmd{Lane: lane, GroupID: group}})
	if err != nil {
		return false, err
	}
	if r, ok := res.(*fsm.GroupOpResult); ok {
		return r.Affected > 0, nil
	}
	return false, nil
}

// RedriveDeadLetter re-publishes a dead letter back onto its lane as a fresh
// READY message (attempt reset) and deletes the DLQ row. Mutating (leader-only).
// Idempotent: a missing DLQ row returns ok=false with no new message.
func (n *Node) RedriveDeadLetter(lane, group string, msgID uint64) (bool, uint64, error) {
	res, err := n.apply(fsm.Command{Type: fsm.CmdRedrive, Redrive: &fsm.RedriveCmd{
		Lane: lane, GroupID: group, MsgID: msgID, NowMs: nowMs(),
	}})
	if err != nil {
		return false, 0, err
	}
	if r, ok := res.(*fsm.RedriveResult); ok {
		if r.OK {
			observe.Publishes.Inc() // a redrive enqueues a fresh message
		}
		return r.OK, r.NewMsgID, nil
	}
	return false, 0, nil
}

func (n *Node) TeardownGroup(group string) ([]string, uint64, error) {
	res, err := n.apply(fsm.Command{Type: fsm.CmdTeardownGroup, Teardown: &fsm.TeardownCmd{GroupID: group}})
	if err != nil {
		return nil, 0, err
	}
	if r, ok := res.(*fsm.TeardownResult); ok {
		return r.AffectedLanes, r.Affected, nil
	}
	return nil, 0, nil
}

// reapLoop is a leader-only sweep that reaps fully-drained, idle groups so their
// metadata rows don't accumulate.
func (n *Node) reapLoop(ctx context.Context) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if n.cfg.IdleReapMs == 0 || n.raft.State() != raft.Leader {
			continue
		}
		n.reapSweep()
	}
}

func (n *Node) reapSweep() {
	now := nowMs()
	lo, hi := storage.GroupMetaBounds()
	it, err := n.store.DB.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return
	}
	type tg struct{ lane, group string }
	var targets []tg
	for it.First(); it.Valid(); it.Next() {
		gm := &rotav1.GroupMeta{}
		if proto.Unmarshal(it.Value(), gm) != nil {
			continue
		}
		if gm.TotalCount == 0 && gm.LastActivityMs > 0 && now-gm.LastActivityMs > n.cfg.IdleReapMs {
			targets = append(targets, tg{gm.Lane, gm.GroupId})
		}
	}
	_ = it.Close()
	for _, t := range targets {
		_, _ = n.ReapGroup(t.lane, t.group)
	}
}
