package node

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/robfig/cron/v3"

	"github.com/tinkerhaus/rota/internal/fsm"
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

func (n *Node) Complete(token []byte, success bool, meta map[string]string) (deadLettered, unknown bool, err error) {
	h := sha256.Sum256(token)
	res, e := n.apply(fsm.Command{Type: fsm.CmdComplete, Complete: &fsm.CompleteCmd{
		TokenHash: h[:], Success: success, Meta: meta, NowMs: nowMs(),
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
