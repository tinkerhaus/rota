package fsm

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/storage"
)

// FSM is the Raft state machine. Apply turns a committed Command into exactly one
// atomic Pebble batch (data + meta:applied_index), committed with Sync.
type FSM struct {
	s            *storage.Store
	mu           sync.Mutex
	appliedIndex uint64
}

func New(s *storage.Store) (*FSM, error) {
	f := &FSM{s: s}
	if v, ok, err := s.GetRaw(storage.MetaKey("applied_index")); err != nil {
		return nil, err
	} else if ok {
		f.appliedIndex = beU64(v)
	}
	return f, nil
}

func (f *FSM) AppliedIndex() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.appliedIndex
}

func (f *FSM) Apply(l *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	if l.Type != raft.LogCommand {
		return nil
	}
	if l.Index <= f.appliedIndex {
		return nil // already applied (log replay after restart)
	}
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return err
	}
	b := f.s.DB.NewBatch()
	defer b.Close()

	var res interface{}
	var err error
	switch cmd.Type {
	case CmdPublish:
		res, err = f.applyPublish(b, cmd.Publish)
	case CmdPublishBatch:
		res, err = f.applyPublishBatch(b, cmd.PublishBatch)
	case CmdLease:
		res, err = f.applyLease(b, cmd.Lease)
	case CmdAck:
		res, err = f.applyAck(b, cmd.Ack)
	case CmdNack:
		res, err = f.applyNack(b, cmd.Nack)
	case CmdExtend:
		res, err = f.applyExtend(b, cmd.Extend)
	case CmdFireTimer:
		res, err = f.applyFireTimer(b, cmd.Fire)
	case CmdSetPolicy:
		err = b.Set(storage.PolicyKey(cmd.Policy.Lane), cmd.Policy.Binding, nil)
	case CmdGroupConfig:
		res, err = f.applyGroupConfig(b, cmd.GroupConfig)
	case CmdGroupLifecycle:
		res, err = f.applyGroupLifecycle(b, cmd.GroupLifecycle)
	case CmdCron:
		res, err = f.applyCron(b, cmd.Cron)
	case CmdFireCron:
		res, err = f.applyFireCron(b, cmd.FireCron)
	case CmdIssueToken:
		res, err = f.applyIssueToken(b, cmd.IssueToken)
	case CmdComplete:
		res, err = f.applyComplete(b, cmd.Complete)
	case CmdSingleton:
		res, err = f.applySingleton(b, cmd.Singleton)
	case CmdSetLaneConfig:
		res, err = f.applySetLaneConfig(b, cmd.LaneConfig)
	case CmdReapGroup:
		res, err = f.applyReapGroup(b, cmd.ReapGroup)
	case CmdTeardownGroup:
		res, err = f.applyTeardownGroup(b, cmd.Teardown)
	case CmdRedrive:
		res, err = f.applyRedrive(b, cmd.Redrive)
	case CmdWFStartRun:
		res, err = f.applyWFStartRun(b, cmd.WFStart)
	case CmdWFAppendEvents:
		res, err = f.applyWFAppendEvents(b, cmd.WFAppend)
	case CmdWFCompleteActivity:
		res, err = f.applyWFCompleteActivity(b, cmd.WFCompleteActivity)
	case CmdWFSignal:
		res, err = f.applyWFSignal(b, cmd.WFSignal)
	case CmdWFCancel:
		res, err = f.applyWFCancel(b, cmd.WFCancel)
	default:
		// Unknown CmdType: this binary is older than the command set already
		// committed to the log (a mixed-version rollout, or a downgrade). The old
		// behaviour — fall through and advance applied_index below without applying
		// anything — silently and permanently diverges this node from the rest of
		// the cluster. Halt loudly instead: a crash is recoverable (restart on a
		// compatible binary resumes from the log); silent divergence is not.
		panic(fmt.Sprintf("fsm: unknown CmdType %d at raft index %d: this node's binary predates a command in the committed log — upgrade it", cmd.Type, l.Index))
	}
	if err != nil {
		return err
	}
	if err := b.Set(storage.MetaKey("applied_index"), u64b(l.Index), nil); err != nil {
		return err
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return err
	}
	f.appliedIndex = l.Index
	return res
}

// loadGroupMeta returns the group's meta, initialized with defaults if absent.
func (f *FSM) loadGroupMeta(lane, group string) *rotav1.GroupMeta {
	gm := &rotav1.GroupMeta{}
	if found, _ := f.s.GetProto(storage.GroupMetaKey(lane, group), gm); !found {
		gm.Lane, gm.GroupId, gm.Weight, gm.BatchSize, gm.NextSeq = lane, group, 1.0, 1, 1
	}
	return gm
}

// publishOne writes one message into the batch and mutates the caller-owned
// group meta (which may be shared across a multi-item batch). The CALLER
// persists gm. Returns the assigned per-group message id.
func (f *FSM) publishOne(b *pebble.Batch, c *PublishCmd, gm *rotav1.GroupMeta) (uint64, error) {
	if c.HasWeight {
		gm.Weight = c.Weight
	}
	if c.HasBatch && c.BatchSize > 0 {
		gm.BatchSize = c.BatchSize
	}
	if gm.Weight == 0 {
		gm.Weight = 1.0
	}

	msgID := gm.NextSeq
	gm.NextSeq++
	gm.TotalCount++
	gm.LastActivityMs = c.NowMs

	msg := &rotav1.Message{
		MsgId:         msgID,
		Lane:          c.Lane,
		GroupId:       c.GroupID,
		Payload:       c.Payload,
		Headers:       c.Headers,
		NotBeforeMs:   c.NotBeforeMs,
		MaxAttempts:   c.MaxAttempts,
		EnqueueMs:     c.NowMs,
		IssueToken:    c.IssueToken,
		ExternalToken: c.ExternalToken,
	}
	if c.NotBeforeMs > c.NowMs {
		// Delayed: not leasable until T. Same mechanism as retry backoff.
		msg.State = rotav1.MessageState_DELAYED
		gm.DelayedCount++
		if err := addTimer(b, c.NotBeforeMs, storage.TimerReadyAt, storage.ReadyAtRef(c.Lane, c.GroupID, msgID), msg.Epoch); err != nil {
			return 0, err
		}
	} else {
		msg.State = rotav1.MessageState_READY
		gm.ReadyCount++
	}

	// TTL: auto-expire (dead-letter, reason "ttl") if still undelivered at the cap.
	// Not epoch-fenced — the cap is absolute from enqueue regardless of redelivery,
	// and the fire handler only acts while the message is still leasable.
	if c.TtlExpiryMs > c.NowMs {
		msg.TtlMs = c.TtlExpiryMs - c.NowMs
		if err := addTimer(b, c.TtlExpiryMs, storage.TimerMsgExpiry, storage.ReadyAtRef(c.Lane, c.GroupID, msgID), 0); err != nil {
			return 0, err
		}
	}

	if err := putProto(b, storage.MessageKey(c.Lane, c.GroupID, msgID), msg); err != nil {
		return 0, err
	}
	return msgID, nil
}

func (f *FSM) applyPublish(b *pebble.Batch, c *PublishCmd) (interface{}, error) {
	if c.DedupKey != "" {
		if id, dup := f.checkDedup(c); dup {
			return &PublishResult{MsgID: id, Duplicate: true}, nil
		}
	}
	gm := f.loadGroupMeta(c.Lane, c.GroupID)
	id, err := f.publishOne(b, c, gm)
	if err != nil {
		return nil, err
	}
	if err := putProto(b, storage.GroupMetaKey(c.Lane, c.GroupID), gm); err != nil {
		return nil, err
	}
	if c.DedupKey != "" {
		if err := f.recordDedup(b, c, id); err != nil {
			return nil, err
		}
	}
	return &PublishResult{MsgID: id}, nil
}

// ─── Producer idempotency (dedup_key) ───────────────────────────────────────────
//
// A dedup row stores the original msg id + the absolute window expiry, so a lazy
// read can treat an expired row as absent even before its sweep timer fires. The
// row is leader-stamped (expiry = NowMs + window) and applied deterministically.

func dedupVal(msgID, expiryMs uint64) []byte {
	v := make([]byte, 16)
	binary.BigEndian.PutUint64(v[:8], msgID)
	binary.BigEndian.PutUint64(v[8:], expiryMs)
	return v
}

func parseDedupVal(v []byte) (msgID, expiryMs uint64, ok bool) {
	if len(v) < 16 {
		return 0, 0, false
	}
	return binary.BigEndian.Uint64(v[:8]), binary.BigEndian.Uint64(v[8:]), true
}

// checkDedup returns (originalMsgID, true) when c.DedupKey is inside a live window.
// NOTE: it reads committed state, so two items sharing a dedup_key WITHIN one batch
// are not de-duplicated against each other (only against already-committed publishes).
func (f *FSM) checkDedup(c *PublishCmd) (uint64, bool) {
	v, ok, _ := f.s.GetRaw(storage.DedupKey(c.Lane, c.DedupKey))
	if !ok {
		return 0, false
	}
	msgID, expiry, valid := parseDedupVal(v)
	if !valid || expiry <= c.NowMs {
		return 0, false // absent/malformed/expired ⇒ not a duplicate
	}
	return msgID, true
}

// recordDedup writes the dedup row and arms its expiry sweep.
func (f *FSM) recordDedup(b *pebble.Batch, c *PublishCmd, msgID uint64) error {
	if c.DedupExpiryMs <= c.NowMs {
		return nil // no window ⇒ nothing to record
	}
	if err := b.Set(storage.DedupKey(c.Lane, c.DedupKey), dedupVal(msgID, c.DedupExpiryMs), nil); err != nil {
		return err
	}
	return addTimer(b, c.DedupExpiryMs, storage.TimerDedupExpiry, storage.DedupRef(c.Lane, c.DedupKey), 0)
}

// applyPublishBatch applies many publishes in one entry. Same-group items share
// one evolving GroupMeta (so per-group seq ids stay unique within the batch).
func (f *FSM) applyPublishBatch(b *pebble.Batch, c *PublishBatchCmd) (interface{}, error) {
	cache := map[string]*rotav1.GroupMeta{}
	order := make([]string, 0, len(c.Items))
	// batchDedup collapses duplicates WITHIN this batch: checkDedup only sees
	// committed state (the prior items' dedup rows aren't committed yet), so a
	// repeated (lane, dedup_key) inside one batch would otherwise publish twice.
	batchDedup := map[string]uint64{}
	res := &PublishBatchResult{Items: make([]PublishItemResult, len(c.Items))}
	for i := range c.Items {
		item := &c.Items[i]
		if item.DedupKey != "" {
			if id, dup := f.checkDedup(item); dup {
				res.Items[i] = PublishItemResult{MsgID: id, OK: true, Duplicate: true}
				continue
			}
			if id, dup := batchDedup[item.Lane+"\x00"+item.DedupKey]; dup {
				res.Items[i] = PublishItemResult{MsgID: id, OK: true, Duplicate: true}
				continue
			}
		}
		key := item.Lane + "\x00" + item.GroupID
		gm := cache[key]
		if gm == nil {
			gm = f.loadGroupMeta(item.Lane, item.GroupID)
			cache[key] = gm
			order = append(order, key)
		}
		id, err := f.publishOne(b, item, gm)
		if err != nil {
			if c.Atomic {
				return nil, err // all-or-nothing: abort, nothing commits
			}
			res.Items[i] = PublishItemResult{Err: err.Error()}
			continue
		}
		if item.DedupKey != "" {
			if err := f.recordDedup(b, item, id); err != nil {
				if c.Atomic {
					return nil, err
				}
				res.Items[i] = PublishItemResult{Err: err.Error()}
				continue
			}
			// Only record for within-batch collapse AFTER a successful publish, so a
			// failed best-effort item never masks a later same-key item as its dup.
			batchDedup[item.Lane+"\x00"+item.DedupKey] = id
		}
		res.Items[i] = PublishItemResult{MsgID: id, OK: true}
	}
	for _, key := range order {
		gm := cache[key]
		if err := putProto(b, storage.GroupMetaKey(gm.Lane, gm.GroupId), gm); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func (f *FSM) applyLease(b *pebble.Batch, c *LeaseCmd) (interface{}, error) {
	head, err := f.headReady(c.Lane, c.GroupID)
	if err != nil {
		return nil, err
	}
	if head == nil {
		return &LeaseResult{Empty: true}, nil
	}

	leaseID := f.nextLeaseID(b)
	head.State = rotav1.MessageState_LEASED
	head.CurLease = leaseID
	head.Epoch++

	lease := &rotav1.Lease{
		LeaseId:        leaseID,
		MsgId:          head.MsgId,
		Lane:           c.Lane,
		GroupId:        c.GroupID,
		ConsumerId:     c.ConsumerID,
		DeadlineMs:     c.DeadlineMs,
		AttemptAtLease: head.Attempt,
		Epoch:          head.Epoch,
		GrantedMs:      c.DeadlineMs,
	}

	gm := &rotav1.GroupMeta{}
	if _, err := f.s.GetProto(storage.GroupMetaKey(c.Lane, c.GroupID), gm); err != nil {
		return nil, err
	}
	if gm.ReadyCount > 0 {
		gm.ReadyCount--
	}
	gm.InflightCount++

	if err := addTimer(b, c.DeadlineMs, storage.TimerLeaseDeadline, storage.LeaseDeadlineRef(leaseID), head.Epoch); err != nil {
		return nil, err
	}
	// Absolute lifetime cap (opt-in): a separate timer that is NOT reset by extends,
	// so a perpetually-extended or wedged async lease is force-failed at the cap.
	// Epoch 0 ⇒ not message-epoch-fenced; the fire handler matches on lease id.
	if c.MaxLifeMs > 0 {
		if err := addTimer(b, c.MaxLifeMs, storage.TimerLeaseMaxLife, storage.LeaseDeadlineRef(leaseID), 0); err != nil {
			return nil, err
		}
	}
	if err := putProto(b, storage.MessageKey(c.Lane, c.GroupID, head.MsgId), head); err != nil {
		return nil, err
	}
	if err := putProto(b, storage.LeaseKey(leaseID), lease); err != nil {
		return nil, err
	}
	if err := putProto(b, storage.GroupMetaKey(c.Lane, c.GroupID), gm); err != nil {
		return nil, err
	}
	return &LeaseResult{
		LeaseID:       leaseID,
		MsgID:         head.MsgId,
		Lane:          c.Lane,
		GroupID:       c.GroupID,
		Payload:       head.Payload,
		Headers:       head.Headers,
		Attempt:       head.Attempt,
		DeadlineMs:    c.DeadlineMs,
		IssueToken:    head.IssueToken,
		ExternalToken: head.ExternalToken,
	}, nil
}

func (f *FSM) applyAck(b *pebble.Batch, c *AckCmd) (interface{}, error) {
	lease := &rotav1.Lease{}
	found, err := f.s.GetProto(storage.LeaseKey(c.LeaseID), lease)
	if err != nil {
		return nil, err
	}
	if !found {
		return &AckResult{OK: false}, nil
	}
	if err := b.Delete(storage.MessageKey(lease.Lane, lease.GroupId, lease.MsgId), nil); err != nil {
		return nil, err
	}
	if err := b.Delete(storage.LeaseKey(c.LeaseID), nil); err != nil {
		return nil, err
	}
	if err := deleteLeaseToken(b, lease); err != nil {
		return nil, err
	}
	if err := delTimer(b, lease.DeadlineMs, storage.TimerLeaseDeadline, storage.LeaseDeadlineRef(c.LeaseID)); err != nil {
		return nil, err
	}
	gm := &rotav1.GroupMeta{}
	if found2, _ := f.s.GetProto(storage.GroupMetaKey(lease.Lane, lease.GroupId), gm); found2 {
		decr(&gm.InflightCount)
		decr(&gm.TotalCount)
		if err := putProto(b, storage.GroupMetaKey(lease.Lane, lease.GroupId), gm); err != nil {
			return nil, err
		}
	}
	return &AckResult{OK: true}, nil
}

func (f *FSM) applyNack(b *pebble.Batch, c *NackCmd) (interface{}, error) {
	lease := &rotav1.Lease{}
	found, err := f.s.GetProto(storage.LeaseKey(c.LeaseID), lease)
	if err != nil {
		return nil, err
	}
	if !found {
		return &NackResult{OK: false}, nil
	}
	// The lease is going away regardless of mode.
	if err := b.Delete(storage.LeaseKey(c.LeaseID), nil); err != nil {
		return nil, err
	}
	if err := deleteLeaseToken(b, lease); err != nil {
		return nil, err
	}
	if err := delTimer(b, lease.DeadlineMs, storage.TimerLeaseDeadline, storage.LeaseDeadlineRef(c.LeaseID)); err != nil {
		return nil, err
	}

	msg := &rotav1.Message{}
	mfound, _ := f.s.GetProto(storage.MessageKey(lease.Lane, lease.GroupId, lease.MsgId), msg)
	gm := &rotav1.GroupMeta{}
	gfound, _ := f.s.GetProto(storage.GroupMetaKey(lease.Lane, lease.GroupId), gm)
	if !mfound || msg.State != rotav1.MessageState_LEASED || msg.CurLease != c.LeaseID {
		return &NackResult{OK: false}, nil // stale lease; nothing to requeue
	}
	decr(&gm.InflightCount)

	switch c.Mode {
	case NackRetry:
		msg.Attempt++
		max := msg.MaxAttempts
		if max == 0 {
			max = defaultMaxAttempts
		}
		if msg.Attempt >= max {
			if err := f.deadLetter(b, msg, "max_attempts", c.FailureMeta, c.NowMs); err != nil {
				return nil, err
			}
			decr(&gm.TotalCount)
			if gfound {
				_ = putProto(b, storage.GroupMetaKey(lease.Lane, lease.GroupId), gm)
			}
			return &NackResult{OK: true, DeadLettered: true}, nil
		}
		// A caller-supplied delay (e.g. a failed complete-by-token requesting a
		// specific retry delay) overrides the default exponential backoff.
		delay := backoffMs(msg.MsgId, msg.Attempt)
		if c.DelayMs > 0 {
			delay = c.DelayMs
		}
		readyAt := c.NowMs + delay
		if err := f.makeReady(b, msg, gm, readyAt, c.NowMs); err != nil {
			return nil, err
		}

	case NackDeadLetter:
		if err := f.deadLetter(b, msg, "terminal_nack", c.FailureMeta, c.NowMs); err != nil {
			return nil, err
		}
		decr(&gm.TotalCount)
		if gfound {
			_ = putProto(b, storage.GroupMetaKey(lease.Lane, lease.GroupId), gm)
		}
		return &NackResult{OK: true, DeadLettered: true}, nil

	default: // NackRequeueNoPenalty: attempt unchanged
		readyAt := c.NowMs + c.DelayMs
		if err := f.makeReady(b, msg, gm, readyAt, c.NowMs); err != nil {
			return nil, err
		}
	}

	if gfound {
		if err := putProto(b, storage.GroupMetaKey(lease.Lane, lease.GroupId), gm); err != nil {
			return nil, err
		}
	}
	return &NackResult{OK: true}, nil
}

func (f *FSM) applyExtend(b *pebble.Batch, c *ExtendCmd) (interface{}, error) {
	lease := &rotav1.Lease{}
	found, err := f.s.GetProto(storage.LeaseKey(c.LeaseID), lease)
	if err != nil || !found {
		return &ExtendResult{OK: false}, err
	}
	msg := &rotav1.Message{}
	mfound, _ := f.s.GetProto(storage.MessageKey(lease.Lane, lease.GroupId, lease.MsgId), msg)
	if !mfound || msg.State != rotav1.MessageState_LEASED || msg.CurLease != c.LeaseID {
		return &ExtendResult{OK: false}, nil
	}
	if err := delTimer(b, lease.DeadlineMs, storage.TimerLeaseDeadline, storage.LeaseDeadlineRef(c.LeaseID)); err != nil {
		return nil, err
	}
	msg.Epoch++
	lease.DeadlineMs = c.NewDeadlineMs
	lease.ExtendCount++
	lease.Epoch = msg.Epoch
	if err := addTimer(b, c.NewDeadlineMs, storage.TimerLeaseDeadline, storage.LeaseDeadlineRef(c.LeaseID), msg.Epoch); err != nil {
		return nil, err
	}
	if err := putProto(b, storage.MessageKey(lease.Lane, lease.GroupId, lease.MsgId), msg); err != nil {
		return nil, err
	}
	if err := putProto(b, storage.LeaseKey(c.LeaseID), lease); err != nil {
		return nil, err
	}
	return &ExtendResult{OK: true}, nil
}

// applyFireTimer is idempotent: it re-reads the timer row (gone ⇒ already fired)
// and epoch-fences every state change, so a re-proposed fire after failover is a
// benign no-op and never double-fires.
func (f *FSM) applyFireTimer(b *pebble.Batch, c *FireTimerCmd) (interface{}, error) {
	tkey := storage.TimeIndexKey(c.DueTs, c.Kind, c.Ref)
	val, ok, err := f.s.GetRaw(tkey)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil // already fired or cancelled
	}
	timerEpoch := uint32(beU64(val))

	switch c.Kind {
	case storage.TimerReadyAt:
		lane, group, msgID, okRef := storage.ParseReadyAtRef(c.Ref)
		if okRef {
			msg := &rotav1.Message{}
			if mf, _ := f.s.GetProto(storage.MessageKey(lane, group, msgID), msg); mf &&
				msg.State == rotav1.MessageState_DELAYED && msg.Epoch == timerEpoch {
				msg.State = rotav1.MessageState_READY
				msg.Epoch++
				gm := &rotav1.GroupMeta{}
				if gf, _ := f.s.GetProto(storage.GroupMetaKey(lane, group), gm); gf {
					gm.ReadyCount++
					decr(&gm.DelayedCount)
					_ = putProto(b, storage.GroupMetaKey(lane, group), gm)
				}
				if err := putProto(b, storage.MessageKey(lane, group, msgID), msg); err != nil {
					return nil, err
				}
			}
		}

	case storage.TimerLeaseDeadline:
		leaseID, okRef := storage.ParseLeaseDeadlineRef(c.Ref)
		if okRef {
			lease := &rotav1.Lease{}
			if lf, _ := f.s.GetProto(storage.LeaseKey(leaseID), lease); lf {
				msg := &rotav1.Message{}
				mf, _ := f.s.GetProto(storage.MessageKey(lease.Lane, lease.GroupId, lease.MsgId), msg)
				if mf && msg.State == rotav1.MessageState_LEASED && msg.CurLease == leaseID && msg.Epoch == timerEpoch {
					// Genuine visibility timeout: reclaim the lease.
					if err := b.Delete(storage.LeaseKey(leaseID), nil); err != nil {
						return nil, err
					}
					if err := deleteLeaseToken(b, lease); err != nil {
						return nil, err
					}
					gm := &rotav1.GroupMeta{}
					gf, _ := f.s.GetProto(storage.GroupMetaKey(lease.Lane, lease.GroupId), gm)
					decr(&gm.InflightCount)
					if penaliseExpiry {
						msg.Attempt++
						max := msg.MaxAttempts
						if max == 0 {
							max = defaultMaxAttempts
						}
						if msg.Attempt >= max {
							if err := f.deadLetter(b, msg, "max_attempts", nil, c.FireAt); err != nil {
								return nil, err
							}
							decr(&gm.TotalCount)
						} else {
							readyAt := c.FireAt + backoffMs(msg.MsgId, msg.Attempt)
							if err := f.makeReady(b, msg, gm, readyAt, c.FireAt); err != nil {
								return nil, err
							}
						}
					} else {
						if err := f.makeReady(b, msg, gm, c.FireAt, c.FireAt); err != nil {
							return nil, err
						}
					}
					if gf {
						if err := putProto(b, storage.GroupMetaKey(lease.Lane, lease.GroupId), gm); err != nil {
							return nil, err
						}
					}
				}
			}
		}

	case storage.TimerLeaseMaxLife:
		// Absolute lifetime cap: force terminal dead-letter regardless of extends.
		// Self-cleaning — if the lease already ended (ack/nack/reclaim), this is a
		// benign no-op and only the timer row below is removed.
		if leaseID, okRef := storage.ParseLeaseDeadlineRef(c.Ref); okRef {
			lease := &rotav1.Lease{}
			if lf, _ := f.s.GetProto(storage.LeaseKey(leaseID), lease); lf {
				msg := &rotav1.Message{}
				mf, _ := f.s.GetProto(storage.MessageKey(lease.Lane, lease.GroupId, lease.MsgId), msg)
				if mf && msg.State == rotav1.MessageState_LEASED && msg.CurLease == leaseID {
					if err := b.Delete(storage.LeaseKey(leaseID), nil); err != nil {
						return nil, err
					}
					if err := deleteLeaseToken(b, lease); err != nil {
						return nil, err
					}
					_ = delTimer(b, lease.DeadlineMs, storage.TimerLeaseDeadline, storage.LeaseDeadlineRef(leaseID))
					gm := &rotav1.GroupMeta{}
					gf, _ := f.s.GetProto(storage.GroupMetaKey(lease.Lane, lease.GroupId), gm)
					decr(&gm.InflightCount)
					if err := f.deadLetter(b, msg, "max_lease_lifetime", nil, c.FireAt); err != nil {
						return nil, err
					}
					decr(&gm.TotalCount)
					if gf {
						if err := putProto(b, storage.GroupMetaKey(lease.Lane, lease.GroupId), gm); err != nil {
							return nil, err
						}
					}
				}
			}
		}

	case storage.TimerWFFired:
		// A durable workflow timer (workflow.sleep) fired: append TIMER_FIRED to the
		// run. The tidx row (deleted below) is the once-only guard.
		if runID, startedID, okRef := storage.ParseWFTimerRef(c.Ref); okRef {
			if err := f.recordTimerFired(b, runID, startedID, c.FireAt); err != nil {
				return nil, err
			}
		}

	case storage.TimerMsgExpiry:
		// Message TTL elapsed. Dead-letter (reason "ttl") only if the message is
		// still undelivered (READY/DELAYED); a LEASED message was delivered in time,
		// and a gone message already completed — both benign no-ops.
		if lane, group, msgID, okRef := storage.ParseReadyAtRef(c.Ref); okRef {
			msg := &rotav1.Message{}
			if mf, _ := f.s.GetProto(storage.MessageKey(lane, group, msgID), msg); mf &&
				(msg.State == rotav1.MessageState_READY || msg.State == rotav1.MessageState_DELAYED) {
				gm := &rotav1.GroupMeta{}
				gf, _ := f.s.GetProto(storage.GroupMetaKey(lane, group), gm)
				if msg.State == rotav1.MessageState_READY {
					decr(&gm.ReadyCount)
				} else {
					decr(&gm.DelayedCount)
				}
				if err := f.deadLetter(b, msg, "ttl", nil, c.FireAt); err != nil {
					return nil, err
				}
				decr(&gm.TotalCount)
				if gf {
					if err := putProto(b, storage.GroupMetaKey(lane, group), gm); err != nil {
						return nil, err
					}
				}
			}
		}

	case storage.TimerDedupExpiry:
		if lane, key, okRef := storage.ParseDedupRef(c.Ref); okRef {
			dk := storage.DedupKey(lane, key)
			// Only sweep if the window has actually closed — a re-publish that renewed
			// the key to a LATER expiry must survive this (older) timer firing.
			if v, ok2, _ := f.s.GetRaw(dk); ok2 {
				if _, expiry, valid := parseDedupVal(v); valid && expiry <= c.FireAt {
					if err := b.Delete(dk, nil); err != nil {
						return nil, err
					}
				}
			}
		}
	}

	return nil, b.Delete(tkey, nil)
}

// makeReady requeues a message: READY now, or DELAYED with a READY_AT timer if
// readyAtMs is in the future. Bumps epoch so any stale timer is fenced off.
func (f *FSM) makeReady(b *pebble.Batch, msg *rotav1.Message, gm *rotav1.GroupMeta, readyAtMs, nowMs uint64) error {
	msg.CurLease = 0
	msg.Epoch++
	if readyAtMs > nowMs {
		msg.State = rotav1.MessageState_DELAYED
		gm.DelayedCount++
		if err := addTimer(b, readyAtMs, storage.TimerReadyAt, storage.ReadyAtRef(msg.Lane, msg.GroupId, msg.MsgId), msg.Epoch); err != nil {
			return err
		}
	} else {
		msg.State = rotav1.MessageState_READY
		gm.ReadyCount++
	}
	return putProto(b, storage.MessageKey(msg.Lane, msg.GroupId, msg.MsgId), msg)
}

func (f *FSM) deadLetter(b *pebble.Batch, msg *rotav1.Message, reason string, meta map[string]string, deadTs uint64) error {
	dl := &rotav1.DeadLetter{
		Original:       msg,
		FinalAttempt:   msg.Attempt,
		Reason:         reason,
		FailureHeaders: meta,
		DeadAtMs:       deadTs,
	}
	if err := putProto(b, storage.DLQKey(msg.Lane, msg.GroupId, deadTs, msg.MsgId), dl); err != nil {
		return err
	}
	if err := b.Delete(storage.MessageKey(msg.Lane, msg.GroupId, msg.MsgId), nil); err != nil {
		return err
	}
	// Liveness bridge: an activity task that dead-letters must surface to its run as
	// an ACTIVITY_FAILED event, or the run would wait on it forever. Idempotent via
	// recordActivityTerminal's done-marker, so it safely races a late success.
	if strings.HasPrefix(msg.Lane, ActivityLanePrefix) {
		task := &rotav1.ActivityTask{}
		if proto.Unmarshal(msg.Payload, task) == nil && task.GetRunId() != 0 {
			if _, _, _, err := f.recordActivityTerminal(b, task.GetRunId(), task.GetScheduledEventId(), true, []byte("activity_dead_lettered: "+reason), deadTs); err != nil {
				return err
			}
		}
	}
	return nil
}

// headReady returns the lowest-msgID READY message of a group, or nil.
func (f *FSM) headReady(lane, group string) (*rotav1.Message, error) {
	lo := storage.MessagePrefix(lane, group)
	hi := storage.PrefixEnd(lo)
	it, err := f.s.DB.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		m := &rotav1.Message{}
		if err := proto.Unmarshal(it.Value(), m); err != nil {
			return nil, err
		}
		if m.State == rotav1.MessageState_READY {
			return m, nil
		}
	}
	return nil, nil
}

func (f *FSM) nextLeaseID(b *pebble.Batch) uint64 {
	var id uint64 = 1
	if v, ok, _ := f.s.GetRaw(storage.MetaKey("next_lease_id")); ok {
		id = beU64(v)
	}
	_ = b.Set(storage.MetaKey("next_lease_id"), u64b(id+1), nil)
	return id
}

// Snapshot captures a consistent point-in-time of the APPLICATION keyspace (not
// the raft log, which lives under its own tags) via a Pebble snapshot, streamed
// as length-prefixed key/value pairs. This bounds the raft log (truncation after
// a snapshot) and lets a lagging/new follower catch up via InstallSnapshot.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &kvSnapshot{snap: f.s.DB.NewSnapshot()}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	f.mu.Lock()
	defer f.mu.Unlock()
	lo, hi := storage.AppKeyspaceBounds()
	if err := f.s.DB.DeleteRange(lo, hi, pebble.Sync); err != nil {
		return err
	}
	r := bufio.NewReaderSize(rc, 1<<16)
	b := f.s.DB.NewBatch()
	defer b.Close()
	for {
		key, err := readChunk(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		val, err := readChunk(r)
		if err != nil {
			return err
		}
		if err := b.Set(key, val, nil); err != nil {
			return err
		}
		if b.Len() > 4<<20 {
			if err := b.Commit(pebble.Sync); err != nil {
				return err
			}
			b = f.s.DB.NewBatch()
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return err
	}
	if v, ok, _ := f.s.GetRaw(storage.MetaKey("applied_index")); ok {
		f.appliedIndex = beU64(v)
	}
	return nil
}

type kvSnapshot struct{ snap *pebble.Snapshot }

func (k *kvSnapshot) Persist(sink raft.SnapshotSink) error {
	lo, hi := storage.AppKeyspaceBounds()
	it, err := k.snap.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	w := bufio.NewWriterSize(sink, 1<<16)
	for it.First(); it.Valid(); it.Next() {
		if err := writeChunk(w, it.Key()); err != nil {
			_ = it.Close()
			_ = sink.Cancel()
			return err
		}
		if err := writeChunk(w, it.Value()); err != nil {
			_ = it.Close()
			_ = sink.Cancel()
			return err
		}
	}
	_ = it.Close()
	if err := w.Flush(); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (k *kvSnapshot) Release() { _ = k.snap.Close() }

func writeChunk(w *bufio.Writer, b []byte) error {
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(b)))
	if _, err := w.Write(l[:]); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func readChunk(r *bufio.Reader) ([]byte, error) {
	var l [4]byte
	if _, err := io.ReadFull(r, l[:]); err != nil {
		return nil, err // io.EOF at a record boundary ends the stream
	}
	buf := make([]byte, binary.BigEndian.Uint32(l[:]))
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func addTimer(b *pebble.Batch, dueTs uint64, kind byte, ref []byte, epoch uint32) error {
	return b.Set(storage.TimeIndexKey(dueTs, kind, ref), u64b(uint64(epoch)), nil)
}

func delTimer(b *pebble.Batch, dueTs uint64, kind byte, ref []byte) error {
	return b.Delete(storage.TimeIndexKey(dueTs, kind, ref), nil)
}

func putProto(b *pebble.Batch, key []byte, m proto.Message) error {
	data, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	return b.Set(key, data, nil)
}

func decr(v *uint64) {
	if *v > 0 {
		*v--
	}
}

func u64b(v uint64) []byte {
	x := make([]byte, 8)
	binary.BigEndian.PutUint64(x, v)
	return x
}

func beU64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}
