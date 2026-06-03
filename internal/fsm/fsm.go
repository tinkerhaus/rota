package fsm

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"io"
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

func (f *FSM) applyPublish(b *pebble.Batch, c *PublishCmd) (interface{}, error) {
	gm := &rotav1.GroupMeta{}
	found, err := f.s.GetProto(storage.GroupMetaKey(c.Lane, c.GroupID), gm)
	if err != nil {
		return nil, err
	}
	if !found {
		gm.Lane = c.Lane
		gm.GroupId = c.GroupID
		gm.Weight = 1.0
		gm.BatchSize = 1
		gm.NextSeq = 1
	}
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
		MsgId:       msgID,
		Lane:        c.Lane,
		GroupId:     c.GroupID,
		Payload:     c.Payload,
		Headers:     c.Headers,
		NotBeforeMs: c.NotBeforeMs,
		MaxAttempts: c.MaxAttempts,
		EnqueueMs:   c.NowMs,
	}
	if c.NotBeforeMs > c.NowMs {
		// Delayed: not leasable until T. Same mechanism as retry backoff.
		msg.State = rotav1.MessageState_DELAYED
		if err := addTimer(b, c.NotBeforeMs, storage.TimerReadyAt, storage.ReadyAtRef(c.Lane, c.GroupID, msgID), msg.Epoch); err != nil {
			return nil, err
		}
	} else {
		msg.State = rotav1.MessageState_READY
		gm.ReadyCount++
	}

	if err := putProto(b, storage.MessageKey(c.Lane, c.GroupID, msgID), msg); err != nil {
		return nil, err
	}
	if err := putProto(b, storage.GroupMetaKey(c.Lane, c.GroupID), gm); err != nil {
		return nil, err
	}
	return &PublishResult{MsgID: msgID}, nil
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
		LeaseID:    leaseID,
		MsgID:      head.MsgId,
		Lane:       c.Lane,
		GroupID:    c.GroupID,
		Payload:    head.Payload,
		Headers:    head.Headers,
		Attempt:    head.Attempt,
		DeadlineMs: c.DeadlineMs,
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
		readyAt := c.NowMs + backoffMs(msg.MsgId, msg.Attempt)
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
	return b.Delete(storage.MessageKey(msg.Lane, msg.GroupId, msg.MsgId), nil)
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
