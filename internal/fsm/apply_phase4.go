package fsm

import (
	"encoding/json"

	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/storage"
)

// CronSpec / SingletonRec are stored as JSON (kept out of the wire proto's
// bytes-typed fields for simplicity). The Control layer converts to/from proto.
type CronSpec struct {
	CronID     string            `json:"id"`
	Lane       string            `json:"lane"`
	GroupID    string            `json:"g"`
	Payload    []byte            `json:"pl,omitempty"`
	Headers    map[string]string `json:"h,omitempty"`
	Schedule   string            `json:"s"`
	NextFireMs uint64            `json:"nf"`
	LastFireMs uint64            `json:"lf"`
	Paused     bool              `json:"p"`
}

type SingletonRec struct {
	Name       string `json:"n"`
	Holder     string `json:"h"`
	DeadlineMs uint64 `json:"d"`
	Fence      uint64 `json:"f"`
}

// LaneConfigRec is the replicated lane rate-limit config.
type LaneConfigRec struct {
	RatePerSec float64 `json:"r"`
	Burst      uint32  `json:"b"`
}

func (f *FSM) applySetLaneConfig(b *pebble.Batch, c *LaneConfigCmd) (interface{}, error) {
	data, _ := json.Marshal(LaneConfigRec{RatePerSec: c.RatePerSec, Burst: c.Burst})
	return nil, b.Set(storage.LaneConfigKey(c.Lane), data, nil)
}

// applyReapGroup deletes a group's metadata only if it is fully drained
// (total_count == 0). Idempotent: a re-proposed reap on a now-repopulated group
// is a no-op.
func (f *FSM) applyReapGroup(b *pebble.Batch, c *ReapGroupCmd) (interface{}, error) {
	gm := &rotav1.GroupMeta{}
	found, _ := f.s.GetProto(storage.GroupMetaKey(c.Lane, c.GroupID), gm)
	if !found || gm.TotalCount != 0 {
		return &GroupOpResult{}, nil
	}
	return &GroupOpResult{Affected: 1}, b.Delete(storage.GroupMetaKey(c.Lane, c.GroupID), nil)
}

// applyTeardownGroup drops a group across every lane it appears in (one call).
func (f *FSM) applyTeardownGroup(b *pebble.Batch, c *TeardownCmd) (interface{}, error) {
	lo, hi := storage.GroupMetaBounds()
	it, err := f.s.DB.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	var lanes []string
	for it.First(); it.Valid(); it.Next() {
		gm := &rotav1.GroupMeta{}
		if proto.Unmarshal(it.Value(), gm) == nil && gm.GroupId == c.GroupID {
			lanes = append(lanes, gm.Lane)
		}
	}
	_ = it.Close()

	var affected uint64
	for _, lane := range lanes {
		gm := &rotav1.GroupMeta{}
		if ok, _ := f.s.GetProto(storage.GroupMetaKey(lane, c.GroupID), gm); !ok {
			continue
		}
		d, err := f.dropLeasable(b, lane, c.GroupID, gm)
		if err != nil {
			return nil, err
		}
		affected += d
		if err := b.Delete(storage.GroupMetaKey(lane, c.GroupID), nil); err != nil {
			return nil, err
		}
	}
	return &TeardownResult{AffectedLanes: lanes, Affected: affected}, nil
}

func (f *FSM) applyGroupConfig(b *pebble.Batch, c *GroupConfigCmd) (interface{}, error) {
	gm := &rotav1.GroupMeta{}
	found, _ := f.s.GetProto(storage.GroupMetaKey(c.Lane, c.GroupID), gm)
	if !found {
		gm.Lane, gm.GroupId, gm.Weight, gm.BatchSize, gm.NextSeq = c.Lane, c.GroupID, 1.0, 1, 1
	}
	if c.Weight != nil {
		gm.Weight = *c.Weight
	}
	if c.BatchSize != nil && *c.BatchSize > 0 {
		gm.BatchSize = *c.BatchSize
	}
	if gm.Weight == 0 {
		gm.Weight = 1
	}
	return &GroupOpResult{}, putProto(b, storage.GroupMetaKey(c.Lane, c.GroupID), gm)
}

func (f *FSM) applyGroupLifecycle(b *pebble.Batch, c *GroupLifecycleCmd) (interface{}, error) {
	gm := &rotav1.GroupMeta{}
	found, _ := f.s.GetProto(storage.GroupMetaKey(c.Lane, c.GroupID), gm)
	if !found {
		return &GroupOpResult{}, nil
	}
	switch c.Op {
	case GroupPause:
		gm.Paused = true
		return &GroupOpResult{}, putProto(b, storage.GroupMetaKey(c.Lane, c.GroupID), gm)
	case GroupResume:
		gm.Paused = false
		return &GroupOpResult{}, putProto(b, storage.GroupMetaKey(c.Lane, c.GroupID), gm)
	case GroupCancel, GroupPurge:
		dropped, err := f.dropLeasable(b, c.Lane, c.GroupID, gm)
		return &GroupOpResult{Affected: dropped}, err
	}
	return &GroupOpResult{}, nil
}

// dropLeasable deletes every READY/DELAYED message of a group (leaving in-flight
// leases to drain). Orphaned READY_AT timers self-clean when they fire (the
// message is gone, so the fire is a no-op).
func (f *FSM) dropLeasable(b *pebble.Batch, lane, group string, gm *rotav1.GroupMeta) (uint64, error) {
	lo := storage.MessagePrefix(lane, group)
	hi := storage.PrefixEnd(lo)
	it, err := f.s.DB.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return 0, err
	}
	var dropped uint64
	for it.First(); it.Valid(); it.Next() {
		m := &rotav1.Message{}
		if err := proto.Unmarshal(it.Value(), m); err != nil {
			_ = it.Close()
			return 0, err
		}
		if m.State == rotav1.MessageState_READY || m.State == rotav1.MessageState_DELAYED {
			if err := b.Delete(storage.MessageKey(lane, group, m.MsgId), nil); err != nil {
				_ = it.Close()
				return 0, err
			}
			dropped++
		}
	}
	_ = it.Close()
	gm.ReadyCount = 0
	if gm.TotalCount >= dropped {
		gm.TotalCount -= dropped
	} else {
		gm.TotalCount = gm.InflightCount
	}
	return dropped, putProto(b, storage.GroupMetaKey(lane, group), gm)
}

func (f *FSM) applyCron(b *pebble.Batch, c *CronCmd) (interface{}, error) {
	switch c.Op {
	case CronSchedule:
		spec := CronSpec{
			CronID: c.CronID, Lane: c.Lane, GroupID: c.GroupID, Payload: c.Payload,
			Headers: c.Headers, Schedule: c.Schedule, NextFireMs: c.NextFireMs,
		}
		data, _ := json.Marshal(spec)
		if err := b.Set(storage.CronKey(c.CronID), data, nil); err != nil {
			return nil, err
		}
		return nil, addTimer(b, c.NextFireMs, storage.TimerCronDue, storage.CronDueRef(c.CronID), 0)
	case CronDelete:
		if spec, ok := f.loadCron(c.CronID); ok {
			_ = delTimer(b, spec.NextFireMs, storage.TimerCronDue, storage.CronDueRef(c.CronID))
		}
		return nil, b.Delete(storage.CronKey(c.CronID), nil)
	case CronPause:
		if spec, ok := f.loadCron(c.CronID); ok {
			_ = delTimer(b, spec.NextFireMs, storage.TimerCronDue, storage.CronDueRef(c.CronID))
			spec.Paused = true
			data, _ := json.Marshal(spec)
			return nil, b.Set(storage.CronKey(c.CronID), data, nil)
		}
	case CronResume:
		if spec, ok := f.loadCron(c.CronID); ok {
			spec.Paused = false
			spec.NextFireMs = c.NextFireMs
			data, _ := json.Marshal(spec)
			if err := b.Set(storage.CronKey(c.CronID), data, nil); err != nil {
				return nil, err
			}
			return nil, addTimer(b, c.NextFireMs, storage.TimerCronDue, storage.CronDueRef(c.CronID), 0)
		}
	}
	return nil, nil
}

// applyFireCron is idempotent: it fires only if the cron's CronDue timer at the
// spec's current NextFireMs still exists (the leader-stamped fire deletes it).
func (f *FSM) applyFireCron(b *pebble.Batch, c *FireCronCmd) (interface{}, error) {
	spec, ok := f.loadCron(c.CronID)
	if !ok {
		return nil, nil
	}
	tkey := storage.TimeIndexKey(spec.NextFireMs, storage.TimerCronDue, storage.CronDueRef(c.CronID))
	if _, present, _ := f.s.GetRaw(tkey); !present {
		return nil, nil // already fired
	}
	if _, err := f.publishReady(b, spec.Lane, spec.GroupID, spec.Payload, spec.Headers, c.FireAt); err != nil {
		return nil, err
	}
	if err := b.Delete(tkey, nil); err != nil {
		return nil, err
	}
	spec.LastFireMs = c.FireAt
	spec.NextFireMs = c.NextFireMs
	data, _ := json.Marshal(spec)
	if err := b.Set(storage.CronKey(c.CronID), data, nil); err != nil {
		return nil, err
	}
	return nil, addTimer(b, c.NextFireMs, storage.TimerCronDue, storage.CronDueRef(c.CronID), 0)
}

func (f *FSM) loadCron(id string) (CronSpec, bool) {
	raw, ok, _ := f.s.GetRaw(storage.CronKey(id))
	if !ok {
		return CronSpec{}, false
	}
	var spec CronSpec
	if json.Unmarshal(raw, &spec) != nil {
		return CronSpec{}, false
	}
	return spec, true
}

// publishReady appends an immediately-leasable message (used by cron fires).
func (f *FSM) publishReady(b *pebble.Batch, lane, group string, payload []byte, headers map[string]string, nowMs uint64) (uint64, error) {
	gm := &rotav1.GroupMeta{}
	found, _ := f.s.GetProto(storage.GroupMetaKey(lane, group), gm)
	if !found {
		gm.Lane, gm.GroupId, gm.Weight, gm.BatchSize, gm.NextSeq = lane, group, 1.0, 1, 1
	}
	if gm.Weight == 0 {
		gm.Weight = 1
	}
	id := gm.NextSeq
	gm.NextSeq++
	gm.TotalCount++
	gm.ReadyCount++
	gm.LastActivityMs = nowMs
	msg := &rotav1.Message{
		MsgId: id, Lane: lane, GroupId: group, Payload: payload, Headers: headers,
		EnqueueMs: nowMs, State: rotav1.MessageState_READY,
	}
	if err := putProto(b, storage.MessageKey(lane, group, id), msg); err != nil {
		return 0, err
	}
	return id, putProto(b, storage.GroupMetaKey(lane, group), gm)
}

func (f *FSM) applyIssueToken(b *pebble.Batch, c *IssueTokenCmd) (interface{}, error) {
	lease := &rotav1.Lease{}
	found, _ := f.s.GetProto(storage.LeaseKey(c.LeaseID), lease)
	if !found {
		return &AckResult{OK: false}, nil
	}
	lease.CompletionTokenHash = c.TokenHash
	if err := putProto(b, storage.LeaseKey(c.LeaseID), lease); err != nil {
		return nil, err
	}
	return &AckResult{OK: true}, b.Set(storage.TokenKey(c.TokenHash), u64b(c.LeaseID), nil)
}

func (f *FSM) applyComplete(b *pebble.Batch, c *CompleteCmd) (interface{}, error) {
	raw, ok, _ := f.s.GetRaw(storage.TokenKey(c.TokenHash))
	if !ok {
		return &CompleteResult{Unknown: true}, nil
	}
	leaseID := beU64(raw)
	if err := b.Delete(storage.TokenKey(c.TokenHash), nil); err != nil {
		return nil, err
	}
	if c.Success {
		r, err := f.applyAck(b, &AckCmd{LeaseID: leaseID})
		if err != nil {
			return nil, err
		}
		ar, _ := r.(*AckResult)
		return &CompleteResult{OK: ar != nil && ar.OK}, nil
	}
	r, err := f.applyNack(b, &NackCmd{LeaseID: leaseID, Mode: NackRetry, NowMs: c.NowMs, FailureMeta: c.Meta})
	if err != nil {
		return nil, err
	}
	nr, _ := r.(*NackResult)
	return &CompleteResult{OK: nr != nil && nr.OK, DeadLettered: nr != nil && nr.DeadLettered}, nil
}

func (f *FSM) applySingleton(b *pebble.Batch, c *SingletonCmd) (interface{}, error) {
	var rec SingletonRec
	found := false
	if raw, ok, _ := f.s.GetRaw(storage.SingletonKey(c.Name)); ok {
		found = json.Unmarshal(raw, &rec) == nil
	}
	switch c.Op {
	case SingletonAcquire:
		held := found && rec.Holder != "" && rec.DeadlineMs > c.NowMs && rec.Holder != c.Holder
		if held {
			return &SingletonResult{OK: false, Fence: rec.Fence, Holder: rec.Holder}, nil
		}
		rec = SingletonRec{Name: c.Name, Holder: c.Holder, DeadlineMs: c.NowMs + c.TTLms, Fence: rec.Fence + 1}
		data, _ := json.Marshal(rec)
		return &SingletonResult{OK: true, Fence: rec.Fence, Holder: c.Holder}, b.Set(storage.SingletonKey(c.Name), data, nil)
	case SingletonRenew:
		if !found || rec.Holder != c.Holder || rec.Fence != c.Fence {
			return &SingletonResult{OK: false, Fence: rec.Fence, Holder: rec.Holder}, nil
		}
		rec.DeadlineMs = c.NowMs + c.TTLms
		data, _ := json.Marshal(rec)
		return &SingletonResult{OK: true, Fence: rec.Fence, Holder: c.Holder}, b.Set(storage.SingletonKey(c.Name), data, nil)
	case SingletonRelease:
		if found && rec.Holder == c.Holder && rec.Fence == c.Fence {
			// Keep the record (and its fence) so fencing tokens stay strictly
			// monotone across release/re-acquire; just vacate the holder.
			rec.Holder = ""
			rec.DeadlineMs = 0
			data, _ := json.Marshal(rec)
			return &SingletonResult{OK: true}, b.Set(storage.SingletonKey(c.Name), data, nil)
		}
		return &SingletonResult{OK: false}, nil
	}
	return &SingletonResult{OK: false}, nil
}
