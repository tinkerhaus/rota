package node

import (
	"encoding/hex"
	"strconv"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/storage"
)

// ─── Dashboard read API (paginated, follower-servable) ──────────────────────────

const (
	defaultPageSize = 100
	maxPageSize     = 1000
)

func clampPage(n uint32) int {
	if n == 0 || int(n) > maxPageSize {
		return defaultPageSize
	}
	return int(n)
}

// pageStart turns an opaque page token (hex of the previous page's last key) into
// the inclusive lower bound for the next page — strictly after that last key.
func pageStart(prefixLo []byte, token string) []byte {
	if token == "" {
		return prefixLo
	}
	raw, err := hex.DecodeString(token)
	if err != nil || len(raw) == 0 {
		return prefixLo
	}
	return append(raw, 0x00) // append 0x00 ⇒ smallest key strictly greater than raw
}

// ListGroupStats returns per-group fairness stats for a lane (the dashboard grid),
// one page at a time. nextToken is "" once the last page has been returned.
func (n *Node) ListGroupStats(lane string, pageSize uint32, pageToken string) ([]*rotav1.GroupStats, string, error) {
	lo := storage.GroupMetaLanePrefix(lane)
	hi := storage.PrefixEnd(lo)
	it, err := n.store.DB.NewIter(&pebble.IterOptions{LowerBound: pageStart(lo, pageToken), UpperBound: hi})
	if err != nil {
		return nil, "", err
	}
	defer it.Close()
	limit := clampPage(pageSize)
	var out []*rotav1.GroupStats
	var lastKey []byte
	for it.First(); it.Valid(); it.Next() {
		gm := &rotav1.GroupMeta{}
		if proto.Unmarshal(it.Value(), gm) != nil {
			continue
		}
		out = append(out, &rotav1.GroupStats{
			Lane: gm.Lane, GroupId: gm.GroupId, Weight: gm.Weight, Paused: gm.Paused,
			Ready: gm.ReadyCount, Delayed: gm.DelayedCount, Inflight: gm.InflightCount,
			Total: gm.TotalCount, VirtualTime: gm.VirtualTime, Deficit: gm.Deficit,
			LastActivityMs: gm.LastActivityMs,
		})
		lastKey = append(lastKey[:0], it.Key()...)
		if len(out) >= limit {
			it.Next()
			if it.Valid() {
				return out, hex.EncodeToString(lastKey), nil
			}
			break
		}
	}
	return out, "", nil
}

// ListDeadLetters returns a lane's dead letters (the DLQ inspector), one page at a time.
func (n *Node) ListDeadLetters(lane string, pageSize uint32, pageToken string) ([]*rotav1.DeadLetterInfo, string, error) {
	lo := storage.DLQLanePrefix(lane)
	hi := storage.PrefixEnd(lo)
	it, err := n.store.DB.NewIter(&pebble.IterOptions{LowerBound: pageStart(lo, pageToken), UpperBound: hi})
	if err != nil {
		return nil, "", err
	}
	defer it.Close()
	limit := clampPage(pageSize)
	var out []*rotav1.DeadLetterInfo
	var lastKey []byte
	for it.First(); it.Valid(); it.Next() {
		dl := &rotav1.DeadLetter{}
		if proto.Unmarshal(it.Value(), dl) != nil {
			continue
		}
		orig := dl.GetOriginal()
		out = append(out, &rotav1.DeadLetterInfo{
			Lane: lane, GroupId: orig.GetGroupId(), MsgId: orig.GetMsgId(),
			FinalAttempt: dl.GetFinalAttempt(), Reason: dl.GetReason(), DeadAtMs: dl.GetDeadAtMs(),
			FailureHeaders: dl.GetFailureHeaders(), Headers: orig.GetHeaders(), Payload: orig.GetPayload(),
		})
		lastKey = append(lastKey[:0], it.Key()...)
		if len(out) >= limit {
			it.Next()
			if it.Valid() {
				return out, hex.EncodeToString(lastKey), nil
			}
			break
		}
	}
	return out, "", nil
}

// ListLeases returns a lane's in-flight leases (the lease inspector), one page at
// a time. Leases are keyed globally by lease id (not lane-partitioned on disk),
// so this scans the whole lease table and filters by lane; the page token is the
// hex of the last lease key returned. Follower-servable; read-only.
func (n *Node) ListLeases(lane string, pageSize uint32, pageToken string) ([]*rotav1.LeaseInfo, string, error) {
	lo, hi := storage.LeaseBounds()
	it, err := n.store.DB.NewIter(&pebble.IterOptions{LowerBound: pageStart(lo, pageToken), UpperBound: hi})
	if err != nil {
		return nil, "", err
	}
	defer it.Close()
	limit := clampPage(pageSize)
	var out []*rotav1.LeaseInfo
	var lastKey []byte
	for it.First(); it.Valid(); it.Next() {
		ls := &rotav1.Lease{}
		if proto.Unmarshal(it.Value(), ls) != nil {
			continue
		}
		if ls.Lane != lane {
			continue // global table; skip other lanes (token still advances over scanned keys)
		}
		out = append(out, &rotav1.LeaseInfo{
			Lane: ls.Lane, GroupId: ls.GroupId, MsgId: ls.MsgId, LeaseId: ls.LeaseId,
			ConsumerId: ls.ConsumerId, DeadlineMs: ls.DeadlineMs, Attempt: ls.AttemptAtLease,
			Epoch: ls.Epoch, ExtendCount: ls.ExtendCount,
		})
		lastKey = append(lastKey[:0], it.Key()...)
		if len(out) >= limit {
			it.Next()
			if it.Valid() {
				return out, hex.EncodeToString(lastKey), nil
			}
			break
		}
	}
	return out, "", nil
}

// peekLimit caps a PeekMessages read so a deep group can't be dumped in one call.
const peekLimit = 200

// PeekMessages returns the head messages of a group without mutating any state
// (a non-destructive inspector view). Read-only; follower-servable.
func (n *Node) PeekMessages(lane, group string, limit uint32) ([]*rotav1.MessagePeek, error) {
	lo := storage.MessagePrefix(lane, group)
	hi := storage.PrefixEnd(lo)
	it, err := n.store.DB.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	cap := int(limit)
	if cap <= 0 || cap > peekLimit {
		cap = peekLimit
	}
	var out []*rotav1.MessagePeek
	for it.First(); it.Valid() && len(out) < cap; it.Next() {
		m := &rotav1.Message{}
		if proto.Unmarshal(it.Value(), m) != nil {
			continue
		}
		out = append(out, &rotav1.MessagePeek{
			MsgId: m.MsgId, State: m.State, Attempt: m.Attempt,
			EnqueueMs: m.EnqueueMs, NotBeforeMs: m.NotBeforeMs,
		})
	}
	return out, nil
}

type GroupConfigInfo struct {
	Weight    float64
	BatchSize uint32
	Paused    bool
	Exists    bool
}

func (n *Node) GroupConfig(lane, group string) GroupConfigInfo {
	gm := &rotav1.GroupMeta{}
	ok, _ := n.store.GetProto(storage.GroupMetaKey(lane, group), gm)
	if !ok {
		return GroupConfigInfo{Weight: 1, BatchSize: 1}
	}
	return GroupConfigInfo{Weight: gm.Weight, BatchSize: gm.BatchSize, Paused: gm.Paused, Exists: true}
}

type LaneStat struct {
	Lane          string
	Leasable      uint64
	Delayed       uint64
	Inflight      uint64
	GroupCount    uint64
	DLQ           uint64
	PolicyVersion uint64
	PublishRate   float64 // smoothed events/sec (leader-local meter)
	LeaseRate     float64
	AckRate       float64
	OldestAgeMs   uint64
}

// Stats aggregates per-lane depth from group metadata (optionally filtered).
func (n *Node) Stats(filterLane, filterGroup string) ([]LaneStat, error) {
	lo, hi := storage.GroupMetaBounds()
	it, err := n.store.DB.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	agg := map[string]*LaneStat{}
	now := nowMs()
	for it.First(); it.Valid(); it.Next() {
		gm := &rotav1.GroupMeta{}
		if proto.Unmarshal(it.Value(), gm) != nil {
			continue
		}
		if filterLane != "" && gm.Lane != filterLane {
			continue
		}
		if filterGroup != "" && gm.GroupId != filterGroup {
			continue
		}
		s := agg[gm.Lane]
		if s == nil {
			s = &LaneStat{Lane: gm.Lane}
			agg[gm.Lane] = s
		}
		s.Leasable += gm.ReadyCount
		s.Delayed += gm.DelayedCount
		s.Inflight += gm.InflightCount
		s.GroupCount++
		if age := n.oldestGroupMessageAgeMs(gm.Lane, gm.GroupId, now); age > s.OldestAgeMs {
			s.OldestAgeMs = age
		}
	}
	out := make([]LaneStat, 0, len(agg))
	for lane, s := range agg {
		if c, err := n.store.CountDLQ(lane); err == nil {
			s.DLQ = uint64(c)
		}
		n.polMu.Lock()
		s.PolicyVersion = n.loadedPolVer[lane]
		n.polMu.Unlock()
		r := n.meter.rates(lane)
		s.PublishRate, s.LeaseRate, s.AckRate = r.Publish, r.Lease, r.Ack
		out = append(out, *s)
	}
	return out, nil
}

func (n *Node) oldestGroupMessageAgeMs(lane, group string, now uint64) uint64 {
	lo := storage.MessagePrefix(lane, group)
	hi := storage.PrefixEnd(lo)
	it, err := n.store.DB.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return 0
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		msg := &rotav1.Message{}
		if proto.Unmarshal(it.Value(), msg) != nil {
			continue
		}
		if msg.GetState() == rotav1.MessageState_DONE || msg.GetState() == rotav1.MessageState_DEAD {
			continue
		}
		if msg.GetEnqueueMs() == 0 || msg.GetEnqueueMs() > now {
			return 0
		}
		return now - msg.GetEnqueueMs()
	}
	return 0
}

type PeerData struct{ ID, Addr, Suffrage string }

type ClusterInfoData struct {
	LeaderID     string
	LeaderAddr   string
	Term         uint64
	AppliedIndex uint64
	Peers        []PeerData
}

func (n *Node) ClusterInfo() ClusterInfoData {
	addr, id := n.raft.LeaderWithID()
	out := ClusterInfoData{LeaderID: string(id), LeaderAddr: string(addr), AppliedIndex: n.fsm.AppliedIndex()}
	if t, err := strconv.ParseUint(n.raft.Stats()["term"], 10, 64); err == nil {
		out.Term = t
	}
	if fut := n.raft.GetConfiguration(); fut.Error() == nil {
		for _, s := range fut.Configuration().Servers {
			out.Peers = append(out.Peers, PeerData{ID: string(s.ID), Addr: string(s.Address), Suffrage: suffrage(s.Suffrage)})
		}
	}
	return out
}

func (n *Node) Health() (serving, hasQuorum, isLeader bool) {
	isLeader = n.raft.State() == raft.Leader
	addr, _ := n.raft.LeaderWithID()
	return true, addr != "", isLeader
}

func suffrage(s raft.ServerSuffrage) string {
	if s == raft.Voter {
		return "Voter"
	}
	return "Nonvoter"
}
