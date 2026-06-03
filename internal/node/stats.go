package node

import (
	"strconv"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/storage"
)

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
	Inflight      uint64
	GroupCount    uint64
	DLQ           uint64
	PolicyVersion uint64
}

// Stats aggregates per-lane depth from group metadata (optionally filtered).
func (n *Node) Stats(filterLane string) ([]LaneStat, error) {
	lo, hi := storage.GroupMetaBounds()
	it, err := n.store.DB.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	agg := map[string]*LaneStat{}
	for it.First(); it.Valid(); it.Next() {
		gm := &rotav1.GroupMeta{}
		if proto.Unmarshal(it.Value(), gm) != nil {
			continue
		}
		if filterLane != "" && gm.Lane != filterLane {
			continue
		}
		s := agg[gm.Lane]
		if s == nil {
			s = &LaneStat{Lane: gm.Lane}
			agg[gm.Lane] = s
		}
		s.Leasable += gm.ReadyCount
		s.Inflight += gm.InflightCount
		s.GroupCount++
	}
	out := make([]LaneStat, 0, len(agg))
	for lane, s := range agg {
		if c, err := n.store.CountDLQ(lane); err == nil {
			s.DLQ = uint64(c)
		}
		n.polMu.Lock()
		s.PolicyVersion = n.loadedPolVer[lane]
		n.polMu.Unlock()
		out = append(out, *s)
	}
	return out, nil
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
