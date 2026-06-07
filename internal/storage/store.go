package storage

import (
	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
)

// Store is a thin wrapper over a single Pebble DB. The FSM mutates it via atomic
// batches; reads during Apply see committed state.
type Store struct{ DB *pebble.DB }

func Open(dir string) (*Store, error) {
	return OpenWithOptions(dir, &pebble.Options{})
}

func OpenWithOptions(dir string, opts *pebble.Options) (*Store, error) {
	if opts == nil {
		opts = &pebble.Options{}
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &Store{DB: db}, nil
}

func (s *Store) Close() error { return s.DB.Close() }

// GetRaw returns a copy of the value for key, or ok=false if absent.
func (s *Store) GetRaw(key []byte) ([]byte, bool, error) {
	v, closer, err := s.DB.Get(key)
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	out := make([]byte, len(v))
	copy(out, v)
	_ = closer.Close()
	return out, true, nil
}

// GetProto unmarshals the value for key into m. ok=false if the key is absent.
func (s *Store) GetProto(key []byte, m proto.Message) (bool, error) {
	v, ok, err := s.GetRaw(key)
	if err != nil || !ok {
		return ok, err
	}
	return true, proto.Unmarshal(v, m)
}

// GroupInfo is the per-group scheduling snapshot the node feeds to the scheduler.
type GroupInfo struct {
	ID       string
	Ready    uint64
	InFlight uint64
	Weight   float64
	Paused   bool
}

// ListGroups returns every group in a lane (the scheduler filters by Ready>0).
func (s *Store) ListGroups(lane string) ([]GroupInfo, error) {
	lo := GroupMetaLanePrefix(lane)
	hi := PrefixEnd(lo)
	it, err := s.DB.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []GroupInfo
	for it.First(); it.Valid(); it.Next() {
		gm := &rotav1.GroupMeta{}
		if err := proto.Unmarshal(it.Value(), gm); err != nil {
			return nil, err
		}
		out = append(out, GroupInfo{ID: gm.GroupId, Ready: gm.ReadyCount, InFlight: gm.InflightCount, Weight: gm.Weight, Paused: gm.Paused})
	}
	return out, nil
}

// CountDLQ returns the number of dead letters in a lane.
func (s *Store) CountDLQ(lane string) (int, error) {
	lo := DLQLanePrefix(lane)
	hi := PrefixEnd(lo)
	it, err := s.DB.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	n := 0
	for it.First(); it.Valid(); it.Next() {
		n++
	}
	return n, nil
}
