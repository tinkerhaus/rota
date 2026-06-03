// Package raftpebble implements hashicorp/raft's LogStore and StableStore over
// the same Pebble DB that backs the FSM, so the raft log and the application
// state share one engine and one crash-recovery story.
package raftpebble

import (
	"encoding/binary"
	"encoding/json"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"

	"github.com/tinkerhaus/rota/internal/storage"
)

type Store struct{ s *storage.Store }

func New(s *storage.Store) *Store { return &Store{s: s} }

// --- raft.LogStore ---

func (p *Store) FirstIndex() (uint64, error) { return p.edgeIndex(false) }
func (p *Store) LastIndex() (uint64, error)  { return p.edgeIndex(true) }

func (p *Store) edgeIndex(last bool) (uint64, error) {
	lo := storage.RaftLogPrefix()
	hi := storage.PrefixEnd(lo)
	it, err := p.s.DB.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	var ok bool
	if last {
		ok = it.Last()
	} else {
		ok = it.First()
	}
	if !ok {
		return 0, nil
	}
	k := it.Key()
	if len(k) < 9 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(k[1:9]), nil
}

func (p *Store) GetLog(index uint64, log *raft.Log) error {
	v, ok, err := p.s.GetRaw(storage.RaftLogKey(index))
	if err != nil {
		return err
	}
	if !ok {
		return raft.ErrLogNotFound
	}
	return json.Unmarshal(v, log)
}

func (p *Store) StoreLog(log *raft.Log) error { return p.StoreLogs([]*raft.Log{log}) }

func (p *Store) StoreLogs(logs []*raft.Log) error {
	b := p.s.DB.NewBatch()
	defer b.Close()
	for _, l := range logs {
		data, err := json.Marshal(l)
		if err != nil {
			return err
		}
		if err := b.Set(storage.RaftLogKey(l.Index), data, nil); err != nil {
			return err
		}
	}
	return b.Commit(pebble.Sync)
}

func (p *Store) DeleteRange(min, max uint64) error {
	return p.s.DB.DeleteRange(storage.RaftLogKey(min), storage.RaftLogKey(max+1), pebble.Sync)
}

// --- raft.StableStore ---

func (p *Store) Set(key, val []byte) error {
	return p.s.DB.Set(storage.RaftKVKey(key), val, pebble.Sync)
}

func (p *Store) Get(key []byte) ([]byte, error) {
	v, ok, err := p.s.GetRaw(storage.RaftKVKey(key))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil // raft tolerates an empty value for an unset key
	}
	return v, nil
}

func (p *Store) SetUint64(key []byte, val uint64) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, val)
	return p.Set(key, b)
}

func (p *Store) GetUint64(key []byte) (uint64, error) {
	v, err := p.Get(key)
	if err != nil || len(v) < 8 {
		return 0, err
	}
	return binary.BigEndian.Uint64(v), nil
}
