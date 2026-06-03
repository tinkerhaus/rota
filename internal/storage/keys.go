// Package storage owns the Pebble keyspace: order-preserving key builders and a
// thin store wrapper. The first byte of every key is a table tag so each logical
// table is a contiguous prefix (cheap iteration, cheap range-delete).
package storage

import "encoding/binary"

const (
	tagMeta       byte = 0x00
	tagMessage    byte = 0x01
	tagGroupMeta  byte = 0x02
	tagLease      byte = 0x03
	tagTimeIndex  byte = 0x04
	tagDLQ        byte = 0x05
	tagPolicy     byte = 0x06
	tagCron       byte = 0x07
	tagSingleton  byte = 0x08
	tagToken      byte = 0x09
	tagLaneConfig byte = 0x0A
	tagRaftLog    byte = 0xFE
	tagRaftKV     byte = 0xFF
)

// Timer kinds carried in the time index. The same ordered structure drives
// delayed publish, retry backoff, and visibility-timeout redelivery.
const (
	TimerReadyAt       byte = 0x01 // a DELAYED message becomes READY
	TimerLeaseDeadline byte = 0x02 // a lease's visibility window expires
	TimerCronDue       byte = 0x03 // a cron schedule fires (Phase 4)
)

func u64be(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// lp is a length-prefixed string: u16be(len) ++ bytes. It makes composite key
// prefixes unambiguous so a prefix scan over one lane never bleeds into another.
func lp(s string) []byte {
	b := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(b, uint16(len(s)))
	copy(b[2:], s)
	return b
}

// takeLP reads one length-prefixed string from the front of b, returning it and
// the remainder.
func takeLP(b []byte) (string, []byte, bool) {
	if len(b) < 2 {
		return "", nil, false
	}
	n := int(binary.BigEndian.Uint16(b[:2]))
	if len(b) < 2+n {
		return "", nil, false
	}
	return string(b[2 : 2+n]), b[2+n:], true
}

// MetaKey: 0x00 ++ name
func MetaKey(name string) []byte { return append([]byte{tagMeta}, name...) }

// MessageKey: 0x01 ++ LP(lane) ++ LP(group) ++ u64be(msgID)
func MessageKey(lane, group string, msgID uint64) []byte {
	k := []byte{tagMessage}
	k = append(k, lp(lane)...)
	k = append(k, lp(group)...)
	return append(k, u64be(msgID)...)
}

// MessagePrefix: all messages in (lane, group), ascending by msgID.
func MessagePrefix(lane, group string) []byte {
	k := []byte{tagMessage}
	k = append(k, lp(lane)...)
	return append(k, lp(group)...)
}

// GroupMetaKey: 0x02 ++ LP(lane) ++ LP(group)
func GroupMetaKey(lane, group string) []byte {
	k := []byte{tagGroupMeta}
	k = append(k, lp(lane)...)
	return append(k, lp(group)...)
}

// GroupMetaLanePrefix: all group-meta rows in a lane.
func GroupMetaLanePrefix(lane string) []byte {
	k := []byte{tagGroupMeta}
	return append(k, lp(lane)...)
}

// GroupMetaBounds: the [lo, hi) range over ALL group-meta rows (every lane).
func GroupMetaBounds() (lo, hi []byte) { return []byte{tagGroupMeta}, []byte{tagGroupMeta + 1} }

// LeaseKey: 0x03 ++ u64be(leaseID)
func LeaseKey(leaseID uint64) []byte {
	return append([]byte{tagLease}, u64be(leaseID)...)
}

// TimeIndexKey: 0x04 ++ u64be(dueTs) ++ kind ++ ref. due_ts sorts first so
// "everything due by now" is a single bounded forward scan.
func TimeIndexKey(dueTs uint64, kind byte, ref []byte) []byte {
	k := []byte{tagTimeIndex}
	k = append(k, u64be(dueTs)...)
	k = append(k, kind)
	return append(k, ref...)
}

// TimeIndexPrefix / TimeIndexUpTo bound a sweep of all timers due at or before ts.
func TimeIndexPrefix() []byte { return []byte{tagTimeIndex} }
func TimeIndexUpTo(ts uint64) []byte {
	return append([]byte{tagTimeIndex}, u64be(ts+1)...)
}

// ParseTimeIndexKey splits a time-index key back into (dueTs, kind, ref).
func ParseTimeIndexKey(k []byte) (dueTs uint64, kind byte, ref []byte, ok bool) {
	if len(k) < 10 || k[0] != tagTimeIndex {
		return 0, 0, nil, false
	}
	return binary.BigEndian.Uint64(k[1:9]), k[9], k[10:], true
}

// ReadyAtRef / ParseReadyAtRef encode a delayed/retry timer's target message.
func ReadyAtRef(lane, group string, msgID uint64) []byte {
	r := lp(lane)
	r = append(r, lp(group)...)
	return append(r, u64be(msgID)...)
}
func ParseReadyAtRef(ref []byte) (lane, group string, msgID uint64, ok bool) {
	lane, ref, ok = takeLP(ref)
	if !ok {
		return "", "", 0, false
	}
	group, ref, ok = takeLP(ref)
	if !ok || len(ref) < 8 {
		return "", "", 0, false
	}
	return lane, group, binary.BigEndian.Uint64(ref[:8]), true
}

// LeaseDeadlineRef / ParseLeaseDeadlineRef encode a visibility-timeout timer.
func LeaseDeadlineRef(leaseID uint64) []byte { return u64be(leaseID) }
func ParseLeaseDeadlineRef(ref []byte) (leaseID uint64, ok bool) {
	if len(ref) < 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(ref[:8]), true
}

// DLQKey: 0x05 ++ LP(lane) ++ LP(group) ++ u64be(deadTs) ++ u64be(msgID)
func DLQKey(lane, group string, deadTs, msgID uint64) []byte {
	k := []byte{tagDLQ}
	k = append(k, lp(lane)...)
	k = append(k, lp(group)...)
	k = append(k, u64be(deadTs)...)
	return append(k, u64be(msgID)...)
}

// DLQLanePrefix: all dead letters for a lane.
func DLQLanePrefix(lane string) []byte {
	k := []byte{tagDLQ}
	return append(k, lp(lane)...)
}

// PolicyKey: 0x06 ++ LP(lane) — the lane's replicated policy binding.
func PolicyKey(lane string) []byte {
	return append([]byte{tagPolicy}, lp(lane)...)
}

// CronKey: 0x07 ++ cron_id
func CronKey(cronID string) []byte { return append([]byte{tagCron}, cronID...) }

// CronPrefix: all cron specs (for listing).
func CronPrefix() []byte { return []byte{tagCron} }

// SingletonKey: 0x08 ++ name
func SingletonKey(name string) []byte { return append([]byte{tagSingleton}, name...) }

// TokenKey: 0x09 ++ token_hash (sha256) — maps a completion token to a lease id.
func TokenKey(hash []byte) []byte { return append([]byte{tagToken}, hash...) }

// LaneConfigKey: 0x0A ++ lane — the lane's replicated rate-limit config.
func LaneConfigKey(lane string) []byte { return append([]byte{tagLaneConfig}, lane...) }

// AppKeyspaceBounds returns the [lo, hi) range covering all application tables
// (everything EXCEPT the raft log/stable store at 0xFE/0xFF). Used by FSM
// snapshots so a snapshot/restore never clobbers a node's own raft log.
func AppKeyspaceBounds() (lo, hi []byte) { return []byte{tagMeta}, []byte{tagLaneConfig + 1} }

// CronDueRef / ParseCronDueRef encode a cron timer's target spec.
func CronDueRef(cronID string) []byte   { return []byte(cronID) }
func ParseCronDueRef(ref []byte) string { return string(ref) }

// RaftLogKey / RaftLogPrefix: 0xFE ++ u64be(index)
func RaftLogKey(index uint64) []byte { return append([]byte{tagRaftLog}, u64be(index)...) }
func RaftLogPrefix() []byte          { return []byte{tagRaftLog} }

// RaftKVKey: 0xFF ++ key (raft StableStore values)
func RaftKVKey(key []byte) []byte { return append([]byte{tagRaftKV}, key...) }

// PrefixEnd returns the smallest key strictly greater than every key with the
// given prefix (the exclusive upper bound for a prefix scan). nil ⇒ no bound.
func PrefixEnd(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	return nil
}
