// Package storage owns the Pebble keyspace: order-preserving key builders and a
// thin store wrapper. The first byte of every key is a table tag so each logical
// table is a contiguous prefix (cheap iteration, cheap range-delete).
package storage

import "encoding/binary"

const (
	tagMeta         byte = 0x00
	tagMessage      byte = 0x01
	tagGroupMeta    byte = 0x02
	tagLease        byte = 0x03
	tagTimeIndex    byte = 0x04
	tagDLQ          byte = 0x05
	tagPolicy       byte = 0x06
	tagCron         byte = 0x07
	tagSingleton    byte = 0x08
	tagToken        byte = 0x09
	tagLaneConfig   byte = 0x0A
	tagDedup        byte = 0x0B // producer-side idempotency window: dedup_key -> msg id
	tagWFRun        byte = 0x0C // durable-execution run record (RunMeta)
	tagWFHistory    byte = 0x0D // durable-execution per-run event history (append-only)
	tagWFActivityDn byte = 0x0E // per-activity completion done-marker (scheduled_event_id dedup)
	tagRaftLog      byte = 0xFE
	tagRaftKV       byte = 0xFF
)

// Timer kinds carried in the time index. The same ordered structure drives
// delayed publish, retry backoff, and visibility-timeout redelivery.
const (
	TimerReadyAt       byte = 0x01 // a DELAYED message becomes READY
	TimerLeaseDeadline byte = 0x02 // a lease's visibility window expires
	TimerCronDue       byte = 0x03 // a cron schedule fires (Phase 4)
	TimerDedupExpiry   byte = 0x04 // a producer dedup window closes (the row is swept)
	TimerLeaseMaxLife  byte = 0x05 // a lease's absolute lifetime cap (survives extends)
	TimerWFFired       byte = 0x06 // a durable workflow timer (workflow.sleep) fires
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

// LeaseBounds: the [lo, hi) range over ALL leases (keyed globally by lease id).
// Leases are not lane-partitioned on disk, so the lease inspector scans this
// whole range and filters by lane in memory.
func LeaseBounds() (lo, hi []byte) { return []byte{tagLease}, []byte{tagLease + 1} }

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

// DLQGroupPrefix: all dead letters for a (lane, group). The dead_ts is part of
// the key (so a single msg id cannot be addressed directly), so a redrive scans
// this prefix to find the row whose trailing msg id matches.
func DLQGroupPrefix(lane, group string) []byte {
	k := []byte{tagDLQ}
	k = append(k, lp(lane)...)
	return append(k, lp(group)...)
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

// DedupKey: 0x0B ++ LP(lane) ++ key — a producer dedup_key's idempotency record.
// Lane-scoped so the same key in different lanes is independent.
func DedupKey(lane, key string) []byte {
	k := []byte{tagDedup}
	k = append(k, lp(lane)...)
	return append(k, key...)
}

// DedupRef / ParseDedupRef encode a dedup row's (lane, key) for its expiry timer.
func DedupRef(lane, key string) []byte {
	r := lp(lane)
	return append(r, key...)
}
func ParseDedupRef(ref []byte) (lane, key string, ok bool) {
	lane, rest, ok := takeLP(ref)
	if !ok {
		return "", "", false
	}
	return lane, string(rest), true
}

// WFRunKey: 0x0C ++ u64be(run_id) — a workflow run's RunMeta record.
func WFRunKey(runID uint64) []byte { return append([]byte{tagWFRun}, u64be(runID)...) }

// WFRunBounds: the [lo, hi) range over ALL run records (for listing/scan).
func WFRunBounds() (lo, hi []byte) { return []byte{tagWFRun}, []byte{tagWFRun + 1} }

// WFHistoryKey: 0x0D ++ u64be(run_id) ++ u64be(event_id) — one history event.
// Contiguous per run (range-deletable), ordered by event_id within a run.
func WFHistoryKey(runID, eventID uint64) []byte {
	k := []byte{tagWFHistory}
	k = append(k, u64be(runID)...)
	return append(k, u64be(eventID)...)
}

// WFHistoryPrefix: all history events of one run, ascending by event_id.
func WFHistoryPrefix(runID uint64) []byte {
	return append([]byte{tagWFHistory}, u64be(runID)...)
}

// WFTimerRef / ParseWFTimerRef encode a durable workflow timer's (run_id,
// started_event_id) into a TimerWFFired time-index entry's ref.
func WFTimerRef(runID, startedEventID uint64) []byte {
	r := u64be(runID)
	return append(r, u64be(startedEventID)...)
}
func ParseWFTimerRef(ref []byte) (runID, startedEventID uint64, ok bool) {
	if len(ref) < 16 {
		return 0, 0, false
	}
	return binary.BigEndian.Uint64(ref[:8]), binary.BigEndian.Uint64(ref[8:16]), true
}

// WFActivityDoneKey: 0x0E ++ u64be(run_id) ++ u64be(scheduled_event_id) — the
// presence marker that makes an activity's terminal (COMPLETED/FAILED) append
// idempotent, so an at-least-once redelivery records the outcome exactly once.
func WFActivityDoneKey(runID, schedEventID uint64) []byte {
	k := []byte{tagWFActivityDn}
	k = append(k, u64be(runID)...)
	return append(k, u64be(schedEventID)...)
}

// AppKeyspaceBounds returns the [lo, hi) range covering all application tables
// (everything EXCEPT the raft log/stable store at 0xFE/0xFF). Used by FSM
// snapshots so a snapshot/restore never clobbers a node's own raft log.
//
// INVARIANT: every application table tag must fall inside [lo, hi). When you add
// a tag, bump the upper bound here — TestAppKeyspaceBoundsCoverAllAppTags enforces it.
func AppKeyspaceBounds() (lo, hi []byte) { return []byte{tagMeta}, []byte{tagWFActivityDn + 1} }

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
