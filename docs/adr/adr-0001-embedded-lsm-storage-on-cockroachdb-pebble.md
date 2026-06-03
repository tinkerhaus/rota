# 0001. Embedded LSM storage on cockroachdb/pebble

## Decision

Use cockroachdb/pebble as the single embedded LSM store for both the application keyspace and (via custom raft.LogStore/StableStore) the Raft log and stable store, so there is exactly one durable engine and one fsync domain.

## Rationale

The locked constraint demands ONE durable system of record for both messages and scheduling state, written in one atomic batch. Pebble gives atomic WriteBatch (one batch per Apply), consistent Snapshot + Checkpoint (Raft FSM snapshot/transfer, hard-linked and cheap), prefix iteration (the tidx time-index sweep and per-group scans), and DeleteRange (cheap reap/purge of thousands of ephemeral groups and log truncation). It is the same engine CockroachDB runs in production at high cardinality. Sharing one Pebble for log+data removes the 'log durable but FSM lost' and 'FSM durable but log torn' inconsistency classes.

## Consequences

Custom Pebble-backed LogStore is non-standard (most hashicorp/raft users use raft-boltdb); range-deleted truncated log entries leave tombstones until compaction, so compaction must be tuned and benchmarked against raft-boltdb before final commitment. The escape hatch is raft-boltdb for the log only (simpler, proven, but two engines / two fsync domains).
