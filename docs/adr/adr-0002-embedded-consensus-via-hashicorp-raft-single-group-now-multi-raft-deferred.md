# 0002. Embedded consensus via hashicorp/raft, single group now, multi-raft deferred

## Decision

Ship a single-Raft-group design on hashicorp/raft whose FSM is a host-agnostic Apply([]byte)->[]byte with all state lane-prefixed, so re-hosting under lni/dragonboat per-lane (RaftGroupID=hash(lane)) later is a hosting swap, not a rewrite.

## Rationale

hashicorp/raft is a single, well-understood FSM that ships fast and matches the 'broker is not the bottleneck' reality (consumer tasks run seconds-to-minutes; cardinality scales with concurrent-group count, not QPS). It provides mature leader election/failover, single-server membership changes, snapshot+truncation hooks, and a single-voter dev mode identical to prod. dragonboat is pulled in only IF a single lane's throughput becomes the ceiling.

## Consequences

All lanes share one Raft log, so total commit throughput is one group's ceiling (acceptable given the perf profile + per-entry fsync). Moving to multi-raft splits cross-lane atomicity: a TeardownGroup across lanes would no longer be one batch and would need a saga/2PC, so the lane sharding key must keep cross-lane commands rare.
