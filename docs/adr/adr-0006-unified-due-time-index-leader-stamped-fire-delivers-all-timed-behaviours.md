# 0006. Unified due-time index + leader-stamped fire delivers all timed behaviours

## Decision

Build ONE durable due-time index (tidx:<due_ts>:<kind>:<ref>) in Pebble that powers delayed publish, retry backoff, visibility-timeout redelivery, lease-deadline sweeps, and cron, accelerated by a leader-only in-memory hierarchical timing wheel. The leader proposes idempotent, epoch-fenced FireTimer/FireCron entries with a stamped fire_at; all nodes apply identically.

## Rationale

The five features are the same primitive ('becomes actionable at time T, durably, exactly-once on a cluster') viewed five ways. Collapsing them removes four bespoke schedulers and four recovery paths. Correctness lives entirely in Pebble+Raft; the wheel is a pure accelerator, so an empty/early/late wheel is a latency event, never a correctness event (Apply re-validates due_at<=fire_at, and a new leader rebuilds the wheel by scanning tidx:). Epoch-fencing makes every fire idempotent so the leader can re-propose freely after failover.

## Consequences

Wheel rebuild on failover must use a sliding horizon + lazy paging (a full scan of millions of far-future timers is too slow); MaxFireBatch trades fire latency vs apply cost and a burst of simultaneous expiries needs backpressure so it does not starve normal publishes; backoff jitter must be deterministic (seeded by msg_id+attempt) or the FSM diverges.
