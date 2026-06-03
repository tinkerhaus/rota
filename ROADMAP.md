# Rota Roadmap

## Phase 0 — Single-node walking skeleton: A binary that compiles and runs the full publish -> fair lease -> ack loop end to end on one node, exercising every layer (gRPC, raft single-voter, Pebble FSM, built-in DRR) so the seams are real from day one.

**Scope:**
- cmd/rota main: boot a single-voter raft cluster (BootstrapCluster self), open one Pebble DB, start gRPC servers, signal handling
- Pebble keyspace + order-preserving key builders for msg:/grp:/lease:/meta: (+ rlog:/rkv: for the custom LogStore/StableStore)
- Custom raft.LogStore + StableStore over Pebble (Sync writes); RotaFSM.Apply writing one atomic batch + meta:applied_index
- FSM commands: Publish (implicit group create, per-group seq, ready_count), LeaseDecision, Ack
- Built-in DRR mechanism in Go (no policy engine yet): leader reads GroupMeta, rotates groups, emits LeaseDecision with deficit math
- Broker gRPC: Publish (unary) + Work (bidi) with LeaseRequest{credit}/Lease/Ack only; credit=1 serial path
- Demand-driven leader scheduling tick (propose LeaseDecision as credit arrives)
- proto/rota.proto for the Phase-0 subset; buf codegen for Go
- Smoke test: publish 500 messages to group A + 20 to group B in one lane, assert the 500-message group does not head-of-line-block the 20-message group (A,B,A,B rotation). Groups are keyed by an opaque group id (e.g. a job, tenant, session, or batch id).

**Deliverable:** go build ./... succeeds; a single rota node serves Publish + Work(LeaseRequest/Lease/Ack); a demo proves cross-group DRR fairness for the 500-vs-20 case. No HA, no retry, no time, no policy engine.

## Phase 1 — Delivery correctness (lease lifecycle + unified time): Make delivery robust on the single node: visibility timeouts, retries, DLQ, and the unified due-time index that also gives delayed publish and backoff for free.

**Scope:**
- Unified tidx: due-time index + leader-only timing wheel + fire loop (sweep tidx<=now -> propose FireTimer)
- Message/Lease epoch fencing; idempotent FireTimer (READY_AT, LEASE_DEADLINE)
- Visibility timeout -> redelivery (LEASE_DEADLINE -> requeue; no-penalty requeue default / penalty if lane configured)
- Broker-owned attempt counter; two-outcome Nack (REQUEUE_NO_PENALTY / RETRY / DEAD_LETTER); ExtendVisibility
- Native bounded retry with deterministic-jitter backoff -> auto-promote to DLQ; DeadLetter records + redrive
- Delayed publish (not_before) and retry backoff as READY_AT timers (same mechanism)
- ttl expiry (drop or dead-letter per lane)
- Work-stream: Nack/Extend frames, StreamError(LEASE_EXPIRED/UNKNOWN_LEASE), graceful drain on context cancel
- Integration tests: redelivery on missed ack, retry->DLQ, delayed becomes leasable at T, drain returns un-acked leases

**Deliverable:** Single-node broker with full at-least-once lease lifecycle: SQS-style visibility redelivery, retry->DLQ, delayed publish, backoff, ttl, clean SIGTERM drain. Time is one mechanism over tidx:.

## Phase 2 — HA hardening (cluster, durability, failover): Make it a real 3/5-node HA cluster with quorum-acked durability, snapshot/restore, and lossless leader failover for both decisions and timers.

**Scope:**
- 3/5-voter bootstrap + membership (AddVoter/AddNonvoter/RemoveServer); config-file seed list
- Quorum-before-ack on every write (wait on Apply future post-commit-post-apply); Sync on log + data
- FSMSnapshot via pebble.Checkpoint; Restore via SST ingest; log truncation via LogStore.DeleteRange after snapshot; compaction tuning
- NotLeader detection + redirect detail (genproto errdetails) on writes; in-stream NOT_LEADER
- Leader fire-loop start/stop on raft.LeaderCh; rebuildWheelFromPebble on leadership gain (sliding horizon + lazy paging)
- Idempotent re-propose of FireTimer/LeaseDecision after failover (epoch + cfence); leader clock sanity guard (no backwards jumps)
- Crash recovery: Pebble WAL replay + applied_index + raft log replay; restore tests for lease_id/msg_id counters surviving snapshot
- Leader-served reads with read-index/Barrier (read-your-writes)
- Chaos/integration tests: kill leader mid-lease, mid-fire; assert no double-fire, no lost timer, no lost message

**Deliverable:** A 3-node cluster with automatic failover, quorum durability, snapshot/restore, log truncation, and verified lossless failover for leases and timers. Single-node dev mode unchanged (same code path).

## Phase 3 — Programmable policy hot-reload: Replace the hard-coded DRR mechanism's eligibility signal with the programmable, hot-reloadable, sandboxed policy engine (CEL default + WASM/wazero first-class; optional Starlark), keeping the broker as the arithmetic authority.

**Scope:**
- Single-sourced frozen ABI schema (GroupView/LaneView/ConsumerView) driving every engine binding
- CEL SCORE engine: env whitelist, compile, EstimateCost gate, one map-expression over lane.groups -> []float64; CostLimit
- WASM/wazero engine (first-class): any-language SCORE/DECIDE modules implementing the pure ABI; precompiled module cache; hard linear-memory cap + fuel/CPU bound + crash containment; host-call marshalling of the snapshot
- Optional Starlark DECIDE tier: SetMaxExecutionSteps + watchdog Cancel + frozen globals + recover()
- LanePolicy atomic.Pointer hot-reload swap; compile cache per (lane, policy_version)
- SetPolicy/GetPolicy/ValidatePolicy control RPCs; validation pipeline (parse/type-check/cost/smoke-eval) BEFORE FSM
- Mechanisms in Go (DRR/strict-priority/WFQ/lottery) consuming policy scores; built-in policy presets as example sources
- Runtime fault path: NaN/Inf/panic/budget/trap -> silent DRR fallback (used_fallback flag) + loud alert + policy_faults_total
- Quarantine breaker: K faults in a window -> pin lane to DRR (recorded in FSM config) until a new version is submitted
- Recompile from pol:<lane> on leadership change before first tick
- Tests: hot-reload swap mid-traffic, faulting policy degrades not stalls, quarantine, WFQ/strict/lottery fidelity, a WASM module per supported source language

**Deliverable:** Operators can install/validate/hot-reload per-lane policies (CEL expression, or a WASM module in any language) with no restart; faults fall back to DRR and auto-quarantine; built-ins ship as editable examples in the same mechanism.

## Phase 4 — Scheduling, async, hooks, SDK: Land the remaining first-class capabilities (cron, complete-by-token, singleton, group lifecycle, rate-limit/pause hooks) and the full Python SDK + observability.

**Scope:**
- Cron: ScheduleCron/ListCron/DeleteCron/PauseCron; FireCron exactly-once (cfence) with coalesce + misfire-grace; next-N introspection. Native scheduling replaces reaching for an external cron/scheduler sidecar.
- SingletonLease: Acquire/Renew/Release with fencing token (replaces an external lock, e.g. a Redis SET NX EX lease)
- Complete-by-token: IssueToken, in-stream Complete + unary CompleteByToken; lookup by stored hash; max-lease-lifetime backstop. This handles async/out-of-band completion natively, replacing an external async-completion side-table + polling worker.
- Group lifecycle control RPCs: SetGroupConfig, Pause/Resume/Cancel/Purge/Reap(teardown-across-lanes); idle reap sweep
- Lane rate_limit token bucket + dequeue-pause / per-key rate-limit hooks; broker-pushed PAUSE_LANE/RESUME control frames + paused-on-resubscribe signal
- PublishBatch (atomic + best-effort); dedup_key producer-idempotency window; protovalidate size caps on payload/headers
- Python SDK: Publisher + Worker(process()->ack/nack/complete, credit=1 serial — i.e. prefetch=1, a strictly-serial consumer — auto-extend, PAUSE handling, SIGTERM drain, leader-follow reconnect via tenacity); SingletonLease/Cron/Control helpers
- Observability: Prometheus metrics, GetStats/DescribeCluster/Health + grpc.health.v1
- End-to-end validation against representative workloads: cross-group fairness (500-vs-20), async completion via complete-by-token (e.g. a rate-limited external API fan-out whose results arrive out-of-band), cron + singleton scheduling, and dead-lettering modeled as a lane (dlq-as-lane)

**Deliverable:** Feature-complete generic broker: programmable fairness, at-least-once delivery, delayed+cron submission, complete-by-token, group lifecycle + hooks, full Python SDK, and observability. A single system that subsumes what would otherwise require a queue broker plus a bolt-on, external fairness/scheduling layer plus an external async-completion side-table + polling worker plus an external cron/scheduler sidecar — and it avoids the failure class where fairness/scheduling state kept in a SEPARATE cache is lost on a cache flush or restart while the messages remain queued, since all such state lives in the replicated FSM alongside the messages.

## Phase 5 (deferred) — Multi-raft / hot-lane scaling: Scale a hot lane independently if (and only if) single-group commit throughput becomes the ceiling, without changing the FSM, keyspace, command set, or policy engine.

**Scope:**
- Re-host the FSM under lni/dragonboat as N state machines (RaftGroupID = hash(lane)) behind the unchanged Apply/keyspace seam
- Per-lane leader routing; make NotLeader redirect per-lane (detail already forward-compatible)
- Resolve cross-lane atomicity for TeardownGroup (saga/2PC across groups, or accept eventual per-lane teardown)
- Chronos horizon/paging refinements for very large far-future timer sets; FireBatch backpressure vs normal publishes
- Load tests proving a hot lane scales without disturbing other lanes

**Deliverable:** Per-lane Raft groups for hot lanes, gated on demonstrated need; everything below the FSM/keyspace seam is unchanged. This phase is a contingency, not a commitment.
