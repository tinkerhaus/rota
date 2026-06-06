# Rota — Design

> A generic, high-performance, robust message broker in Go (1.26) whose defining feature is a **programmable, broker-native fair scheduler**. Rota contains **zero business logic**. Its only vocabulary is **Lane, Group, Message, Lease, Policy, Consumer, Cron, Singleton, DeadLetter**. It does not know what a "job", "tenant", "batch", "rate-limited fan-out", or any other domain concept is — those are the consumer's concepts, carried opaquely in `payload` bytes and a `headers` map.

---

## 1. Overview

Rota is a single statically-linked Go binary that collapses four jobs commonly stitched together from separate systems — a message queue, a bolt-on external fairness/scheduling layer, an external async-completion side-table plus polling worker, and an external cron/scheduler sidecar — into one broker. It runs as a 3-node (or 5-node) HA cluster with quorum writes and automatic leader failover, with a single-node (single-voter) mode for local dev. There are **no external dependencies**: no Redis, no Postgres, no Kafka, no ZooKeeper/etcd. State lives in one embedded LSM store (cockroachdb/pebble) replicated by one embedded consensus engine (hashicorp/raft).

**The core thesis.** Common message brokers have no cross-group fair delivery (RabbitMQ, for instance, has no native cross-group fair delivery). The unit of fairness is the **Group**. Within a **Lane**, messages are partitioned by an opaque `group_id`, and must be delivered to competing consumers in a fair order decided by a **Policy**, so no group is fully drained before others get serviced (noisy-neighbour isolation). A 500-message group must not head-of-line-block a 20-message group.

**Three architectural pillars hold the whole design together:**

1. **One durable system of record, one atomic batch.** Messages *and* scheduling/coordination state (group knobs, deficits, leases, timers, cron, singletons, dead letters) live in one Pebble keyspace. The Raft log/stable-store live in the *same* Pebble under a reserved prefix, so there is exactly one fsync domain and one crash-recovery story. Every Raft `Apply` is one atomic Pebble `WriteBatch` committed with `Sync`.

2. **Leader decides, followers apply the outcome.** Only the Raft leader evaluates the policy and only the leader's clock decides when a timer is due. The leader appends the *decision* (a `LeaseDecision` carrying the resolved assignment + fairness-state mutations) or the *fire* (`FireTimer`/`FireCron`) to the Raft log. Followers apply outcomes; they never run policy code and never fire on their own clock. This relaxes cross-node bit-determinism for the policy engine (it may use floats, map order, a PRNG) while keeping the FSM bit-deterministic — because the FSM contains zero policy logic.

3. **Publishing implies schedulable.** A Group is not a physical queue and has no register step. A Group is schedulable iff it has ≥1 leasable message in a Lane and is not paused. Membership is *derived from message presence*, so a retry-republish to a drained Group can never be invisible, and there is no second store to fall out of sync. Leasing from an empty/absent Group is a benign empty result, never a fatal/session-killing error.

**Performance posture.** The broker will not be the bottleneck: consumer tasks typically run seconds-to-minutes, and cardinality scales with concurrent-group count, not QPS. We optimize for correctness, robustness, fairness fidelity, and operational simplicity over raw throughput — but the FSM, keyspace, and command set are lane-scoped so a hot lane can later get its own Raft group (multi-raft) with no rewrite.

---

## 2. Goals & Non-Goals

### Goals
- Broker-native, **programmable** fair scheduling per lane (DRR default; strict-priority, WFQ, lottery built-ins; custom policies), **hot-reloadable** without restart.
- At-least-once delivery via SQS-style lease/visibility-timeout with a broker-owned per-message attempt counter, native bounded retry, and native DLQ carrying arbitrary failure-metadata headers.
- Two-outcome negative-ack: no-penalty requeue (does not increment attempt; optional redelivery delay) and terminal dead-letter.
- **Delayed publish** (`not_before`) and **scheduled/recurring publish** (generic cron) as first-class capabilities, fired exactly-once cluster-wide, surviving restart, with coalesce/misfire-grace.
- **Complete-by-token**: hold a leased message in-flight (long, extendable visibility) while a consumer hands work to an external system, then complete/fail it later by an external id — from any process, with no consumer-side table.
- High-cardinality, ephemeral groups (thousands, churning), auto-reaped when drained+idle; pause/resume/cancel/purge a single group in place; one-call teardown across all lanes.
- gRPC/HTTP-2 transport with a bidirectional streaming **Work** RPC + unary control plane; thin generic Python & TypeScript SDKs.
- HA: 3/5-node Raft cluster, quorum-acked durability, automatic failover; single-node dev mode on the same code path.

### Non-Goals (explicitly NOT built)
- Replayable log / stream / event-sourcing / seek. This is a work-queue, not Kafka: unlike Kafka, there is no replay log. The durable store exists for crash-recovery + fairness state, **never** for replay.
- Broker-level exactly-once delivery. Consumers must be idempotent; that is their job.
- Strict FIFO / total order within a group. Only cross-group fairness is ordered. (Within a group, key order is a stable *scan hint*, not a guarantee.)
- Large/blob payloads. Messages are tiny (ids + flags, KB-scale).
- Any business/domain concept.
- Owning a consumer's credential pools or circuit-breaker **state**. Rota offers only generic **dequeue-pause** + per-key **rate-limit hooks**.

---

## 3. Data Model (the nouns)

Vocabulary is strictly domain-neutral. Every noun below maps cleanly onto common workloads — multi-tenant job queues, batch processing, rate-limited API fan-out — without the broker ever knowing which.

### 3.1 Lane — the unit of scheduling configuration and isolation
Groups in one Lane never affect scheduling in another.

| Field | Type | Notes |
|---|---|---|
| `name` | string (identity) | Opaque. Dotted names are a convention, never parsed. |
| `policy_binding` | PolicyBinding | The active policy + version (see §5). |
| `priority_mode` / mechanism | enum | `WEIGHTED` (DRR/WFQ) / `STRICT_PRIORITY` / `LOTTERY` — what fairness *means* here. |
| `quantum` | double | DRR quantum (mechanism config). |
| `rate_limit` | RateLimit? | Optional broker-side lane-wide token bucket {rate, burst}. |
| `visibility_default` | duration | Default lease visibility timeout. |
| `max_attempts_default` | int | Default bounded-retry ceiling before auto-DLQ. |
| `penalise_expiry` | bool | If true, a lease *timeout* increments attempt; default false (timeout = no-penalty requeue). |
| `dlq_lane` | string? | Lane to dead-letter into; null ⇒ system DeadLetter store. |
| `state` | enum | `ACTIVE` / `QUIESCED` (drain-only). |

**Lifecycle:** created lazily on first publish, or declared by the control plane with a non-default policy. **Invariants:** policy evaluated only on the leader; hot-reloadable; config changes take effect on the next decision and never retroactively shorten an outstanding lease.

### 3.2 Group — the unit of fairness
Implied by publishing a message tagged with an opaque `group_id` (e.g. a job, tenant, session, or batch id). **There is no `RegisterGroup` RPC.**

| Field | Type | Notes |
|---|---|---|
| `(lane, group_id)` | (string, opaque) | Composite identity. Same `group_id` in two lanes ⇒ two independent groups. |
| `weight` | double (default 1.0) | Scheduling **frequency** (turns per cycle). Does NOT change messages-per-turn. Live-mutable. `≤0` ⇒ treated as paused. |
| `batch_size` | uint32 (default 1) | Messages leased per turn before rotation. Live-mutable. |
| `paused` | bool | Policy skips it; weight preserved. Live-mutable. |
| `deficit` | double | DRR deficit counter (broker-maintained; fed to policy, mutated by broker). |
| `virtual_time` | double | WFQ virtual finish time (broker-maintained). |
| `ready_count` / `inflight_count` / `total_count` | uint64 | Broker-maintained derived counters. |
| `next_seq` | uint64 | Next msg seq within group. |
| `last_served_seq` / `last_served_ts` | int64 | When last served (for ranking). |
| `last_activity_ms` | uint64 | For idle reap. |

**Lifecycle:** `(absent) → ACTIVE → PAUSED ↔ ACTIVE → IDLE → REAPED`. A group is `ACTIVE` the instant its first leasable message lands (in the same atomic batch). When `ready_count==0 && inflight_count==0` for `idle_ttl`, a committed `ReapGroup` GCs the knob row; a later publish re-creates it (re-supply `weight` on the re-creating publish to stay weight-stable across reap). pause/resume flips `paused` (weight preserved, in-flight unaffected). cancel/purge atomically drops leasable messages; teardown removes the group across every lane in one batch.

**Invariants:** weight/batch_size/paused are stored in the same Raft-replicated store as messages (no separate cache to flush, no out-of-band reconciliation step). Membership is *derived* — never a stored authoritative registry. Leasing an empty/absent group is a benign empty result.

### 3.3 Message — the unit of work (tiny; KB-scale)

| Field | Type | Notes |
|---|---|---|
| `msg_id` | uint64 (per-group seq, leader-assigned in Apply) | Stable across attempts; key-orders FIFO-ish within group (scan hint, not a guarantee). |
| `lane` / `group_id` | refs | `group_id` required; "default group" is `group_id=""`. |
| `payload` | bytes | Opaque. |
| `headers` | map<string,string> | App metadata; surfaced on delivery, carried into DLQ. |
| `not_before_ms` | uint64 | Delayed publish: not leasable until T. Same mechanism as retry-backoff and visibility. |
| `ttl_ms` | uint64? | Auto-dropped or dead-lettered (lane-configurable) if not delivered by then. |
| `attempt` | uint32 (**broker-owned**) | Incremented on penalty redelivery. Publishers cannot set it. |
| `max_attempts` | uint32 | 0 ⇒ lane default. |
| `enqueue_ms` | uint64 | Broker-set. |
| `state` | enum | `READY` / `DELAYED` / `LEASED` / `DEAD` / `DONE`. |
| `epoch` | uint32 | Bumped on every state change; invalidates stale timers/leases (idempotency linchpin). |
| `cur_lease` | uint64 | Lease id while LEASED. |

### 3.4 Lease — a time-bounded exclusive grant (SQS-style)

| Field | Type | Notes |
|---|---|---|
| `lease_id` | uint64 (leader-assigned) | Identity. |
| `msg_id` / `lane` / `group_id` | refs | What is leased. |
| `consumer_id` | string | Holder (for fencing/drain). |
| `deadline_ms` | uint64 | Visibility expiry; mirrored as a `LEASE_DEADLINE` timer. |
| `attempt_at_lease` | uint32 | Snapshot for the consumer. |
| `epoch` | uint32 | Must match message epoch; any stale fire/ack/nack is a benign no-op. |
| `extend_count` | uint32 | Extensions so far. |
| `completion_token` | bytes? | Set ⇒ complete-by-token mode (see §7). |
| `granted_ms` | uint64 | Lease grant time. |

**Negative-ack is two-outcome:** (a) **no-penalty requeue** — `attempt` unchanged, optional redelivery delay (shutdown, rate-limit, circuit-open, paused-group race); (b) **terminal dead-letter** — message → DeadLetter with failure headers. A pure visibility *timeout* defaults to no-penalty requeue (or penalty if `penalise_expiry`).

### 3.5 Policy — see §5. CronSchedule — see §6.4. SingletonLease / DeadLetter — see §6.5 / §8.

### 3.6 Configurability matrix (per-scope split, by cardinality + lifetime + owner)

| Knob | Scope | Owner | Cardinality / churn | Read when |
|---|---|---|---|---|
| policy + policy_config, mechanism, quantum | **Lane** | operator | low, hot-reloaded | every decision |
| rate_limit (lane-wide) | **Lane** | operator | low | every decision |
| visibility_default, max_attempts_default, penalise_expiry | **Lane** | operator | low | on lease / on nack / on expiry |
| **weight** (frequency) | **Group** | app | high, churning | every decision (policy input) |
| **batch_size** (msgs/turn) | **Group** | app | high, churning | every decision (policy input) |
| **paused** | **Group** | app | high, churning | every decision (skip) |
| **delay / not_before**, **ttl** | **Message** | app | per-item | eligibility / expiry |
| **attempt** | **Message** | **broker** | per-item | retry / DLQ decision |
| **headers** | **Message** | app | per-item | on delivery / DLQ |

---

## 4. Storage + Consensus

### 4.1 Engine topology (one Pebble, one Raft group — for now)

```
                 ┌─────────────────────── rota-node ───────────────────────┐
   gRPC Work ───▶│  gRPC server  ──proposes──▶  Raft (hashicorp/raft)       │
   gRPC Ctrl ───▶│      ▲                          │ commit (quorum+fsync)   │
                 │      │NotLeader redirect         ▼                         │
                 │  leader-only        FSM.Apply(cmd) ── one Pebble batch ──▶ │
                 │  policy eval        snapshot/restore ──── Pebble checkpoint│
                 │      │                                                     │
                 │  ┌───┴───────────────────── Pebble (single LSM) ────────┐ │
                 │  │  application keyspace (msgs, groups, leases, timers…) │ │
                 │  │  raft log + stable store (HardState) under 0xFE/0xFF  │ │
                 │  └───────────────────────────────────────────────────────┘
                 └──────────────────────────────────────────────────────────┘
```

One Pebble DB holds both the application keyspace and the Raft log/stable store (custom `raft.LogStore`/`raft.StableStore` over Pebble). The FSM's only side effect is a Pebble `*Batch` committed with `Sync`. The Raft snapshot is a Pebble `Checkpoint` (hard-linked, cheap). The FSM is a host-agnostic `Apply([]byte) -> []byte`; re-hosting it under `lni/dragonboat` per-lane (multi-raft) is a hosting swap, not a rewrite, because every key is lane-prefixed.

### 4.2 Pebble keyspace

Keys are big-endian-ordered byte strings. The first byte is a **table tag** so each logical table is a contiguous prefix (cheap iteration; cheap range-delete on reap/purge/truncation). `LP(x)` = `u16be(len(x)) ++ x`; `u64be` = fixed big-endian; `ts` = `u64be` unix-millis (sortable). Values are protobuf (the *same* schema as the gRPC transport — one schema for wire + disk).

```
tag 0x01  MESSAGE     msg:<lane>:<group>:<msg_id u64be>     -> Message
tag 0x02  GROUPMETA   grp:<lane>:<group>                     -> GroupMeta   (knobs + deficit + counters)
tag 0x03  LEASE       lease:<lease_id u64be>                 -> Lease
tag 0x04  TIMEINDEX   tidx:<due_ts u64be>:<kind u8>:<ref>    -> TimerRef    (the unified due-time wheel)
tag 0x05  DLQ         dlq:<lane>:<group>:<dl_ts>:<msg_id>    -> DeadLetter
tag 0x06  POLICY      pol:<lane>                             -> PolicyBinding (active + version + source)
tag 0x07  CRON        cron:<cron_id>                         -> CronSpec
tag 0x08  SINGLETON   sing:<name>                            -> SingletonLease
tag 0x09  TOKENINDEX  tok:<token_hash [32]byte>             -> <lease_id>   (complete-by-token)
tag 0x0A  GROUPINDEX  gidx:<group>:<lane>                    -> ()          (reverse: group -> lanes, teardown)
tag 0x0B  CRONFENCE   cfence:<cron_id>                       -> <last_fired_slot u64be> (cron exactly-once)
tag 0x00  META        meta:applied_index / meta:cluster_id / meta:next_lease_id / meta:schema_ver
tag 0xFE  RAFTLOG     rlog:<index u64be>                     -> raft log entry bytes
tag 0xFF  RAFTSTABLE  rkv:<key>                              -> raft StableStore values (HardState…)
```

Notes:
- **`msg_id`** is a per-group monotonic seq from `GroupMeta.next_seq`, assigned by the leader inside the Publish Apply (deterministic; part of the committed result). Big-endian seq gives a stable FIFO-ish scan order for the policy/lease to read the head — not a FIFO guarantee.
- **`lease_id`** is a global FSM counter in `meta:next_lease_id` (survives snapshot/restore — covered in restore tests; a reset would collide ids).
- **GROUPMETA** holds *both* the live knobs and the fairness counters (`deficit`, `virtual_time`, `ready_count`, etc.) so the policy reads them as inputs and the broker mutates them via committed commands. It is **not** a cache — it is quorum-written, which is what kills the failure class where fairness/scheduling state kept in a SEPARATE cache is lost on a cache flush or restart while the messages remain queued.

### 4.3 The unified due-time index (the "Chronos" subsystem)

**ONE ordered structure powers all five timed behaviours**: (a) delayed publish, (b) visibility-timeout redelivery, (c) retry backoff, (d) cron, (e) lease-deadline sweeps. They are the same primitive — "a thing that becomes actionable at time T, durably, exactly-once on the cluster" — viewed five ways.

```
tidx:<due_ts>:<kind>:<ref>     kind = 0x01 READY_AT      ref = <lane>:<group>:<msg_id>   (delayed / retry -> READY)
                               kind = 0x02 LEASE_DEADLINE ref = <lease_id>                (visibility expiry -> requeue)
                               kind = 0x03 CRON_DUE       ref = <cron_id>                 (cron fire -> publish + reschedule)
```

Because `due_ts` is the first sort component, "everything due now" is a single bounded forward iterator `[tidx:0 .. tidx:now]`. That ordered scan **is** the durable timing wheel; an in-memory hierarchical timing wheel (leader-only) is a pure latency accelerator over it. A lost/empty/early/late wheel is a latency event, never a correctness event:

- On leader election, the new leader scans `tidx:` (within a sliding horizon; see roadmap risk) to rebuild its wheel. No timer lived only in RAM.
- Every `FireTimer`/`FireCron` Apply **re-validates** `timer.due_at <= fire_at` against the persisted record. A wheel that fired early is rejected and left armed; firing is idempotent and monotone.
- **Followers never scan `tidx:` to fire anything.** They only learn an entry fired by applying the leader's committed fire command, which deletes the row.

### 4.4 Layering: truth vs accelerator, and why the leader stamps time

`FSM.Apply` must be deterministic and identical on every peer, so it must never call `time.Now()`. Instead:

1. The leader's wheel says timer T is due.
2. The leader reads its clock once: `fire_at = leaderNow`.
3. The leader proposes `FireTimer{kind, ref, fire_at, term, seq}` (or `FireCron`).
4. Raft commits it; **all nodes apply the same `fire_at`**.
5. The FSM uses the committed `fire_at` for every downstream time decision in that apply — next retry's `ready_at = fire_at + backoff`, cron's `last_fire = fire_at`, etc.

This mirrors the policy rule (leader decides, followers apply the outcome) and is what makes visibility-timeout, retry-backoff, delayed-publish, and cron one mechanism.

### 4.5 Raft FSM command set

Every command is a protobuf `Command{ oneof }` proposed by the leader, applied identically by all nodes. `Apply` builds ONE Pebble `*Batch`, sets `meta:applied_index`, commits with `Sync`, and returns a typed result to the leader's waiting RPC. No command runs policy code, opens sockets, or reads the wall clock for branching; any "now" needed is carried in `LeaderNow`/`fire_at` stamped by the leader.

| Command | Mutates atomically (one batch) |
|---|---|
| **Publish**{lane,group,payload,headers,not_before,ttl,max_attempts,weight?,batch_size?,dedup_key?} | upsert GroupMeta (create if absent, ++next_seq, ++total_count, ++ready_count or stay DELAYED + `tidx READY_AT`); write Message; write `gidx`; bump last_activity. Returns assigned `msg_id`. dedup_key drops a re-accepted publish within a bounded window. |
| **LeaseDecision**{[(lane,group,msg_id,consumer_id,deadline,completion_token?)…], fairness mutations} | THE DECIDED ASSIGNMENT (leader already ran policy). Per item: msg READY→LEASED, ++epoch, write Lease, insert `tidx LEASE_DEADLINE@deadline`, --ready_count ++inflight_count; apply `new_deficit`/`new_virtual_time`/`new_turn`/`served_seq`/`served_ts`. Returns leased messages to stream. |
| **Ack**{lease_id, epoch} | epoch-checked. Delete Lease, its `tidx`, Message, token; --inflight_count --total_count; bump last_activity. No-op if stale. |
| **Nack**{lease_id, mode=NO_PENALTY\|RETRY\|DEAD_LETTER, delay_ms?, failure_meta?, epoch} | NO_PENALTY: msg→READY (attempt unchanged) or DELAYED+`tidx READY_AT` if delay. RETRY: attempt++; if `attempt>=max` → DLQ else READY/DELAYED with backoff. DEAD_LETTER: write `dlq:` with failure_meta+headers+attempt, delete msg. Always delete Lease + its `tidx`; ++epoch. |
| **Extend**{lease_id, new_deadline, epoch} | update Lease.deadline + extend_count; ++epoch; delete old `tidx LEASE_DEADLINE`, insert new. (Same path for async hold.) |
| **IssueToken**{lease_id} | generate opaque token (server ULID), store `token_hash` on Lease + `tok:<hash> -> lease_id`. Returns the secret token once. |
| **Complete**{token, outcome=SUCCESS\|FAILURE, result_meta?, delay?} | resolve via `tok:`; epoch-check; SUCCESS == Ack; FAILURE == RETRY-or-DEAD_LETTER per lane. Powers async complete-by-token with no consumer side-table. |
| **SetGroupConfig**{lane,group, weight?, batch_size?} | upsert GroupMeta knobs (implicit create; preserves weight). |
| **PauseGroup / ResumeGroup**{lane,group} | set/clear `paused` (weight preserved). |
| **CancelGroup**{lane,group} | drop READY/DELAYED messages + their `tidx`; leave in-flight leases to drain (or fence); --counts. |
| **PurgeGroup**{lane,group} | range-delete `msg:<lane>:<group>:` + its `tidx` refs + outstanding tokens; zero ready/delayed counts; keep GroupMeta. |
| **TeardownGroup**{group} | one call across ALL lanes: read `gidx:<group>:*`, per lane PurgeGroup + delete GroupMeta/cron/dlq, delete `gidx`. Returns affected lanes. |
| **ReapGroup**{lane,group} | idle-reap: assert total_count==0; delete GroupMeta, `gidx`. Leader-proposed from the reap sweep (committed, not local). |
| **SetPolicy**{lane, engine, mode, mechanism, source, params, version} | upsert `pol:<lane>`, bump version. Hot reload (see §5.7). Followers persist source so a future leader can compile. |
| **FireTimer**{kind, ref, fire_at} | leader-proposed when the `tidx` sweep finds a due entry. READY_AT: msg DELAYED→READY ++ready_count, delete `tidx`. LEASE_DEADLINE: requeue per lane (no-penalty default / penalty if `penalise_expiry`), delete Lease + `tidx`. Idempotent: no-op if state already moved (handles failover double-propose). |
| **Cron**{Schedule\|Update\|Delete\|Pause} | upsert/mutate CronSpec, compute `next_fire` (leader-stamped), insert/replace `tidx CRON_DUE`. |
| **FireCron**{cron_id, fire_at} | leader-proposed on cron due: publish the cron's message (Publish path, same batch), recompute `next_fire` (misfire-grace + coalesce), update CronSpec + `cfence`, replace `tidx CRON_DUE`. Exactly-once via committed-log linearization + `cfence` dedupe on the fire slot. |
| **Singleton**{Acquire\|Renew\|Release}{name,holder,ttl} | CAS on `sing:<name>` with monotone `fence`; expiry via `tidx` or acquire-time check. |
| **NoOp / Barrier** | empty batch that advances applied_index (read-freshness / post-election flush). |

**Idempotency discipline.** FireTimer/FireCron/LeaseDecision are written to be safe if re-proposed after a leadership change: each checks current state (via the `epoch` and the row's existence) and no-ops if already applied. This is what makes "leader proposes timers/decisions" safe across failover, with no double-fire and no lost timer.

### 4.6 Lease lifecycle state machine

```
                       publish(not_before>now)
                ┌──────────────────────────────────┐
                ▼                                   │
            [DELAYED] --READY_AT fires (sweep)--> [READY] <───────────┐
                                                    │                 │
                                       leader policy decides lease    │
                                                    │ (LeaseDecision) │
                                                    ▼                 │
                                            [LEASED(deadline)]        │
                                                    │                 │
   ┌────────────────────────────────────────────┬──┴───────────┐     │
   │ Ack / Complete(SUCCESS)  Nack NO_PENALTY    │ Nack RETRY    │     │
   ▼                          (attempt unchanged)│ (attempt++)   │     │
 [DONE/deleted]   delay? -> [DELAYED] | -> [READY]   attempt<max? ─────┘
                                                  yes: backoff>0? -> DELAYED|READY
                                                  no : -> DEAD_LETTER -> [DEAD] -> dlq/

   LEASE_DEADLINE fires while still LEASED:  default = Nack NO_PENALTY (no attempt++),
        or Nack RETRY (attempt++) if lane.penalise_expiry.
```

`epoch` is the linchpin: every state change bumps `Message.epoch`; every timer/lease carries the epoch it was created at; any fire/ack/nack whose epoch ≠ current is a benign no-op. So all timer firing is idempotent and immune to fired-twice / fired-after-the-message-already-moved.

### 4.7 Determinism boundary (the heart of the design)

**(A) Time fires through committed entries, never local clocks** (§4.4). Failover safety: the new leader re-derives due entries from `tidx:` (durable). If the old leader already committed a fire, the row is gone (no re-fire); if it committed the fire but died before deleting, the idempotent Apply makes the re-propose a no-op.

**(B) Leader evaluates policy, then replicates the decision** (§5). The policy reads committed GroupMeta/counters, produces a concrete assignment, and that assignment (not "run the policy") is the `LeaseDecision` log entry. The policy never needs cross-node bit-determinism. It MUST still be sandboxed, resource-bounded, and crash-safe: a panicking/timing-out policy fails the decision (falls back to built-in DRR — §5.9), never the node. On leadership change, the new leader compiles the policy from `pol:<lane>` and rebuilds its scheduling projection by scanning GroupMeta; the deficits/weights it reads are the committed ones, so DRR/WFQ resume coherently.

### 4.8 Snapshotting, truncation, recovery
- **Snapshot = Pebble `Checkpoint(dir)`** — a consistent, hard-linked point-in-time at `applied_index`, cheap to materialize and transfer (SST ingest on restore). All state is one keyspace, so the snapshot is the whole application keyspace as of `applied_index`.
- **Truncation:** after a snapshot at index I commits, `LogStore.DeleteRange(0, I)` range-deletes `rlog:` ≤ I — same engine, one compaction reclaims both data and log. Tune compaction so log-prefix tombstones don't bloat the active log tail (benchmark vs raft-boltdb before committing to single-engine).
- **No replay store.** Snapshots exist for crash-recovery + catch-up, never for consumer replay. Size `SnapshotThreshold` modestly (8–16k entries): the dataset is small and churny, so frequent cheap snapshots keep the log short and recovery fast.
- **Crash recovery:** open Pebble (WAL replays the last batch), read `meta:applied_index`, hand raft the snapshot at-or-below it, replay `rlog:` > applied_index through Apply. Because `applied_index` is written *in the same atomic batch* as the data, a torn restart cannot double-apply.

### 4.9 Durability (fsync / quorum-before-ack)
- A client RPC returns success **only after** the Raft entry is committed (replicated to and fsync'd by a majority of voters, then applied by the leader). We wait on the `raft.Apply(cmd, timeout).Response()` future. No fire-and-forget ack.
- The Pebble-backed LogStore writes with `Sync:true` (follower durable before it counts toward quorum); the FSM data batch commits with `Sync:true`. Log and data share one fsync domain — this removes the classic "log durable but FSM lost" and "FSM durable but log torn" inconsistency classes.
- Per-entry fsync is accepted: the broker is not throughput-bound, correctness wins. We never ack before commit, so we never lose an accepted message; we are at-least-once, never at-most-once.

### 4.10 Membership, leader-routing, startup
- **Single-node dev:** `BootstrapCluster` with self as the only voter; leader immediately, quorum=1, same code path as prod.
- **Cluster bootstrap:** one node (lowest node-id) runs `BootstrapCluster` with the initial voter set (3/5); others start un-bootstrapped and are added via `AddVoter` (single-server changes; a new node receives a Pebble checkpoint, then tails the log). `AddNonvoter` for read replicas / lagging catch-up.
- **NotLeader redirect:** all writes are leader-only. A follower returns gRPC `FAILED_PRECONDITION` + a `rota.v1.NotLeader{leader_addr, leader_id}` detail (or, in-stream, `WorkServerMsg.error{NOT_LEADER, leader_addr}`). The Work stream is pinned to the leader; on leader change the stream breaks, the client reconnects, re-advertises credit, and any unacked leases hit their visibility timeout and are redelivered (safe). Reads default to leader-served (read-index/Barrier) for read-your-writes; best-effort follower reads carry a staleness hint.
- **Operational simplicity:** one Pebble dir per node, no external systems. Add a node = copy binary + config + empty data dir + `AddVoter`. Backups = `pebble.Checkpoint`.

---

## 5. Programmable Fair-Scheduler (Policy-as-Code Engine)

### 5.1 Where the policy sits in the lease path
```
consumer demand (credit) on Work stream
        │
        ▼
LEADER scheduling tick for lane L
  1. read FSM scheduling state for L (active group set + per-group knobs/counters, lane turn/quantum, consumer credit)
  2. build an immutable PolicyInput (reused buffers)
  3. EVALUATE POLICY  ──►  per-group score (SCORE mode) OR {group_id, take} (DECIDE mode)
  4. broker applies the fairness MECHANISM around the policy output
     (DRR deficit math / strict ordering / WFQ virtual-time), picks winner group + count,
     decrements credit, advances deficit/turn
  5. append ONE Raft entry: LeaseDecision{ lease(group,msg_ids,consumer,deadline),
                                           deficit/turn/last_served mutations }
        │ replicate
        ▼
FOLLOWERS apply the OUTCOME deterministically. They never run the policy.
```

The policy is a **leaf**: it consumes a read-only snapshot and returns numbers. It owns no state, performs no I/O, sees no clock except `lane.now_ms` we hand it, and cannot mutate the FSM. All mutation is done by Go code in step 4 and is what gets replicated. This is the property that makes leader-only evaluation safe: a new leader rebuilds identical scheduling state from the log without re-running any historical policy.

### 5.2 The contract: pure scoring, broker-owned bookkeeping (RESOLVED)

> **Resolution.** Two engine models were on the table: (1) CEL "pure scoring, broker owns DRR/WFQ math" and (2) a WASM `Policy.Decide()` returning concrete assignments. **We adopt the pure-function contract as the canonical ABI for every engine** (SCORE returns scores, DECIDE returns `{group_id, take}`), and we ship **WASM (wazero) as a first-class engine** alongside CEL — WASM gives any-language authoring with hard memory/CPU bounds, but it implements the *same pure ABI*; it does not get to mutate scheduler state. Two load-bearing reasons keep the contract pure regardless of engine:
>
> - **Reconstructability.** Deficit counters, turn clocks, `virtual_time`, `last_served` are FSM state and are Raft-replicated. If they lived inside the engine's heap (a mutable WASM/Lua/Starlark global), a leader failover would lose them and they would be unreconstructable from the log. So they MUST live in the FSM, and the policy reads them as inputs. A "fuller program that mutates scheduler state" is therefore unsafe by construction.
> - **Sandbox simplicity.** A function that only reads a record and returns floats needs no loops, no allocation budget, no mutation. That is exactly CEL's design center.
>
> The broker remains the **arithmetic authority** (DRR/WFQ add/subtract done in Go and replicated); the policy is an **advisory ranking opinion**. A buggy/malicious policy can corrupt only its own ranking, bounded by clamps — never fairness state. This keeps both ABI shapes (below) pure and keeps the FSM deterministic.

**Two ABI shapes (the lane config picks one):**
- **`SCORE` mode (default, CEL-friendly):** the policy returns one `float64` score per group (higher = serve sooner). Enough for strict-priority, lottery weighting, age-based starvation breaks, and the *priority signal* fed into DRR/WFQ. Evaluated as **one CEL map-expression** over `lane.groups` (`lane.groups.map(g, <score_expr>)`) so a tick is a single `Program.Eval` returning a `[]float64`.
- **`DECIDE` mode (advanced):** the policy receives the whole lane snapshot and returns `{group_id, take}`. The broker still owns deficit accounting; DECIDE only relocates the *selection* into the policy (for genuinely cross-group strategies). `take` is clamped to `[0, min(batch_size, backlog, credit)]`.

Both shapes are pure: no mutation of inputs, no persistence between calls.

### 5.3 The exact Policy ABI (frozen, versioned schema)

**`group`** (the candidate in SCORE; an element of `lane.groups` in DECIDE):
| field | type | meaning |
|---|---|---|
| `group.id` | string | opaque group id |
| `group.weight` | double | live weight knob |
| `group.batch_size` | int | live max-take-per-turn |
| `group.backlog` | int | leasable (not delayed/in-flight) count |
| `group.in_flight` | int | leased-but-not-acked count |
| `group.deficit` | double | DRR deficit (broker-maintained, fed in) |
| `group.virtual_time` | double | WFQ virtual finish time (fed in) |
| `group.last_served_seq` | int | lane turn when last served |
| `group.last_served_ts` | int | epoch ms when last served (leader clock) |
| `group.age_of_oldest_ms` | int | now − enqueue of oldest leasable msg |
| `group.paused` | bool | broker excludes regardless of score (pre-filtered) |
| `group.attrs` | map(string,dyn) | arbitrary publisher/operator attributes (size-bounded at publish) |

**`lane`**: `id`, `groups` (list, pre-filtered: paused & empty removed), `total_weight`, `turn`, `quantum`, `now_ms` (leader clock, passed in; never read by the engine).
**`consumer`**: `credit` (remaining in-flight budget), `attrs` (capability tags).

**Output:** SCORE ⇒ `double` per group (NaN/Inf ⇒ −∞ / do-not-serve + counted as a fault). DECIDE ⇒ `{group_id, take}` (`group_id` must be in `lane.groups` or it's a fault).

### 5.4 Built-in policies (each a short example in the same mechanism)

**Deficit Round Robin (the sane default).** Mechanism = DRR (broker adds `quantum*weight` per round, serves while `deficit ≥ cost`, decrements). Policy expresses the eligibility/priority signal:
```cel
group.deficit + (group.age_of_oldest_ms / 1000.0) * 0.001
```
**Strict priority.** `double(int(group.attrs.priority)) * 1e9 + group.age_of_oldest_ms`
**WFQ.** `-group.virtual_time` (broker advances `virtual_time += cost/weight`; a fresh group is initialized to the lane's current min).
**Lottery.** `group.weight * (1.0 + double(group.id.hashCode() % 997) / 997.0)` (non-determinism is fine; only the leader evaluates and only the outcome is replicated).
**Advanced (Starlark, DECIDE) — tiered: drain priority-0 backlog first, else WFQ:**
```python
def policy(lane, consumer):
    hot = [g for g in lane.groups if int(g.attrs.get("priority", 0)) == 0 and g.backlog > 0]
    pool = hot if hot else lane.groups
    pick = min(pool, key = lambda g: g.virtual_time)
    take = min(pick.batch_size, pick.backlog, consumer.credit)
    return {"group_id": pick.id, "take": take}
```

### 5.5 Engine choice & sandbox (CEL default, WASM/wazero first-class, Starlark optional)
- **CEL (primary):** non-Turing-complete (no unbounded loop exists to hang), `CostLimit`/`CostTrackerLimit` caps runtime cost, `cel.Program` is stateless/thread-safe/cachable (perfect for the hot path and atomic-pointer hot-reload), `EstimateCost` for static validation, host-controlled function whitelist, microsecond eval. Sandboxing is structural, not bolted on.
- **Starlark (advanced):** `Thread.SetMaxExecutionSteps` + `Thread.Cancel` (CPU/step bounds), `Freeze()`d globals (purity across reused `Program.Init`), reusable compiled `Program`. No native memory cap, so we forbid unbounded comprehensions in validation, cap input list sizes, run under `recover()`, and treat OOM as bounded by the step limit. Acceptable because it is leader-only and side-effect-free.
- **WASM via wazero (first-class):** author policies in ANY language (Rust, Go/TinyGo, AssemblyScript, C) compiled to a WASM module that implements the pure SCORE/DECIDE ABI. wazero is pure-Go (keeps the single static binary), and gives the strongest sandbox of the three: a hard linear-memory cap, a fuel/CPU bound, and crash containment, instantiated from a precompiled module. The per-call instantiation + host-call marshalling cost is real but acceptable — evaluation is leader-only and the broker is not throughput-bound. A WASM module is still a *pure function*: it reads the snapshot and returns numbers; any state it keeps in its own linear memory is per-evaluation and never authoritative (fairness counters stay in the FSM, §5.2).
- **Starlark (optional middle tier):** a Python-like language with `Thread.SetMaxExecutionSteps` + `Thread.Cancel` (CPU/step bounds) and `Freeze()`d globals (purity across reused `Program.Init`). It occupies the niche between CEL one-liners and full WASM modules; since WASM now covers any-language rich logic, Starlark is optional and may be dropped if it earns no adoption. No native memory cap, so validation forbids unbounded comprehensions, caps input list sizes, and runs under `recover()`.
- Rejected: Lua (resource bounds bolt-on; mutable globals fight reconstructability), expr-lang (CEL-minus, no compensating advantage).

### 5.6 Hot path performance
SCORE mode is one CEL map-expression over `lane.groups`, so a tick is a single `Program.Eval` returning `[]float64`, linear in group count, microseconds for thousands of groups. The compiled program is cached per `(lane, policy_version)`; reload swaps an `atomic.Pointer[CompiledPolicy]`; an in-flight tick finishes on the old program, the next uses the new. Because eval is leader-only and the broker is not the bottleneck, there is ample headroom; we still bound it so a pathological policy cannot stall the tick loop.

### 5.7 Hot-reload
Policies are content-addressed (`sha256` of source) and versioned per lane. Submit → validate (§5.8) → compile → store `{version, engine, mode, mechanism, source, hash, params}` in the FSM via `SetPolicy` (replicated as **config**, not a hot-path entry) → leader compiles and publishes via `atomic.Pointer` swap. No restart, no drain. Followers store the source so a new leader can compile on promotion. Rollback = re-point to a previous version id.

### 5.8 Validation & versioning (before it can ever run on the hot path)
1. **Parse + type-check** against the frozen ABI schema (CEL `Env.Compile` / Starlark `syntax` + static checker). Reject unknown fields, wrong types, disallowed functions, disallowed Starlark constructs (no `while`, no recursion past a depth, no `load`/imports).
2. **Static cost estimate** (CEL `EstimateCost`) under the lane ceiling.
3. **Smoke-eval** against synthetic fixtures (empty lane, 1 group, N groups, paused, zero-backlog) in a throwaway sandbox at the production budget; must return finite scores / a valid `{group_id, take}` for every fixture.
4. Assign a monotonic `policy_version`. Old versions retained for rollback/audit. A submission that fails any step is rejected at the control-plane RPC (`INVALID_ARGUMENT` + diagnostics); it never reaches the FSM.

### 5.9 Safe fallback when a policy errors at runtime
All leader-local; none touches the replicated FSM except via normal scheduling entries.
1. **Per-call:** budget overrun, error, panic (`recover`), NaN/Inf, or invalid `{group_id, take}` ⇒ the tick **silently falls back to built-in DRR** using the same broker-owned deficits (which always exist). The lease still happens; fairness degrades gracefully, never stalls. `LeaseDecision.used_fallback=true`.
2. **Counters & alerts:** `policy_faults_total{lane,version,reason}` + structured log; the fault must be *loud* (alert, not just a counter) so operators aren't surprised that their policy is silently inert.
3. **Circuit breaker:** if a version faults more than `K` times in a window, the leader **auto-quarantines** it (pins the lane to DRR), records the quarantine in FSM config, and alerts. An operator must submit a new version to clear it.
4. **Leader-only blast radius:** a policy that wedges the engine can at worst trigger a leader step-down (watchdog → lose leadership) and clean failover to a node that applies the same fallback. A bad policy can never crash the cluster or lose messages.

### 5.10 What the policy explicitly cannot do
No I/O, no real clock, no FSM mutation, no message payloads, no persistence, no call-out, no effect on followers. It is a bounded, pure, hot-reloadable ranking opinion over broker-owned fairness state. Everything that must be correct and replicated stays in Go.

---

## 6. Unified Timers, Leases, Delayed/Cron (Chronos), Singleton

### 6.1 Delayed publish, retry backoff, visibility, expiry — one mechanism
All four are rows in `tidx:` (§4.3) that the leader turns into committed `FireTimer` commands (§4.4–4.5). A delayed message becomes leasable at the same logical point (same log index) on every replica. Backoff uses **deterministic jitter** seeded by `(msg_id, attempt)` (NOT an RNG) so the next `ready_at` computed in Apply is identical on all nodes:
```
backoff(attempt) = min(base * 2^min(attempt, maxShift) + detJitter(msg_id, attempt), maxBackoff)
ready_at = fire_at + backoff
```

### 6.2 ExtendVisibility (heartbeat) and the long-task case
`Extend{lease_id, new_ttl}` sets `deadline = fire_at + new_ttl`, ++extend_count, ++epoch (the OLD `LEASE_DEADLINE` timer becomes stale and is ignored when it fires), deletes the old and writes a fresh `tidx LEASE_DEADLINE`. Crucially, the transport does **not** require app-side heartbeats for *liveness* — HTTP/2 keepalive PINGs keep the stream alive during multi-minute tasks. `Extend` exists only to push the *visibility deadline* further out for genuinely long work. The SDK auto-extends at ~⅔ of the deadline while a handler runs.

### 6.3 Sweep / fire loop (leader-only)
```go
func (s *Chronos) fireDue(candidate []TimerRef) {
    now := s.clock.NowMs()                                   // ONE clock read
    it := s.db.NewIter(prefixRange("tidx:", u64(now)))       // [tidx:0 .. tidx:now]
    for it.First(); it.Valid() && len(batch) < MaxFireBatch; it.Next() {
        batch = append(batch, fireCmdFor(it.Key(), now))
    }
    for _, c := range batch { s.raft.Apply(c, timeout) }      // COMMIT; all nodes apply; idempotent
}
// FSM side (every node, deterministic, uses fire_at not time.Now()):
func (f *FSM) applyFireTimer(c FireTimerCmd) {
    tr := f.loadTimer(c.Kind, c.Ref); if tr == nil { return }            // already consumed -> no-op
    if tr.DueAt > c.FireAt { return }                                     // wheel too early -> leave armed
    switch c.Kind {
    case READY_AT:        f.fireReadyAt(tr, c.FireAt)        // (a) delayed / (c) retry  -> READY
    case LEASE_DEADLINE:  f.fireLeaseDeadline(tr, c.FireAt)  // (b) visibility / (e) expiry -> requeue
    case CRON_DUE:        f.fireCron(tr, c.FireAt)           // (d) scheduled -> publish + reschedule
    }
}
```
On `LeaderCh→true` the new leader runs `rebuildWheelFromPebble()` (scan `tidx:` within a sliding horizon, immediate-bucket past-due) plus a `safetyTick` Pebble scan as backstop. No timer lives only in RAM.

### 6.4 Cron (generic recurring publish)
A `CronSpec` says "publish THIS message envelope to THIS lane on schedule S" — never any domain-specific recurring action.
```protobuf
message CronSpec {
  string cron_id = 1; bytes lane = 2; bytes group = 3;
  bytes message_payload = 4; map<string,string> headers = 5;
  string schedule = 6; string timezone = 7;       // UTC crontab, robfig/cron Next() math
  uint64 next_fire_ms = 8; uint64 last_fire_ms = 9;
  int32 misfire_grace_ms = 10; bool coalesce = 11; bool paused = 12;
}
```
- **Exactly-once cluster-wide:** only the leader proposes the `FireCron`; Raft linearizes commits; `cfence:<cron_id>` holds the last-fired slot so a re-applied fire (post-failover) is a no-op. A single message is published per scheduled instant.
- **`fireCron`:** publishes the template via the normal Publish path in the same batch, sets `last_fire=fire_at`, computes `next_fire=Next(spec, fire_at)`, replaces `tidx CRON_DUE`. Self-perpetuating; survives restart because `next_fire` is durable. Schedule math is pure (called on the committed `fire_at`, never `time.Now()` inside Apply).
- **Coalesce + misfire-grace:** `FIRE_ONCE` (default) publishes once and jumps to the next future slot; `FIRE_ALL` publishes one per missed slot within `misfire_grace` (older dropped + logged), bounded/streamed so a long outage cannot flood or stall an apply.
- **Introspection:** `ListCron` returns the next N occurrences (pure, no mutation). pause removes the `tidx CRON_DUE` (keeps the record); resume recomputes `next_fire`.

### 6.5 SingletonLease (generic single-instance coordination)
Provides cluster-wide single-instance coordination as a Raft-backed primitive, so a workload that must run on exactly one node at a time needs no external lock (e.g. a Redis `SET NX EX` lease) and no "single-instance only" deployment constraint.
```protobuf
message SingletonLease { bytes name = 1; bytes holder = 2; uint64 deadline_ms = 3; uint64 fence = 4; }
```
`Acquire` is a committed CAS; at most one holder per `name` cluster-wide; `fence` is a strictly-increasing fencing token so a paused-then-resumed stale holder is detectably stale. Expiry via `tidx` or acquire-time check.

---

## 7. Complete-by-Token (async completion)

The generalised, broker-native replacement for an external async-completion side-table + polling worker. No business logic.

1. Consumer leases a message, hands work to an external system, and calls `IssueCompletionToken{lease_id}` (or sets `issue_token=true` at lease/publish, or supplies an `external_token`). The FSM stores `token_hash=sha256(token)` on the Lease and `tok:<token_hash> -> lease_id`, and returns the secret token once (lookup is by stored hash so **any** node can complete it — no HMAC re-derivation needed on failover).
2. The consumer typically `Extend`s to a long visibility and may then close its stream / die — the lease persists in the FSM, addressable by token. The `LEASE_DEADLINE` timer is the safety net: if nobody completes within visibility, the lease re-READYs the work (no penalty) for re-lease + re-submit.
3. Later — possibly from a **different process** (webhook handler, poller) — `Complete{token, SUCCESS|FAILURE}` arrives, in-stream or via the unary `CompleteByToken` control RPC. The FSM resolves `tok:` → Lease, **epoch-checks** (rejects completing an already-expired/re-leased token), then SUCCESS == Ack / FAILURE == RETRY-or-DEAD_LETTER per lane.

**Backstop:** a token that is never resolved must still dead-letter, so the lease carries a **max-lease-lifetime** (a hard ceiling independent of `Extend`) after which it dead-letters, preventing a wedged external job from pinning a group's `inflight_count` forever (which would keep the group out of IDLE/reap). On `PurgeGroup`/`TeardownGroup`, outstanding tokens for that group are purged in the same batch, so a late `CompleteByToken` after a cancel is a benign no-op.

> **At-least-once reality (document for consumers):** if a lease EXPIRES (re-READY, epoch bumped) and is re-leased+re-submitted externally, a late `Complete(token)` for the *first* submission is rejected (epoch mismatch) but the external system may then have two in-flight submissions. This is inherent to at-least-once; idempotency is the consumer's job (non-goal). Size default visibility generously for async work, and prefer binding the token to the current attempt for fencing.

---

## 8. DeadLetter / DLQ

```protobuf
message DeadLetter {
  Message original = 1; uint32 final_attempt = 2;
  string reason = 3;                       // "max_attempts" | "terminal_nack" | "ttl" | "max_lease_lifetime"
  map<string,string> failure_headers = 4;  // arbitrary consumer-supplied failure metadata
  uint64 dead_at_ms = 5;
}
```
Dead-lettering and removing the original message are one atomic batch (no double-delivery to both the lane and the DLQ). A DLQ is **just another Lane with a Consumer** (set `dlq_lane`), or read/redriven via the control plane. Redrive republishes selected dead-letters and deletes the dlq rows in one batch. The DLQ is browsable/redrivable but is **not** a replay log.

---

## 9. gRPC API surface (`rota.v1`)

One proto package, two services: **`Broker`** (data plane) and **`Control`** (control plane). All messages strictly domain-neutral.

### 9.1 Broker (data plane)
- `rpc Publish(PublishRequest) returns (PublishResponse);`
- `rpc PublishBatch(PublishBatchRequest) returns (PublishBatchResponse);` (`atomic=true` ⇒ one Raft write all-or-nothing; `atomic=false` ⇒ best-effort per-item results)
- `rpc Work(stream WorkClientMsg) returns (stream WorkServerMsg);`

**`MessageSpec`** carries `lane`, `group_id` (required), `payload`, `headers`, `oneof not_before { Duration delay; Timestamp at }`, optional `external_token`, live group-knob hints `weight`/`batch_size`, optional `max_attempts`/`retry_backoff`/`ttl`, and optional `dedup_key` (producer-side idempotency window). Hints upsert the group's live config so a brand-new group's first message can carry its fairness weight without a second round-trip; authoritative config is `SetGroupConfig`.

**`Work` — client → server (`WorkClientMsg` oneof):**
- `LeaseRequest{ lane; uint32 credit; repeated string group_allow/deny }` — additive credit. **`credit=1` ⇒ strictly serial** (a prefetch=1, strictly-serial consumer). `credit=N` is a pipelining knob (hides Ack→next-Lease latency), NOT in-process concurrency (locked: the broker provides none).
- `Ack{ lease_id }`
- `Nack{ lease_id; NackMode mode; Duration delay?; map failure_meta }`, `enum NackMode { REQUEUE_NO_PENALTY=0; RETRY=1; DEAD_LETTER=2 }`. `REQUEUE_NO_PENALTY` is exactly a no-penalty requeue.
- `ExtendVisibility{ lease_id; Duration ttl }`
- `Complete{ external_token; Outcome outcome; map result_meta; Duration delay? }`, `enum Outcome { SUCCESS=0; FAILURE=1 }` — keyed by `external_token`, so it can also arrive off-stream.

**`Work` — server → client (`WorkServerMsg` oneof):**
- `Lease{ lease_id; lane; group_id; payload; uint32 attempt; headers; Timestamp visibility_deadline; external_token?; message_id }`
- `Credit{ lane; uint32 granted }` (optional reconciliation)
- `Control{ ControlKind kind; lane; group_id? }`, `enum ControlKind { PAUSE_LANE; RESUME_LANE; PAUSE_GROUP; RESUME_GROUP }` — broker-pushed flow-control so consumers back off *before* leases dry up (no busy poll). A late-connecting consumer learns a paused lane via empty leases + a "lane paused" bit on (re)subscribe.
- `StreamError{ ErrorCode code; detail; lease_id?; leader_addr? }`, `enum ErrorCode { OK; NOT_LEADER; LEASE_EXPIRED; UNKNOWN_LEASE; POLICY_FAULT; RATE_LIMITED; UNKNOWN_TOKEN }` — non-fatal; never tears the stream.

**Visibility/drain:** a `Lease` is exclusive until `visibility_deadline`; on miss the message re-leases (attempt unchanged for a pure timeout). Context cancellation of the stream ⇒ the consumer's outstanding leases become immediately re-leasable (clean graceful drain), so SIGTERM never waits a full visibility timeout.

### 9.2 Control (control plane, unary)
- Group config: `SetGroupConfig`, `GetGroupConfig`.
- Group lifecycle: `PauseGroup`, `ResumeGroup`, `CancelGroup`, `PurgeGroup`, `ReapGroup` (= one-call teardown across all lanes; returns affected lanes). Operating on an absent group is a benign no-op with zeroed counts.
- Policy: `SetPolicy` (hot-reload; returns `policy_version`), `GetPolicy`, `ValidatePolicy` (compile + dry-run without installing). `PolicySource{ PolicyKind kind; bytes code; map params }`, `enum PolicyKind { DRR; STRICT_PRIORITY; WFQ; LOTTERY; CUSTOM }`.
- Cron: `ScheduleCron` (idempotent on `cron_id`; carries `MisfirePolicy`, `coalesce`, optional `start_at`/`end_at`), `ListCron` (`next_fires[]`), `DeleteCron`, `PauseCron`.
- Singleton: `AcquireSingletonLease` (returns `fence_token`), `RenewSingletonLease`, `ReleaseSingletonLease`.
- Async: `CompleteByToken` (off-stream twin of in-stream `Complete`; `unknown_token=true` is benign for at-least-once callbacks).
- Introspection: `GetStats` (non-destructive lane/group depth, leasable/delayed/inflight, dlq_depth, EWMA publish/lease/ack rates, oldest-age, `policy_version`), `DescribeCluster`, `Health` (+ standard `grpc.health.v1.Health` for LB probes).

### 9.3 Error model
1. **Benign empty lease** — no work / empty / paused group yields no `Lease` frame; stream stays open. Never an error.
2. **NotLeader redirect** — writes hit the leader; a follower returns `FAILED_PRECONDITION` + `NotLeader{leader_addr, leader_id}` detail (or in-stream `NOT_LEADER`). The redirect detail is forward-compatible with future per-lane multi-raft (the leader may become per-lane).
3. **Policy-error fallback** — runtime fault ⇒ DRR for that tick + `POLICY_FAULT` + `PolicyInfo.fault_count`; install-time failure ⇒ `INVALID_ARGUMENT` + diagnostics.
4. **Lease errors** — Ack/Nack/Extend/Complete against an expired/unknown lease ⇒ non-fatal `LEASE_EXPIRED`/`UNKNOWN_LEASE`; drop the result.
5. **Standard codes** — `INVALID_ARGUMENT` (malformed spec / oversized payload / bad cron — enforced via `protovalidate`: required lane/group, header map size caps, payload bound), `RESOURCE_EXHAUSTED` (lane quota / backpressure), `UNAVAILABLE` (draining / no quorum → retry), `DEADLINE_EXCEEDED`.

### 9.4 HTTP/2 keepalive (multi-minute tasks)
| Setting | Side | Value | Why |
|---|---|---|---|
| `Time` (PING interval) | server | 30s | Probe idle streams so a dead consumer's leases reclaim fast |
| `Timeout` (PING ack wait) | both | 10s | Declare peer dead if no ack |
| `EnforcementPolicy.MinTime` | server | 15s | Reject abusive fast-PINGers |
| `EnforcementPolicy.PermitWithoutStream` | server | true | PINGs on an open-but-idle stream (the multi-minute case) — else server GOAWAYs a long idle task |
| `Time` | client | 25s | Keep NAT/LB state warm |
| `PermitWithoutStream` | client | true | Keep conn alive between streams during drain |
| `MaxConnectionIdle` | server | 0 (disabled) | Never close a conn just because its stream is idle mid-task |
| `MaxConnectionAge` | server | 30m + 5m grace | Gentle rebalance; grace ≥ longest task so GOAWAY drains cleanly |

Default visibility 60s (comfortably exceeds the keepalive failure-detection window), extendable; the SDK auto-extends at ~⅔ of the deadline.

---

## 10. SDKs (thin, generic)

Two SDKs ship — **Python** ([`sdk/python`](sdk/python)) and **TypeScript/Node**
([`sdk/typescript`](sdk/typescript)) — with deliberately symmetric surfaces
(`Publisher` / `Worker` / `Control` / `WorkflowClient` + the workflow/activity
worker loops). Both follow the leader on `NOT_LEADER` and are covered by an
end-to-end suite that drives a real broker (the TypeScript suite also boots a real
3-node cluster to exercise leader-following on both the unary and `Work`-stream
paths). The description below is the Python surface; the TypeScript SDK mirrors it
method-for-method (camelCase, options objects, Promises). Both preserve the
familiar `process(message) -> ack/nack` consumer contract with zero business
concepts.

**`Publisher`** — lazy connect; transparent leader-following (catch `NotLeader`, re-point, retry with bounded exponential backoff + jitter via `tenacity`). `publish(...)` returns `message_id`; `publish_batch(msgs, atomic=...)` is the fan-out path; `complete(external_token=..., outcome=...)` and `cancel_group`/`reap_group` are thin `Control` pass-throughs for the async-callback process.

**`Worker(targets, lane, handler, credit=1)`** — `run()` blocks. It opens the `Work` stream, sends `LeaseRequest{lane, credit}`, dispatches each `Lease` to `handler` **one at a time** (credit=1 ⇒ strictly serial), and maps outcomes: clean return ⇒ `Ack`; exception ⇒ `Nack{RETRY}`; `raise rota.Requeue(delay=...)` ⇒ `Nack{REQUEUE_NO_PENALTY, delay}` (the no-penalty requeue for circuit-open/rate-limit/paused); `raise rota.DeadLetter(meta=...)` ⇒ `Nack{DEAD_LETTER}`. The handler may instead call `msg.complete(token=...)` / `msg.extend(ttl=...)`. A background helper auto-extends long tasks. On `Control{PAUSE_LANE}` it stops requesting credit and lets in-flight finish (no busy poll). On SIGTERM it stops crediting, finishes in-flight (bounded by `drain_timeout`), cancels the stream context (un-started leases become immediately re-leasable), then closes. On `UNAVAILABLE`/drop/`NOT_LEADER` it reconnects + leader-follows and re-advertises credit; idempotency covers any message re-leased across the gap.

**Helpers:** `rota.SingletonLease(key, ttl)` (context manager: Acquire → renew-on-timer → Release), `rota.Cron`, `rota.Control` (stats/health/policy/group-admin for ops scripts).

---

## 11. Deployment, HA, Observability, Security

**Deployment / HA.** Single static binary; 1 voter (dev) or 3/5 voters (prod). One Pebble dir per node; quorum writes; automatic leader failover; `AddVoter`/`AddNonvoter`/`RemoveServer` for membership; `pebble.Checkpoint` for backups and node bootstrap. No external systems.

**Observability.** Per-group counters (ready/inflight/total), DLQ depth, lease in-flight gauge, fire latency (`fire_at − due_at`), expiry/redelivery rate, cron misfire/coalesce counts, token in-flight count, `policy_faults_total{lane,version,reason}`, quarantine events, `applied_index`, raft term/leader, EWMA publish/lease/ack rates. Exposed via `GetStats`/`DescribeCluster`/`Health` and a Prometheus endpoint. Structured logs.

**Security / sandboxing.** Policy code runs leader-only, off the FSM apply path, sandboxed and resource-bounded (CEL cost ceiling / Starlark step budget + watchdog + `recover()`), with a quarantine breaker. Completion tokens are server-minted and looked up by stored hash (no forgeable client tokens). `protovalidate` caps payload/header sizes so the "tiny messages / no business concept" invariant is enforced uniformly. gRPC supports TLS; the control plane can carry a stricter authz posture than the data plane. Fencing tokens on singleton leases.

---

## 12. Phased Roadmap (summary; details in `roadmap_phases`)

- **Phase 0 — Walking skeleton (single node):** publish → fair lease → ack, built-in DRR (broker-owned mechanism, no engine yet), Pebble storage, raft single-voter, gRPC `Work` stream. Compiles and runs.
- **Phase 1 — Delivery correctness:** visibility timeout + redelivery, attempt counter, two-outcome Nack, native bounded retry → DLQ, the unified `tidx` time index + leader fire loop, delayed publish + retry backoff.
- **Phase 2 — HA hardening:** 3-node cluster, quorum-acked durability, snapshot/restore via Pebble checkpoint, log truncation, NotLeader redirect + leader-following SDK, crash/failover recovery + idempotent fires.
- **Phase 3 — Programmable policy hot-reload:** the CEL SCORE engine + the WASM/wazero engine (any-language SCORE/DECIDE modules, hard memory/CPU bounds) as first-class tiers, validation/versioning, `SetPolicy`/`ValidatePolicy`/`GetPolicy`, runtime fallback + quarantine, strict-priority/WFQ/lottery built-ins. (Optional Starlark middle tier.)
- **Phase 4 — Scheduling + async + hooks:** cron (exactly-once, coalesce/misfire), singleton leases, complete-by-token (+ max-lease-lifetime backstop), pause/resume/cancel/purge/teardown, lane rate-limit + dequeue-pause hooks, full Python SDK + control-plane RPCs, observability.
- **Phase 5 (deferred) — Multi-raft / hot-lane scaling:** per-lane Raft groups via dragonboat, behind the unchanged FSM/keyspace seam; per-lane leader routing; horizon/paging for the wheel.
