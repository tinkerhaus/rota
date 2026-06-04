All four verdicts' load-bearing claims are confirmed against the live code:

- **Keyspace**: tag-partitioned, contiguous-prefix, range-deletable; last app tag is `tagLaneConfig = 0x0A`, so `0x0B`+ are free. `AppKeyspaceBounds()` would need extending to cover any new tag.
- **FSM commands**: `CmdType` iota enum + `Command` struct with per-type pointers — the "oneof" the verdicts describe. Append-only extension point.
- **Timer wheel**: unified `tagTimeIndex` keyed by `due_ts` with a `kind` byte (`TimerReadyAt`/`TimerLeaseDeadline`/`TimerCronDue`) — a new run-expiry/dedup-expiry kind drops straight in.
- **dedup_key**: present in `rota.proto:179`, `rota.pb.go`, the Python SDK, and `ROADMAP.md:73` — but **no** `DedupKey` field on `PublishCmd` and **no** dedup table tag. Exactly "half-built; only FSM wiring missing."
- **Snapshot**: `kvSnapshot.Persist` streams length-prefixed K/V pairs (not a hard-linked checkpoint) — confirming the large-payload re-serialization write-amp argument.

Here is the brief.

---

# DECISION BRIEF — Rota Non-Goals vs. Workflow Engine + UI

**Scope of the decision:** these verdicts are evaluated under the *durable-execution-as-code* ("beat Temporal") model. Where the answer would differ under a declarative/task-queue model, that fork is called out — it never changes a broker non-goal, only how much engine-private machinery the workflow layer needs.

## 1. Summary

| Non-goal | Verdict | What changes | Gated on which workflow model |
|---|---|---|---|
| No replayable log / stream / event-sourcing (work-queue, not Kafka) | **relax-partial** *(engine-internal only)* | New engine-private, TTL-bounded, per-run append-only **WorkflowRun event history** in a fresh Pebble keyspace, written in the same atomic Raft batch as run state. Broker messages stay delete-on-ack; **no public subscribe/seek/offset/tail** is added. | **Decisive.** Durable-exec ⇒ private replay history is mandatory (relax-partial). Declarative ⇒ downgrade to *keep* + a narrow run-state/audit table with TTL; replay refused outright. |
| No broker-level exactly-once (consumers idempotent) | **keep** | Finish the already-half-specced **producer-side `dedup_key` window** (bounded engine-private idempotency index). This is effectively-once, not exactly-once; it is not at-most-once delivery and not a stream. | Barely model-dependent (its model-independence is the argument to keep). Declarative model raises `dedup_key`'s value from "nice" to "load-bearing"; the verdict is unchanged. |
| No strict in-group ordering | **keep** | **Nothing in the broker.** The engine takes all ordering from the already-totally-ordered Raft log via its private per-run history (signals/timers re-recorded at apply time to pin position). | Unchanged under both models. Broker delivery order is never relied upon by construction. |
| No large/blob payloads | **keep** | **Nothing in the broker.** Hard-enforce the small-payload cap, document **claim-check** as a first-class pattern, and give the engine's run-state table its own per-step size cap + history truncation. | Unchanged under both models; only *where* bounded state lives differs (internal journal vs. external result reference). |

**Net: one relax (engine-internal), three keeps. Zero new public broker features. Single-binary / no-external-deps ethos fully intact.**

## 2. Minimum concrete mechanisms (for the one relax)

Only the replayable-log non-goal moves, and only on the engine side. The mechanism reuses existing seams verified in code:

**A. New keyspace (engine-private, contiguous-prefix, range-deletable)**
- `tagWorkflowRun byte = 0x0B` → `wfrun:<run_id>` → `RunMeta` (head pointer: next `seq`, status, terminal-at).
- `tagWorkflowEvent byte = 0x0C` → `wfhist:<run_id>:<seq u64be>` → `WorkflowEvent`.
- Extend `AppKeyspaceBounds()` (currently `[tagMeta, tagLaneConfig+1)`) so snapshot/restore covers the new tables — they live in the same single atomic Pebble batch and single fsync domain, sharing one durability story. No new store.

**B. New FSM command(s)** appended to the `CmdType` iota / `Command` struct (the existing extension pattern): `CmdAppendWorkflowEvent` (+ `CmdAdvanceRun`). Events are a deterministic projection the **leader stamps** — identical discipline to `FireTimer`/`Lease` stamping `fire_at`/`DeadlineMs` today; the per-run monotonic `seq` is assigned inside `Apply` exactly as `Publish` assigns `GroupMeta.next_seq`. Followers apply the committed outcome identically.

**C. Retention/GC** independent of message GC: reuse the unified time index. Add `TimerRunExpiry byte = 0x04` alongside the existing `TimerReadyAt/LeaseDeadline/CronDue` kinds; on terminal state, stamp a per-run TTL (default **72h**, Temporal's number) whose sweep range-deletes `wfhist:<run_id>:`. Add a **max-events-per-run cap** (Temporal's continue-as-new pressure-relief) so a pathological run can't grow unbounded.

**D. Read-only Control RPCs** for the UI — point/range reads only: `GetRun(run_id)`, `GetRunHistory(run_id)` (ordered forward scan over the contiguous prefix, same access shape as the message/timer scans), `ListRuns(filter)`. **No** subscribe / seek / tail / offset surface. This is the governance line, not a code line.

**E. protovalidate size caps** on workflow-event payloads — store references/summaries, not blobs (preserve the tiny-message invariant; fat payloads are Temporal's documented cost driver).

**F. `dedup_key` (the keep that still requires wiring):** add `DedupKey` to `PublishCmd`; new `tagDedup byte = 0x0D` → `dedup:<lane>:<hash>` → `{msg_id, expiry_ms}`; a drop-if-present check in `applyPublish` returning the original `msg_id`; TTL row in the existing time index. One field, one tag, one get-before-write, all on the leader in the existing batch. Bind the completion token to the current attempt/epoch for fencing so a stale `Complete(token)` is epoch-rejected.

## 3. The scope-discipline reframing (the key deliverable)

The original clause conflates **three separable things**; only one is forced, and only at the engine layer. Hold this seam:

> **The BROKER still has no public replayable stream.** Messages, leases, and groups remain delete-on-ack, are not event-sourced, and are not consumer-subscribable, seekable, or offset-tailable. **The WORKFLOW ENGINE keeps a private, TTL-bounded, per-run event history** — addressed by `run_id`, mutated only by committed commands, read only by the engine (for deterministic replay) and the UI (point/range reads). It is a reconstruction mechanism for a new noun, not a feed any consumer subscribes to.

Apply the same "internal mechanism ≠ public feature" framing to the three keeps:

- **Ordering:** Rota *already* has a stronger primitive than the non-goal withholds — the Raft log is a single-writer total order (the same thing Temporal calls a "single History shard"). The engine re-records signals/timer-fires into the per-run history at apply time, so they get a fixed index; broker in-group delivery order is irrelevant **by construction**. *History orders; the queue does not* — the canonical split. The broker stays unordered.
- **Exactly-once:** the engine needs effectively-once **effects**, satisfied by a bounded producer-side `dedup_key` index (engine-private, GC'd via the time wheel). That is not at-most-once delivery and not a guarantee against arbitrary failure (a crash between effect and dedup-commit still double-fires). Broker-level exactly-once stays refused.
- **Blobs:** the broker message stays tiny; the engine's *own* run-state table holds bounded accumulated state (per-step cap + truncation), and large step I/O goes external by claim-check reference. Raising `Message.payload` would multiply Raft fsync, balloon the **streamed** `InstallSnapshot` (confirmed: `kvSnapshot.Persist` re-serializes every value, so a payload in a still-leased message is re-copied into every snapshot until ack), and inflate compaction. The cap stays hard.

The discipline cost is **governance, not code**: never let the engine's private history grow a `tail`/`subscribe`/`offset`/`seek` API, or you have silently shipped the Kafka stream you refused. Every new mechanism above is point/range reads over a contiguous prefix.

## 4. Net effect on identity — honest read

We are **still a work-queue, not Kafka** — but we are now a work-queue with a durable-execution engine bolted *on top of* it, and that engine is, internally, event-sourced. That is not a contradiction; it is precisely the Temporal architecture, where a totally-ordered per-run history is the durability mechanism and the task queue underneath makes no ordering, exactly-once, or replay promise. The honest tension is this: three of four non-goals survive **verbatim** at the broker boundary, and the fourth is narrowed rather than deleted — but the *system* now contains an event-sourced execution engine, so anyone reading "Rota does no event-sourcing" at the product level would be misled. The correct claim going forward is sharper, not softer: **the broker is a tiny-message, at-least-once, unordered, delete-on-ack work-queue with no public stream; the workflow layer is an event-sourced durable-execution engine that keeps its own private per-run journal and never exposes it as a feed.** The single binary, the one Pebble+Raft system of record, the one atomic batch per `Apply`, and the no-external-deps ethos all survive unbroken — the new history rides the exact same rails as `Publish`'s per-group sequence and the unified time wheel. We did not become Kafka. We grew a Temporal-shaped second story on a foundation that was, deliberately, already poured to carry it.

Relevant files: `/Users/harsha/rota/internal/storage/keys.go` (keyspace tags, time-index kinds, `AppKeyspaceBounds`), `/Users/harsha/rota/internal/fsm/command.go` (`CmdType` enum, `Command` struct, `PublishCmd` — no `DedupKey`), `/Users/harsha/rota/internal/fsm/fsm.go:557-635` (`kvSnapshot.Persist`, streamed not hard-linked), `/Users/harsha/rota/proto/rota/v1/rota.proto:179` (`dedup_key`), `/Users/harsha/rota/ROADMAP.md:73`. New ADR to author: `docs/adr/adr-0011-*` drawing the per-run-history-is-internal line.