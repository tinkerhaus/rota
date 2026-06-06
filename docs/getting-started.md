# Getting started with Rota

A from-scratch guide to the fundamental concepts. By the end you'll understand
*why* Rota exists, the handful of primitives everything is built from, and how to
publish, consume, and run a durable workflow.

> **One-sentence version.** Rota is a **single Go binary** that is two things at
> once: a **fair message broker** (a work queue that won't let one noisy tenant
> starve everyone else) and a **durable workflow engine** built on top of it —
> with **no external database** ([Pebble](https://github.com/cockroachdb/pebble) +
> [Raft](https://github.com/hashicorp/raft) are embedded).

If you remember one idea, remember this: **the unit of fairness is the *group*, and
the scheduling policy is code you control.**

---

## 1. Why Rota exists

Off-the-shelf brokers (RabbitMQ, SQS, …) have no notion of *fairness across
groups*. Give one tenant a 5,000-message backlog and a naïve queue hands out all
5,000 first — head-of-line-blocking every other tenant for an hour. Durable
workflow engines (Temporal, …) are powerful but operationally heavy and equally
fairness-blind.

Rota makes the **group the unit of fairness**, lets you write the scheduling
**policy as code**, and runs a **durable-execution engine on the same substrate**,
so workflows inherit the one thing the field lacks: programmable, completion-aware,
cross-tenant fairness — on one static binary with no external store.

---

## 2. The mental model: five primitives

Everything is built from five nouns. Learn these and the rest follows.

| Primitive | What it is | Analogy |
|---|---|---|
| **Lane** | A named stream of work. | A topic / queue name. |
| **Group** | A partition *within* a lane, keyed by an opaque `group_id`. **The fairness unit.** | A tenant, customer, user. |
| **Message** | One unit of work (small payload + headers). | A task / job. |
| **Lease** | A *temporary, exclusive* grant of a message to one consumer, with a deadline. | Borrowing a library book — return it or it's reclaimed. |
| **Policy** | Per-lane code that decides **which group is served next**. | The bouncer deciding whose turn it is. |

A message always belongs to **one lane** and **one group**:

```
lane = "emails"
  ├─ group "tenant-A"  → [msg, msg, msg, … 5000 queued]
  ├─ group "tenant-B"  → [msg, msg]
  └─ group "tenant-C"  → [msg]
```

A dumb queue drains tenant-A first and blocks B and C. Rota's policy **interleaves**
the groups so B and C are served too. That's *group fairness* — the thing nothing
else gives you out of the box.

---

## 3. The heart: fairness as code

When a worker requests work, the **Raft leader** runs the lane's policy to pick a
group, hands out one message, and **replicates only the decision** (never the
computation). Policies are **hot-reloadable, per lane**.

Built-in policies (select by name, no code):

| Kind | Behaviour |
|---|---|
| `drr` (default) / `wfq` | Weighted-fair: each group's share ∝ its `weight`. |
| `strict_priority` | Higher-weight groups first (low ones *may* starve — by design). |
| `lottery` | Weighted-random selection. |
| `completion_aware` | WFQ that also accounts for **in-flight work**: a tenant holding many un-acked leases is throttled, not just one that takes many turns. |

The mechanism is **weighted fair queuing via virtual time**: each group has a
virtual clock, serving it advances the clock by `1/weight`, and the lowest clock is
served next. Equal weights → round-robin; unequal weights → proportional shares.
(In a live contention test, weights 5:4:3:2:1 produced 33 / 27 / 20 / 13 / 7 % of
service — to the decimal.)

Need something bespoke? Write the policy in **CEL** or **WASM** (`cel` / `wasm`
kinds) — e.g. "serve the shortest queue" or "boost a group unseen for 10s". It runs
only on the leader, so it's consistent and side-effect-free.

```python
from rota import Control
ctrl = Control("127.0.0.1:7100")
ctrl.set_policy("emails", kind=3)                 # 3 = WFQ (see PolicyKind enum)
ctrl.set_group_config("emails", "tenant-A", weight=3.0)   # 3× the share
```

---

## 4. Delivery semantics — a message's life

Rota is **at-least-once, lease-based** (the SQS model):

1. A worker **leases** a message → invisible to others until a **visibility deadline**.
2. The worker does the work, then:
   - **ack** → done, message deleted.
   - **nack (requeue, no-penalty)** → back-pressure; *doesn't* burn a retry.
   - **nack (retry)** → failed; broker-owned attempt counter ++, redelivered with backoff.
   - **nack (dead-letter)** → terminal failure → the lane's **DLQ**.
3. Worker crashes / deadline passes → the lease is **reclaimed** and redelivered.
4. After `max_attempts`, the message is **dead-lettered** automatically (inspect / redrive later).

> **Consequence: make your consumers idempotent.** At-least-once means a message
> can be delivered more than once (e.g. a crash after the work but before the ack).

Two safety valves:
- **Max lease lifetime** caps total in-flight time for one message, so a wedged
  async holder can't pin it forever.
- **No head-of-line blocking** is guaranteed: a huge backlog in one group never
  blocks a small one (verified: 500 vs 20 → the 20 all served within the first 40).

---

## 5. Beyond a plain queue (still the broker layer)

- **Delayed publish** — `not_before` a delay/timestamp ("run this in 30s").
- **Cron** — recurring publishes, fired **exactly once cluster-wide** on schedule.
- **Producer idempotency** — `dedup_key` suppresses duplicate publishes for a window.
- **Complete-by-token** — hold a lease while handing work to an *external* system,
  then complete/fail it later by a token (replaces async submit/poll side-tables).
- **Singleton leases** — cluster-wide mutual exclusion ("only one of these at a time").

---

## 6. The durable workflow engine (the second layer)

A **workflow** is long-running logic-as-code that survives crashes by **replaying
its history**. The model (Temporal-style, on Rota's substrate):

- A **workflow run** has a **per-run, append-only, ordered event history** — its
  system of record (`WorkflowStarted`, `ActivityScheduled/Completed`, `TimerFired`,
  `SignalReceived`, …).
- **Your workflow code runs in a worker, never in the broker.** A worker **polls a
  task**, receives the history, **replays it deterministically**, and emits
  **commands** ("schedule activity X", "sleep 5s", "complete with result R").
- **Activities** (the side-effecting steps — call an API, charge a card) dispatch as
  **ordinary fair-scheduled leases** (lane `__act/<type>`, group = tenant). So
  **workflows inherit cross-tenant fairness for free**.
- First-class: **durable timers** (`sleep`), **signals** (external events into a
  running workflow), **cancellation**, and **continue-as-new** (bounds history growth).

**Why it stays correct (the hard part).** Because progress is Raft-committed, a
naïve design could commit *divergent* state. Rota prevents that with an
optimistic-concurrency **fence** on `(run_epoch, history_seq)` plus a **leader-side
validation gate** that checks a checksum of the recorded history prefix and
**rejects — never commits —** a decision computed against stale history. The
residual non-determinism is the same irreducible one Temporal has.

The worker protocol is **gRPC**, so an SDK in **any language** can drive the engine.

---

## 7. How it's stored and made highly-available

- **One embedded keyspace** in Pebble (an LSM key-value store) holds *everything* —
  messages, group metadata, leases, timers, cron, workflow runs & history, and the
  Raft log. Keys are tag-partitioned by a leading byte (table tag).
- **Embedded Raft** replicates it: run **single-voter** for dev, or a **3/5-node
  quorum** for HA with automatic failover.
- Every change is applied as **one atomic batch** that co-commits the data *and* the
  `applied_index`, so a node can never come back up half-applied.

No Redis, no Postgres, no Kafka, no ZooKeeper. One binary.

---

## 8. Hands-on: zero to running

**Run a node.** Broker + Control + Workflow on `:7100`; metrics + dashboard on `:7101`:

```bash
go run ./cmd/rota serve --grpc :7100 --metrics :7101    # data dir defaults to ./data
go run ./cmd/rota demo                                   # self-contained 500-vs-20 fairness demo
```

**Build the dashboard once** (served from the same binary, no extra daemon):

```bash
cd web && pnpm install && pnpm build      # → web/dist
# then: go run ./cmd/rota serve --grpc :7100 --metrics :7101 --web web/dist
# open http://localhost:7101
```

**Publish & consume** (Python SDK in [`sdk/python`](../sdk/python)):

```python
from rota import Publisher, Worker

Publisher("127.0.0.1:7100").publish("emails", group_id="tenant-A", payload=b"hello")

# leases, runs your handler, acks on return; raise to nack/retry/dead-letter
Worker("127.0.0.1:7100", lane="emails",
       handler=lambda m: print(m.group_id, m.payload)).run()
```

**A durable workflow** — the workflow worker replays history and decides; the
activity worker performs the side effect:

```python
from rota.workflow import (WorkflowClient, run_workflow_worker, run_activity_worker,
                           schedule_activity, complete_workflow)
from rota._gen.rota.v1 import rota_pb2 as pb

def decide(run_id, history):
    sched = any(e.event_type == pb.HET_ACTIVITY_SCHEDULED for e in history)
    done  = any(e.event_type == pb.HET_ACTIVITY_COMPLETED for e in history)
    if not sched: return [schedule_activity("charge", b"100")]
    if done:      return [complete_workflow(b"ok")]
    return []                                  # still waiting on the activity

run_activity_worker("127.0.0.1:7100", "charge", "act-1", lambda t: (b"ok", True))  # in a thread
run_workflow_worker("127.0.0.1:7100", "orders", "wf-1", decide)                    # in a thread
WorkflowClient("127.0.0.1:7100").start_workflow("orders", tenant_id="tenant-A")
```

Command helpers for the `decide` function: `schedule_activity`, `start_timer`,
`continue_as_new`, `complete_workflow`, `fail_workflow`.

**Watch it happen** — the **Observatory dashboard** at `http://localhost:7101`: a
live served-order ribbon (who got served, in order; each tenant its own colour),
expected-vs-actual share bars, a starvation radar, per-lane throughput sparklines,
and a swimlane execution timeline for each workflow run.

---

## 9. A map of where things live

```
                ┌──────────────── one binary ────────────────┐
   producers ──▶│  Broker gRPC    Control gRPC   Workflow gRPC│◀── workers (any lang)
                │      │              │               │        │
                │      ▼              ▼               ▼        │
                │   ┌───────────────────────────────────┐     │
   dashboard ──▶│   │  FSM (one atomic batch per apply)  │     │
   (HTTP/SSE)   │   └───────────────────────────────────┘     │
                │     leader-only: fairness policy + timers    │
                │   ┌───────────────────────────────────┐     │
                │   │  Pebble keyspace  ◀── Raft log ──▶ │ ◀──▶ peers (HA)
                │   │  msgs·groups·leases·timers·cron·    │     │
                │   │  workflow runs + history            │     │
                │   └───────────────────────────────────┘     │
                └─────────────────────────────────────────────┘
```

---

## The 30-second recap

- **Lane** = stream, **group** = fairness unit, **message** = work, **lease** = a
  deadline-bound grant, **policy** = code that picks the next group.
- Fairness is **programmable** and **leader-decided**; a big tenant can't starve a
  small one.
- Delivery is **at-least-once** with leases, retries, and a DLQ → **make consumers
  idempotent**.
- **Workflows** are durable and replay-based, and run their activities **through the
  same fair scheduler**.
- It's **one binary**: Pebble + Raft embedded, no external dependencies.

## Where to go next

- [`README.md`](../README.md) — feature overview and status.
- [`DESIGN.md`](../DESIGN.md) — full architecture.
- [`docs/design/`](design/) — durable-execution design, the UI synthesis, non-goals.
- [`docs/adr/`](adr/) — the decision records.
- [`sdk/python/`](../sdk/python) — the Python SDK and examples.
