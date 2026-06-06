# Rota

A single-binary, zero-dependency **fair message broker** *and* **durable workflow engine** in Go.

Rota exists because two things are usually bolted on after the fact and never quite fit: **fairness
across tenants**, and **durable execution**. Off-the-shelf brokers (RabbitMQ, SQS, …) have no notion
of fairness across groups — one tenant's backlog head-of-line-blocks everyone else. And durable
workflow engines (Temporal, …) are powerful but operationally heavy and fairness-blind. Rota makes the
**group the unit of fairness**, lets you write the scheduling **policy as code**, and runs a
**durable-execution engine on the same substrate** — so workflows inherit the one thing the field
lacks: programmable, completion-aware, cross-tenant fairness, on one static binary with no external
store.

> **Status — pre-production, but feature-complete and tested across the stack.** The broker
> (Phases 0–4), the durable-execution engine, the language-agnostic worker protocol + Python SDK, and
> the embedded operator dashboard are all implemented and covered by Go unit/integration tests, a
> Python end-to-end test, and an adversarial audit of the replay/divergence protocol. APIs may still
> change. The determinism story is at Temporal-parity (see [Durable execution](#durable-execution));
> build-id pinning for mixed-version worker fleets is the next hardening.
>
> Build `go build ./...`, test `go test ./...`, try `go run ./cmd/rota demo`.
> Design docs live in [`docs/design/`](docs/design/) and [`DESIGN.md`](DESIGN.md); decision records in
> [`docs/adr/`](docs/adr/); the phase plan in [`docs/design/workflow-engine-roadmap.md`](docs/design/workflow-engine-roadmap.md).

## What makes it different

- **Programmable cross-tenant fairness — the moat.** The serving order across groups is a
  hot-reloadable CEL/WASM **policy**, evaluated only on the Raft leader, with just the *decision*
  replicated. The same `Pick` governs raw messages, **workflow tasks, and activities uniformly**, so a
  noisy tenant's 10k workflows cannot starve a quiet tenant's three — on either surface. A
  `completion_aware` built-in even accounts for in-flight work, not just lease turns.
- **Durable execution without the sprawl.** Workflows-as-code with deterministic replay, activities,
  durable timers, signals, cancellation, and continue-as-new — backed by a per-run event history in
  the same embedded store. No Cassandra/Postgres/Elasticsearch to operate.
- **One static binary, zero external deps.** Messages, scheduling state, timers, cron, leases, tokens,
  workflow runs/history, *and* the Raft log all live in one embedded
  [Pebble](https://github.com/cockroachdb/pebble) keyspace, replicated by embedded
  [Raft](https://github.com/hashicorp/raft). Runs single-voter for dev or as a 3/5-node quorum cluster.
- **An operator dashboard that you can read at a glance** — embedded in the binary; see
  [Observatory](#observatory-dashboard).

## Quickstart

> **New to Rota?** Start with the [**getting-started guide**](docs/getting-started.md) —
> it explains the core concepts (lanes, groups, leases, fair policies, durable workflows) from scratch.

```bash
go run ./cmd/rota demo                                  # self-contained fairness demo (500-vs-20)
go run ./cmd/rota serve --grpc :7300 --metrics :7301    # a node: Broker+Control+Workflow on :7300,
                                                        # metrics + dashboard on :7301
```

Build the dashboard once (served from the same binary, no extra daemon):

```bash
cd web && pnpm install && pnpm build      # → web/dist
# then: go run ./cmd/rota serve --grpc :7300 --metrics :7301 --web web/dist
# open http://localhost:7301
```

### Python — broker

```python
from rota import Publisher, Worker
Publisher("127.0.0.1:7300").publish("orders", group_id="tenant-A", payload=b"...")
Worker("127.0.0.1:7300", lane="orders", handler=lambda m: print(m.payload)).run()
```

### Python — durable workflows

```python
from rota.workflow import (
    WorkflowClient, run_workflow_worker, run_activity_worker,
    schedule_activity, complete_workflow,
)
from rota._gen.rota.v1 import rota_pb2 as pb

# A worker replays a run's history and decides the next step (deterministic on history).
def decide(run_id, history):
    sched = any(e.event_type == pb.HET_ACTIVITY_SCHEDULED for e in history)
    done  = any(e.event_type == pb.HET_ACTIVITY_COMPLETED for e in history)
    if not sched:   return [schedule_activity("charge", b"100")]
    if done:        return [complete_workflow(b"ok")]
    return []                                             # waiting on the activity

run_activity_worker("127.0.0.1:7300", "charge", "act-1", lambda task: (b"ok", True))   # in a thread
run_workflow_worker("127.0.0.1:7300", "orders", "wf-1", decide)                        # in a thread

run_id = WorkflowClient("127.0.0.1:7300").start_workflow("orders", tenant_id="tenant-A")
```

Activities dispatch as ordinary, fair-scheduled leases; the worker protocol is gRPC, so an SDK in any
language can drive the engine.

## The two layers

### Fair broker (Phases 0–4)

- **Group-fair scheduling.** Messages in a lane are partitioned by an opaque `group_id`; a programmable
  policy decides the serving order so no group starves. Deficit Round Robin ships as default;
  strict-priority, WFQ, lottery, and `completion_aware` ship as examples in the same mechanism.
- **At-least-once, lease-based delivery.** SQS-style visibility timeouts, broker-owned attempt counter,
  native retry + dead-letter, a two-outcome nack (no-penalty requeue vs terminal dead-letter), and a
  max-lease-lifetime backstop.
- **Delayed & cron submission, first-class**, on one unified due-time wheel; cron fires exactly-once
  cluster-wide.
- **Complete-by-token** — hold a lease while work goes to an external system, complete it later by
  token. **Producer idempotency** via `dedup_key`. **Singleton leases** for cluster-wide mutual
  exclusion.

### Durable execution

A workflow run's system of record is a **per-run, append-only, totally-ordered event history**. User
workflow code runs in a **worker** (never in the state machine); it replays history, decides, and emits
commands the leader validates and commits.

- **Activities = leases.** An activity dispatches as a fair-scheduled message; completion routes back
  into history, idempotent by `scheduled_event_id`. A dead-lettered activity surfaces as
  `ACTIVITY_FAILED` so a run can never silently wedge.
- **Durable timers** (`workflow.sleep`) ride the same time wheel; **signals**, **cancellation**, and
  **continue-as-new** (history bounding) are first-class.
- **Committed-divergence protocol (the hard part).** Because progress is recorded via Raft-committed
  events, a naïve design could commit divergent state cluster-wide. Rota prevents this with an
  apply-time **optimistic-concurrency fence** (a second append against the same `(run_epoch,
  history_seq)` is a benign no-op) plus a **leader-side validation gate** (recorded-prefix checksum +
  OCC) that rejects — never commits — a divergent decision. The residual (a run's *first* forward
  decision being non-deterministic) is byte-for-byte Temporal's irreducible limit; the protocol was
  adversarially audited and the confirmed bugs fixed.
- **Per-tenant fair workflows.** Because workflow tasks and activities flow through the same `Pick`,
  one programmable fairness policy governs them too — `tenant → group`.

## Observatory dashboard

An embedded single-page operator console (compiled into the binary, served from the metrics port):

- a live **served-order ribbon** — *who got served, in order*, each tenant its own stable color;
- **expected-vs-actual share bars** with a fair-line and a **starvation radar** that flags under-served
  tenants;
- per-lane **throughput sparklines** and a cluster/Raft readout;
- a **workflows board** and a per-run **swimlane execution timeline** (activities + decisions on a real
  time axis), with start/signal/cancel actions.

## Architecture at a glance

| Decision | Choice |
|---|---|
| Storage | Embedded Pebble (LSM) — one keyspace for messages, coordination, and workflow state |
| Consensus / HA | Embedded hashicorp/raft, quorum cluster; one atomic batch per apply |
| Deployment | Single static Go binary; no external deps |
| Fairness | Fully programmable policy-as-code, leader-evaluated, completion-aware |
| Delivery | At-least-once, lease/visibility-timeout |
| Durable execution | Per-run event-sourced history; worker-side replay; leader-validated commits |
| Transport | gRPC + protobuf (Broker, Control, Workflow + worker protocol); Python SDK; HTTP/JSON gateway |
| Scope | Generic broker + generic workflow engine; zero business logic in the core |

## Non-goals

The **broker** stays a tiny-message, at-least-once, unordered, delete-on-ack **work-queue** — it is
*not* Kafka: no public replayable/seekable/offset stream, no broker-level exactly-once, no strict
in-group ordering, no large/blob payloads. The **workflow engine** keeps a private, per-run event
history for deterministic replay — that history is event-sourced *internally*, never exposed as a
consumer feed. (See [`docs/design/non-goals-reevaluation.md`](docs/design/non-goals-reevaluation.md).)
No business/domain concepts live in the core.
