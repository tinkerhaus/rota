# Rota

A generic, high-performance message broker in Go whose defining feature is a **programmable,
broker-native fair scheduler**. Rota exists because off-the-shelf brokers (RabbitMQ, etc.) have no
notion of *fairness across groups*: a large backlog for one group head-of-line-blocks everyone else.
Rota makes the **group** the unit of fairness and lets you define the scheduling **policy as code**.

> Status: Phases 0–4 implemented and tested (fair scheduling, lease lifecycle + timers, retry/DLQ,
> TCP cluster HA + failover + snapshots, the programmable CEL/WASM policy engine, cron, complete-by-token,
> singleton leases, group lifecycle, the Control gRPC plane, Prometheus metrics, and a Python SDK). See
> [`DESIGN.md`](DESIGN.md), the ADRs in [`docs/adr/`](docs/adr/), and the [`ROADMAP.md`](ROADMAP.md).
> Build with `go build ./...`, test with `go test ./...`, try it with `go run ./cmd/rota demo`.
> Pre-production: APIs may still change.

## Quickstart

```bash
go run ./cmd/rota demo                 # self-contained fairness demo (500-vs-20)
go run ./cmd/rota serve --grpc :7100   # run a node (Broker+Control on :7100, metrics on :7101)
```

Python (see [`sdk/python`](sdk/python)):

```python
from rota import Publisher, Worker
Publisher("127.0.0.1:7100").publish("orders", group_id="tenant-A", payload=b"...")
Worker("127.0.0.1:7100", "orders", handler=lambda m: print(m.payload)).run()
```

## What it is (and is not)

Rota is **generic and domain-neutral**. It knows about `lane`, `group`, `message`, `lease`, and
`policy`. It does **not** know about your business concepts. Your application maps its domain onto
Rota's primitives; no business logic lives in the broker.

## Core ideas

- **Group-fair scheduling.** Messages in a lane are partitioned by an opaque `group_id`. A
  programmable policy decides the serving order across groups so no group starves another.
- **Policy as code.** The scheduling policy is hot-reloadable code/config, per lane. Deficit Round
  Robin ships as the default; strict-priority, WFQ, and lottery ship as examples in the same mechanism.
  Only the Raft leader evaluates a policy; the *decision* is what gets replicated.
- **One durable system of record.** Messages **and** scheduling/coordination state live together in
  embedded [Pebble](https://github.com/cockroachdb/pebble), replicated by embedded
  [Raft](https://github.com/hashicorp/raft). No external Redis/Postgres/Kafka/ZooKeeper.
- **Single binary, HA cluster.** One static binary; runs as a 3-node (or 5-node) quorum cluster, or
  single-voter for local dev.
- **At-least-once, lease-based delivery.** SQS-style visibility timeouts, broker-owned attempt
  counter, native retry + dead-letter, and a two-outcome nack (no-penalty requeue vs terminal
  dead-letter) so back-pressure never burns retry budget.
- **Delayed & scheduled submission, first-class.** Publish with a `not_before` delay, or register a
  recurring cron publish ("publish this message to this lane on schedule S"), fired exactly-once
  cluster-wide.
- **Complete-by-token.** Hold a lease while work is handed to an external system, then complete or
  fail it later by an external token. Replaces async submit/poll side-tables.
- **gRPC transport.** A bidirectional streaming Work RPC with HTTP/2 keepalive (idle pull streams
  survive multi-minute consumer tasks), plus a thin Python SDK.

## Design decisions (locked)

| Decision | Choice |
|---|---|
| Storage | Embedded Pebble (LSM) |
| Consensus / HA | Embedded hashicorp/raft, quorum cluster |
| Deployment | Single static Go binary; no external deps |
| Fairness | Fully programmable policy-as-code, leader-evaluated |
| Delivery | At-least-once, lease/visibility-timeout |
| Delayed / scheduled | First-class (net-new capability) |
| Transport | gRPC + protobuf; Python SDK |
| Scope | Generic broker, zero business logic |

See [`DESIGN.md`](DESIGN.md) for the full architecture, [`docs/adr/`](docs/adr/) for the decision
records, and [`ROADMAP.md`](ROADMAP.md) for the phased build plan.

## Non-goals

No replayable log / stream / event-sourcing (this is a work-queue, not Kafka). No broker-level
exactly-once (consumers are idempotent). No strict in-group ordering. No large/blob payloads. No
business/domain concepts in the broker.
