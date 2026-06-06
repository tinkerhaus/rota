# Part 1 — Getting running

**Goal:** build the Rota binary, run a node, prove fairness with the built-in
demo, inspect the live gRPC API, and open the dashboard. No SDK yet — just the
broker itself.

← [Tutorial index](README.md) · Next: [Part 2 — Broker basics](02-broker-basics.md) →

---

## 1.1 Build and run a node

Rota is a single Go binary with no external dependencies. From the repo root:

```bash
$ go build ./...            # compiles everything; should be silent
$ go run ./cmd/rota serve --grpc :7100 --metrics :7101
rota: Broker+Control on :7100, metrics+dashboard on :7101 (data=./data, id=node1, web=web/dist)
```

That one process now serves three gRPC services on `:7100`:

- **Broker** — publish messages, and the bidirectional *Work* stream consumers
  lease from.
- **Control** — configuration, policy, cron, singletons, stats, health.
- **Workflow** — durable execution (we get to this in Part 5).

…and an HTTP port `:7101` for Prometheus metrics, a health endpoint, and the
operator dashboard. State lives in `./data` (a [Pebble](https://github.com/cockroachdb/pebble)
keyspace + the Raft log). Stop it with Ctrl-C; delete `./data` to start fresh.

> **A single dev node is its own Raft leader.** You don't need a cluster to use
> Rota. We add real clustering in Part 6.

Leave this running in one terminal, or background it and move on.

---

## 1.2 See fairness in 5 seconds: the demo

Before writing any code, run the self-contained demo. It publishes **500 messages
to group "A"** and **20 to group "B"** in one lane, then drains them with a single
serial consumer:

```bash
$ go run ./cmd/rota demo
rota demo: publishing 500 messages to group A and 20 to group B in lane "demo"
first 40 served: A B A B A B A B A B A B A B A B A B A B A B A B A B A B A B A B A B A B A B
group B's 20 messages were all delivered within the first 40 of 520 total deliveries.
PASS: the 500-message group did not head-of-line-block the 20-message group.
```

Read that result carefully. A naïve FIFO queue would serve all 500 of A's messages
first, making B wait behind the entire backlog. Rota **interleaves the two
groups** — B's 20 messages all land within the first 40 deliveries — because *the
group is the unit of fairness*. This is the whole reason Rota exists, and you just
watched it work with zero configuration.

---

## 1.3 Inspect the live API with grpcurl

The broker has gRPC **server reflection** enabled, so you can explore the API with
no `.proto` file in hand. Install [`grpcurl`](https://github.com/fullstorydev/grpcurl)
and point it at the running node:

```bash
$ grpcurl -plaintext localhost:7100 list
grpc.health.v1.Health
rota.v1.Broker
rota.v1.Control
rota.v1.Workflow

$ grpcurl -plaintext localhost:7100 list rota.v1.Control
rota.v1.Control.AcquireSingletonLease
rota.v1.Control.DescribeCluster
rota.v1.Control.GetStats
rota.v1.Control.Health
...

# Call a unary RPC directly — a health probe:
$ grpcurl -plaintext localhost:7100 rota.v1.Control.Health
{
  "serving": true,
  "hasQuorum": true,
  "isLeader": true
}

# Publish a message by hand:
$ grpcurl -plaintext -d '{"message":{"lane":"hello","groupId":"g1","payload":"aGk="}}' \
    localhost:7100 rota.v1.Broker.Publish
{
  "messageId": "1"
}
```

(`payload` is base64 over the wire — `"aGk="` is `"hi"`.) You won't normally call
the API this way; the SDKs do it for you. But reflection means `grpcurl`,
`grpc-health-probe`, and k8s probes all work out of the box, which is handy for
debugging.

---

## 1.4 Open the dashboard

Rota ships an operator dashboard — the **Observatory** — embedded in the same
binary. Build the SPA once (it compiles to static files), then point `serve` at it:

```bash
$ cd web && pnpm install && pnpm build      # → web/dist
$ cd ..
$ go run ./cmd/rota serve --grpc :7100 --metrics :7101 --web web/dist
# open http://localhost:7101
```

The dashboard gives you, live:

- a **served-order ribbon** — who got served, in order, each group its own colour;
- **expected-vs-actual share** bars per group;
- a **starvation radar** flagging groups falling behind;
- per-lane **throughput sparklines**;
- a **swimlane timeline** for each workflow run (Part 5).

Re-run the demo (or, after Part 2, publish some messages) and watch the ribbon
interleave groups in real time. Keep this tab open — we'll come back to it in
Parts 3 and 6.

> If you don't have `pnpm`, you can skip the dashboard; everything in this tutorial
> is also observable via the SDK's `getStats()` / `describeCluster()` calls.

---

## What you learned

- Rota is **one binary** — `go run ./cmd/rota serve` is a complete broker +
  workflow engine with no external services.
- The **demo** proves the core promise: a 500-message group does not
  head-of-line-block a 20-message group.
- The gRPC API is **reflection-enabled**, so `grpcurl` explores it live.
- The **dashboard** is embedded and served from the same binary on `:7101`.

Next, we'll talk to the broker from code. → [Part 2 — Broker basics](02-broker-basics.md)
